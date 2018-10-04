package uk.ac.ebi.ddi.downloas.logs;



import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import org.apache.http.client.config.RequestConfig;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.slice.SliceBuilder;

import java.io.IOException;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;


/**
 * @author Robert Petryszak rpetry
 *         This class encapsulates the client functionality for accessing ElasticSearch behind Kibana,
 *         where both ftp and Aspera data download logs are stored.
 */
public class ElasticSearchWsClient {

    private static final org.apache.log4j.Logger log = Logger.getLogger(ElasticSearchWsClient.class);

    private RestHighLevelClient restHighLevelClient;

    public void setParallel(boolean parallel) {
        this.parallel = parallel;
    }

    public boolean parallel = false;

    // Hashmap for storing results aggregated by period (yyyy/mm)

    private static final Map<ElasticSearchWsConfigProd.DB, Map<String, Map<String, Map<String, Multiset<String>>>>> dbToAccessionToPeriodToAnonymisedIPAddressToFileName =
            new ConcurrentHashMap<ElasticSearchWsConfigProd.DB, Map<String, Map<String, Map<String, Multiset<String>>>>>() {
                {
                    for (ElasticSearchWsConfigProd.DB db : ElasticSearchWsConfigProd.DB.values()) {
                        put(db, new HashMap<>());
                    }
                }
            };

    /**
     * Constructor that instantiates RestHighLevelClient object using constants in config
     *
     * @param config
     */
    public ElasticSearchWsClient(ElasticSearchWsConfigProd config) {
        ElasticSearchWsConfigProd config1 = config;
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(ElasticSearchWsConfigProd.USERNAME, ElasticSearchWsConfigProd.PASSWORD));
        RestClientBuilder builder = RestClient.builder(
                new HttpHost(ElasticSearchWsConfigProd.HOST, ElasticSearchWsConfigProd.PORT))
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)).setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.setConnectTimeout(ElasticSearchWsConfigProd.CONNECT_TIMEOUT)
                        .setSocketTimeout(ElasticSearchWsConfigProd.SOCKET_TIMEOUT)).setMaxRetryTimeoutMillis(ElasticSearchWsConfigProd.MAX_RETRY_TIMEOUT);
        this.restHighLevelClient = new RestHighLevelClient(builder);
    }

    /**
     * @param db
     * @param accession
     * @param yearLocalDate
     * @return For a given database, dataset accession and a year (represented by yearLocalDate),
     * return a Map between each Period (yyyy/mm) and a map of anonymised IP addresses pointing Multisets of their corresponding file names/download counts
     */

    public Map<String, Map<String, Multiset<String>>> getDataDownloads(ElasticSearchWsConfigProd.DB db, String accession, LocalDate yearLocalDate) {
        Map<String, Map<String, Multiset<String>>> anonymisedIPAddressToFileNames = null;
        if(parallel)
            parallelRetrieveAllDataFromElasticSearch(null, null, null, yearLocalDate);
        else
            retrieveAllDataFromElasticSearch(null, null, null, yearLocalDate);
        if (dbToAccessionToPeriodToAnonymisedIPAddressToFileName.containsKey(db)) {
            Map<String, Map<String, Map<String, Multiset<String>>>> accessionToPeriodToAnonymisedIPAddressToFileName =
                    dbToAccessionToPeriodToAnonymisedIPAddressToFileName.get(db);
            if (accessionToPeriodToAnonymisedIPAddressToFileName.containsKey(accession)) {
                anonymisedIPAddressToFileNames = dbToAccessionToPeriodToAnonymisedIPAddressToFileName.get(db).get(accession);
            } else {
                log.warn("No accession: '" + accession + "' could be found in the data retrieved for db: '" + db.toString() + "'from ElasticSearch");
            }
        } else {
            log.warn("No db: '" + db.toString() + "' could be found in the data retrieved from ElasticSearch");
        }
        return anonymisedIPAddressToFileNames;
    }

    /**
     * @return False if for at least on DB no data downloads are present; otherwise return True
     */
    private boolean resultsReady() {
        boolean resultsReady = true;
        for (ElasticSearchWsConfigProd.DB db : dbToAccessionToPeriodToAnonymisedIPAddressToFileName.keySet()) {
            resultsReady = dbToAccessionToPeriodToAnonymisedIPAddressToFileName.get(db).isEmpty();
            if (!resultsReady)
                break;
        }
        return resultsReady;
    }

    /**
     * Function to retrieve all relevant data download entries for the current year from ftp- and Aspera-specific ElasticSearch indexes,
     * and aggregate them in the static dbToAccessionToPeriodToFileName data structure
     *
     * @param batchSize          If not null, size of each batch to be retrieved from ElasticSearch
     * @param reportingFrequency If not null, the total so far of the records retrieved from ElasticSearch is output every reportingFrequency records
     * @param maxHits            If not null, the maximum number of records to be retrieved (used for testing)
     * @param yearLocalDate      Date representing the year for which the data should be retrieved from ElasticSearch; cannot be null
     */
    private void retrieveAllDataFromElasticSearch(Integer batchSize, Integer reportingFrequency, Integer maxHits, LocalDate yearLocalDate) {
        if (resultsReady()) {
            if (batchSize == null) {
                batchSize = ElasticSearchWsConfigProd.DEFAULT_QUERY_BATCH_SIZE;
            }
            if (reportingFrequency == null) {
                reportingFrequency = ElasticSearchWsConfigProd.DEFAULT_PROGRESS_REPORTING_FREQ;
            }

            // Retrieve year from queryYear
            int year = yearLocalDate.getYear();

            for (ElasticSearchWsConfigProd.Protocol protocol : ElasticSearchWsConfigProd.Protocol.values()) {
                log.info("Starting on protocol: " + protocol.toString());
                String protocolStr = protocol.toString();
                // C.f. https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/java-rest-high-search-scroll.html
                // Initialise the search scroll context
                SearchRequest searchRequest = new SearchRequest(protocolStr + "logs-*");
                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                searchSourceBuilder.query(QueryBuilders.termQuery("year", Integer.toString(year)));
                searchSourceBuilder.size(batchSize);
                searchRequest.source(searchSourceBuilder);
                searchRequest.scroll(TimeValue.timeValueMinutes(ElasticSearchWsConfigProd.SCROLL_VALID_PERIOD));
                try {
                    SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
                    String scrollId = searchResponse.getScrollId();
                    SearchHit[] searchHits = searchResponse.getHits().getHits();
                    getValuesFromHits(searchHits, protocol);

                    // Retrieve all the relevant documents
                    int searchHitsCount = 0;
                    while (searchHits != null && searchHits.length > 0 && (maxHits == null || searchHitsCount < maxHits)) {
                        SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                        scrollRequest.scroll(TimeValue.timeValueSeconds(ElasticSearchWsConfigProd.SCROLL_VALID_PERIOD));
                        SearchResponse searchScrollResponse = restHighLevelClient.scroll(scrollRequest, RequestOptions.DEFAULT);
                        scrollId = searchScrollResponse.getScrollId();
                        searchHits = searchScrollResponse.getHits().getHits();
                        getValuesFromHits(searchHits, protocol);
                        searchHitsCount += batchSize;
                        if ((searchHitsCount % reportingFrequency) == 0) {
                            log.info(searchHitsCount + "");
                        }
                    }
                    log.info("Done retrieving " + protocolStr + " download data");
                } catch (IOException ioe) {
                    log.error("Exception in retrieving data from ElasticSearch via RestHighLevelClient" + ioe.getMessage());
                }
            }
        }
    }

    /**
     * Function to retrieve all relevant data download entries for the current year from ftp- and Aspera-specific ElasticSearch indexes,
     * and aggregate them in the static dbToAccessionToPeriodToFileName data structure
     *
     * @param batchSize          If not null, size of each batch to be retrieved from ElasticSearch
     * @param reportingFrequency If not null, the total so far of the records retrieved from ElasticSearch is output every reportingFrequency records
     * @param maxHits            If not null, the maximum number of records to be retrieved (used for testing)
     * @param yearLocalDate      Date representing the year for which the data should be retrieved from ElasticSearch; cannot be null
     */
    private void parallelRetrieveAllDataFromElasticSearch(Integer batchSize, Integer reportingFrequency, Integer maxHits, LocalDate yearLocalDate) {
        if (resultsReady()) {
            if (batchSize == null) {
                batchSize = ElasticSearchWsConfigProd.DEFAULT_QUERY_BATCH_SIZE;
            }
            if (reportingFrequency == null) {
                reportingFrequency = ElasticSearchWsConfigProd.DEFAULT_PROGRESS_REPORTING_FREQ;
            }

            // Retrieve year from queryYear
            int year = yearLocalDate.getYear();

            Integer finalBatchSize = batchSize;
            Integer finalReportingFrequency = reportingFrequency;

            Arrays.asList(ElasticSearchWsConfigProd.Protocol.values()).parallelStream().forEach(
                    protocol -> {
                        log.info("Starting on protocol: " + protocol.toString());
                        String protocolStr = protocol.toString();
                        SearchRequest searchRequest = new SearchRequest(protocolStr + "logs-*");
                        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

                        searchSourceBuilder.query(QueryBuilders.termQuery("year", Integer.toString(year)));
                        searchSourceBuilder.size(finalBatchSize);

                        int slices = 10;
                        IntStream.range(0, slices).parallel().forEach( slice -> {

                            SliceBuilder sliceBuilder = new SliceBuilder(slice, slices);
                            searchSourceBuilder.slice(sliceBuilder);
                            searchRequest.source(searchSourceBuilder);

                            searchRequest.scroll(TimeValue.timeValueMinutes(ElasticSearchWsConfigProd.SCROLL_VALID_PERIOD));
                            try {
                                SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
                                String scrollId = searchResponse.getScrollId();
                                SearchHit[] searchHits = searchResponse.getHits().getHits();
                                getValuesFromHits(searchHits, protocol);

                                // Retrieve all the relevant documents
                                int searchHitsCount = 0;

                                while (searchHits != null && searchHits.length > 0 && (maxHits == null || searchHitsCount < maxHits)) {
                                    SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                                    scrollRequest.scroll(TimeValue.timeValueSeconds(ElasticSearchWsConfigProd.SCROLL_VALID_PERIOD));
                                    SearchResponse searchScrollResponse = restHighLevelClient.scroll(scrollRequest, RequestOptions.DEFAULT);
                                    scrollId = searchScrollResponse.getScrollId();
                                    searchHits = searchScrollResponse.getHits().getHits();
                                    getValuesFromHits(searchHits, protocol);
                                    searchHitsCount += finalBatchSize;
                                    if ((searchHitsCount % finalReportingFrequency) == 0) {
                                        log.info(protocol + " " + searchHitsCount + "");
                                    }
                                }
                            } catch (IOException ioe) {
                                log.error("Exception in retrieving data from ElasticSearch via RestHighLevelClient" + ioe.getMessage());
                            }

                        });
                        log.info("Done retrieving " + protocolStr + " download data");

                    }
            );
        }
    }

    /**
     * @param source
     * @return Date String in format: yyyy/mm retrieved from source
     */
    private static String getYearMonth(String source) {
        Matcher matcher = Pattern.compile(ElasticSearchWsConfigProd.YEAR_MONTH_REGEX).matcher(source);
        boolean b = matcher.find();
        String dateStr = null;
        if (b) {
            dateStr = matcher.group(0);
        } else {
            log.error("Failed to retrieve date string from: " + source);
        }
        return dateStr;
    }

    /**
     * @param db
     * @param filePath
     * @param accessionRegex
     * @return A tuple first accession and then the name of the file being downloaded
     */
    private static Tuple<String, String> getAccessionAndFileName(ElasticSearchWsConfigProd.DB db, String filePath, String accessionRegex) {
        // c.f. [/\\.] below for retrieval of e.g. /GCA_002757455 from /GCA_002757455.1_
        Matcher matcher = Pattern.compile("/" + accessionRegex + "[/\\.]").matcher(filePath);
        boolean b = matcher.find();
        // Retrieve file name
        String[] arr = filePath.split("/");
        String fileName = arr[arr.length - 1];
        // Retrieve accession
        String accession = null;
        if (b) {
            accession = matcher.group(0).replaceAll("^/|[_/]+$", "");
        } else {
            log.error("ERROR: Failed to retrieve accession from: " + filePath + " - using accession regex: " + accessionRegex);
        }
        return new Tuple<>(accession, fileName);
    }

    /**
     * Add argument values to the aggregated results in dbToAccessionToDateToFileName
     *
     * @param db
     * @param accession
     * @param period
     * @param fileName
     */

    private static void addToResults(ElasticSearchWsConfigProd.DB db, String accession, String period, String anonymisedIPAddress, String fileName) {
        if (!dbToAccessionToPeriodToAnonymisedIPAddressToFileName.get(db).containsKey(accession)) {
            // We haven't seen this accession before
            Map<String, Map<String, Multiset<String>>> periodToAnonymisedIPAddressToFileNames = new HashMap<>();
            Map<String, Multiset<String>> anonymisedIPAddressToFileNames = new HashMap<>();
            // N.B. We use Multiset to maintain counts per individual download file
            anonymisedIPAddressToFileNames.put(anonymisedIPAddress, HashMultiset.<String>create());
            anonymisedIPAddressToFileNames.get(anonymisedIPAddress).add(fileName);
            periodToAnonymisedIPAddressToFileNames.put(period, anonymisedIPAddressToFileNames);
            dbToAccessionToPeriodToAnonymisedIPAddressToFileName.get(db).put(accession, periodToAnonymisedIPAddressToFileNames);
        } else {
            // We've seen this accession before
            if (!dbToAccessionToPeriodToAnonymisedIPAddressToFileName.get(db).get(accession).containsKey(period)) {
                // We haven't seen this period for this accession before
                Map<String, Multiset<String>> anonymisedIPAddressToFileNames = new HashMap<>();
                anonymisedIPAddressToFileNames.put(anonymisedIPAddress, HashMultiset.<String>create());
                dbToAccessionToPeriodToAnonymisedIPAddressToFileName.get(db).get(accession).put(period, anonymisedIPAddressToFileNames);
            } else {
                // We have seen this period for this accession before
                if (!dbToAccessionToPeriodToAnonymisedIPAddressToFileName.get(db).get(accession).get(period).containsKey(anonymisedIPAddress)) {
                    // We haven't seen this anonymisedIPAddress for that accession and period before
                    Map<String, Multiset<String>> anonymisedIPAddressToFileNames = new HashMap<>();
                    anonymisedIPAddressToFileNames.put(anonymisedIPAddress, HashMultiset.<String>create());
                    dbToAccessionToPeriodToAnonymisedIPAddressToFileName.get(db).get(accession).put(period, anonymisedIPAddressToFileNames);
                }
            }
            dbToAccessionToPeriodToAnonymisedIPAddressToFileName.get(db).get(accession).get(period).get(anonymisedIPAddress).add(fileName);
        }
    }

    /**
     * Auxiliary function used for testing that ES retrieval works (for a smaller subset of hits)
     * @param batchSize
     * @param reportingFrequency
     * @param maxHits
     * @param yearLocalDate
     * @return
     */

    public Map<ElasticSearchWsConfigProd.DB, Map<String, Map<String, Map<String, Multiset<String>>>>> getResults(Integer batchSize, Integer reportingFrequency, Integer maxHits, LocalDate yearLocalDate) {
        if(parallel)
            parallelRetrieveAllDataFromElasticSearch(batchSize, reportingFrequency, maxHits, yearLocalDate);
        else
            retrieveAllDataFromElasticSearch(batchSize, reportingFrequency, maxHits, yearLocalDate);

        return dbToAccessionToPeriodToAnonymisedIPAddressToFileName;
    }

    /**
     * Retrieves the required fields from each element in searchHits, retrieved for a given protocol
     *
     * @param searchHits
     * @param protocol
     */
    private static void getValuesFromHits(SearchHit[] searchHits, ElasticSearchWsConfigProd.Protocol protocol) {
        for (SearchHit hit : searchHits) {
            Map k2v = hit.getSourceAsMap();
            String source = k2v.get("source").toString();
            String anonymisedIPAddress = k2v.get("uhost").toString();  // Anonymised IP address
            String filePath = k2v.get("file_name").toString();
            long fileSize = Long.parseLong(k2v.get("file_size").toString());
            // Ignore files with 0 download size
            if (fileSize == 0)
                continue;
            for (ElasticSearchWsConfigProd.DB db : ElasticSearchWsConfigProd.DB.values()) {
                Map<ElasticSearchWsConfigProd.RegexType, String> typeToRegex = ElasticSearchWsConfigProd.protocol2DB2Regex.get(protocol).get(db);
                if (typeToRegex.keySet().isEmpty())
                    continue;
                String resource = null;
                if (Pattern.compile(typeToRegex.get(ElasticSearchWsConfigProd.RegexType.positive)).matcher(filePath).find()) {
                    if (typeToRegex.get(ElasticSearchWsConfigProd.RegexType.negative) != null) {
                        if (!Pattern.compile(typeToRegex.get(ElasticSearchWsConfigProd.RegexType.negative)).matcher(filePath).find()) {
                            resource = db.toString();
                        }
                    } else {
                        resource = db.toString();
                    }
                }
                if (resource != null) {
                    Tuple<String, String> accessionFileName = getAccessionAndFileName(db, filePath, typeToRegex.get(ElasticSearchWsConfigProd.RegexType.accession));
                    String accession = accessionFileName.v1();
                    String fileName = accessionFileName.v2();
                    if (accession != null) {
                        String yearMonth = getYearMonth(source);
                        if (yearMonth != null) {
                            addToResults(db, accession, yearMonth, anonymisedIPAddress, fileName);
                            break;
                        }
                    }
                }
            }
        }
    }
}