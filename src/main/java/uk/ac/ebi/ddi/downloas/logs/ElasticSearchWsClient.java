package uk.ac.ebi.ddi.downloas.logs;


import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ebi.ddi.downloas.ena.ENAWsClient;
import uk.ac.ebi.ddi.downloas.ena.ENAWsConfigProd;
import uk.ac.ebi.ddi.downloas.utils.DateUtils;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * @author Robert Petryszak rpetry
 * This class encapsulates the client functionality for accessing ElasticSearch behind Kibana,
 * where both ftp and Aspera data download logs are stored.
 */
public class ElasticSearchWsClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchWsClient.class);

    private static final int ELASTIC_TIMERANGE_LIMIT_DAYS = 30;

    private RestHighLevelClient restHighLevelClient;

    private static Pattern datePattern = Pattern.compile(ElasticSearchWsConfigProd.YEAR_MONTH_DATE_REGEX);

    // Client used for retrieving ENA project accessions corresponding to ENA accessions retrieved from ElasticSearch
    private ENAWsClient enaWsClient = new ENAWsClient(new ENAWsConfigProd());

    // Hashmap for storing results aggregated by period (yyyy/mm)
    // Because the set of ElasticSearchWsConfigProd.DB is static & will never be change,
    //                                              so, use hashmap instead of concurrent hashmap
    // DB_TO_ACCESSION_TO_PERIOD_TO_ANONYMISED_IP_ADDRESS_TO_FILE_NAME
    private static final Map<ElasticSearchWsConfigProd.DB, Map<String, Map<String, Map<String, Multiset<String>>>>>
            DB_DATA = new HashMap<ElasticSearchWsConfigProd.DB,
            Map<String, Map<String, Map<String, Multiset<String>>>>>() {
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
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(config.getUsername(), config.password));
        RestClientBuilder builder = RestClient.builder(new HttpHost(config.getHost(), config.port))
                .setHttpClientConfigCallback(
                        httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider))
                .setRequestConfigCallback(
                        requestConfigBuilder -> requestConfigBuilder
                                .setConnectTimeout(ElasticSearchWsConfigProd.CONNECT_TIMEOUT)
                                .setSocketTimeout(ElasticSearchWsConfigProd.SOCKET_TIMEOUT))
                .setMaxRetryTimeoutMillis(ElasticSearchWsConfigProd.MAX_RETRY_TIMEOUT);
        restHighLevelClient = new RestHighLevelClient(builder);
    }

    public void initialiseData(Date from, Date to) {
        enaWsClient.populateCache();
        List<Tuple<Date, Date>> partitions = DateUtils.partition(from, to, ELASTIC_TIMERANGE_LIMIT_DAYS);
        LOGGER.info("Time ranger to be retrieved: ");
        partitions.forEach(x -> LOGGER.info("--> {}", x));
        partitions.forEach(x -> retrieveDataFromElasticSearch(null, null, null, x));
    }

    /**
     * Return Downloads data of a specific database
     * @param db
     * @return
     */
    public Map<String, Map<String, Map<String, Multiset<String>>>> getDownloadsData(ElasticSearchWsConfigProd.DB db) {
        if (DB_DATA.containsKey(db)) {
            return DB_DATA.get(db);
        }
        return Collections.emptyMap();
    }

    /**
     * Function to retrieve all relevant data download entries for the current year from ftp- and
     * Aspera-specific ElasticSearch indexes,
     * and aggregate them in the static dbToAccessionToPeriodToFileName data structure
     *
     * @param batchSize          If not null, size of each batch to be retrieved from ElasticSearch
     * @param reportingFrequency If not null, the total so far of the records retrieved from ElasticSearch
     *                           is output every reportingFrequency records
     * @param maxHits            If not null, the maximum number of records to be retrieved (used for testing)
     */
    private void retrieveDataFromElasticSearch(Integer batchSize, Integer reportingFrequency, Integer maxHits,
                                               Tuple<Date, Date> timeRange) {
        LOGGER.info("Starting to retrieve data {}", timeRange);

        if (batchSize == null) {
            batchSize = ElasticSearchWsConfigProd.DEFAULT_QUERY_BATCH_SIZE;
        }
        if (reportingFrequency == null) {
            reportingFrequency = ElasticSearchWsConfigProd.DEFAULT_PROGRESS_REPORTING_FREQ;
        }

        Date firstDateOfPreviousMonth = timeRange.v1();
        Date lastDateOfPreviousMonth = timeRange.v2();

        for (ElasticSearchWsConfigProd.Protocol protocol : ElasticSearchWsConfigProd.Protocol.values()) {
            LOGGER.info("Starting on protocol: {}", protocol.toString());
            String protocolStr = protocol.toString();
            // C.f. https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/java-rest-high-search-scroll.html
            // Initialise the search scroll context
            SearchRequest searchRequest = new SearchRequest(protocolStr + "logs-*");
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            BoolQueryBuilder builder = QueryBuilders.boolQuery()
                    .must(QueryBuilders.rangeQuery("@timestamp")
                            .to(lastDateOfPreviousMonth)
                            .from(firstDateOfPreviousMonth))
                    .must(QueryBuilders.existsQuery("source"))
                    .must(QueryBuilders.existsQuery("uhost"))
                    .mustNot(QueryBuilders.termQuery("file_size", "0"));

            searchSourceBuilder.query(builder);
            searchSourceBuilder.size(batchSize);
            searchRequest.source(searchSourceBuilder);
            searchRequest.scroll(TimeValue.timeValueMinutes(ElasticSearchWsConfigProd.SCROLL_VALID_PERIOD));
            try {
                SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
                String scrollId = searchResponse.getScrollId();
                SearchHit[] searchHits = searchResponse.getHits().getHits();
                getValuesFromHits(searchHits, protocol, enaWsClient);

                // Retrieve all the relevant documents
                int searchHitsCount = 0;
                while (searchHits != null && searchHits.length > 0 && (maxHits == null || searchHitsCount < maxHits)) {
                    SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                    scrollRequest.scroll(TimeValue.timeValueSeconds(ElasticSearchWsConfigProd.SCROLL_VALID_PERIOD));
                    SearchResponse searchScrollResponse = restHighLevelClient.scroll(
                            scrollRequest, RequestOptions.DEFAULT);
                    scrollId = searchScrollResponse.getScrollId();
                    searchHits = searchScrollResponse.getHits().getHits();
                    if (searchHits != null) {
                        getValuesFromHits(searchHits, protocol, enaWsClient);
                        searchHitsCount += batchSize;
                        if ((searchHitsCount % reportingFrequency) == 0) {
                            LOGGER.info("Hit count: {}", searchHitsCount);
                        }
                    }
                }
//                ClearScrollRequest request = new ClearScrollRequest();
//                request.addScrollId(scrollId);
//                restHighLevelClient.clearScroll(request, RequestOptions.DEFAULT);
                LOGGER.info("Done retrieving {} download data", protocolStr);
            } catch (IOException ioe) {
                LOGGER.error("Exception occurred, {}", ioe);
            }
        }
    }

    /**
     * @param db
     * @param filePath
     * @param accessionRegex
     * @return A tuple first accession and then the name of the file being downloaded
     */
    private static Tuple<String, String> getAccessionAndFileName(ElasticSearchWsConfigProd.DB db, String filePath,
                                                                 Pattern accessionRegex, ENAWsClient enaWsClient) {
        // c.f. [/\\.] below for retrieval of e.g. /GCA_002757455 from /GCA_002757455.1_
        Matcher matcher = accessionRegex.matcher(filePath);
        boolean b = matcher.find();
        // Retrieve file name
        String[] arr = filePath.split("/");
        String fileName = arr[arr.length - 1];
        // Retrieve accession
        String omicsDIAccession = null;
        if (b) {
            String esAccession = matcher.group(0).replaceAll("^/|[_/]+$", "");
            if (db == ElasticSearchWsConfigProd.DB.ENA) {
                omicsDIAccession = enaWsClient.getProjectAccession(esAccession);
            } else {
                omicsDIAccession = esAccession;
            }
        } else {
            LOGGER.debug("Failed to retrieve accession from: {} - using accession regex: " + accessionRegex, filePath);
        }
        return new Tuple<>(omicsDIAccession, fileName);
    }

    /**
     * Add argument values to the aggregated results in dbToAccessionToDateToFileName
     *
     * @param db
     * @param accession
     * @param period
     * @param fileName
     */

    public static synchronized void addToResults(ElasticSearchWsConfigProd.DB db, String accession, String period,
                                    String anonymisedIPAddress, String fileName) {
        if (!DB_DATA.get(db).containsKey(accession)) {
            // We haven't seen this accession before
            Map<String, Map<String, Multiset<String>>> periodToAnonymisedIPAddressToFileNames = new HashMap<>();
            Map<String, Multiset<String>> anonymisedIPAddressToFileNames = new HashMap<>();
            // N.B. We use Multiset to maintain counts per individual download file
            anonymisedIPAddressToFileNames.put(anonymisedIPAddress, HashMultiset.create());
            anonymisedIPAddressToFileNames.get(anonymisedIPAddress).add(fileName);
            periodToAnonymisedIPAddressToFileNames.put(period, anonymisedIPAddressToFileNames);
            DB_DATA.get(db).put(accession, periodToAnonymisedIPAddressToFileNames);
        } else {
            // We've seen this accession before
            if (!DB_DATA.get(db).get(accession).containsKey(period)) {
                // We haven't seen this period for this accession before
                Map<String, Multiset<String>> anonymisedIPAddressToFileNames = new HashMap<>();
                anonymisedIPAddressToFileNames.put(anonymisedIPAddress, HashMultiset.create());
                DB_DATA.get(db).get(accession).put(period, anonymisedIPAddressToFileNames);
            } else {
                // We have seen this period for this accession before
                if (!DB_DATA.get(db).get(accession).get(period)
                        .containsKey(anonymisedIPAddress)) {
                    // We haven't seen this anonymisedIPAddress for that accession and period before
                    Map<String, Multiset<String>> anonymisedIPAddressToFileNames = new HashMap<>();
                    anonymisedIPAddressToFileNames.put(anonymisedIPAddress, HashMultiset.create());
                    DB_DATA.get(db).get(accession).put(period, anonymisedIPAddressToFileNames);
                }
            }
            DB_DATA.get(db).get(accession).get(period).get(anonymisedIPAddress).add(fileName);
        }
    }

    /**
     * Retrieves the required fields from each element in searchHits, retrieved for a given protocol
     *
     * @param searchHits
     * @param protocol
     */
    private static void getValuesFromHits(SearchHit[] searchHits, ElasticSearchWsConfigProd.Protocol protocol,
                                          ENAWsClient enaWsClient) {
        Arrays.stream(searchHits).parallel().forEach(hit -> {
            Map k2v = hit.getSourceAsMap();
            String anonymisedIPAddress = k2v.get("uhost").toString();  // Anonymised IP address
            String filePath = k2v.get("file_name").toString();
            for (ElasticSearchWsConfigProd.DB db : ElasticSearchWsConfigProd.DB.values()) {
                Map<ElasticSearchWsConfigProd.RegexType, Pattern> typeToRegex =
                        ElasticSearchWsConfigProd.PROTOCOL_2_DB_2_REGEX.get(protocol).get(db);
                if (typeToRegex.keySet().isEmpty()) {
                    continue;
                }
                String resource = null;
                if (typeToRegex.get(ElasticSearchWsConfigProd.RegexType.positive).matcher(filePath).find()) {
                    if (typeToRegex.get(ElasticSearchWsConfigProd.RegexType.negative) != null) {
                        if (!typeToRegex.get(ElasticSearchWsConfigProd.RegexType.negative).matcher(filePath).find()) {
                            resource = db.toString();
                        }
                    } else {
                        resource = db.toString();
                    }
                }
                if (resource != null) {
                    Tuple<String, String> accessionFileName = getAccessionAndFileName(db, filePath,
                            typeToRegex.get(ElasticSearchWsConfigProd.RegexType.accessionSpecial), enaWsClient);
                    String accession = accessionFileName.v1();
                    String fileName = accessionFileName.v2();
                    if (accession != null) {
                        String date = k2v.get("@timestamp").toString().split("\\.")[0];
                        addToResults(db, accession, date, anonymisedIPAddress, fileName);
                        break;
                    }
                }
            }
        });
    }
}
