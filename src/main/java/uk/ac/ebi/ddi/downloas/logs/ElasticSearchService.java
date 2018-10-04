package uk.ac.ebi.ddi.downloas.logs;

import com.google.common.collect.Multiset;

import java.io.IOException;
import java.time.LocalDate;
import java.util.Map;

/**
 * @author Robert Petryszak (rpetry)
 */
public class ElasticSearchService {

    private static ElasticSearchService instance;

    ElasticSearchWsClient elasticSearchClient = new ElasticSearchWsClient(new ElasticSearchWsConfigProd());

    /**
     * Private Constructor
     */
    private ElasticSearchService() {
    }

    /**
     * Public instance to be retrieved
     *
     * @return Public-Unique instance
     */
    public static ElasticSearchService getInstance() {
        if (instance == null) {
            instance = new ElasticSearchService();
        }
        return instance;
    }

//    /**
//     * Add data downloads-related additional fields to dataset
//     *
//     * @param db
//     * @param dataset
//     * @throws IOException
//     */
//    public void annotateDataDownloads(String db, Dataset dataset, LocalDate yearLocalDate) {
//        Map<String, Multiset<String>> periodToFileNames = elasticSearchClient.getDataDownloads(DB.valueOf(db), dataset.getAccession(), yearLocalDate);
//        for (String period : periodToFileNames.keySet()) {
//            // Period: yyyy/mm
//            String downloadCount = Integer.toString(periodToFileNames.get(period).size());
//            // TODO: Populate a new data downloads field in dataset
//        }
//    }

    /**
     * An auxiliary method used for testing
     * @param batchSize
     * @param reportingFrequency
     * @param maxHits
     * @param yearLocalDate
     * @return  the Map of all ftp/Aspera data downloads results for year stored in yearLocalDate
     */
    public Map<ElasticSearchWsConfigProd.DB, Map<String, Map<String, Multiset<String>>>> getResults(Integer batchSize, Integer reportingFrequency, Integer maxHits, LocalDate yearLocalDate) {
        return elasticSearchClient.getResults(batchSize, reportingFrequency, maxHits, yearLocalDate);
    }
}