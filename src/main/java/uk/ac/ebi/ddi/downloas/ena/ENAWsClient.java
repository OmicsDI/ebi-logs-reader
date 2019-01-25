package uk.ac.ebi.ddi.downloas.ena;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.util.UriComponentsBuilder;
import uk.ac.ebi.ddi.downloas.logs.ElasticSearchWsConfigProd;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Client to query ENA API to retrieve a project accession for a given ENA (non-project) accession
 *
 * @author datasome
 */

public class ENAWsClient extends WsClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(ENAWsClient.class);
    private ENAWsConfigProd config;

    // A cache of all the mapping between non-project ENA accessions and their corresponding project accessions
    private static final Map<String, String> enaAccessionToProject = new ConcurrentHashMap<>();

    private Pattern enaPattern = Pattern.compile(ElasticSearchWsConfigProd.ENA_PRJ_ACCESSION_REGEX);

    // Because concurrentHasMap not allowed to put null as value, this will be used instead
    private static final String NULL_VALUE = "NULL";

    /**
     * Default constructor for Ws clients
     *
     * @param config
     */
    public ENAWsClient(ENAWsConfigProd config) {
        super(config);
        this.config = config;
    }

    /**
     * Populate enaAccessionToProject cache if it has not yet been populated
     */
    public void populateCache() {
        long startTime = System.currentTimeMillis();
        Map<ENAWsConfigProd.AccessionTypes, String> accType2Url = new HashMap<>();
        for (ENAWsConfigProd.AccessionTypes accType : ENAWsConfigProd.AccessionTypes.values()) {
            if (accType != ENAWsConfigProd.AccessionTypes.submission) {
                // N.B. that because https://www.ebi.ac.uk/ena/portal/api/returnFields?result=read_study does not contain
                // submission_accession as one of its return fields, we are unable to populate the cache with
                // submission accession to project mappings
                UriComponentsBuilder builder = UriComponentsBuilder.newInstance()
                        .scheme(config.getProtocol())
                        .host(config.getHostName())
                        .path("/search")
                        .queryParam("result", ENAWsConfigProd.getReturnObjectType(accType))
                        .queryParam("fields", ENAWsConfigProd.getAccessionField(accType) + ",study_accession")
                        .queryParam("limit", "0")
                        .queryParam("format", "json");
                accType2Url.put(accType, builder.build().toString());
            }
        }

        for (ENAWsConfigProd.AccessionTypes accType : accType2Url.keySet()) {
            String url = accType2Url.get(accType);
            LOGGER.info("Fetching {}", url);
            for (ENAProjectAccessionMapping pAcc : restTemplate.getForObject(url, ENAProjectAccessionMapping[].class)) {
                String accessionInCache = accType == ENAWsConfigProd.AccessionTypes.sequence
                        ? pAcc.getAccession(accType).replace(ENAWsConfigProd.getLookupPostfix(accType), "")
                        : pAcc.getAccession(accType);
                enaAccessionToProject.put(accessionInCache, pAcc.getProjectAccession());
            }
        }
        long estimatedTime = (System.currentTimeMillis() - startTime) / 1000; // secs
        LOGGER.info("enaAccessionToProject cache initialised in: " + estimatedTime + " secs");
    }

    /**
     * @param enaAccession
     * @return ENA project accession corresponding to enaAccession
     */
    public String getProjectAccession(String enaAccession) {
        if (enaAccession == null || enaAccession.length() > 0) {
            return null;
        }

        if (enaAccessionToProject.containsKey(enaAccession)) {
            if (enaAccessionToProject.get(enaAccession).equals(NULL_VALUE)) {
                return null;
            }
            return enaAccessionToProject.get(enaAccession);
        } else {
            Matcher mPRJ = enaPattern.matcher(enaAccession);
            if (mPRJ.matches()) {
                enaAccessionToProject.put(enaAccession, enaAccession);
                return enaAccession;
            }
        }
        // Find ENAWsConfigProd.AccessionTypes value corresponding to enaAccession
        ENAWsConfigProd.AccessionTypes accTypeFound = null;
        for (ENAWsConfigProd.AccessionTypes accType : ENAWsConfigProd.AccessionTypes.values()) {
            Matcher matcher = ENAWsConfigProd.getRegexPattern(accType).matcher(enaAccession);
            if (matcher.matches()) {
                accTypeFound = accType;
                break;
            }
        }
        // Retrieve project accession corresponding to enaAccession of type accTypeFound
        if (accTypeFound != null) {
            UriComponentsBuilder builder = UriComponentsBuilder.newInstance()
                    .scheme(config.getProtocol())
                    .host(config.getHostName())
                    .path("/search")
                    .queryParam("result", ENAWsConfigProd.getReturnObjectType(accTypeFound))
                    .queryParam("query",
                            String.format("(%s=%s)",
                                    ENAWsConfigProd.getAccessionField(accTypeFound),
                                    accTypeFound == ENAWsConfigProd.AccessionTypes.sequence
                                            ? enaAccession + ENAWsConfigProd.getLookupPostfix(accTypeFound)
                                            : enaAccession))
                    .queryParam("fields", "study_accession")
                    .queryParam("limit", "1")
                    .queryParam("format", "json");

            URI uri = builder.build().toUri();
            String projectAccession = null;
            ENAProjectAccessionMapping[] results = getRetryTemplate().execute(
                    ctx -> restTemplate.getForObject(uri, ENAProjectAccessionMapping[].class));
            if (results != null && results.length > 0) {
                projectAccession = results[0].getProjectAccession();
                enaAccessionToProject.put(enaAccession, projectAccession);
            } else {
                enaAccessionToProject.put(enaAccession, NULL_VALUE);
            }
            return projectAccession;
        }

        return null;
    }
}
