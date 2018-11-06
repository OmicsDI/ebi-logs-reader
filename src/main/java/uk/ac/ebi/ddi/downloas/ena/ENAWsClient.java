package uk.ac.ebi.ddi.downloas.ena;

import org.apache.log4j.Logger;
import uk.ac.ebi.ddi.downloas.logs.ElasticSearchWsConfigProd;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Client to query ENA API to retrieve a project accession for a given ENA (non-project) accession
 *
 * @author datasome
 */

public class ENAWsClient extends WsClient {

    private static final org.apache.log4j.Logger log = Logger.getLogger(ENAWsClient.class);
    private ENAWsConfigProd config;

    // A cache of all the mapping between non-project ENA accessions and their corresponding project accessions
    private static final Map<String, String> enaAccessionToProject = new HashMap<>();

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
        if (cacheReady())
            return;
        long startTime = System.currentTimeMillis();
        Map<ENAWsConfigProd.AccessionTypes, String> accType2Url = new HashMap<>();
        for (ENAWsConfigProd.AccessionTypes accType : ENAWsConfigProd.AccessionTypes.values()) {
            if (accType != ENAWsConfigProd.AccessionTypes.submission) {
                // N.B. that because https://www.ebi.ac.uk/ena/portal/api/returnFields?result=read_study does not contain
                // submission_accession as one of its return fields, we are unable to populate the cache with
                // submission accession to project mappings
                accType2Url.put(accType, String.format(String.format("%s://%s/search?result=%s&fields=%s,study_accession&limit=0&format=json",
                        config.getProtocol(), config.getHostName(),
                        ENAWsConfigProd.getReturnObjectType(accType),
                        ENAWsConfigProd.getAccessionField(accType))));
            }
        }

        for (ENAWsConfigProd.AccessionTypes accType : accType2Url.keySet()) {
            String url = accType2Url.get(accType);
            log.info(url);
            for (ENAProjectAccessionMapping pAcc : this.restTemplate.getForObject(url, ENAProjectAccessionMapping[].class)) {
                String accessionInCache = accType == ENAWsConfigProd.AccessionTypes.sequence ?
                        pAcc.getAccession(accType).replace(ENAWsConfigProd.getLookupPostfix(accType),"") : pAcc.getAccession(accType);
                enaAccessionToProject.put(accessionInCache, pAcc.getProjectAccession());
            }
        }
        long estimatedTime = (System.currentTimeMillis() - startTime) / 1000; // secs
        log.info("enaAccessionToProject cache initialised in: " + estimatedTime + " secs");
    }

    /**
     * A dumb method to check if cache has been instantiated
     */
    private boolean cacheReady() {
        return enaAccessionToProject.keySet().size() > 6000000l;
    }

    /**
     * @param enaAccession
     * @return ENA project accession corresponding to enaAccession
     */
    public String getProjectAccession(String enaAccession) {
        if (enaAccession != null && enaAccession.length() > 0) {
            if (enaAccessionToProject.containsKey(enaAccession)) {
                return enaAccessionToProject.get(enaAccession);
            } else {
                Matcher mPRJ = Pattern.compile(ElasticSearchWsConfigProd.ENA_PRJ_ACCESSION_REGEX).matcher(enaAccession);
                if (mPRJ.matches()) {
                    return enaAccession;
                }
            }
            // Find ENAWsConfigProd.AccessionTypes value corresponding to enaAccession
            ENAWsConfigProd.AccessionTypes accTypeFound = null;
            for (ENAWsConfigProd.AccessionTypes accType : ENAWsConfigProd.AccessionTypes.values()) {
                Matcher matcher = Pattern.compile(ENAWsConfigProd.getRegex(accType)).matcher(enaAccession);
                if (matcher.matches()) {
                    accTypeFound = accType;
                    break;
                }
            }
            // Retrieve project accession corresponding to enaAccession of type accTypeFound
            if (accTypeFound != null) {
                String url = String.format("%s://%s/search?result=%s&query=(%s=%s)&fields=study_accession&limit=1&format=json",
                        config.getProtocol(), config.getHostName(),
                        ENAWsConfigProd.getReturnObjectType(accTypeFound),
                        ENAWsConfigProd.getAccessionField(accTypeFound),
                        accTypeFound == ENAWsConfigProd.AccessionTypes.sequence ?
                                enaAccession + ENAWsConfigProd.getLookupPostfix(accTypeFound) : enaAccession);
                log.debug(url);

                String projectAccession = null;
                ENAProjectAccessionMapping[] results = this.restTemplate.getForObject(url, ENAProjectAccessionMapping[].class);
                if (results != null && results.length > 0) {
                    projectAccession = this.restTemplate.getForObject(url, ENAProjectAccessionMapping[].class)[0].getProjectAccession();
                    enaAccessionToProject.put(enaAccession, projectAccession);
                }
                return projectAccession;
            }
        }
        return null;
    }
}
