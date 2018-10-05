package uk.ac.ebi.ddi.downloas.ena;

import uk.ac.ebi.ddi.downloas.logs.ElasticSearchWsConfigProd;

/**
 * The configuration class for the ENA API client
 *
 * @author datasome
 */

public class ENAWsConfigProd {

    private String hostName;
    private String protocol;

    public ENAWsConfigProd(String protocol, String hostName) {
        this.hostName = hostName;
        this.protocol = protocol;
    }

    public ENAWsConfigProd() {
        this("https", "www.ebi.ac.uk/ena/portal/api");
    }

    public String getHostName() {
        return hostName;
    }

    public String getProtocol() {
        return protocol;
    }

    // Config for different types of ENA accessions
    public enum AccessionTypes {
        study_experiment_run, analysis, submission, assembly, sequence
    }

    /**
     * @param accType
     * @return Regular expression specific to the type of ENA accession: accType
     */
    public static String getRegex(AccessionTypes accType) {
        String regex = null;
        switch (accType) {
            case study_experiment_run:
                regex = ElasticSearchWsConfigProd.ENA_SER_ACCESSION_REGEX;
                break;
            case analysis:
                regex = ElasticSearchWsConfigProd.ENA_ANALYSIS_ACCESSION_REGEX;
                break;
            case submission:
                regex = ElasticSearchWsConfigProd.ENA_SUBMISSION_ACCESSION_REGEX;
                break;
            case assembly:
                regex = ElasticSearchWsConfigProd.ENA_GCA_ACCESSION_REGEX;
                break;
            case sequence:
                regex = ElasticSearchWsConfigProd.ENA_SEQ_ACCESSION_REGEX;
                break;
        }
        return regex;
    }

    /**
     * @param accType
     * @return Return ENA object type specific to the type of ENA accession: accType
     */
    public static String getReturnObjectType(AccessionTypes accType) {
        String retObject = null;
        switch (accType) {
            case study_experiment_run:
                retObject = "read_run";
                break;
            case analysis:
                retObject = "analysis";
                break;
            case submission:
                retObject = "read_study";
                break;
            case assembly:
                retObject = "assembly";
                break;
            case sequence:
                retObject = "wgs_set";
                break;
        }
        return retObject;
    }

    /**
     * @param accType
     * @return The field name storing the non-project ENA accession, specific to the type of ENA accession: accType
     */
    public static String getAccessionField(AccessionTypes accType) {
        String accField = null;
        switch (accType) {
            case study_experiment_run:
                accField = "run_accession";
                break;
            case analysis:
                accField = "analysis_accession";
                break;
            case submission:
                accField = "submission_accession";
                break;
            case assembly:
                accField = "accession";
                break;
            case sequence:
                accField = "accession";
                break;
        }
        return accField;
    }

    /**
     * @param accType
     * @return A postfix required to be appended to the non-project ENA accession in order
     * to retrieve the corresponding project accession, specific to the type of ENA accession: accType
     */
    public static String getLookupPostfix(AccessionTypes accType) {
        String postfix = null;
        switch (accType) {
            case sequence:
                postfix = "000000";
                break;
        }
        return postfix;
    }

}
