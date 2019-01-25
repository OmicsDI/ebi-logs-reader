package uk.ac.ebi.ddi.downloas.ena;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import uk.ac.ebi.ddi.downloas.logs.ElasticSearchWsConfigProd;


/**
 * A class representing various mappings between non-project ENA accessions
 *                      and their corresponding ENA project accessions.
 *
 * @author Robert Petryszak (rpetry)
 */
@JsonIgnoreProperties(ignoreUnknown = true)

public class ENAProjectAccessionMapping {

    @JsonProperty("study_accession")
    String studyAccesion;

    @JsonProperty("accession")
    String accession;

    @JsonProperty("run_accession")
    String runAccesion;

    @JsonProperty("analysis_accession")
    String analysisAccesion;

    public String getProjectAccession() {
        return studyAccesion;
    }

    public String getAccession(ENAWsConfigProd.AccessionTypes accType) {
        String retAcc = null;
        switch (accType) {
            case study_experiment_run:
                retAcc = runAccesion;
                break;
            case analysis:
                retAcc = analysisAccesion;
                break;
            case assembly:
                retAcc = accession;
                break;
            case sequence:
                retAcc = accession;
                break;
        }
        return retAcc;
    }
}
