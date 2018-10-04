package uk.ac.ebi.ddi.downloas.logs;



import java.util.HashMap;
import java.util.Map;

/**
 * @author Robert Petryszak rpetry
 *         This class contains configuration for accessing ElasticSearch behind Kibana, where both ftp and Aspera data download logs are stored.
 */
public class ElasticSearchWsConfigProd {

    public static final Integer PORT = 9200;
    // Available ElasticSearch nodes can be viewed at: https://meter.ebi.ac.uk/app/monitoring#/elasticsearch/nodes?_g=(cluster_uuid:KybG-9d6Q0Cn4HXIjnfVzw)
    // C.f. For available timeout config, c.f. https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/_timeouts.html
    public static final String HOST = "10.3.10.28";
    public static final String USERNAME = "readall";
    public static final String PASSWORD = USERNAME;

    // ElasticSearch query-related constants
    public static final Long SCROLL_VALID_PERIOD = 1440L; // 24h
    public static final int DEFAULT_QUERY_BATCH_SIZE = 10000; // records - this is also the maximum batch size for ElasticSearch queries
    public static final int DEFAULT_PROGRESS_REPORTING_FREQ = 500000; // records
    // C.f. info on timeouts in https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/_timeouts.html
    public static final int MAX_RETRY_TIMEOUT = 300000; // ms = 5 mins (default: 30s)
    public static final int SOCKET_TIMEOUT = 300000; // ms = 5 mins (default: 30s)
    public static final int CONNECT_TIMEOUT = 5000; // ms = 5 secs (default: 1s)

    // Regex constants
    public static final String ARRAYEXPRESS_ACCESSION_REGEX = "E\\-[A-Z]{4}\\-\\d+";
    public static final String EGA_ACCESSION_REGEX = "EGA[DBCDFNPRSXZ]\\d{11}";
    public static final String ENA_SER_ACCESSION_REGEX = "[EDS]{1}R[PRX]\\d{7}|[EDS]{1}R[PRX]\\d{6}";
    public static final String ENA_SUBMISSION_ACCESSION_REGEX = "[EDS]{1}RA\\d{7}|[EDS]{1}RA\\d{6}";
    public static final String ENA_ANALYSIS_ACCESSION_REGEX = "[EDS]{1}RZ\\d{7}|[EDS]{1}RZ\\d{6}";
    public static final String ENA_GCA_ACCESSION_REGEX = "GCA_\\d{9}";
    public static final String ENA_SEQ_ACCESSION_REGEX = "[A-Z]{4}\\d{2}";
    public static final String ENA_PRJ_ACCESSION_REGEX = "PRJ[\\w\\d]+";
    public static final String ENA_ACCESSION_REGEX = ENA_SER_ACCESSION_REGEX + "|[A-Z]{2}\\d{6}|ERS\\d{6}|" +
            ENA_ANALYSIS_ACCESSION_REGEX + "|" +
            ENA_SUBMISSION_ACCESSION_REGEX + "|" +
            ENA_GCA_ACCESSION_REGEX + "|[A-Z]{4}\\d{2}|[A-Z]{3}\\d{5}|" +
            ENA_SEQ_ACCESSION_REGEX;
    public static final String EVA_ACCESSION_REGEX = "esv\\d+|estd\\d+|essv\\d+|[rs]s\\d+|PRJEB\\d+|PRJNA\\d+|PRJX\\d+";
    public static final String METABOLIGHTS_ACCESSION_REGEX = "MTBLS\\d+";
    public static final String PRIDE_ACCESSION_REGEX = "PXD\\d{6}|PRD\\d{6}";
    public static final String EXPRESSION_ATLAS_FTP_ROOT = "/pub/databases/(arrayexpress|microarray)/data/atlas/";
    // Regex used for retrieving date string from ftp/aspera log entries
    public static final String YEAR_MONTH_REGEX = "\\d{4}/\\d{2}";

    // Data download protocol types
    public enum Protocol {
        ftp, aspera
    }

    // Regex types for mapping data download log entries to OmicsDI resources and their accessions
    public enum RegexType {
        accession, positive, negative
    }

    // EBI resources in OmicsDI:
    // N.B. these values need to match the corresponding ddi.common.*.database.name field values
    // in src/main/resources/prop/common.properties inside index-pipeline
    // N.B. BioModels, EGA and ENA have been excluded from the first round of prototyping, due to additional complications in
    // retrieval of dataset accessions (WIP)
    public enum DB {
        ArrayExpress, Pride, ExpressionAtlas, EVA, Metabolights
    }

    // Hashmap for storing regexes
    public static final Map<Protocol, Map<DB, Map<RegexType, String>>> protocol2DB2Regex
            = new HashMap<Protocol, Map<DB, Map<RegexType, String>>>() {
        {
            // Initialise all sub-maps
            for (Protocol protocol : Protocol.values()) {
                put(protocol, new HashMap<>());
                for (DB db : DB.values()) {
                    get(protocol).put(db, new HashMap<>());
                }
            }
            for (Protocol protocol : Protocol.values()) {
                get(protocol).get(DB.ArrayExpress).put(RegexType.accession, ARRAYEXPRESS_ACCESSION_REGEX);
                get(protocol).get(DB.ArrayExpress).put(RegexType.positive, "/arrayexpress/data/experiment/\\w{4}/" + ARRAYEXPRESS_ACCESSION_REGEX);
                get(protocol).get(DB.ArrayExpress).put(RegexType.negative, EXPRESSION_ATLAS_FTP_ROOT + "|/data/array/");

                // TODO: Uncomment once data downloads feed from EGA is established (outside of ElasticSearch)
//                get(protocol).get(DB.EGA).put(RegexType.accession, EGA_ACCESSION_REGEX);
//                get(protocol).get(DB.EGA).put(RegexType.positive, "/ega/");
//                get(protocol).get(DB.EGA).put(RegexType.negative, "/ega_\\w+_prod_dump|/pub/contrib");

                // TODO: Uncomment once a reliable logic is implemented to extract (from ENA API) Project accessions corresponding
                // TODO: to ENA accessions reported in ElasticSearch.
//                get(protocol).get(DB.ENA).put(RegexType.accession, ENA_ACCESSION_REGEX);
//                get(protocol).get(DB.ENA).put(RegexType.negative,
//                        "/atlas/rnaseq/|/ena/doc/|/ena/report/|/ena/sequence/misc/|/ena/tsa_master/|vol1/\\.welcome|/ena/taxonomy/|/suppressed/|/trace/");

                get(protocol).get(DB.ExpressionAtlas).put(RegexType.accession, ARRAYEXPRESS_ACCESSION_REGEX);
                get(protocol).get(DB.ExpressionAtlas).put(RegexType.positive,
                        EXPRESSION_ATLAS_FTP_ROOT + "(experiments/|rnaseq/studies/arrayexpress/)" + ARRAYEXPRESS_ACCESSION_REGEX);
                get(protocol).get(DB.ExpressionAtlas).put(RegexType.negative,
                        "/ontology/|/atlas/software/|/gsa/|/atlas/curation/|zoomage_reports|bioentity_properties|atlas/experiments/.*\\.(xml|json|tar\\.gz)");

                get(protocol).get(DB.Metabolights).put(RegexType.accession, METABOLIGHTS_ACCESSION_REGEX);
                get(protocol).get(DB.Metabolights).put(RegexType.positive, "/metabolights/studies/public/" + METABOLIGHTS_ACCESSION_REGEX);
                get(protocol).get(DB.Metabolights).put(RegexType.negative, null);

                get(protocol).get(DB.Pride).put(RegexType.accession, PRIDE_ACCESSION_REGEX);
                get(protocol).get(DB.Pride).put(RegexType.positive, "/pride/data/archive/\\d{4}/\\d{2}");
                get(protocol).get(DB.Pride).put(RegexType.negative, null);
            }

            // ftp-specific regexes
            // TODO: Uncomment once a reliable logic is implemented to extract (from ENA API) Project accessions corresponding
            // TODO: to ENA accessions reported in ElasticSearch.
//            get(Protocol.ftp).get(DB.ENA).put(RegexType.positive, "/ena/");

            // TODO: Uncomment once downloads data per BioModel accession is made available from BioModels
//            get(Protocol.ftp).get(DB.BioModels).put(RegexType.accession, "(BIOMD|MODEL)\\d{10}|BMID\\d{12}");
//            get(Protocol.ftp).get(DB.BioModels).put(RegexType.positive, "/biomodels/logical/|/biomodels/metabolic/|/biomodels/pdgsmm/|/biomodels/releases/");
//            get(Protocol.ftp).get(DB.BioModels).put(RegexType.negative, "README.txt");

            get(Protocol.ftp).get(DB.EVA).put(RegexType.accession, EVA_ACCESSION_REGEX);
            get(Protocol.ftp).get(DB.EVA).put(RegexType.positive, "/eva/(" + EVA_ACCESSION_REGEX + ")");
            get(Protocol.ftp).get(DB.EVA).put(RegexType.negative, "/eva/ClinVar/");

            // aspera-specific regexes
            // TODO: Uncomment once a reliable logic is implemented to extract (from ENA API) Project accessions corresponding
            // TODO: to ENA accessions reported in ElasticSearch.
//            get(Protocol.aspera).get(DB.ENA).put(RegexType.positive, "/era-pub");

        }
    };

}