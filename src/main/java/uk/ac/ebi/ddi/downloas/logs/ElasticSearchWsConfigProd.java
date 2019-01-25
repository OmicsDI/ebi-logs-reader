package uk.ac.ebi.ddi.downloas.logs;



import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author Robert Petryszak rpetry
 *         This class contains configuration for accessing ElasticSearch behind Kibana, where both ftp and Aspera data download logs are stored.
 */
public class ElasticSearchWsConfigProd {

    public Integer port;
    public String  host;
    public String  username;
    public String  password;

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
    public static final String YEAR_MONTH_DATE_REGEX = "\\d{4}/\\d{2}/\\d{2}";

    // Data download protocol types
    public enum Protocol {
        ftp, aspera
    }

    // Regex types for mapping data download log entries to OmicsDI resources and their accessions
    public enum RegexType {
        accession, positive, negative, accessionSpecial
    }

    // EBI resources in OmicsDI:
    // N.B. these values need to match the corresponding ddi.common.*.database.name field values
    // in src/main/resources/prop/common.properties inside index-pipeline
    // N.B. BioModels and EGA have been excluded from the first round of prototyping, due to additional complications in
    // retrieval of dataset accessions (WIP)
    public enum DB {
        ArrayExpress, Pride, ExpressionAtlas, EVA, MetaboLights, ENA
    }

    public ElasticSearchWsConfigProd(Integer port, String host, String username, String password) {
        this.port = port;
        this.host = host;
        this.username = username;
        this.password = password;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    // Hashmap for storing regexes
    public static final Map<Protocol, Map<DB, Map<RegexType, Pattern>>> PROTOCOL_2_DB_2_REGEX
            = new HashMap<Protocol, Map<DB, Map<RegexType, Pattern>>>() {
        {
            // Initialise all sub-maps
            for (Protocol protocol : Protocol.values()) {
                put(protocol, new HashMap<>());
                for (DB db : DB.values()) {
                    get(protocol).put(db, new HashMap<>());
                }
            }
            for (Protocol protocol : Protocol.values()) {
                get(protocol).get(DB.ArrayExpress).put(RegexType.accession,
                        Pattern.compile(ARRAYEXPRESS_ACCESSION_REGEX));
                get(protocol).get(DB.ArrayExpress).put(RegexType.positive,
                        Pattern.compile("/arrayexpress/data/experiment/\\w{4}/" + ARRAYEXPRESS_ACCESSION_REGEX));
                get(protocol).get(DB.ArrayExpress).put(RegexType.negative,
                        Pattern.compile(EXPRESSION_ATLAS_FTP_ROOT + "|/data/array/"));

                // TODO: Uncomment once data downloads feed from EGA is established (outside of ElasticSearch)
//                get(protocol).get(DB.EGA).put(RegexType.accession, EGA_ACCESSION_REGEX);
//                get(protocol).get(DB.EGA).put(RegexType.positive, "/ega/");
//                get(protocol).get(DB.EGA).put(RegexType.negative, "/ega_\\w+_prod_dump|/pub/contrib");

                get(protocol).get(DB.ENA).put(RegexType.accession, Pattern.compile(ENA_ACCESSION_REGEX));
                get(protocol).get(DB.ENA).put(RegexType.negative,
                        Pattern.compile("/atlas/rnaseq/|/ena/doc/|/ena/report/|/ena/sequence/misc/|/ena/tsa_master/" +
                                "|vol1/\\.welcome|/ena/taxonomy/|/suppressed/|/trace/"));

                get(protocol).get(DB.ExpressionAtlas).put(RegexType.accession,
                        Pattern.compile(ARRAYEXPRESS_ACCESSION_REGEX));
                get(protocol).get(DB.ExpressionAtlas).put(RegexType.positive,
                        Pattern.compile(EXPRESSION_ATLAS_FTP_ROOT
                                + "(experiments/|rnaseq/studies/arrayexpress/)" + ARRAYEXPRESS_ACCESSION_REGEX));
                get(protocol).get(DB.ExpressionAtlas).put(RegexType.negative,
                        Pattern.compile("/ontology/|/atlas/software/|/gsa/|/atlas/curation/|zoomage_reports" +
                                "|bioentity_properties|atlas/experiments/.*\\.(xml|json|tar\\.gz)"));

                get(protocol).get(DB.MetaboLights).put(RegexType.accession,
                        Pattern.compile(METABOLIGHTS_ACCESSION_REGEX));
                get(protocol).get(DB.MetaboLights).put(RegexType.positive,
                        Pattern.compile("/metabolights/studies/public/" + METABOLIGHTS_ACCESSION_REGEX));
                get(protocol).get(DB.MetaboLights).put(RegexType.negative, null);

                get(protocol).get(DB.Pride).put(RegexType.accession, Pattern.compile(PRIDE_ACCESSION_REGEX));
                get(protocol).get(DB.Pride).put(RegexType.positive,
                        Pattern.compile("/pride/data/archive/\\d{4}/\\d{2}"));
                get(protocol).get(DB.Pride).put(RegexType.negative, null);
            }

            // ftp-specific regexes
            get(Protocol.ftp).get(DB.ENA).put(RegexType.positive, Pattern.compile("/ena/"));

            get(Protocol.ftp).get(DB.EVA).put(RegexType.accession, Pattern.compile(EVA_ACCESSION_REGEX));
            get(Protocol.ftp).get(DB.EVA).put(RegexType.positive,
                    Pattern.compile("/eva/(" + EVA_ACCESSION_REGEX + ")"));
            get(Protocol.ftp).get(DB.EVA).put(RegexType.negative, Pattern.compile("/eva/ClinVar/"));

            // aspera-specific regexes
            get(Protocol.aspera).get(DB.ENA).put(RegexType.positive, Pattern.compile("/era-pub"));

            for (Protocol protocol : Protocol.values()) {
                for (DB db : DB.values()) {
                    if (!get(protocol).get(db).containsKey(RegexType.accession)) {
                        continue;
                    }
                    String original = get(protocol).get(db).get(RegexType.accession).pattern();
                    get(protocol).get(db).put(RegexType.accessionSpecial, Pattern.compile("/" + original + "[/\\.]"));
                }
            }
        }
    };

}