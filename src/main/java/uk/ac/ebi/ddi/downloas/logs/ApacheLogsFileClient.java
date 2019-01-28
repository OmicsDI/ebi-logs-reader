package uk.ac.ebi.ddi.downloas.logs;


import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * @author datasome
 * This class encapsulates the client functionality for accessing file downloads information
 * from Apache access LOGGER files
 */
public class ApacheLogsFileClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(ApacheLogsFileClient.class);
    private ApacheLogsFileConfigProd config;

    // Hashmap for storing results aggregated by period (yyyy/mm)
    private static final Map<ApacheLogsFileConfigProd.DB, Map<String, Map<String, Map<String, Multiset<String>>>>>
            DB_TO_ACCESSION_TO_PERIOD_TO_ANONYMISED_IP_ADDRESS_TO_FILE_NAME =
            new HashMap<ApacheLogsFileConfigProd.DB, Map<String, Map<String, Map<String, Multiset<String>>>>>() {
                {
                    for (ApacheLogsFileConfigProd.DB db : ApacheLogsFileConfigProd.DB.values()) {
                        put(db, new HashMap<>());
                    }
                }
            };

    /**
     * Constructor
     *
     * @param config
     */
    public ApacheLogsFileClient(ApacheLogsFileConfigProd config) {
        this.config = config;
    }

    /**
     * @param db
     * @param accession
     * @return For a given database, dataset accession and a year (represented by yearLocalDate),
     * return a Map between each Period (yyyy/mm) and a map of anonymised IP addresses pointing Multisets
     * of their corresponding file names/download counts
     */
    public Map<String, Map<String, Multiset<String>>> getDataDownloads(ApacheLogsFileConfigProd.DB db,
                                                                       String accession) {
        Map<String, Map<String, Multiset<String>>> anonymisedIPAddressToFileNames = null;
        retrieveAllDataFromApacheLogs();
        if (DB_TO_ACCESSION_TO_PERIOD_TO_ANONYMISED_IP_ADDRESS_TO_FILE_NAME.containsKey(db)) {
            Map<String, Map<String, Map<String, Multiset<String>>>> accessionToPeriodToAnonymisedIPAddressToFileName =
                    DB_TO_ACCESSION_TO_PERIOD_TO_ANONYMISED_IP_ADDRESS_TO_FILE_NAME.get(db);
            if (accessionToPeriodToAnonymisedIPAddressToFileName.containsKey(accession)) {
                anonymisedIPAddressToFileNames =
                        DB_TO_ACCESSION_TO_PERIOD_TO_ANONYMISED_IP_ADDRESS_TO_FILE_NAME.get(db).get(accession);
            } else {
                LOGGER.warn("No accession: '" + accession + "' could be found in the data retrieved for db: '"
                        + db.toString() + "'from Apache access LOGGER files");
            }
        } else {
            LOGGER.warn("No db: '" + db.toString()
                    + "' could be found in the data retrieved from Apache access LOGGER files");
        }
        return anonymisedIPAddressToFileNames;
    }

    /**
     * @return False if for at least one DB no data downloads are present; otherwise return True
     */
    private boolean resultsReady() {
        boolean resultsReady = true;
        for (ApacheLogsFileConfigProd.DB db :
                DB_TO_ACCESSION_TO_PERIOD_TO_ANONYMISED_IP_ADDRESS_TO_FILE_NAME.keySet()) {
            resultsReady = !DB_TO_ACCESSION_TO_PERIOD_TO_ANONYMISED_IP_ADDRESS_TO_FILE_NAME.get(db).isEmpty();
            if (!resultsReady) {
                break;
            }
        }
        return resultsReady;
    }

    /**
     * Function to retrieve all relevant data download entries for the current year from Apache access LOGGER files,
     * and aggregate them in the static dbToAccessionToPeriodToFileName data structure
     */
    private void retrieveAllDataFromApacheLogs() {
        if (!resultsReady()) {
            for (ApacheLogsFileConfigProd.DB db : ApacheLogsFileConfigProd.DB.values()) {
                String pattern = ApacheLogsFileConfigProd.DB_2_REGEX.get(db).get(ApacheLogsFileConfigProd.FIELD.Regex);
                Pattern r = Pattern.compile(pattern);
                int currentYear = Calendar.getInstance().getWeekYear();
                File path = new File(ApacheLogsFileConfigProd.DB_2_REGEX.get(db)
                        .get(ApacheLogsFileConfigProd.FIELD.LogsDir));

                FilenameFilter filter = new FilenameFilter() {
                    public boolean accept(File directory, String fileName) {
                        return fileName.matches(ApacheLogsFileConfigProd.LOGFILENAME_PREFIX
                                + currentYear + ApacheLogsFileConfigProd.LOGFILENAME_POSTFIX);
                    }
                };
                File[] files = path.listFiles(filter);
                for (int i = 0; i < files.length; i++) {
                    if (files[i].isFile()) { //this line weeds out other directories/folders
                        String period = getYearMonth(files[i].getName());
                        try {
                            BufferedReader reader = new BufferedReader(new FileReader(files[i]));
                            String line;
                            while ((line = reader.readLine()) != null) {
                                Matcher m = r.matcher(line);
                                while (m.find()) {
                                    String anonymisedIPAddress = getMd5(line.split("\\s+")[0]);
                                    String[] downloadPathArr = m.group(0).split("/");
                                    String[] downloadEntryArr =
                                            downloadPathArr[downloadPathArr.length - 1].split("\\?filename=");
                                    String accession = downloadEntryArr[0].split("\\.")[0];
                                    String fileName = downloadEntryArr[1].split("\\&")[0];
                                    addToResults(db, accession, period, anonymisedIPAddress, fileName);
                                }
                            }
                        } catch (IOException ex) {
                            LOGGER.error("Exception occurred, db:  {}", db, ex);
                        }
                    }
                }
            }

        }
    }


    /**
     * @param apacheAccessLogFileName
     * @return Date String in format: yyyy/mm retrieved from source
     */
    private static String getYearMonth(String apacheAccessLogFileName) {
        String[] dateArr = apacheAccessLogFileName.replace("access_", "").split("-");
        return dateArr[0] + "/" + dateArr[1];
    }

    /**
     * Add argument values to the aggregated results in dbToAccessionToDateToFileName
     *
     * @param db
     * @param accession
     * @param period
     * @param fileName
     */

    private static void addToResults(ApacheLogsFileConfigProd.DB db, String accession, String period,
                                     String anonymisedIPAddress, String fileName) {
        if (!DB_TO_ACCESSION_TO_PERIOD_TO_ANONYMISED_IP_ADDRESS_TO_FILE_NAME.get(db).containsKey(accession)) {
            // We haven't seen this accession before
            Map<String, Map<String, Multiset<String>>> periodToAnonymisedIPAddressToFileNames = new HashMap<>();
            Map<String, Multiset<String>> anonymisedIPAddressToFileNames = new HashMap<>();
            // N.B. We use Multiset to maintain counts per individual download file
            anonymisedIPAddressToFileNames.put(anonymisedIPAddress, HashMultiset.<String>create());
            anonymisedIPAddressToFileNames.get(anonymisedIPAddress).add(fileName);
            periodToAnonymisedIPAddressToFileNames.put(period, anonymisedIPAddressToFileNames);
            DB_TO_ACCESSION_TO_PERIOD_TO_ANONYMISED_IP_ADDRESS_TO_FILE_NAME.get(db)
                    .put(accession, periodToAnonymisedIPAddressToFileNames);
        } else {
            // We've seen this accession before
            if (!DB_TO_ACCESSION_TO_PERIOD_TO_ANONYMISED_IP_ADDRESS_TO_FILE_NAME.get(db)
                    .get(accession).containsKey(period)) {
                // We haven't seen this period for this accession before
                Map<String, Multiset<String>> anonymisedIPAddressToFileNames = new HashMap<>();
                anonymisedIPAddressToFileNames.put(anonymisedIPAddress, HashMultiset.<String>create());
                DB_TO_ACCESSION_TO_PERIOD_TO_ANONYMISED_IP_ADDRESS_TO_FILE_NAME.get(db).get(accession)
                        .put(period, anonymisedIPAddressToFileNames);
            } else {
                // We have seen this period for this accession before
                if (!DB_TO_ACCESSION_TO_PERIOD_TO_ANONYMISED_IP_ADDRESS_TO_FILE_NAME.get(db).get(accession)
                        .get(period).containsKey(anonymisedIPAddress)) {
                    // We haven't seen this anonymisedIPAddress for that accession and period before
                    Map<String, Multiset<String>> anonymisedIPAddressToFileNames = new HashMap<>();
                    anonymisedIPAddressToFileNames.put(anonymisedIPAddress, HashMultiset.<String>create());
                    DB_TO_ACCESSION_TO_PERIOD_TO_ANONYMISED_IP_ADDRESS_TO_FILE_NAME.get(db)
                            .get(accession).put(period, anonymisedIPAddressToFileNames);
                }
            }
            DB_TO_ACCESSION_TO_PERIOD_TO_ANONYMISED_IP_ADDRESS_TO_FILE_NAME.get(db).get(accession)
                    .get(period).get(anonymisedIPAddress).add(fileName);
        }
    }

    /**
     * From: https://www.geeksforgeeks.org/md5-hash-in-java/
     *
     * @param input
     * @return MD5 hash value nputof i
     */
    public static String getMd5(String input) {
        try {

            // Static getInstance method is called with hashing MD5
            MessageDigest md = MessageDigest.getInstance("MD5");

            // digest() method is called to calculate message digest
            //  of an input digest() return array of byte
            byte[] messageDigest = md.digest(input.getBytes());

            // Convert byte array into signum representation
            BigInteger no = new BigInteger(1, messageDigest);

            // Convert message digest into hex value
            String hashtext = no.toString(16);
            while (hashtext.length() < 32) {
                hashtext = "0" + hashtext;
            }
            return hashtext;
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
