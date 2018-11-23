package uk.ac.ebi.ddi.downloas.logs;

import com.google.common.base.Joiner;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author datasome
 *         This class contains configuration for accessing file downloads information in Apache access log files, for example for BioModels
 */

public class ApacheLogsFileConfigProd {
    public static final String LOGFILENAME_PREFIX = "access_";
    public static final String LOGFILENAME_POSTFIX = ".*\\.log";

    public enum DB {
        BioModels
    }

    public enum FIELD {
        Regex, LogsDir
    }

    public static final Map<DB, Map<FIELD, String>> db2Regex = new HashMap<DB, Map<FIELD, String>>() {
        {
            // Initialise all sub-maps
            for (DB db : DB.values()) {
                put(db, new HashMap<>());
            }

            get(DB.BioModels).put(FIELD.Regex, "download\\/MODEL\\d+.*?\\&");
            get(DB.BioModels).put(FIELD.LogsDir, File.separator + Joiner.on(File.separator).join(Arrays.asList("nfs","public","rw","webadmin","tomcat","bases","biomodels.net","tc-pst-biomodels_jummp_staging","logs","ves-hx-4b")));
        }
    };


}
