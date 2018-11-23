package uk.ac.ebi.ddi.downloas.logs;

import com.google.common.collect.Multiset;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 *
 * @author datasome
 */
public class ApacheLogsFileClientTest {

    ApacheLogsFileClient apacheFileLogsClient = new ApacheLogsFileClient(new ApacheLogsFileConfigProd());

    @Test
    public void getDataDownloads() {
        Map<String, Map<String, Multiset<String>>> bioModelsDownloads =
                apacheFileLogsClient.getDataDownloads(ApacheLogsFileConfigProd.DB.BioModels, "MODEL1402200000");
        Assert.assertTrue(bioModelsDownloads.size() > 0);

    }
}