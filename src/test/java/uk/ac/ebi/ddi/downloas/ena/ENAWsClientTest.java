package uk.ac.ebi.ddi.downloas.ena;

import com.google.common.collect.Multiset;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import uk.ac.ebi.ddi.downloas.ena.ENAWsClient;
import uk.ac.ebi.ddi.downloas.ena.ENAWsConfigProd;

import java.time.LocalDate;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * This code is licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * ==Overview==
 *
 * @author datasome on 04/10/2018.
 */
public class ENAWsClientTest {

    ENAWsClient enaWsClient = new ENAWsClient(new ENAWsConfigProd());

    @Test
    public void getProjectAccessions() {
        Assert.assertTrue(enaWsClient.getProjectAccession("ERR975925").equals("PRJEB4848")); // Run
        Assert.assertTrue(enaWsClient.getProjectAccession("ERZ675274").equals("PRJNA183850")); // Analysis
        Assert.assertTrue(enaWsClient.getProjectAccession("GCA_000001735").equals("PRJNA10719")); // Assembly
        Assert.assertTrue(enaWsClient.getProjectAccession("ERA1502191").equals("PRJEB6403")); // Submission
        Assert.assertTrue(enaWsClient.getProjectAccession("AFTI01").equals("PRJNA70283"));     // Sequence
    }

}