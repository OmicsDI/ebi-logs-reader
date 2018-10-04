package uk.ac.ebi.ddi.downloas.logs;

import com.google.common.collect.Multiset;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.time.LocalDate;
import java.util.Map;


/**
 * This code is licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * ==Overview==
 *
 * @author ypriverol on 04/10/2018.
 */
public class ElasticSearchTest {

    private static final org.apache.log4j.Logger log = Logger.getLogger(ElasticSearchTest.class);

    private static final ElasticSearchService  elasticSearchInstance = ElasticSearchService.getInstance();
    private static Map<ElasticSearchWsConfigProd.DB, Map<String, Map<String, Map<String, Multiset<String>>>>> dbToAccessionToPeriodToAnonymisedIPAddressToFileName;

    @Test
    public void displayResults() {
        dbToAccessionToPeriodToAnonymisedIPAddressToFileName = elasticSearchInstance.getResults(100,100,1000, LocalDate.now());

        for (ElasticSearchWsConfigProd.DB db : dbToAccessionToPeriodToAnonymisedIPAddressToFileName.keySet()) {
            for (String accession : dbToAccessionToPeriodToAnonymisedIPAddressToFileName.get(db).keySet()) {
                for (String period : dbToAccessionToPeriodToAnonymisedIPAddressToFileName.get(db).get(accession).keySet()) {
                    for (String anonymisedIPAddress : dbToAccessionToPeriodToAnonymisedIPAddressToFileName.get(db).get(accession).get(period).keySet()) {
                        for (Multiset.Entry entry : dbToAccessionToPeriodToAnonymisedIPAddressToFileName.get(db).get(accession).get(period).get(anonymisedIPAddress).entrySet()) {
                            System.out.println(db.toString() + "\t" + accession + "\t" + period + "\t" + anonymisedIPAddress + "\t" + entry.getElement() + "\t" + entry.getCount());
                        }
                    }
                }
            }
        }
    }
}