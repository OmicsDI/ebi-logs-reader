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


    @Test
    public void displayResults() {
        Map<ElasticSearchWsConfigProd.DB, Map<String, Map<String, Multiset<String>>>> dbToAccessionToPeriodToFileName = elasticSearchInstance.getResults(100, 100, 1000, LocalDate.now());

        for (ElasticSearchWsConfigProd.DB db : dbToAccessionToPeriodToFileName.keySet()) {
            for (String accession : dbToAccessionToPeriodToFileName.get(db).keySet()) {
                for (String period : dbToAccessionToPeriodToFileName.get(db).get(accession).keySet()) {
                    for (Multiset.Entry entry : dbToAccessionToPeriodToFileName.get(db).get(accession).get(period).entrySet()) {
                        log.info(db.toString() + "\t" + accession + "\t" + period + "\t" + entry.getElement() + "\t" + entry.getCount());
                    }

                }
            }
        }
    }
}