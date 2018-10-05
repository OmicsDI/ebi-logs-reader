
This library provides easy computation of all the downloads per file for each EBI resource.

How to use it:
--------------

The name of the databases can be found here:


| Database Name    | Database URL                 |
|------------------|:----------------------------:|
|  Pride           | www.ebi.ac.uk/pride          |
|  ExpressionAtlas | www.ebi.ac.uk/gxa            |
|  ArrayExpress    | www.ebi.ac.uk/arrayexpress   |
|  EVA             | www.ebi.ac.uk/eva            |
| Metabolights     | www.ebi.ac.uk/metabolights   |
| ENA              | www.ebi.ac.uk/ena            |


Code:

```java

    ElasticSearchWsClient elasticSearchClient = new ElasticSearchWsClient(new ElasticSearchWsConfigProd(port,machine,user, port));

    elasticSearchClient.setParallel(true);
            Map<String, Map<String, Multiset<String>>> prideDownloads =
                    elasticSearchClient.getDataDownloads(ElasticSearchWsConfigProd.DB.Pride, "PXD000533", LocalDate.now());
            Assert.assertTrue(prideDownloads.size() > 0);
        }

```

Be aware that this needs more than 12G memory.



