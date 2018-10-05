package uk.ac.ebi.ddi.downloas.ena;

import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;


/**
 * Client superclass to query ENA API to retrieve a project accession for a given ENA (non-project) accession
 *
 * @author datasome
 */

public class WsClient {

    protected RestTemplate restTemplate;
    protected ENAWsConfigProd config;

    /**
     * Default constructor for Archive clients
     * @param config
     */
    public WsClient(ENAWsConfigProd config){
        this.config = config;
        this.restTemplate = new RestTemplate(clientHttpRequestFactory());
    }

    private ClientHttpRequestFactory clientHttpRequestFactory() {
        HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory();
        factory.setReadTimeout(30000); // 30 s
        factory.setConnectTimeout(2000); // 2 s
        return factory;
    }

    public RestTemplate getRestTemplate() {
        return restTemplate;
    }

    public void setRestTemplate(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    public ENAWsConfigProd getConfig() {
        return config;
    }

    public void setConfig(ENAWsConfigProd config) {
        this.config = config;
    }
}
