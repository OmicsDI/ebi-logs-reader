package uk.ac.ebi.ddi.downloas.utils;

import org.springframework.retry.backoff.ExponentialRandomBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.Collections;

public class RetryClient {

    private static final int RETRIES = 5;
    private RetryTemplate retryTemplate = new RetryTemplate();

    public RetryClient() {
        SimpleRetryPolicy policy =
                new SimpleRetryPolicy(RETRIES, Collections.singletonMap(Exception.class, true));
        retryTemplate.setRetryPolicy(policy);
        ExponentialRandomBackOffPolicy backOffPolicy = new ExponentialRandomBackOffPolicy();
        backOffPolicy.setInitialInterval(2000);
        backOffPolicy.setMultiplier(2.0);
        retryTemplate.setBackOffPolicy(backOffPolicy);
    }

    protected RetryTemplate getRetryTemplate() {
        return retryTemplate;
    }
}
