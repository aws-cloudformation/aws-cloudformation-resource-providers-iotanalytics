package com.amazonaws.iotanalytics.dataset;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.iotanalytics.IoTAnalyticsClient;

class ClientBuilder {
    private static volatile IoTAnalyticsClient ioTAnalyticsClient;

    private ClientBuilder() {}

    static IoTAnalyticsClient getClient() {
        if (ioTAnalyticsClient != null) {
            return ioTAnalyticsClient;
        }

        synchronized (ClientBuilder.class) {
            final Region region = Region.of(getEnvironmentValue("AWS_REGION", "us-west-2"));
            ioTAnalyticsClient = IoTAnalyticsClient.builder().region(region)
                    .overrideConfiguration(ClientOverrideConfiguration.builder()
                            .retryPolicy(RetryPolicy.builder().numRetries(3).build())
                            .apiCallTimeout(Duration.ofSeconds(60L))
                            .build())
                    .build();
            return ioTAnalyticsClient;
        }
    }

    private static String getEnvironmentValue(final String environmentVariable, final String defaultValue) {
        final String value = System.getenv(environmentVariable);
        return StringUtils.isNullOrEmpty(value) ? defaultValue : value;
    }
}
