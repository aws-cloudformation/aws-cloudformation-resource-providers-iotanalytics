package com.amazonaws.iotanalytics.channel;

import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.services.iotanalytics.IoTAnalyticsClient;

class ClientBuilder {
    private static volatile IoTAnalyticsClient ioTAnalyticsClient;

    static IoTAnalyticsClient getClient() {
        if (ioTAnalyticsClient != null) {
            return ioTAnalyticsClient;
        }

        synchronized (ClientBuilder.class) {
            ioTAnalyticsClient = IoTAnalyticsClient.builder()
                    .overrideConfiguration(ClientOverrideConfiguration.builder()
                            .retryPolicy(RetryPolicy.builder().numRetries(3).build())
                            .build())
                    .build();
            return ioTAnalyticsClient;
        }
    }
}
