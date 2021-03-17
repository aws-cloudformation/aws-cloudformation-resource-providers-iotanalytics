package com.amazonaws.iotanalytics.datastore;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ClientBuilderTest {
    @Test
    public void testClientBuilder() {
        assertThat(ClientBuilder.getClient()).isSameAs(ClientBuilder.getClient());
    }
}
