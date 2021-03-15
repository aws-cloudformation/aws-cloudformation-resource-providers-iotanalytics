package com.amazonaws.iotanalytics.datastore;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ConfigurationTest {
    @Test
    public void testConfig() {
        final Configuration configuration = new Configuration();
        assertThat(configuration.schemaFilename).isEqualTo("aws-iotanalytics-datastore.json");
    }
}
