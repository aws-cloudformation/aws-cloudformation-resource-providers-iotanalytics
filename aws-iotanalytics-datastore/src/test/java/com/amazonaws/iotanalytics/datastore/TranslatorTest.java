package com.amazonaws.iotanalytics.datastore;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;


public class TranslatorTest {
    @Test
    public void testSchemaDefinitionTranslation() {
        assertThat(Translator.translateSchemaDefinitionToCfn(null)).isNull();
        assertThat(Translator.translateSchemaDefinitionFromCfn(null)).isNull();
    }
}
