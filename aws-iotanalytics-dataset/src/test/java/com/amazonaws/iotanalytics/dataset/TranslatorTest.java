package com.amazonaws.iotanalytics.dataset;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.iotanalytics.model.IoTAnalyticsException;
import software.amazon.cloudformation.exceptions.CfnAccessDeniedException;
import software.amazon.cloudformation.exceptions.CfnServiceInternalErrorException;

import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;

public class TranslatorTest {
    @Test
    public void GIVE_AccessDeniedException_WHEN_call_translate_exception_THEN_return_CfnAccessDeniedException() {
        final IoTAnalyticsException e = (IoTAnalyticsException) IoTAnalyticsException
                .builder()
                .awsErrorDetails(AwsErrorDetails
                        .builder()
                        .errorCode("AccessDeniedException")
                        .errorMessage("test_message")
                        .serviceName("test_service")
                        .build())
                .build();
        assertThat(Translator.translateExceptionToHandlerException(
                e,
                "operation",
                "name") instanceof CfnAccessDeniedException).isTrue();
    }

    @Test
    public void GIVE_NonAccessDeniedException_WHEN_call_translate_exception_THEN_return_CfnServiceInternalErrorException() {
        final IoTAnalyticsException e = (IoTAnalyticsException) IoTAnalyticsException
                .builder()
                .awsErrorDetails(AwsErrorDetails
                        .builder()
                        .errorCode("some_code")
                        .errorMessage("test_message")
                        .serviceName("test_service")
                        .build())
                .build();
        assertThat(Translator.translateExceptionToHandlerException(
                e,
                "operation",
                "name") instanceof CfnServiceInternalErrorException).isTrue();
    }

    @Test
    public void GIVEN_null_filter_WHEN_call_translateFilters_THEN_return_null() {
        assertThat(Translator.translateFiltersFromCfn(null)).isNull();
        assertThat(Translator.translateFiltersToCfn(null)).isNull();
    }

    @Test
    public void GIVEN_null_filter_WHEN_call_translateVariables_THEN_return_null() {
        assertThat(Translator.translateVariablesFromCfn(null)).isNull();
        assertThat(Translator.translateVariablesToCfn(null)).isNull();
    }

    @Test
    public void GIVEN_null_rule_WHEN_call_translateDatasetContentDeliveryRule_THEN_return_null() {
        assertThat(Translator.translateDatasetContentDeliveryRuleToCfn(null)).isNull();
        assertThat(Translator.translateDatasetContentDeliveryRuleFromCfn(null)).isNull();
    }

    @Test
    public void GIVEN_null_rule_WHEN_call_translateLateDateRules_THEN_return_null() {
        assertThat(Translator.translateLateDateRulesToCfn(null)).isNull();
        assertThat(Translator.translateLateDateRulesFromCfn(null)).isNull();
    }

    @Test
    public void GIVEN_null_trigger_WHEN_call_translateTriggers_THEN_return_null() {
        assertThat(Translator.translateTriggersToCfn(null)).isNull();
        assertThat(Translator.translateTriggersFromCfn(null)).isNull();
    }

    @Test
    public void GIVEN_empty_WHEN_call_translateTags_THEN_return_empty() {
        assertThat(Translator.translateTagsToCfn(null)).isNull();
        assertThat(Translator.translateTagsToCfn(new ArrayList<>())).isNull();

    }
}
