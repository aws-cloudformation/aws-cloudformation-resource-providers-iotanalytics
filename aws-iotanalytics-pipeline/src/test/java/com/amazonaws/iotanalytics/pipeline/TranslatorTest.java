package com.amazonaws.iotanalytics.pipeline;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.iotanalytics.model.IoTAnalyticsException;
import software.amazon.cloudformation.exceptions.CfnAccessDeniedException;

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
}
