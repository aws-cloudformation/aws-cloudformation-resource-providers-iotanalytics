package com.amazonaws.iotanalytics.pipeline;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.iotanalytics.model.AddAttributesActivity;
import software.amazon.awssdk.services.iotanalytics.model.ChannelActivity;
import software.amazon.awssdk.services.iotanalytics.model.CreatePipelineRequest;
import software.amazon.awssdk.services.iotanalytics.model.DatastoreActivity;
import software.amazon.awssdk.services.iotanalytics.model.IoTAnalyticsException;
import software.amazon.awssdk.services.iotanalytics.model.PipelineActivity;
import software.amazon.awssdk.services.iotanalytics.model.UpdatePipelineRequest;
import software.amazon.cloudformation.exceptions.CfnAccessDeniedException;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class TranslatorTest {
    @Test
    public void GIVEN_AccessDeniedException_WHEN_call_translate_exception_THEN_return_CfnAccessDeniedException() {
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
    public void GIVEN_null_list_WHEN_call_translateToCreatePipelineRequest_THEN_return_empty_list() {
        // GIVEN
        final ResourceModel model = ResourceModel.builder().build();

        // WHEN
        final CreatePipelineRequest request = Translator.translateToCreatePipelineRequest(model);

        // THEN
        assertThat(request).isNotNull();
        assertThat(request.pipelineActivities()).isNotNull().hasSize(0);
    }

    @Test
    public void GIVEN_null_list_WHEN_call_translateToUpdatePipelineRequest_THEN_return_empty_list() {
        // GIVEN
        final ResourceModel model = ResourceModel.builder().build();

        // WHEN
        final UpdatePipelineRequest request = Translator.translateToUpdatePipelineRequest(model);

        // THEN
        assertThat(request).isNotNull();
        assertThat(request.pipelineActivities()).isNotNull().hasSize(0);
    }

    @Test
    public void GIVEN_legacy_cfn_activities_WHEN_call_translateToCreatePipelineRequest_THEN_return_valid_list() {
        // GIVEN
        final Channel channel = Channel.builder().build();
        final Datastore datastore = Datastore.builder().build();
        final AddAttributes addAttributes = AddAttributes.builder().build();
        final ResourceModel model = ResourceModel.builder()
                .pipelineActivities(Arrays.asList(Activity.builder()
                                .channel(channel)
                                .datastore(datastore)
                                .build(),
                        Activity.builder()
                                .addAttributes(addAttributes)
                                .build()
                ))
                .build();

        // WHEN
        final CreatePipelineRequest request = Translator.translateToCreatePipelineRequest(model);

        // THEN
        assertThat(request).isNotNull();
        assertThat(request.pipelineActivities()).isNotNull()
                .hasSize(3)
                .hasSameElementsAs(
                        Arrays.asList(
                                PipelineActivity.builder()
                                        .channel(ChannelActivity.builder().build())
                                        .build(),
                                PipelineActivity.builder()
                                        .addAttributes(AddAttributesActivity.builder().build())
                                        .build(),
                                PipelineActivity.builder()
                                        .datastore(DatastoreActivity.builder().build())
                                        .build()
                        )
                );
    }

    @Test
    public void GIVEN_legacy_cfn_activities_WHEN_call_translateToUpdatePipelineRequest_THEN_return_valid_list() {
        // GIVEN
        final Channel channel = Channel.builder().build();
        final Datastore datastore = Datastore.builder().build();
        final AddAttributes addAttributes = AddAttributes.builder().build();
        final ResourceModel model = ResourceModel.builder()
                .pipelineActivities(Arrays.asList(Activity.builder()
                                .channel(channel)
                                .datastore(datastore)
                                .build(),
                        Activity.builder()
                                .addAttributes(addAttributes)
                                .build()
                ))
                .build();

        // WHEN
        final UpdatePipelineRequest request = Translator.translateToUpdatePipelineRequest(model);

        // THEN
        assertThat(request).isNotNull();
        assertThat(request.pipelineActivities()).isNotNull()
                .hasSize(3)
                .hasSameElementsAs(Arrays.asList(
                        PipelineActivity.builder()
                                .channel(ChannelActivity.builder().build())
                                .build(),
                        PipelineActivity.builder()
                                .addAttributes(AddAttributesActivity.builder().build())
                                .build(),
                        PipelineActivity.builder()
                                .datastore(DatastoreActivity.builder().build())
                                .build()
                ));
    }
}
