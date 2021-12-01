package com.amazonaws.iotanalytics.pipeline;

import org.mockito.ArgumentCaptor;
import org.mockito.Captor;

import software.amazon.awssdk.services.iotanalytics.model.CreatePipelineRequest;
import software.amazon.awssdk.services.iotanalytics.model.CreatePipelineResponse;

import software.amazon.awssdk.services.iotanalytics.model.DescribePipelineRequest;
import software.amazon.awssdk.services.iotanalytics.model.DescribePipelineResponse;
import software.amazon.awssdk.services.iotanalytics.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.iotanalytics.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.iotanalytics.model.Pipeline;
import software.amazon.awssdk.services.iotanalytics.model.ResourceAlreadyExistsException;
import software.amazon.cloudformation.exceptions.CfnAlreadyExistsException;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;

import static com.amazonaws.iotanalytics.pipeline.TestConstants.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CreateHandlerTest extends AbstractTestBase {
    private static final String TEST_LOGICAL_RESOURCE_IDENTIFIER = "test_logical_resource_identifier";
    private static final String TEST_CLIENT_REQUEST_TOKEN = "test_client_request_token";

    private CreateHandler handler;

    @Captor
    private ArgumentCaptor<CreatePipelineRequest> createPipelineRequestArgumentCaptor;

    @BeforeEach
    public void setup() {
        super.setUp();
        handler = new CreateHandler();
    }

    @Test
    public void GIVEN_request_WHEN_call_handleRequest_THEN_return_success() {
        // GIVEN
        final ResourceModel model = ResourceModel.builder()
                .pipelineName(TEST_PIPELINE_NAME)
                .pipelineActivities(Arrays.asList(
                        CFN_CHANNEL_ACTIVITY,
                        CFN_ADD_ATTR_ACTIVITY,
                        CFN_RM_ATTR_ACTIVITY,
                        CFN_SELECT_ATTR_ACTIVITY,
                        CFN_DEVICE_REGISTRY_ENRICH_ACTIVITY,
                        CFN_DEVICE_SHADOW_ACTIVITY,
                        CFN_FILTER_ACTIVITY,
                        CFN_MATH_ACTIVITY,
                        CFN_LAMBDA_ACTIVITY,
                        CFN_DATASTORE_ACTIVITY
                ))
                .tags(Arrays.asList(Tag.builder().key(TEST_KEY1).value(TEST_VALUE1).build(),
                        Tag.builder().key(TEST_KEY2).value(TEST_VALUE2).build()))
                .build();

        final CreatePipelineResponse createPipelineResponse = CreatePipelineResponse.builder().build();

        final DescribePipelineResponse describePipelineResponse = DescribePipelineResponse.builder()
                .pipeline(Pipeline
                        .builder()
                        .arn(TEST_PIPELINE_ARN)
                        .name(TEST_PIPELINE_NAME)
                        .activities(
                                IOTA_CHANNEL_ACTIVITY,
                                IOTA_ADD_ATTR_ACTIVITY,
                                IOTA_RM_ATTR_ACTIVITY,
                                IOTA_SELECT_ATTR_ACTIVITY,
                                IOTA_DEVICE_REGISTRY_ENRICH_ACTIVITY,
                                IOTA_DEVICE_SHADOW_ACTIVITY,
                                IOTA_FILTER_ACTIVITY,
                                IOTA_MATH_ACTIVITY,
                                IOTA_LAMBDA_ACTIVITY,
                                IOTA_DATASTORE_ACTIVITY
                        )
                        .build())
                .build();

        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder()
                .tags(software.amazon.awssdk.services.iotanalytics.model.Tag.builder().key(TEST_KEY1).value(TEST_VALUE1).build(),
                        software.amazon.awssdk.services.iotanalytics.model.Tag.builder().key(TEST_KEY2).value(TEST_VALUE2).build())
                .build();

        when(proxyClient.client().createPipeline(createPipelineRequestArgumentCaptor.capture())).thenReturn(createPipelineResponse);
        when(proxyClient.client().describePipeline(any(DescribePipelineRequest.class))).thenReturn(describePipelineResponse);
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class))).thenReturn(listTagsForResourceResponse);

        // WHEN
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        // THEN
        verify(proxyClient.client(), times(1)).createPipeline(any(CreatePipelineRequest.class));

        final CreatePipelineRequest createPipelineRequest = createPipelineRequestArgumentCaptor.getValue();
        assertThat(createPipelineRequest.pipelineName()).isEqualTo(TEST_PIPELINE_NAME);
        assertThat(createPipelineRequest.pipelineActivities()).isEqualTo(Arrays.asList(
                IOTA_CHANNEL_ACTIVITY,
                IOTA_ADD_ATTR_ACTIVITY,
                IOTA_RM_ATTR_ACTIVITY,
                IOTA_SELECT_ATTR_ACTIVITY,
                IOTA_DEVICE_REGISTRY_ENRICH_ACTIVITY,
                IOTA_DEVICE_SHADOW_ACTIVITY,
                IOTA_FILTER_ACTIVITY,
                IOTA_MATH_ACTIVITY,
                IOTA_LAMBDA_ACTIVITY,
                IOTA_DATASTORE_ACTIVITY
        ));

        assertThat(createPipelineRequest.tags().size()).isEqualTo(2);
        assertThat(createPipelineRequest.tags().get(0).key()).isEqualTo(TEST_KEY1);
        assertThat(createPipelineRequest.tags().get(0).value()).isEqualTo(TEST_VALUE1);
        assertThat(createPipelineRequest.tags().get(1).key()).isEqualTo(TEST_KEY2);
        assertThat(createPipelineRequest.tags().get(1).value()).isEqualTo(TEST_VALUE2);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        assertThat(response.getResourceModel().getPipelineName()).isEqualTo(request.getDesiredResourceState().getPipelineName());
        assertThat(response.getResourceModel().getPipelineActivities()).isEqualTo(request.getDesiredResourceState().getPipelineActivities());
        assertThat(response.getResourceModel().getTags()).isEqualTo(request.getDesiredResourceState().getTags());
        assertThat(response.getResourceModel().getId()).isEqualTo(TEST_PIPELINE_ID);
    }

    @Test
    public void GIVEN_request_with_id_WHEN_call_handleRequest_THEN_return_InvalidRequest() {
        // GIVEN
        final ResourceModel model = ResourceModel.builder()
                .id(TEST_PIPELINE_ID)
                .pipelineActivities(Arrays.asList(CFN_CHANNEL_TO_DATASTORE_ACTIVITY, CFN_DATASTORE_ACTIVITY))
                .pipelineName(TEST_PIPELINE_NAME)
                .build();

        // WHEN
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        // THEN
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isEqualTo("Id is a read-only property and cannot be set.");
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.InvalidRequest);
    }

    @Test
    public void GIVEN_iota_already_exist_WHEN_call_handleRequest_THEN_return_AlreadyExists() {
        // GIVEN
        final ResourceModel model = ResourceModel.builder()
                .pipelineActivities(Arrays.asList(CFN_CHANNEL_TO_DATASTORE_ACTIVITY, CFN_DATASTORE_ACTIVITY))
                .pipelineName(TEST_PIPELINE_NAME).build();

        when(proxyClient.client().createPipeline(createPipelineRequestArgumentCaptor.capture()))
                .thenThrow(ResourceAlreadyExistsException.builder().message("already exist").build());

        // WHEN / THEN
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        assertThrows(CfnAlreadyExistsException.class,
                () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));
        assertThat(createPipelineRequestArgumentCaptor.getValue().pipelineName()).isEqualTo(TEST_PIPELINE_NAME);
    }

    @Test
    public void GIVEN_missing_pipelineName_WHEN_call_handleRequest_THEN_return_success_with_generated_pipelineName() {
        // GIVEN
        final ResourceModel model = ResourceModel.builder()
                .pipelineActivities(Arrays.asList(
                        CFN_CHANNEL_ACTIVITY,
                        CFN_ADD_ATTR_ACTIVITY,
                        CFN_RM_ATTR_ACTIVITY,
                        CFN_SELECT_ATTR_ACTIVITY,
                        CFN_DEVICE_REGISTRY_ENRICH_ACTIVITY,
                        CFN_DEVICE_SHADOW_ACTIVITY,
                        CFN_FILTER_ACTIVITY,
                        CFN_MATH_ACTIVITY,
                        CFN_LAMBDA_ACTIVITY,
                        CFN_DATASTORE_ACTIVITY
                ))
                .build();

        final CreatePipelineResponse createPipelineResponse = CreatePipelineResponse.builder()
                .build();
        when(proxyClient.client().createPipeline(createPipelineRequestArgumentCaptor.capture())).thenReturn(createPipelineResponse);

        // WHEN
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .logicalResourceIdentifier(TEST_LOGICAL_RESOURCE_IDENTIFIER)
                .clientRequestToken(TEST_CLIENT_REQUEST_TOKEN)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        // THEN
        verify(proxyClient.client(), times(1)).createPipeline(any(CreatePipelineRequest.class));
        final CreatePipelineRequest createPipelineRequest = createPipelineRequestArgumentCaptor.getValue();
        assertThat(createPipelineRequest.pipelineName()).isNotBlank();
        assertFalse(createPipelineRequest.pipelineName().contains("-"));
    }
}
