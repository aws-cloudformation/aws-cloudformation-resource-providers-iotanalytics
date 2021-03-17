package com.amazonaws.iotanalytics.pipeline;

import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import software.amazon.awssdk.services.iotanalytics.model.DescribePipelineRequest;
import software.amazon.awssdk.services.iotanalytics.model.DescribePipelineResponse;
import software.amazon.awssdk.services.iotanalytics.model.InternalFailureException;
import software.amazon.awssdk.services.iotanalytics.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.iotanalytics.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.iotanalytics.model.Pipeline;
import software.amazon.awssdk.services.iotanalytics.model.ResourceNotFoundException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.exceptions.CfnServiceInternalErrorException;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;

import static com.amazonaws.iotanalytics.pipeline.TestConstants.CFN_ADD_ATTR_ACTIVITY;
import static com.amazonaws.iotanalytics.pipeline.TestConstants.CFN_CHANNEL_ACTIVITY;
import static com.amazonaws.iotanalytics.pipeline.TestConstants.CFN_DATASTORE_ACTIVITY;
import static com.amazonaws.iotanalytics.pipeline.TestConstants.CFN_DEVICE_REGISTRY_ENRICH_ACTIVITY;
import static com.amazonaws.iotanalytics.pipeline.TestConstants.CFN_DEVICE_SHADOW_ACTIVITY;
import static com.amazonaws.iotanalytics.pipeline.TestConstants.CFN_FILTER_ACTIVITY;
import static com.amazonaws.iotanalytics.pipeline.TestConstants.CFN_LAMBDA_ACTIVITY;
import static com.amazonaws.iotanalytics.pipeline.TestConstants.CFN_MATH_ACTIVITY;
import static com.amazonaws.iotanalytics.pipeline.TestConstants.CFN_RM_ATTR_ACTIVITY;
import static com.amazonaws.iotanalytics.pipeline.TestConstants.CFN_SELECT_ATTR_ACTIVITY;
import static com.amazonaws.iotanalytics.pipeline.TestConstants.IOTA_ADD_ATTR_ACTIVITY;
import static com.amazonaws.iotanalytics.pipeline.TestConstants.IOTA_CHANNEL_ACTIVITY;
import static com.amazonaws.iotanalytics.pipeline.TestConstants.IOTA_DATASTORE_ACTIVITY;
import static com.amazonaws.iotanalytics.pipeline.TestConstants.IOTA_DEVICE_REGISTRY_ENRICH_ACTIVITY;
import static com.amazonaws.iotanalytics.pipeline.TestConstants.IOTA_DEVICE_SHADOW_ACTIVITY;
import static com.amazonaws.iotanalytics.pipeline.TestConstants.IOTA_FILTER_ACTIVITY;
import static com.amazonaws.iotanalytics.pipeline.TestConstants.IOTA_LAMBDA_ACTIVITY;
import static com.amazonaws.iotanalytics.pipeline.TestConstants.IOTA_MATH_ACTIVITY;
import static com.amazonaws.iotanalytics.pipeline.TestConstants.IOTA_RM_ATTR_ACTIVITY;
import static com.amazonaws.iotanalytics.pipeline.TestConstants.IOTA_SELECT_ATTR_ACTIVITY;
import static com.amazonaws.iotanalytics.pipeline.TestConstants.TEST_KEY1;
import static com.amazonaws.iotanalytics.pipeline.TestConstants.TEST_KEY2;
import static com.amazonaws.iotanalytics.pipeline.TestConstants.TEST_PIPELINE_ARN;
import static com.amazonaws.iotanalytics.pipeline.TestConstants.TEST_PIPELINE_ID;
import static com.amazonaws.iotanalytics.pipeline.TestConstants.TEST_PIPELINE_NAME;
import static com.amazonaws.iotanalytics.pipeline.TestConstants.TEST_VALUE1;
import static com.amazonaws.iotanalytics.pipeline.TestConstants.TEST_VALUE2;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ReadHandlerTest extends AbstractTestBase{
    @Captor
    private ArgumentCaptor<DescribePipelineRequest> describePipelineRequestArgumentCaptor;

    @Captor
    private ArgumentCaptor<ListTagsForResourceRequest> listTagsForResourceRequestArgumentCaptor;

    private ReadHandler handler;

    private DescribePipelineResponse describePipelineResponse;

    @BeforeEach
    public void setup() {
        super.setUp();
        handler = new ReadHandler();

        describePipelineResponse = DescribePipelineResponse
                .builder()
                .pipeline(
                        Pipeline
                                .builder()
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
                                .arn(TEST_PIPELINE_ARN)
                                .build())
                .build();
    }

    @Test
    public void GIVEN_iota_good_WHEN_call_handleRequest_THEN_return_success() {
        // GIVEN
        final ResourceModel model = ResourceModel.builder().pipelineName(TEST_PIPELINE_NAME).build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder()
                .tags(software.amazon.awssdk.services.iotanalytics.model.Tag.builder().key(TEST_KEY1).value(TEST_VALUE1).build(),
                        software.amazon.awssdk.services.iotanalytics.model.Tag.builder().key(TEST_KEY2).value(TEST_VALUE2).build())
                .build();

        when(proxyClient.client().describePipeline(describePipelineRequestArgumentCaptor.capture()))
                .thenReturn(describePipelineResponse);
        when(proxyClient.client().listTagsForResource(listTagsForResourceRequestArgumentCaptor.capture())).thenReturn(listTagsForResourceResponse);

        // WHEN
        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        // THEN
        assertThat(describePipelineRequestArgumentCaptor.getValue().pipelineName()).isEqualTo(TEST_PIPELINE_NAME);
        assertThat(listTagsForResourceRequestArgumentCaptor.getValue().resourceArn()).isEqualTo(TEST_PIPELINE_ARN);
        verify(proxyClient.client(), times(1)).describePipeline(any(DescribePipelineRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        assertThat(response.getResourceModel().getPipelineName()).isEqualTo(TEST_PIPELINE_NAME);
        assertThat(response.getResourceModel().getId()).isEqualTo(TEST_PIPELINE_ID);
        assertThat(response.getResourceModel().getPipelineActivities()).isEqualTo(Arrays.asList(
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
        ));

        assertThat(response.getResourceModel().getTags().size()).isEqualTo(2);
        assertThat(response.getResourceModel().getTags().get(0).getKey()).isEqualTo(TEST_KEY1);
        assertThat(response.getResourceModel().getTags().get(0).getValue()).isEqualTo(TEST_VALUE1);
        assertThat(response.getResourceModel().getTags().get(1).getKey()).isEqualTo(TEST_KEY2);
        assertThat(response.getResourceModel().getTags().get(1).getValue()).isEqualTo(TEST_VALUE2);
    }

    @Test
    public void GIVEN_iota_describe_exception_WHEN_call_handleRequest_THEN_throw_exception() {
        // GIVEN
        final ResourceModel model = ResourceModel.builder().pipelineName(TEST_PIPELINE_NAME).build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final ResourceNotFoundException resourceNotFoundException = ResourceNotFoundException.builder().build();
        when(proxyClient.client().describePipeline(describePipelineRequestArgumentCaptor.capture())).thenThrow(resourceNotFoundException);

        // WHEN / THEN
        assertThrows(CfnNotFoundException.class,
                () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));
        assertThat(describePipelineRequestArgumentCaptor.getValue().pipelineName()).isEqualTo(TEST_PIPELINE_NAME);
        verify(proxyClient.client(), never()).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void GIVEN_iota_list_tags_exception_WHEN_call_handleRequest_THEN_throw_exception() {
        // GIVEN
        final ResourceModel model = ResourceModel.builder().pipelineName(TEST_PIPELINE_NAME).build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        when(proxyClient.client().describePipeline(describePipelineRequestArgumentCaptor.capture())).thenReturn(describePipelineResponse);

        when(proxyClient.client().listTagsForResource(listTagsForResourceRequestArgumentCaptor.capture())).thenThrow(InternalFailureException.builder().build());

        // WHEN / THEN
        assertThrows(CfnServiceInternalErrorException.class,
                () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));
        assertThat(describePipelineRequestArgumentCaptor.getValue().pipelineName()).isEqualTo(TEST_PIPELINE_NAME);
        assertThat(listTagsForResourceRequestArgumentCaptor.getValue().resourceArn()).isEqualTo(TEST_PIPELINE_ARN);

        verify(proxyClient.client(), times(1)).describePipeline(any(DescribePipelineRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
    }
}
