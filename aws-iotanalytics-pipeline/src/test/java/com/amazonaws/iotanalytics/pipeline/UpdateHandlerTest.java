package com.amazonaws.iotanalytics.pipeline;

import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import software.amazon.awssdk.services.iotanalytics.model.DescribePipelineRequest;
import software.amazon.awssdk.services.iotanalytics.model.DescribePipelineResponse;
import software.amazon.awssdk.services.iotanalytics.model.InvalidRequestException;
import software.amazon.awssdk.services.iotanalytics.model.LimitExceededException;
import software.amazon.awssdk.services.iotanalytics.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.iotanalytics.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.iotanalytics.model.Pipeline;
import software.amazon.awssdk.services.iotanalytics.model.ServiceUnavailableException;
import software.amazon.awssdk.services.iotanalytics.model.TagResourceRequest;
import software.amazon.awssdk.services.iotanalytics.model.TagResourceResponse;
import software.amazon.awssdk.services.iotanalytics.model.UntagResourceRequest;
import software.amazon.awssdk.services.iotanalytics.model.UntagResourceResponse;
import software.amazon.awssdk.services.iotanalytics.model.UpdatePipelineRequest;
import software.amazon.awssdk.services.iotanalytics.model.UpdatePipelineResponse;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnNotUpdatableException;
import software.amazon.cloudformation.exceptions.CfnServiceLimitExceededException;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static com.amazonaws.iotanalytics.pipeline.TestConstants.CFN_ADD_ATTR_ACTIVITY;
import static com.amazonaws.iotanalytics.pipeline.TestConstants.CFN_CHANNEL_ACTIVITY;
import static com.amazonaws.iotanalytics.pipeline.TestConstants.CFN_CHANNEL_TO_DATASTORE_ACTIVITY;
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
import static com.amazonaws.iotanalytics.pipeline.TestConstants.IOTA_CHANNEL_TO_DATASTORE_ACTIVITY;
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
import static com.amazonaws.iotanalytics.pipeline.TestConstants.TEST_KEY3;
import static com.amazonaws.iotanalytics.pipeline.TestConstants.TEST_PIPELINE_ARN;
import static com.amazonaws.iotanalytics.pipeline.TestConstants.TEST_PIPELINE_ID;
import static com.amazonaws.iotanalytics.pipeline.TestConstants.TEST_PIPELINE_NAME;
import static com.amazonaws.iotanalytics.pipeline.TestConstants.TEST_VALUE1;
import static com.amazonaws.iotanalytics.pipeline.TestConstants.TEST_VALUE2;
import static com.amazonaws.iotanalytics.pipeline.TestConstants.TEST_VALUE22;
import static com.amazonaws.iotanalytics.pipeline.TestConstants.TEST_VALUE3;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class UpdateHandlerTest extends AbstractTestBase {

    @Captor
    private ArgumentCaptor<UpdatePipelineRequest> updatePipelineRequestArgumentCaptor;

    @Captor
    private ArgumentCaptor<TagResourceRequest> tagResourceRequestArgumentCaptor;

    @Captor
    private ArgumentCaptor<UntagResourceRequest> untagResourceRequestArgumentCaptor;

    @Captor
    private ArgumentCaptor<DescribePipelineRequest> describePipelineRequestArgumentCaptor;

    @Captor
    private ArgumentCaptor<ListTagsForResourceRequest> listTagsForResourceRequestArgumentCaptor;

    private UpdateHandler handler;
    private ResourceModel preModel;
    private ResourceModel newModel;
    private DescribePipelineResponse describePipelineResponseFull;
    private ListTagsForResourceResponse listTagsForResourceResponseFull;
    private DescribePipelineResponse describePipelineResponseSimple;
    private ListTagsForResourceResponse listTagsForResourceResponseSimple;

    @BeforeEach
    public void setup() {
        super.setUp();
        handler = new UpdateHandler();

        preModel = ResourceModel.builder()
                .pipelineName(TEST_PIPELINE_NAME)
                .id(TEST_PIPELINE_ID)
                .pipelineActivities(Arrays.asList(
                        CFN_CHANNEL_TO_DATASTORE_ACTIVITY,
                        CFN_DATASTORE_ACTIVITY
                ))
                .tags(Arrays.asList(Tag.builder().key(TEST_KEY1).value(TEST_VALUE1).build(),
                        Tag.builder().key(TEST_KEY2).value(TEST_VALUE2).build()))
                .build();

        newModel = ResourceModel.builder()
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
                .tags(Arrays.asList(Tag.builder().key(TEST_KEY3).value(TEST_VALUE3).build(),
                        Tag.builder().key(TEST_KEY2).value(TEST_VALUE22).build()))
                .build();

        describePipelineResponseFull = DescribePipelineResponse.builder().pipeline(
                software.amazon.awssdk.services.iotanalytics.model.Pipeline.builder()
                        .name(TEST_PIPELINE_NAME)
                        .arn(TEST_PIPELINE_ARN)
                        .activities(Arrays.asList(
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
                        ))
                        .build())
                .build();

        listTagsForResourceResponseFull = ListTagsForResourceResponse.builder()
                .tags(Arrays.asList(software.amazon.awssdk.services.iotanalytics.model.Tag.builder().key(TEST_KEY1).value(TEST_VALUE1).build(),
                        software.amazon.awssdk.services.iotanalytics.model.Tag.builder().key(TEST_KEY2).value(TEST_VALUE22).build(),
                        software.amazon.awssdk.services.iotanalytics.model.Tag.builder().key(TEST_KEY3).value(TEST_VALUE3).build()))
                .build();

        describePipelineResponseSimple = DescribePipelineResponse.builder()
                .pipeline(Pipeline.builder()
                        .name(TEST_PIPELINE_NAME)
                        .activities(Arrays.asList(
                                IOTA_CHANNEL_TO_DATASTORE_ACTIVITY,
                                IOTA_DATASTORE_ACTIVITY
                        ))
                        .build())
                .build();

        listTagsForResourceResponseSimple = ListTagsForResourceResponse.builder().build();
    }

    @Test
    public void GIVEN_request_WHEN_call_handleRequest_THEN_return_success() {

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(newModel)
                .previousResourceState(preModel)
                .build();

        when(proxyClient.client().updatePipeline(updatePipelineRequestArgumentCaptor.capture()))
                .thenReturn(UpdatePipelineResponse.builder().build());

        when(proxyClient.client().untagResource(untagResourceRequestArgumentCaptor.capture()))
                .thenReturn(UntagResourceResponse.builder().build());

        when(proxyClient.client().tagResource(tagResourceRequestArgumentCaptor.capture()))
                .thenReturn(TagResourceResponse.builder().build());

        when(proxyClient.client().describePipeline(describePipelineRequestArgumentCaptor.capture()))
                .thenReturn(describePipelineResponseFull);

        when(proxyClient.client().listTagsForResource(listTagsForResourceRequestArgumentCaptor.capture()))
                .thenReturn(listTagsForResourceResponseFull);

        // WHEN
        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        // THEN
        final UpdatePipelineRequest updatePipelineRequest = updatePipelineRequestArgumentCaptor.getValue();
        assertThat(updatePipelineRequest.pipelineName()).isEqualTo(TEST_PIPELINE_NAME);
        assertThat(updatePipelineRequest.pipelineActivities()).isEqualTo(
                Arrays.asList(
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
        );

        final TagResourceRequest tagResourceRequest = tagResourceRequestArgumentCaptor.getValue();
        assertThat(tagResourceRequest.resourceArn()).isEqualTo(TEST_PIPELINE_ARN);
        assertThat(tagResourceRequest.tags().size()).isEqualTo(2);
        final List<software.amazon.awssdk.services.iotanalytics.model.Tag> tagsToAdd
                = new ArrayList<>(tagResourceRequest.tags());
        tagsToAdd.sort(Comparator.comparing(software.amazon.awssdk.services.iotanalytics.model.Tag::key));
        assertThat(tagsToAdd.get(0).key()).isEqualTo(TEST_KEY2);
        assertThat(tagsToAdd.get(0).value()).isEqualTo(TEST_VALUE22);
        assertThat(tagsToAdd.get(1).key()).isEqualTo(TEST_KEY3);
        assertThat(tagsToAdd.get(1).key()).isEqualTo(TEST_KEY3);

        final UntagResourceRequest untagResourceRequest = untagResourceRequestArgumentCaptor.getValue();
        assertThat(untagResourceRequest.resourceArn()).isEqualTo(TEST_PIPELINE_ARN);
        assertThat(untagResourceRequest.tagKeys().size()).isEqualTo(1);
        assertThat(untagResourceRequest.tagKeys().contains(TEST_KEY1)).isTrue();

        assertThat(describePipelineRequestArgumentCaptor.getValue().pipelineName())
                .isEqualTo(TEST_PIPELINE_NAME);

        assertThat(listTagsForResourceRequestArgumentCaptor.getValue().resourceArn())
                .isEqualTo(TEST_PIPELINE_ARN);

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

        assertThat(response.getResourceModel().getTags().size()).isEqualTo(3);
        assertThat(response.getResourceModel().getTags().get(0).getKey()).isEqualTo(TEST_KEY1);
        assertThat(response.getResourceModel().getTags().get(0).getValue()).isEqualTo(TEST_VALUE1);
        assertThat(response.getResourceModel().getTags().get(1).getKey()).isEqualTo(TEST_KEY2);
        assertThat(response.getResourceModel().getTags().get(1).getValue()).isEqualTo(TEST_VALUE22);
        assertThat(response.getResourceModel().getTags().get(2).getKey()).isEqualTo(TEST_KEY3);
        assertThat(response.getResourceModel().getTags().get(2).getValue()).isEqualTo(TEST_VALUE3);
    }

    @Test
    public void GIVEN_update_diff_pipeline_name_WHEN_call_handleRequest_THEN_throw_CfnNotUpdatableException() {
        // GIVEN
        final ResourceModel preModel = ResourceModel.builder().pipelineName("name1").build();
        final ResourceModel newModel = ResourceModel.builder().pipelineName("name2").build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(newModel)
                .previousResourceState(preModel)
                .build();

        // WHEN / THEN
        assertThrows(CfnNotUpdatableException.class,
                () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));

        verify(proxyClient.client(), never()).updatePipeline(any(UpdatePipelineRequest.class));
        verify(proxyClient.client(), never()).untagResource(any(UntagResourceRequest.class));
        verify(proxyClient.client(), never()).tagResource(any(TagResourceRequest.class));
        verify(proxyClient.client(), never()).describePipeline(any(DescribePipelineRequest.class));
        verify(proxyClient.client(), never()).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void GIVEN_update_pipeline_arn_WHEN_call_handleRequest_THEN_return_failed_invalid_request() {
        // GIVEN
        final ResourceModel preModel = ResourceModel.builder().pipelineName("name1").id("id1").build();
        final ResourceModel newModel = ResourceModel.builder().pipelineName("name1").id("id2").build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(newModel)
                .previousResourceState(preModel)
                .build();

        // WHEN
        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        // THEN
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackContext()).isNotNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNotNull();
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.InvalidRequest);

        verify(proxyClient.client(), never()).updatePipeline(any(UpdatePipelineRequest.class));
        verify(proxyClient.client(), never()).untagResource(any(UntagResourceRequest.class));
        verify(proxyClient.client(), never()).tagResource(any(TagResourceRequest.class));
        verify(proxyClient.client(), never()).describePipeline(any(DescribePipelineRequest.class));
        verify(proxyClient.client(), never()).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void GIVEN_update_same_pipeline_arn_WHEN_call_handleRequest_THEN_return_success() {
        // GIVEN
        final ResourceModel preModel = ResourceModel.builder()
                .pipelineName(TEST_PIPELINE_NAME)
                .id(TEST_PIPELINE_ID)
                .pipelineActivities(Arrays.asList(
                        CFN_CHANNEL_TO_DATASTORE_ACTIVITY,
                        CFN_DATASTORE_ACTIVITY
                ))
                .build();
        final ResourceModel newModel = ResourceModel.builder()
                .pipelineName(TEST_PIPELINE_NAME)
                .id(TEST_PIPELINE_ID)
                .pipelineActivities(Arrays.asList(
                        CFN_CHANNEL_TO_DATASTORE_ACTIVITY,
                        CFN_DATASTORE_ACTIVITY
                ))
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(newModel)
                .previousResourceState(preModel)
                .build();

        when(proxyClient.client().describePipeline(any(DescribePipelineRequest.class)))
                .thenReturn(describePipelineResponseSimple);
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(listTagsForResourceResponseSimple);

        // WHEN
        final ProgressEvent<ResourceModel, CallbackContext> response =
                handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        // THEN
        verify(proxyClient.client(), times(1)).updatePipeline(any(UpdatePipelineRequest.class));
        verify(proxyClient.client(), never()).untagResource(any(UntagResourceRequest.class));
        verify(proxyClient.client(), never()).tagResource(any(TagResourceRequest.class));
        verify(proxyClient.client(), times(2)).describePipeline(any(DescribePipelineRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
        assertThat(response.getResourceModel().getTags()).isNull();
        assertThat(response.getResourceModel().getPipelineName()).isEqualTo(TEST_PIPELINE_NAME);
        assertThat(response.getResourceModel().getPipelineActivities()).isEqualTo(Arrays.asList(
                CFN_CHANNEL_TO_DATASTORE_ACTIVITY,
                CFN_DATASTORE_ACTIVITY
        ));
    }

    @Test
    public void GIVEN_iota_update_exception_WHEN_call_handleRequest_THEN_throw_Exception() {
        // GIVEN
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(newModel)
                .previousResourceState(preModel)
                .build();

        when(proxyClient.client().updatePipeline(updatePipelineRequestArgumentCaptor.capture()))
                .thenThrow(InvalidRequestException.builder().build());

        // WHEN / THEN
        assertThrows(CfnInvalidRequestException.class,
                () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));

        assertThat(updatePipelineRequestArgumentCaptor.getValue().pipelineName())
                .isEqualTo(TEST_PIPELINE_NAME);
        verify(proxyClient.client(), times(1))
                .updatePipeline(any(UpdatePipelineRequest.class));
        verify(proxyClient.client(), never()).untagResource(any(UntagResourceRequest.class));
        verify(proxyClient.client(), never()).tagResource(any(TagResourceRequest.class));
        verify(proxyClient.client(), never()).describePipeline(any(DescribePipelineRequest.class));
        verify(proxyClient.client(), never()).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void GIVEN_iota_untag_exception_WHEN_call_handleRequest_THEN_throw_Exception() {
        // GIVEN
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(newModel)
                .previousResourceState(preModel)
                .build();

        when(proxyClient.client().updatePipeline(any(UpdatePipelineRequest.class)))
                .thenReturn(UpdatePipelineResponse.builder().build());
        when(proxyClient.client().untagResource(any(UntagResourceRequest.class)))
                .thenThrow(LimitExceededException.builder().build());
        when(proxyClient.client().describePipeline(any(DescribePipelineRequest.class)))
            .thenReturn(describePipelineResponseSimple);

        // WHEN / THEN
        assertThrows(CfnServiceLimitExceededException.class,
                () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));

        verify(proxyClient.client(), times(1))
                .updatePipeline(any(UpdatePipelineRequest.class));
        verify(proxyClient.client(), times(1)).describePipeline(any(DescribePipelineRequest.class));
        verify(proxyClient.client(), times(1)).untagResource(any(UntagResourceRequest.class));
        verify(proxyClient.client(), never()).tagResource(any(TagResourceRequest.class));
        verify(proxyClient.client(), never()).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void GIVEN_iota_tag_exception_WHEN_call_handleRequest_THEN_throw_Exception() {
        // GIVEN
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(newModel)
                .previousResourceState(preModel)
                .build();

        when(proxyClient.client().updatePipeline(any(UpdatePipelineRequest.class)))
                .thenReturn(UpdatePipelineResponse.builder().build());
        when(proxyClient.client().untagResource(any(UntagResourceRequest.class))).thenReturn(UntagResourceResponse.builder().build());
        when(proxyClient.client().tagResource(any(TagResourceRequest.class))).thenThrow(ServiceUnavailableException.builder().build());
        when(proxyClient.client().describePipeline(any(DescribePipelineRequest.class)))
            .thenReturn(describePipelineResponseSimple);

        // WHEN / THEN
        assertThrows(CfnGeneralServiceException.class,
                () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));

        verify(proxyClient.client(), times(1)).updatePipeline(any(UpdatePipelineRequest.class));
        verify(proxyClient.client(), times(1)).untagResource(any(UntagResourceRequest.class));
        verify(proxyClient.client(), times(1)).tagResource(any(TagResourceRequest.class));
        verify(proxyClient.client(), times(1)).describePipeline(any(DescribePipelineRequest.class));
        verify(proxyClient.client(), never()).listTagsForResource(any(ListTagsForResourceRequest.class));
    }
}
