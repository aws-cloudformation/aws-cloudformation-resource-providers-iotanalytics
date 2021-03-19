package com.amazonaws.iotanalytics.dataset;

import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import software.amazon.awssdk.services.iotanalytics.model.DescribeDatasetRequest;
import software.amazon.awssdk.services.iotanalytics.model.DescribeDatasetResponse;
import software.amazon.awssdk.services.iotanalytics.model.InvalidRequestException;
import software.amazon.awssdk.services.iotanalytics.model.LimitExceededException;
import software.amazon.awssdk.services.iotanalytics.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.iotanalytics.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.iotanalytics.model.Dataset;
import software.amazon.awssdk.services.iotanalytics.model.ServiceUnavailableException;
import software.amazon.awssdk.services.iotanalytics.model.TagResourceRequest;
import software.amazon.awssdk.services.iotanalytics.model.TagResourceResponse;
import software.amazon.awssdk.services.iotanalytics.model.UntagResourceRequest;
import software.amazon.awssdk.services.iotanalytics.model.UntagResourceResponse;
import software.amazon.awssdk.services.iotanalytics.model.UpdateDatasetRequest;
import software.amazon.awssdk.services.iotanalytics.model.UpdateDatasetResponse;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnNotUpdatableException;
import software.amazon.cloudformation.exceptions.CfnServiceLimitExceededException;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static com.amazonaws.iotanalytics.dataset.TestConstants.CFN_CONTAINER_ACTION;
import static com.amazonaws.iotanalytics.dataset.TestConstants.CFN_CONTENT_DELIVERY_RULE_IOT_EVENT;
import static com.amazonaws.iotanalytics.dataset.TestConstants.CFN_LATE_DATE_RULE;
import static com.amazonaws.iotanalytics.dataset.TestConstants.CFN_RETENTION_DAYS;
import static com.amazonaws.iotanalytics.dataset.TestConstants.CFN_SQL_ACTION;
import static com.amazonaws.iotanalytics.dataset.TestConstants.CFN_TRIGGER_BY_SCHEDULE;
import static com.amazonaws.iotanalytics.dataset.TestConstants.CFN_VERSION_CONFIG;
import static com.amazonaws.iotanalytics.dataset.TestConstants.IOTA_CONTAINER_ACTION;
import static com.amazonaws.iotanalytics.dataset.TestConstants.IOTA_CONTENT_DELIVERY_RULE_IOT_EVENT;
import static com.amazonaws.iotanalytics.dataset.TestConstants.IOTA_LATE_DATE_RULE;
import static com.amazonaws.iotanalytics.dataset.TestConstants.IOTA_RETENTION_DAYS;
import static com.amazonaws.iotanalytics.dataset.TestConstants.IOTA_SQL_ACTION;
import static com.amazonaws.iotanalytics.dataset.TestConstants.IOTA_TRIGGER_BY_SCHEDULE;
import static com.amazonaws.iotanalytics.dataset.TestConstants.IOTA_VERSION_CONFIG;
import static com.amazonaws.iotanalytics.dataset.TestConstants.TEST_DATASET_ARN;
import static com.amazonaws.iotanalytics.dataset.TestConstants.TEST_DATASET_ID;
import static com.amazonaws.iotanalytics.dataset.TestConstants.TEST_DATASET_NAME;
import static com.amazonaws.iotanalytics.dataset.TestConstants.TEST_KEY1;
import static com.amazonaws.iotanalytics.dataset.TestConstants.TEST_KEY2;
import static com.amazonaws.iotanalytics.dataset.TestConstants.TEST_KEY3;
import static com.amazonaws.iotanalytics.dataset.TestConstants.TEST_VALUE1;
import static com.amazonaws.iotanalytics.dataset.TestConstants.TEST_VALUE2;
import static com.amazonaws.iotanalytics.dataset.TestConstants.TEST_VALUE22;
import static com.amazonaws.iotanalytics.dataset.TestConstants.TEST_VALUE3;
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
    private ArgumentCaptor<UpdateDatasetRequest> updateDatasetRequestArgumentCaptor;

    @Captor
    private ArgumentCaptor<TagResourceRequest> tagResourceRequestArgumentCaptor;

    @Captor
    private ArgumentCaptor<UntagResourceRequest> untagResourceRequestArgumentCaptor;

    @Captor
    private ArgumentCaptor<DescribeDatasetRequest> describeDatasetRequestArgumentCaptor;

    @Captor
    private ArgumentCaptor<ListTagsForResourceRequest> listTagsForResourceRequestArgumentCaptor;

    private UpdateHandler handler;
    private ResourceModel preModel;
    private ResourceModel newModel;
    private DescribeDatasetResponse describeDatasetResponseFull;
    private ListTagsForResourceResponse listTagsForResourceResponseFull;
    private DescribeDatasetResponse describeDatasetResponseSimple;
    private ListTagsForResourceResponse listTagsForResourceResponseSimple;

    @BeforeEach
    public void setup() {
        super.setUp();
        handler = new UpdateHandler();

        preModel = ResourceModel.builder()
                .datasetName(TEST_DATASET_NAME)
                .id(TEST_DATASET_ID)
                .actions(Collections.singletonList(CFN_SQL_ACTION))
                .retentionPeriod(CFN_RETENTION_DAYS)
                .tags(Arrays.asList(Tag.builder().key(TEST_KEY1).value(TEST_VALUE1).build(),
                        Tag.builder().key(TEST_KEY2).value(TEST_VALUE2).build()))
                .build();

        newModel = ResourceModel.builder()
                .datasetName(TEST_DATASET_NAME)
                .versioningConfiguration(CFN_VERSION_CONFIG)
                .triggers(Collections.singletonList(CFN_TRIGGER_BY_SCHEDULE))
                .retentionPeriod(CFN_RETENTION_DAYS)
                .lateDataRules(Collections.singletonList(CFN_LATE_DATE_RULE))
                .contentDeliveryRules(Collections.singletonList(
                        CFN_CONTENT_DELIVERY_RULE_IOT_EVENT
                ))
                .actions(Collections.singletonList(CFN_CONTAINER_ACTION))
                .tags(Arrays.asList(Tag.builder().key(TEST_KEY3).value(TEST_VALUE3).build(),
                        Tag.builder().key(TEST_KEY2).value(TEST_VALUE22).build()))
                .build();

        describeDatasetResponseFull = DescribeDatasetResponse.builder().dataset(
                Dataset.builder()
                        .name(TEST_DATASET_NAME)
                        .arn(TEST_DATASET_ARN)
                        .contentDeliveryRules(Collections.singletonList(
                                IOTA_CONTENT_DELIVERY_RULE_IOT_EVENT
                        ))
                        .actions(Collections.singletonList(IOTA_CONTAINER_ACTION))
                        .versioningConfiguration(IOTA_VERSION_CONFIG)
                        .triggers(Collections.singletonList(IOTA_TRIGGER_BY_SCHEDULE))
                        .retentionPeriod(IOTA_RETENTION_DAYS)
                        .lateDataRules(Collections.singletonList(IOTA_LATE_DATE_RULE))
                        .build())
                .build();

        listTagsForResourceResponseFull = ListTagsForResourceResponse.builder()
                .tags(Arrays.asList(software.amazon.awssdk.services.iotanalytics.model.Tag.builder().key(TEST_KEY1).value(TEST_VALUE1).build(),
                        software.amazon.awssdk.services.iotanalytics.model.Tag.builder().key(TEST_KEY2).value(TEST_VALUE22).build(),
                        software.amazon.awssdk.services.iotanalytics.model.Tag.builder().key(TEST_KEY3).value(TEST_VALUE3).build()))
                .build();

        describeDatasetResponseSimple = DescribeDatasetResponse.builder()
                .dataset(Dataset.builder()
                        .name(TEST_DATASET_NAME)
                        .actions(Collections.singletonList(IOTA_SQL_ACTION))
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

        when(proxyClient.client().updateDataset(updateDatasetRequestArgumentCaptor.capture()))
                .thenReturn(UpdateDatasetResponse.builder().build());

        when(proxyClient.client().untagResource(untagResourceRequestArgumentCaptor.capture()))
                .thenReturn(UntagResourceResponse.builder().build());

        when(proxyClient.client().tagResource(tagResourceRequestArgumentCaptor.capture()))
                .thenReturn(TagResourceResponse.builder().build());

        when(proxyClient.client().describeDataset(describeDatasetRequestArgumentCaptor.capture()))
                .thenReturn(describeDatasetResponseFull);

        when(proxyClient.client().listTagsForResource(listTagsForResourceRequestArgumentCaptor.capture()))
                .thenReturn(listTagsForResourceResponseFull);

        // WHEN
        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        // THEN
        final UpdateDatasetRequest updateDatasetRequest = updateDatasetRequestArgumentCaptor.getValue();
        assertThat(updateDatasetRequest.datasetName()).isEqualTo(TEST_DATASET_NAME);
        assertThat(updateDatasetRequest.actions()).isEqualTo(Collections.singletonList(IOTA_CONTAINER_ACTION));
        assertThat(updateDatasetRequest.contentDeliveryRules()).isEqualTo(Collections.singletonList(
                IOTA_CONTENT_DELIVERY_RULE_IOT_EVENT
        ));
        assertThat(updateDatasetRequest.retentionPeriod()).isEqualTo(IOTA_RETENTION_DAYS);
        assertThat(updateDatasetRequest.lateDataRules()).isEqualTo(Collections.singletonList(IOTA_LATE_DATE_RULE));
        assertThat(updateDatasetRequest.versioningConfiguration()).isEqualTo(IOTA_VERSION_CONFIG);
        assertThat(updateDatasetRequest.triggers()).isEqualTo(Collections.singletonList(IOTA_TRIGGER_BY_SCHEDULE));


        final TagResourceRequest tagResourceRequest = tagResourceRequestArgumentCaptor.getValue();
        assertThat(tagResourceRequest.resourceArn()).isEqualTo(TEST_DATASET_ARN);
        assertThat(tagResourceRequest.tags().size()).isEqualTo(2);
        final List<software.amazon.awssdk.services.iotanalytics.model.Tag> tagsToAdd
                = new ArrayList<>(tagResourceRequest.tags());
        tagsToAdd.sort(Comparator.comparing(software.amazon.awssdk.services.iotanalytics.model.Tag::key));
        assertThat(tagsToAdd.get(0).key()).isEqualTo(TEST_KEY2);
        assertThat(tagsToAdd.get(0).value()).isEqualTo(TEST_VALUE22);
        assertThat(tagsToAdd.get(1).key()).isEqualTo(TEST_KEY3);
        assertThat(tagsToAdd.get(1).key()).isEqualTo(TEST_KEY3);

        final UntagResourceRequest untagResourceRequest = untagResourceRequestArgumentCaptor.getValue();
        assertThat(untagResourceRequest.resourceArn()).isEqualTo(TEST_DATASET_ARN);
        assertThat(untagResourceRequest.tagKeys().size()).isEqualTo(1);
        assertThat(untagResourceRequest.tagKeys().contains(TEST_KEY1)).isTrue();

        assertThat(describeDatasetRequestArgumentCaptor.getValue().datasetName())
                .isEqualTo(TEST_DATASET_NAME);

        assertThat(listTagsForResourceRequestArgumentCaptor.getValue().resourceArn())
                .isEqualTo(TEST_DATASET_ARN);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        assertThat(response.getResourceModel().getDatasetName()).isEqualTo(TEST_DATASET_NAME);
        assertThat(response.getResourceModel().getId()).isEqualTo(TEST_DATASET_ID);
        assertThat(response.getResourceModel().getActions()).isEqualTo(Collections.singletonList(CFN_CONTAINER_ACTION));
        assertThat(response.getResourceModel().getContentDeliveryRules()).isEqualTo(Collections.singletonList(
                CFN_CONTENT_DELIVERY_RULE_IOT_EVENT
        ));
        assertThat(response.getResourceModel().getRetentionPeriod()).isEqualTo(CFN_RETENTION_DAYS);
        assertThat(response.getResourceModel().getLateDataRules()).isEqualTo(Collections.singletonList(CFN_LATE_DATE_RULE));
        assertThat(response.getResourceModel().getVersioningConfiguration()).isEqualTo(CFN_VERSION_CONFIG);
        assertThat(response.getResourceModel().getTriggers()).isEqualTo(Collections.singletonList(CFN_TRIGGER_BY_SCHEDULE));

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
        final ResourceModel preModel = ResourceModel.builder().datasetName("name1").build();
        final ResourceModel newModel = ResourceModel.builder().datasetName("name2").build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(newModel)
                .previousResourceState(preModel)
                .build();

        // WHEN / THEN
        assertThrows(CfnNotUpdatableException.class,
                () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));

        verify(proxyClient.client(), never()).updateDataset(any(UpdateDatasetRequest.class));
        verify(proxyClient.client(), never()).untagResource(any(UntagResourceRequest.class));
        verify(proxyClient.client(), never()).tagResource(any(TagResourceRequest.class));
        verify(proxyClient.client(), never()).describeDataset(any(DescribeDatasetRequest.class));
        verify(proxyClient.client(), never()).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void GIVEN_update_pipeline_arn_WHEN_call_handleRequest_THEN_throw_CfnNotUpdatableException() {
        // GIVEN
        final ResourceModel preModel = ResourceModel.builder().datasetName("name1").id("id1").build();
        final ResourceModel newModel = ResourceModel.builder().datasetName("name1").id("id2").build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(newModel)
                .previousResourceState(preModel)
                .build();

        // WHEN / THEN
        assertThrows(CfnNotUpdatableException.class,
                () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));

        verify(proxyClient.client(), never()).updateDataset(any(UpdateDatasetRequest.class));
        verify(proxyClient.client(), never()).untagResource(any(UntagResourceRequest.class));
        verify(proxyClient.client(), never()).tagResource(any(TagResourceRequest.class));
        verify(proxyClient.client(), never()).describeDataset(any(DescribeDatasetRequest.class));
        verify(proxyClient.client(), never()).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void GIVEN_update_same_pipeline_arn_WHEN_call_handleRequest_THEN_return_success() {
        // GIVEN
        final ResourceModel preModel = ResourceModel.builder()
                .datasetName(TEST_DATASET_NAME)
                .id(TEST_DATASET_ID)
                .actions(Collections.singletonList(CFN_CONTAINER_ACTION))
                .build();
        final ResourceModel newModel = ResourceModel.builder()
                .datasetName(TEST_DATASET_NAME)
                .id(TEST_DATASET_ID)
                .actions(Collections.singletonList(CFN_SQL_ACTION))
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(newModel)
                .previousResourceState(preModel)
                .build();

        when(proxyClient.client().describeDataset(any(DescribeDatasetRequest.class)))
                .thenReturn(describeDatasetResponseSimple);
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(listTagsForResourceResponseSimple);

        // WHEN
        final ProgressEvent<ResourceModel, CallbackContext> response =
                handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        // THEN
        verify(proxyClient.client(), times(1)).updateDataset(any(UpdateDatasetRequest.class));
        verify(proxyClient.client(), never()).untagResource(any(UntagResourceRequest.class));
        verify(proxyClient.client(), never()).tagResource(any(TagResourceRequest.class));
        verify(proxyClient.client(), times(1)).describeDataset(any(DescribeDatasetRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
        assertThat(response.getResourceModel().getTags()).isNull();
        assertThat(response.getResourceModel().getDatasetName()).isEqualTo(TEST_DATASET_NAME);
        assertThat(response.getResourceModel().getActions()).isEqualTo(Collections.singletonList(CFN_SQL_ACTION));
    }


    @Test
    public void GIVEN_iota_update_exception_WHEN_call_handleRequest_THEN_throw_Exception() {
        // GIVEN
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(newModel)
                .previousResourceState(preModel)
                .build();

        when(proxyClient.client().updateDataset(updateDatasetRequestArgumentCaptor.capture()))
                .thenThrow(InvalidRequestException.builder().build());

        // WHEN / THEN
        assertThrows(CfnInvalidRequestException.class,
                () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));

        assertThat(updateDatasetRequestArgumentCaptor.getValue().datasetName())
                .isEqualTo(TEST_DATASET_NAME);
        verify(proxyClient.client(), times(1))
                .updateDataset(any(UpdateDatasetRequest.class));
        verify(proxyClient.client(), never()).untagResource(any(UntagResourceRequest.class));
        verify(proxyClient.client(), never()).tagResource(any(TagResourceRequest.class));
        verify(proxyClient.client(), never()).describeDataset(any(DescribeDatasetRequest.class));
        verify(proxyClient.client(), never()).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void GIVEN_iota_untag_exception_WHEN_call_handleRequest_THEN_throw_Exception() {
        // GIVEN
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(newModel)
                .previousResourceState(preModel)
                .build();

        when(proxyClient.client().updateDataset(any(UpdateDatasetRequest.class)))
                .thenReturn(UpdateDatasetResponse.builder().build());
        when(proxyClient.client().untagResource(any(UntagResourceRequest.class)))
                .thenThrow(LimitExceededException.builder().build());
        // WHEN / THEN
        assertThrows(CfnServiceLimitExceededException.class,
                () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));

        verify(proxyClient.client(), times(1))
                .updateDataset(any(UpdateDatasetRequest.class));
        verify(proxyClient.client(), times(1)).untagResource(any(UntagResourceRequest.class));
        verify(proxyClient.client(), never()).tagResource(any(TagResourceRequest.class));
        verify(proxyClient.client(), never()).describeDataset(any(DescribeDatasetRequest.class));
        verify(proxyClient.client(), never()).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void GIVEN_iota_tag_exception_WHEN_call_handleRequest_THEN_throw_Exception() {
        // GIVEN
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(newModel)
                .previousResourceState(preModel)
                .build();

        when(proxyClient.client().updateDataset(any(UpdateDatasetRequest.class)))
                .thenReturn(UpdateDatasetResponse.builder().build());
        when(proxyClient.client().untagResource(any(UntagResourceRequest.class))).thenReturn(UntagResourceResponse.builder().build());
        when(proxyClient.client().tagResource(any(TagResourceRequest.class))).thenThrow(ServiceUnavailableException.builder().build());

        // WHEN / THEN
        assertThrows(CfnGeneralServiceException.class,
                () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));

        verify(proxyClient.client(), times(1)).updateDataset(any(UpdateDatasetRequest.class));
        verify(proxyClient.client(), times(1)).untagResource(any(UntagResourceRequest.class));
        verify(proxyClient.client(), times(1)).tagResource(any(TagResourceRequest.class));
        verify(proxyClient.client(), never()).describeDataset(any(DescribeDatasetRequest.class));
        verify(proxyClient.client(), never()).listTagsForResource(any(ListTagsForResourceRequest.class));
    }
}
