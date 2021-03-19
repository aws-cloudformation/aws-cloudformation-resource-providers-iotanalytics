package com.amazonaws.iotanalytics.dataset;

import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import software.amazon.awssdk.services.iotanalytics.model.DescribeDatasetRequest;
import software.amazon.awssdk.services.iotanalytics.model.DescribeDatasetResponse;
import software.amazon.awssdk.services.iotanalytics.model.InternalFailureException;
import software.amazon.awssdk.services.iotanalytics.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.iotanalytics.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.iotanalytics.model.Dataset;
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
import java.util.Collections;

import static com.amazonaws.iotanalytics.dataset.TestConstants.CFN_CONTAINER_ACTION;
import static com.amazonaws.iotanalytics.dataset.TestConstants.CFN_CONTENT_DELIVERY_RULE_IOT_EVENT;
import static com.amazonaws.iotanalytics.dataset.TestConstants.CFN_CONTENT_DELIVERY_RULE_S3;
import static com.amazonaws.iotanalytics.dataset.TestConstants.CFN_LATE_DATE_RULE;
import static com.amazonaws.iotanalytics.dataset.TestConstants.CFN_RETENTION_UNLIMITED;
import static com.amazonaws.iotanalytics.dataset.TestConstants.CFN_TRIGGER_BY_SCHEDULE;
import static com.amazonaws.iotanalytics.dataset.TestConstants.CFN_VERSION_CONFIG;
import static com.amazonaws.iotanalytics.dataset.TestConstants.IOTA_CONTAINER_ACTION;
import static com.amazonaws.iotanalytics.dataset.TestConstants.IOTA_CONTENT_DELIVERY_RULE_IOT_EVENT;
import static com.amazonaws.iotanalytics.dataset.TestConstants.IOTA_CONTENT_DELIVERY_RULE_S3;
import static com.amazonaws.iotanalytics.dataset.TestConstants.IOTA_LATE_DATE_RULE;
import static com.amazonaws.iotanalytics.dataset.TestConstants.IOTA_RETENTION_UNLIMITED;
import static com.amazonaws.iotanalytics.dataset.TestConstants.IOTA_TRIGGER_BY_SCHEDULE;
import static com.amazonaws.iotanalytics.dataset.TestConstants.IOTA_VERSION_CONFIG;
import static com.amazonaws.iotanalytics.dataset.TestConstants.TEST_DATASET_ARN;
import static com.amazonaws.iotanalytics.dataset.TestConstants.TEST_DATASET_ID;
import static com.amazonaws.iotanalytics.dataset.TestConstants.TEST_DATASET_NAME;
import static com.amazonaws.iotanalytics.dataset.TestConstants.TEST_KEY1;
import static com.amazonaws.iotanalytics.dataset.TestConstants.TEST_KEY2;
import static com.amazonaws.iotanalytics.dataset.TestConstants.TEST_VALUE1;
import static com.amazonaws.iotanalytics.dataset.TestConstants.TEST_VALUE2;
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
    private ArgumentCaptor<DescribeDatasetRequest> describeDatasetRequestArgumentCaptor;

    @Captor
    private ArgumentCaptor<ListTagsForResourceRequest> listTagsForResourceRequestArgumentCaptor;

    private ReadHandler handler;

    private DescribeDatasetResponse describeDatasetResponse;

    @BeforeEach
    public void setup() {
        super.setUp();
        handler = new ReadHandler();

        describeDatasetResponse = DescribeDatasetResponse
                .builder()
                .dataset(
                        Dataset
                                .builder()
                                .name(TEST_DATASET_NAME)
                                .arn(TEST_DATASET_ARN)
                                .versioningConfiguration(IOTA_VERSION_CONFIG)
                                .triggers(Collections.singletonList(IOTA_TRIGGER_BY_SCHEDULE))
                                .retentionPeriod(IOTA_RETENTION_UNLIMITED)
                                .lateDataRules(Collections.singletonList(IOTA_LATE_DATE_RULE))
                                .contentDeliveryRules(Arrays.asList(
                                        IOTA_CONTENT_DELIVERY_RULE_IOT_EVENT,
                                        IOTA_CONTENT_DELIVERY_RULE_S3
                                ))
                                .actions(Collections.singletonList(IOTA_CONTAINER_ACTION))
                                .build())
                .build();
    }

    @Test
    public void GIVEN_iota_good_WHEN_call_handleRequest_THEN_return_success() {
        // GIVEN
        final ResourceModel model = ResourceModel.builder().datasetName(TEST_DATASET_NAME).build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder()
                .tags(software.amazon.awssdk.services.iotanalytics.model.Tag.builder().key(TEST_KEY1).value(TEST_VALUE1).build(),
                        software.amazon.awssdk.services.iotanalytics.model.Tag.builder().key(TEST_KEY2).value(TEST_VALUE2).build())
                .build();

        when(proxyClient.client().describeDataset(describeDatasetRequestArgumentCaptor.capture()))
                .thenReturn(describeDatasetResponse);
        when(proxyClient.client().listTagsForResource(listTagsForResourceRequestArgumentCaptor.capture())).thenReturn(listTagsForResourceResponse);

        // WHEN
        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        // THEN
        assertThat(describeDatasetRequestArgumentCaptor.getValue().datasetName()).isEqualTo(TEST_DATASET_NAME);
        assertThat(listTagsForResourceRequestArgumentCaptor.getValue().resourceArn()).isEqualTo(TEST_DATASET_ARN);
        verify(proxyClient.client(), times(1)).describeDataset(any(DescribeDatasetRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        assertThat(response.getResourceModel().getDatasetName()).isEqualTo(TEST_DATASET_NAME);
        assertThat(response.getResourceModel().getId()).isEqualTo(TEST_DATASET_ID);
        assertThat(response.getResourceModel().getLateDataRules())
                .isEqualTo(Collections.singletonList(CFN_LATE_DATE_RULE));
        assertThat(response.getResourceModel().getVersioningConfiguration())
                .isEqualTo(CFN_VERSION_CONFIG);
        assertThat(response.getResourceModel().getActions())
                .isEqualTo(Collections.singletonList(CFN_CONTAINER_ACTION));
        assertThat(response.getResourceModel().getContentDeliveryRules())
                .isEqualTo(Arrays.asList(
                        CFN_CONTENT_DELIVERY_RULE_IOT_EVENT,
                        CFN_CONTENT_DELIVERY_RULE_S3
                ));
        assertThat(response.getResourceModel().getTriggers())
                .isEqualTo(Collections.singletonList(CFN_TRIGGER_BY_SCHEDULE));
        assertThat(response.getResourceModel().getRetentionPeriod())
                .isEqualTo(CFN_RETENTION_UNLIMITED);

        assertThat(response.getResourceModel().getTags().size()).isEqualTo(2);
        assertThat(response.getResourceModel().getTags().get(0).getKey()).isEqualTo(TEST_KEY1);
        assertThat(response.getResourceModel().getTags().get(0).getValue()).isEqualTo(TEST_VALUE1);
        assertThat(response.getResourceModel().getTags().get(1).getKey()).isEqualTo(TEST_KEY2);
        assertThat(response.getResourceModel().getTags().get(1).getValue()).isEqualTo(TEST_VALUE2);
    }

    @Test
    public void GIVEN_iota_describe_exception_WHEN_call_handleRequest_THEN_throw_exception() {
        // GIVEN
        final ResourceModel model = ResourceModel.builder().datasetName(TEST_DATASET_NAME).build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final ResourceNotFoundException resourceNotFoundException = ResourceNotFoundException.builder().build();
        when(proxyClient.client().describeDataset(describeDatasetRequestArgumentCaptor.capture()))
                .thenThrow(resourceNotFoundException);

        // WHEN / THEN
        assertThrows(CfnNotFoundException.class,
                () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));
        assertThat(describeDatasetRequestArgumentCaptor.getValue().datasetName()).isEqualTo(TEST_DATASET_NAME);
        verify(proxyClient.client(), never()).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void GIVEN_iota_list_tags_exception_WHEN_call_handleRequest_THEN_throw_exception() {
        // GIVEN
        final ResourceModel model = ResourceModel.builder().datasetName(TEST_DATASET_NAME).build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        when(proxyClient.client().describeDataset(describeDatasetRequestArgumentCaptor.capture()))
                .thenReturn(describeDatasetResponse);

        when(proxyClient.client().listTagsForResource(listTagsForResourceRequestArgumentCaptor.capture())).thenThrow(InternalFailureException.builder().build());

        // WHEN / THEN
        assertThrows(CfnServiceInternalErrorException.class,
                () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));
        assertThat(describeDatasetRequestArgumentCaptor.getValue().datasetName()).isEqualTo(TEST_DATASET_NAME);
        assertThat(listTagsForResourceRequestArgumentCaptor.getValue().resourceArn()).isEqualTo(TEST_DATASET_ARN);

        verify(proxyClient.client(), times(1)).describeDataset(any(DescribeDatasetRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
    }
}
