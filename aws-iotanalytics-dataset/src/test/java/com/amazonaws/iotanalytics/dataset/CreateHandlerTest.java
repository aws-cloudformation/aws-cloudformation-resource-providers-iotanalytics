package com.amazonaws.iotanalytics.dataset;

import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import software.amazon.awssdk.services.iotanalytics.model.CreateDatasetRequest;
import software.amazon.awssdk.services.iotanalytics.model.CreateDatasetResponse;
import software.amazon.awssdk.services.iotanalytics.model.Dataset;
import software.amazon.awssdk.services.iotanalytics.model.DescribeDatasetRequest;
import software.amazon.awssdk.services.iotanalytics.model.DescribeDatasetResponse;
import software.amazon.awssdk.services.iotanalytics.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.iotanalytics.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.iotanalytics.model.ResourceAlreadyExistsException;
import software.amazon.cloudformation.exceptions.CfnAlreadyExistsException;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.Arrays;
import java.util.Collections;

import static com.amazonaws.iotanalytics.dataset.TestConstants.CFN_CONTAINER_ACTION;
import static com.amazonaws.iotanalytics.dataset.TestConstants.CFN_CONTENT_DELIVERY_RULE_IOT_EVENT;
import static com.amazonaws.iotanalytics.dataset.TestConstants.CFN_CONTENT_DELIVERY_RULE_S3;
import static com.amazonaws.iotanalytics.dataset.TestConstants.CFN_LATE_DATE_RULE;
import static com.amazonaws.iotanalytics.dataset.TestConstants.CFN_RETENTION_DAYS;
import static com.amazonaws.iotanalytics.dataset.TestConstants.CFN_SQL_ACTION;
import static com.amazonaws.iotanalytics.dataset.TestConstants.CFN_TRIGGER_BY_DATASET;
import static com.amazonaws.iotanalytics.dataset.TestConstants.CFN_VERSION_CONFIG;
import static com.amazonaws.iotanalytics.dataset.TestConstants.IOTA_CONTENT_DELIVERY_RULE_IOT_EVENT;
import static com.amazonaws.iotanalytics.dataset.TestConstants.IOTA_CONTENT_DELIVERY_RULE_S3;
import static com.amazonaws.iotanalytics.dataset.TestConstants.IOTA_LATE_DATE_RULE;
import static com.amazonaws.iotanalytics.dataset.TestConstants.IOTA_RETENTION_DAYS;
import static com.amazonaws.iotanalytics.dataset.TestConstants.IOTA_SQL_ACTION;
import static com.amazonaws.iotanalytics.dataset.TestConstants.IOTA_TRIGGER_BY_DATASET;
import static com.amazonaws.iotanalytics.dataset.TestConstants.IOTA_VERSION_CONFIG;
import static com.amazonaws.iotanalytics.dataset.TestConstants.TEST_DATASET_ARN;
import static com.amazonaws.iotanalytics.dataset.TestConstants.TEST_DATASET_ID;
import static com.amazonaws.iotanalytics.dataset.TestConstants.TEST_DATASET_NAME;
import static com.amazonaws.iotanalytics.dataset.TestConstants.TEST_KEY1;
import static com.amazonaws.iotanalytics.dataset.TestConstants.TEST_KEY2;
import static com.amazonaws.iotanalytics.dataset.TestConstants.TEST_VALUE1;
import static com.amazonaws.iotanalytics.dataset.TestConstants.TEST_VALUE2;
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
    private ArgumentCaptor<CreateDatasetRequest> createDatasetRequestArgumentCaptor;

    @BeforeEach
    public void setup() {
        super.setUp();
        handler = new CreateHandler();
    }

    @Test
    public void GIVEN_request_WHEN_call_handleRequest_THEN_return_success() {
        // GIVEN
        final ResourceModel model = ResourceModel.builder()
                .datasetName(TEST_DATASET_NAME)
                .versioningConfiguration(CFN_VERSION_CONFIG)
                .triggers(Collections.singletonList(CFN_TRIGGER_BY_DATASET))
                .retentionPeriod(CFN_RETENTION_DAYS)
                .lateDataRules(Collections.singletonList(CFN_LATE_DATE_RULE))
                .contentDeliveryRules(Arrays.asList(
                        CFN_CONTENT_DELIVERY_RULE_IOT_EVENT,
                        CFN_CONTENT_DELIVERY_RULE_S3
                ))
                .actions(Collections.singletonList(CFN_SQL_ACTION))
                .tags(Arrays.asList(Tag.builder().key(TEST_KEY1).value(TEST_VALUE1).build(),
                        Tag.builder().key(TEST_KEY2).value(TEST_VALUE2).build()))
                .build();

        final CreateDatasetResponse createDatasetResponse = CreateDatasetResponse.builder().build();

        final DescribeDatasetResponse describeDatasetResponse = DescribeDatasetResponse.builder()
                .dataset(Dataset
                        .builder()
                        .arn(TEST_DATASET_ARN)
                        .name(TEST_DATASET_NAME)
                        .versioningConfiguration(IOTA_VERSION_CONFIG)
                        .triggers(Collections.singletonList(IOTA_TRIGGER_BY_DATASET))
                        .retentionPeriod(IOTA_RETENTION_DAYS)
                        .lateDataRules(Collections.singletonList(IOTA_LATE_DATE_RULE))
                        .contentDeliveryRules(Arrays.asList(
                                IOTA_CONTENT_DELIVERY_RULE_IOT_EVENT,
                                IOTA_CONTENT_DELIVERY_RULE_S3
                        ))
                        .actions(Collections.singletonList(IOTA_SQL_ACTION))
                        .build())
                .build();

        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder()
                .tags(software.amazon.awssdk.services.iotanalytics.model.Tag.builder().key(TEST_KEY1).value(TEST_VALUE1).build(),
                        software.amazon.awssdk.services.iotanalytics.model.Tag.builder().key(TEST_KEY2).value(TEST_VALUE2).build())
                .build();

        when(proxyClient.client().createDataset(createDatasetRequestArgumentCaptor.capture())).thenReturn(createDatasetResponse);
        when(proxyClient.client().describeDataset(any(DescribeDatasetRequest.class))).thenReturn(describeDatasetResponse);
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class))).thenReturn(listTagsForResourceResponse);

        // WHEN
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        // THEN
        verify(proxyClient.client(), times(1)).createDataset(any(CreateDatasetRequest.class));

        final CreateDatasetRequest createDatasetRequest = createDatasetRequestArgumentCaptor.getValue();
        assertThat(createDatasetRequest.datasetName()).isEqualTo(TEST_DATASET_NAME);
        assertThat(createDatasetRequest.actions()).isEqualTo(Collections.singletonList(IOTA_SQL_ACTION));
        assertThat(createDatasetRequest.contentDeliveryRules()).isEqualTo(Arrays.asList(
                IOTA_CONTENT_DELIVERY_RULE_IOT_EVENT,
                IOTA_CONTENT_DELIVERY_RULE_S3
        ));
        assertThat(createDatasetRequest.retentionPeriod()).isEqualTo(IOTA_RETENTION_DAYS);
        assertThat(createDatasetRequest.lateDataRules()).isEqualTo(Collections.singletonList(IOTA_LATE_DATE_RULE));
        assertThat(createDatasetRequest.versioningConfiguration()).isEqualTo(IOTA_VERSION_CONFIG);
        assertThat(createDatasetRequest.triggers()).isEqualTo(Collections.singletonList(IOTA_TRIGGER_BY_DATASET));

        assertThat(createDatasetRequest.tags().size()).isEqualTo(2);
        assertThat(createDatasetRequest.tags().get(0).key()).isEqualTo(TEST_KEY1);
        assertThat(createDatasetRequest.tags().get(0).value()).isEqualTo(TEST_VALUE1);
        assertThat(createDatasetRequest.tags().get(1).key()).isEqualTo(TEST_KEY2);
        assertThat(createDatasetRequest.tags().get(1).value()).isEqualTo(TEST_VALUE2);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        assertThat(response.getResourceModel().getDatasetName()).isEqualTo(request.getDesiredResourceState().getDatasetName());
        assertThat(response.getResourceModel().getRetentionPeriod()).isEqualTo(request.getDesiredResourceState().getRetentionPeriod());
        assertThat(response.getResourceModel().getActions()).isEqualTo(request.getDesiredResourceState().getActions());
        assertThat(response.getResourceModel().getContentDeliveryRules()).isEqualTo(request.getDesiredResourceState().getContentDeliveryRules());
        assertThat(response.getResourceModel().getTriggers()).isEqualTo(request.getDesiredResourceState().getTriggers());
        assertThat(response.getResourceModel().getVersioningConfiguration()).isEqualTo(request.getDesiredResourceState().getVersioningConfiguration());
        assertThat(response.getResourceModel().getLateDataRules()).isEqualTo(request.getDesiredResourceState().getLateDataRules());
        assertThat(response.getResourceModel().getTags()).isEqualTo(request.getDesiredResourceState().getTags());
        assertThat(response.getResourceModel().getId()).isEqualTo(TEST_DATASET_ID);
    }

    @Test
    public void GIVEN_request_with_id_WHEN_call_handleRequest_THEN_return_InvalidRequest() {
        // GIVEN
        final ResourceModel model = ResourceModel.builder()
                .id(TEST_DATASET_ID)
                .actions(Collections.singletonList(CFN_CONTAINER_ACTION))
                .datasetName(TEST_DATASET_NAME)
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
                .actions(Collections.singletonList(CFN_CONTAINER_ACTION))
                .datasetName(TEST_DATASET_NAME).build();

        when(proxyClient.client().createDataset(createDatasetRequestArgumentCaptor.capture()))
                .thenThrow(ResourceAlreadyExistsException.builder().message("already exist").build());

        // WHEN / THEN
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        assertThrows(CfnAlreadyExistsException.class,
                () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));
        assertThat(createDatasetRequestArgumentCaptor.getValue().datasetName()).isEqualTo(TEST_DATASET_NAME);
    }

    @Test
    public void GIVEN_missing_datasetName_WHEN_call_handleRequest_THEN_return_success_with_generated_datasetName() {
        // GIVEN
        final ResourceModel model = ResourceModel.builder()
                .actions(Collections.singletonList(CFN_SQL_ACTION))
                .build();

        final CreateDatasetResponse createDatasetResponse = CreateDatasetResponse.builder()
                .build();
        when(proxyClient.client().createDataset(createDatasetRequestArgumentCaptor.capture())).thenReturn(createDatasetResponse);

        // WHEN
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .logicalResourceIdentifier(TEST_LOGICAL_RESOURCE_IDENTIFIER)
                .clientRequestToken(TEST_CLIENT_REQUEST_TOKEN)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        // THEN
        verify(proxyClient.client(), times(1)).createDataset(any(CreateDatasetRequest.class));
        final CreateDatasetRequest createDatasetRequest = createDatasetRequestArgumentCaptor.getValue();
        assertThat(createDatasetRequest.datasetName()).isNotBlank();
        assertFalse(createDatasetRequest.datasetName().contains("-"));
    }
}
