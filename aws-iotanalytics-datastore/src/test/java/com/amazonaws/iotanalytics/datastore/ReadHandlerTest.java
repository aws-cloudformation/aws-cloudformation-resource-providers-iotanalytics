package com.amazonaws.iotanalytics.datastore;

import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import software.amazon.awssdk.services.iotanalytics.model.CustomerManagedDatastoreS3Storage;
import software.amazon.awssdk.services.iotanalytics.model.Datastore;
import software.amazon.awssdk.services.iotanalytics.model.DescribeDatastoreRequest;
import software.amazon.awssdk.services.iotanalytics.model.DescribeDatastoreResponse;
import software.amazon.awssdk.services.iotanalytics.model.InternalFailureException;
import software.amazon.awssdk.services.iotanalytics.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.iotanalytics.model.ListTagsForResourceResponse;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ReadHandlerTest extends AbstractTestBase {
    private static final String TEST_DATASTORE_ARN = "test_datastore_arn";
    private static final String TEST_DATASTORE_ID = TEST_DATASTORE_ARN;
    private static final String TEST_DATASTORE_NAME = "test_DATASTORE_name";
    private static final String TEST_S3_BUCKET = "test_s3_bucket";
    private static final String TEST_PREFIX = "test_prefix";
    private static final String TEST_ROLE = "test_role";
    private static final String TEST_COL_NAME1 = "test_col_name1";
    private static final String TEST_COL_TYPE1 = "test_col_type1";
    private static final String TEST_COL_NAME2 = "test_col_name2";
    private static final String TEST_COL_TYPE2 = "test_col_type2";
    private static final String TEST_KEY1 = "key1";
    private static final String TEST_VALUE1 = "value1";
    private static final String TEST_KEY2 = "key2";
    private static final String TEST_VALUE2 = "value2";

    @Captor
    private ArgumentCaptor<DescribeDatastoreRequest> describeDatastoreRequestArgumentCaptor;

    @Captor
    private ArgumentCaptor<ListTagsForResourceRequest> listTagsForResourceRequestArgumentCaptor;

    private ReadHandler handler;

    private DescribeDatastoreResponse describeDatastoreResponse;

    @BeforeEach
    public void setup() {
        super.setUp();
        handler = new ReadHandler();

        describeDatastoreResponse = DescribeDatastoreResponse
                .builder()
                .datastore(
                        Datastore
                                .builder()
                                .arn(TEST_DATASTORE_ARN)
                                .retentionPeriod(software.amazon.awssdk.services.iotanalytics.model.RetentionPeriod
                                        .builder()
                                        .unlimited(true)
                                        .build())
                                .storage(software.amazon.awssdk.services.iotanalytics.model.DatastoreStorage
                                        .builder()
                                        .customerManagedS3(
                                                CustomerManagedDatastoreS3Storage
                                                        .builder()
                                                        .bucket(TEST_S3_BUCKET)
                                                        .keyPrefix(TEST_PREFIX)
                                                        .roleArn(TEST_ROLE)
                                                        .build())
                                        .build())
                                .fileFormatConfiguration(software.amazon.awssdk.services.iotanalytics.model.FileFormatConfiguration
                                        .builder()
                                        .parquetConfiguration(software.amazon.awssdk.services.iotanalytics.model.ParquetConfiguration
                                                .builder()
                                                .schemaDefinition(software.amazon.awssdk.services.iotanalytics.model.SchemaDefinition
                                                        .builder()
                                                        .columns(Arrays.asList(
                                                                software.amazon.awssdk.services.iotanalytics.model.Column.builder().name(TEST_COL_NAME1).type(TEST_COL_TYPE1).build(),
                                                                software.amazon.awssdk.services.iotanalytics.model.Column.builder().name(TEST_COL_NAME2).type(TEST_COL_TYPE2).build()))
                                                        .build())
                                                .build())
                                        .build())
                                .name(TEST_DATASTORE_NAME)
                                .build())
                .build();
    }

    @Test
    public void GIVEN_iota_good_WHEN_call_handleRequest_THEN_return_success() {
        // GIVEN
        final ResourceModel model = ResourceModel.builder().datastoreName(TEST_DATASTORE_NAME).build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder()
                .tags(software.amazon.awssdk.services.iotanalytics.model.Tag.builder().key(TEST_KEY1).value(TEST_VALUE1).build(),
                        software.amazon.awssdk.services.iotanalytics.model.Tag.builder().key(TEST_KEY2).value(TEST_VALUE2).build())
                .build();

        when(proxyClient.client().describeDatastore(describeDatastoreRequestArgumentCaptor.capture())).thenReturn(describeDatastoreResponse);
        when(proxyClient.client().listTagsForResource(listTagsForResourceRequestArgumentCaptor.capture())).thenReturn(listTagsForResourceResponse);

        // WHEN
        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        // THEN
        assertThat(describeDatastoreRequestArgumentCaptor.getValue().datastoreName()).isEqualTo(TEST_DATASTORE_NAME);
        assertThat(listTagsForResourceRequestArgumentCaptor.getValue().resourceArn()).isEqualTo(TEST_DATASTORE_ARN);
        verify(proxyClient.client(), times(1)).describeDatastore(any(DescribeDatastoreRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        assertThat(response.getResourceModel().getDatastoreName()).isEqualTo(TEST_DATASTORE_NAME);
        assertThat(response.getResourceModel().getId()).isEqualTo(TEST_DATASTORE_ID);
        assertThat(response.getResourceModel().getRetentionPeriod().getUnlimited()).isTrue();
        assertThat(response.getResourceModel().getRetentionPeriod().getNumberOfDays()).isNull();
        assertThat(response.getResourceModel().getDatastoreStorage().getServiceManagedS3()).isNull();
        assertThat(response.getResourceModel().getDatastoreStorage().getCustomerManagedS3().getBucket()).isEqualTo(TEST_S3_BUCKET);
        assertThat(response.getResourceModel().getDatastoreStorage().getCustomerManagedS3().getKeyPrefix()).isEqualTo(TEST_PREFIX);
        assertThat(response.getResourceModel().getDatastoreStorage().getCustomerManagedS3().getRoleArn()).isEqualTo(TEST_ROLE);

        assertThat(response.getResourceModel().getFileFormatConfiguration().getJsonConfiguration()).isNull();
        assertThat(response.getResourceModel().getFileFormatConfiguration().getParquetConfiguration()
                .getSchemaDefinition().getColumns().size()).isEqualTo(2);
        assertThat(response.getResourceModel().getFileFormatConfiguration().getParquetConfiguration()
                .getSchemaDefinition().getColumns().get(0).getName()).isEqualTo(TEST_COL_NAME1);
        assertThat(response.getResourceModel().getFileFormatConfiguration().getParquetConfiguration()
                .getSchemaDefinition().getColumns().get(0).getType()).isEqualTo(TEST_COL_TYPE1);
        assertThat(response.getResourceModel().getFileFormatConfiguration().getParquetConfiguration()
                .getSchemaDefinition().getColumns().get(1).getName()).isEqualTo(TEST_COL_NAME2);
        assertThat(response.getResourceModel().getFileFormatConfiguration().getParquetConfiguration()
                .getSchemaDefinition().getColumns().get(1).getType()).isEqualTo(TEST_COL_TYPE2);

        assertThat(response.getResourceModel().getTags().size()).isEqualTo(2);
        assertThat(response.getResourceModel().getTags().get(0).getKey()).isEqualTo(TEST_KEY1);
        assertThat(response.getResourceModel().getTags().get(0).getValue()).isEqualTo(TEST_VALUE1);
        assertThat(response.getResourceModel().getTags().get(1).getKey()).isEqualTo(TEST_KEY2);
        assertThat(response.getResourceModel().getTags().get(1).getValue()).isEqualTo(TEST_VALUE2);
    }

    @Test
    public void GIVEN_iota_describe_exception_WHEN_call_handleRequest_THEN_throw_exception() {
        // GIVEN
        final ResourceModel model = ResourceModel.builder().datastoreName(TEST_DATASTORE_NAME).build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final ResourceNotFoundException resourceNotFoundException = ResourceNotFoundException.builder().build();
        when(proxyClient.client().describeDatastore(describeDatastoreRequestArgumentCaptor.capture())).thenThrow(resourceNotFoundException);

        // WHEN / THEN
        assertThrows(CfnNotFoundException.class,
                () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));
        assertThat(describeDatastoreRequestArgumentCaptor.getValue().datastoreName()).isEqualTo(TEST_DATASTORE_NAME);
        verify(proxyClient.client(), never()).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void GIVEN_iota_list_tags_exception_WHEN_call_handleRequest_THEN_throw_exception() {
        // GIVEN
        final ResourceModel model = ResourceModel.builder().datastoreName(TEST_DATASTORE_NAME).build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        when(proxyClient.client().describeDatastore(describeDatastoreRequestArgumentCaptor.capture())).thenReturn(describeDatastoreResponse);

        when(proxyClient.client().listTagsForResource(listTagsForResourceRequestArgumentCaptor.capture())).thenThrow(InternalFailureException.builder().build());

        // WHEN / THEN
        assertThrows(CfnServiceInternalErrorException.class,
                () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));
        assertThat(describeDatastoreRequestArgumentCaptor.getValue().datastoreName()).isEqualTo(TEST_DATASTORE_NAME);
        assertThat(listTagsForResourceRequestArgumentCaptor.getValue().resourceArn()).isEqualTo(TEST_DATASTORE_ARN);

        verify(proxyClient.client(), times(1)).describeDatastore(any(DescribeDatastoreRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
    }
}
