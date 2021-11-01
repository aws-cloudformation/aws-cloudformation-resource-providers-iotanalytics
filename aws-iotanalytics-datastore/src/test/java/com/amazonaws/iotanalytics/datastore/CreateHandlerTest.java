package com.amazonaws.iotanalytics.datastore;

import org.mockito.ArgumentCaptor;
import org.mockito.Captor;

import software.amazon.awssdk.services.iotanalytics.model.CreateDatastoreRequest;
import software.amazon.awssdk.services.iotanalytics.model.CreateDatastoreResponse;
import software.amazon.awssdk.services.iotanalytics.model.CustomerManagedDatastoreS3Storage;
import software.amazon.awssdk.services.iotanalytics.model.Datastore;
import software.amazon.awssdk.services.iotanalytics.model.DatastoreIotSiteWiseMultiLayerStorage;
import software.amazon.awssdk.services.iotanalytics.model.DescribeDatastoreRequest;
import software.amazon.awssdk.services.iotanalytics.model.DescribeDatastoreResponse;
import software.amazon.awssdk.services.iotanalytics.model.IotSiteWiseCustomerManagedDatastoreS3Storage;
import software.amazon.awssdk.services.iotanalytics.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.iotanalytics.model.ListTagsForResourceResponse;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CreateHandlerTest extends AbstractTestBase {

    private static final String TEST_DATASTORE_ARN = "test_datastore_arn";
    private static final String TEST_DATASTORE_ID = TEST_DATASTORE_ARN;
    private static final String TEST_DATASTORE_NAME = "test_datastore_name";
    private static final int TEST_DAYS = 10;
    private static final String TEST_S3_BUCKET = "test_s3_bucket";
    private static final String TEST_PREFIX = "test_prefix/";
    private static final String TEST_ROLE = "arn:aws:iam::1234567890:role/Test-Role";
    private static final String TEST_COL_NAME1 = "test_col_name1";
    private static final String TEST_COL_TYPE1 = "test_col_type1";
    private static final String TEST_COL_NAME2 = "test_col_name2";
    private static final String TEST_COL_TYPE2 = "test_col_type2";
    private static final String TEST_KEY1 = "key1";
    private static final String TEST_VALUE1 = "value1";
    private static final String TEST_KEY2 = "key2";
    private static final String TEST_VALUE2 = "value2";
    private static final String TEST_ATTRIBUTE_PARTITION = "attribute";
    private static final String TEST_TIMESTAMP_PARTITION_NAME = "timestampAttribute";
    private static final String TEST_TIMESTAMP_PARTITION_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private CreateHandler handler;

    @Captor
    private ArgumentCaptor<CreateDatastoreRequest> createDatastoreRequestArgumentCaptor;

    @BeforeEach
    public void setup() {
        super.setUp();
        handler = new CreateHandler();
    }

    @Test
    public void GIVEN_request_WHEN_call_handleRequest_THEN_return_success() {
        // GIVEN
        final ResourceModel model = ResourceModel.builder()
                .datastoreName(TEST_DATASTORE_NAME)
                .datastoreStorage(DatastoreStorage
                        .builder()
                        .customerManagedS3(CustomerManagedS3
                                .builder()
                                .bucket(TEST_S3_BUCKET)
                                .keyPrefix(TEST_PREFIX)
                                .roleArn(TEST_ROLE)
                                .build())
                        .build())
                .retentionPeriod(RetentionPeriod.builder().numberOfDays(TEST_DAYS).build())
                .fileFormatConfiguration(FileFormatConfiguration
                        .builder()
                        .parquetConfiguration(ParquetConfiguration
                                .builder()
                                .schemaDefinition(SchemaDefinition
                                        .builder()
                                        .columns(Arrays.asList(
                                                Column.builder().name(TEST_COL_NAME1).type(TEST_COL_TYPE1).build(),
                                                Column.builder().name(TEST_COL_NAME2).type(TEST_COL_TYPE2).build()))
                                        .build())
                                .build())
                        .build())
                .tags(Arrays.asList(Tag.builder().key(TEST_KEY1).value(TEST_VALUE1).build(),
                        Tag.builder().key(TEST_KEY2).value(TEST_VALUE2).build()))
                .datastorePartitions(DatastorePartitions
                        .builder()
                        .partitions(Arrays.asList(
                                DatastorePartition.builder()
                                        .partition(Partition.builder()
                                                .attributeName(TEST_ATTRIBUTE_PARTITION)
                                                .build())
                                        .build(),
                                DatastorePartition.builder()
                                        .timestampPartition(TimestampPartition.builder()
                                                .attributeName(TEST_TIMESTAMP_PARTITION_NAME)
                                                .timestampFormat(TEST_TIMESTAMP_PARTITION_FORMAT)
                                                .build())
                                        .build()))
                        .build())
                .build();

        final CreateDatastoreResponse createDatastoreResponse = CreateDatastoreResponse.builder().build();

        final DescribeDatastoreResponse describeDatastoreResponse = DescribeDatastoreResponse.builder()
                .datastore(Datastore
                        .builder()
                        .arn(TEST_DATASTORE_ARN)
                        .name(TEST_DATASTORE_NAME)
                        .retentionPeriod(software.amazon.awssdk.services.iotanalytics.model.RetentionPeriod
                                .builder()
                                .numberOfDays(TEST_DAYS)
                                .build())
                        .storage(software.amazon.awssdk.services.iotanalytics.model.DatastoreStorage
                                .builder()
                                .customerManagedS3(CustomerManagedDatastoreS3Storage
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
                                                .columns(
                                                        software.amazon.awssdk.services.iotanalytics.model.Column
                                                                .builder()
                                                                .name(TEST_COL_NAME1)
                                                                .type(TEST_COL_TYPE1)
                                                                .build(),
                                                        software.amazon.awssdk.services.iotanalytics.model.Column
                                                                .builder()
                                                                .name(TEST_COL_NAME2)
                                                                .type(TEST_COL_TYPE2)
                                                                .build())
                                                .build())
                                        .build())
                                .build())
                        .datastorePartitions(software.amazon.awssdk.services.iotanalytics.model.DatastorePartitions
                                .builder()
                                .partitions(
                                        software.amazon.awssdk.services.iotanalytics.model.DatastorePartition.builder()
                                                .attributePartition(software.amazon.awssdk.services.iotanalytics.model.Partition.builder()
                                                        .attributeName(TEST_ATTRIBUTE_PARTITION)
                                                        .build())
                                                .build(),
                                        software.amazon.awssdk.services.iotanalytics.model.DatastorePartition.builder()
                                                .timestampPartition(software.amazon.awssdk.services.iotanalytics.model.TimestampPartition.builder()
                                                        .attributeName(TEST_TIMESTAMP_PARTITION_NAME)
                                                        .timestampFormat(TEST_TIMESTAMP_PARTITION_FORMAT)
                                                        .build())
                                                .build())
                                .build())
                        .build())
                .build();

        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder()
                .tags(software.amazon.awssdk.services.iotanalytics.model.Tag.builder().key(TEST_KEY1).value(TEST_VALUE1).build(),
                        software.amazon.awssdk.services.iotanalytics.model.Tag.builder().key(TEST_KEY2).value(TEST_VALUE2).build())
                .build();

        when(proxyClient.client().createDatastore(createDatastoreRequestArgumentCaptor.capture()))
                .thenReturn(createDatastoreResponse);
        when(proxyClient.client().describeDatastore(any(DescribeDatastoreRequest.class)))
                .thenReturn(describeDatastoreResponse);
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(listTagsForResourceResponse);

        // WHEN
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        // THEN
        verify(proxyClient.client(), times(1)).createDatastore(any(CreateDatastoreRequest.class));

        final CreateDatastoreRequest createDatastoreRequest = createDatastoreRequestArgumentCaptor.getValue();
        assertThat(createDatastoreRequest.datastoreName()).isEqualTo(TEST_DATASTORE_NAME);
        assertThat(createDatastoreRequest.retentionPeriod().numberOfDays()).isEqualTo(TEST_DAYS);
        assertThat(createDatastoreRequest.datastoreStorage().customerManagedS3().bucket()).isEqualTo(TEST_S3_BUCKET);
        assertThat(createDatastoreRequest.datastoreStorage().customerManagedS3().keyPrefix()).isEqualTo(TEST_PREFIX);
        assertThat(createDatastoreRequest.datastoreStorage().customerManagedS3().roleArn()).isEqualTo(TEST_ROLE);
        assertThat(createDatastoreRequest.datastoreStorage().serviceManagedS3()).isNull();
        assertThat(createDatastoreRequest.fileFormatConfiguration().jsonConfiguration()).isNull();
        assertThat(createDatastoreRequest.fileFormatConfiguration().parquetConfiguration().schemaDefinition().columns().size()).isEqualTo(2);
        assertThat(createDatastoreRequest.fileFormatConfiguration().parquetConfiguration()
                .schemaDefinition()
                .columns()
                .get(0).name()).isEqualTo(TEST_COL_NAME1);
        assertThat(createDatastoreRequest.fileFormatConfiguration().parquetConfiguration()
                .schemaDefinition()
                .columns()
                .get(0).type()).isEqualTo(TEST_COL_TYPE1);
        assertThat(createDatastoreRequest.fileFormatConfiguration().parquetConfiguration()
                .schemaDefinition()
                .columns()
                .get(1).name()).isEqualTo(TEST_COL_NAME2);
        assertThat(createDatastoreRequest.fileFormatConfiguration().parquetConfiguration()
                .schemaDefinition()
                .columns()
                .get(1).type()).isEqualTo(TEST_COL_TYPE2);
        assertThat(createDatastoreRequest.tags().size()).isEqualTo(2);
        assertThat(createDatastoreRequest.tags().get(0).key()).isEqualTo(TEST_KEY1);
        assertThat(createDatastoreRequest.tags().get(0).value()).isEqualTo(TEST_VALUE1);
        assertThat(createDatastoreRequest.tags().get(1).key()).isEqualTo(TEST_KEY2);
        assertThat(createDatastoreRequest.tags().get(1).value()).isEqualTo(TEST_VALUE2);
        assertThat(createDatastoreRequest.datastorePartitions().partitions().size()).isEqualTo(2);
        assertThat(createDatastoreRequest.datastorePartitions().partitions().get(0).attributePartition()
                .attributeName()).isEqualTo(TEST_ATTRIBUTE_PARTITION);
        assertThat(createDatastoreRequest.datastorePartitions().partitions().get(1).timestampPartition()
                .attributeName()).isEqualTo(TEST_TIMESTAMP_PARTITION_NAME);
        assertThat(createDatastoreRequest.datastorePartitions().partitions().get(1).timestampPartition()
                .timestampFormat()).isEqualTo(TEST_TIMESTAMP_PARTITION_FORMAT);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        assertThat(response.getResourceModel().getDatastoreName()).isEqualTo(request.getDesiredResourceState().getDatastoreName());
        assertThat(response.getResourceModel().getDatastoreStorage()).isEqualTo(request.getDesiredResourceState().getDatastoreStorage());
        assertThat(response.getResourceModel().getRetentionPeriod()).isEqualTo(request.getDesiredResourceState().getRetentionPeriod());
        assertThat(response.getResourceModel().getFileFormatConfiguration()).isEqualTo(request.getDesiredResourceState().getFileFormatConfiguration());
        assertThat(response.getResourceModel().getTags()).isEqualTo(request.getDesiredResourceState().getTags());
        assertThat(response.getResourceModel().getId()).isEqualTo(TEST_DATASTORE_ID);
    }

    @Test
    public void GIVEN_request_with_sitewise_multilayer_datastore_WHEN_call_handleRequest_THEN_return_success() {
        // GIVEN
        final ResourceModel model = ResourceModel.builder()
                .datastoreName(TEST_DATASTORE_NAME)
                .datastoreStorage(DatastoreStorage
                        .builder()
                        .iotSiteWiseMultiLayerStorage(IotSiteWiseMultiLayerStorage
                                .builder()
                                .customerManagedS3Storage(CustomerManagedS3Storage
                                        .builder()
                                        .bucket(TEST_S3_BUCKET)
                                        .keyPrefix(TEST_PREFIX)
                                        .build())
                                .build())
                        .build())
                .retentionPeriod(RetentionPeriod.builder().numberOfDays(TEST_DAYS).build())
                .tags(Arrays.asList(Tag.builder().key(TEST_KEY1).value(TEST_VALUE1).build(),
                        Tag.builder().key(TEST_KEY2).value(TEST_VALUE2).build()))
                .build();

        final CreateDatastoreResponse createDatastoreResponse = CreateDatastoreResponse.builder().build();

        final DescribeDatastoreResponse describeDatastoreResponse = DescribeDatastoreResponse.builder()
                .datastore(Datastore
                        .builder()
                        .arn(TEST_DATASTORE_ARN)
                        .name(TEST_DATASTORE_NAME)
                        .retentionPeriod(software.amazon.awssdk.services.iotanalytics.model.RetentionPeriod
                                .builder()
                                .numberOfDays(TEST_DAYS)
                                .build())
                        .storage(software.amazon.awssdk.services.iotanalytics.model.DatastoreStorage
                                .builder()
                                .iotSiteWiseMultiLayerStorage(DatastoreIotSiteWiseMultiLayerStorage
                                        .builder()
                                        .customerManagedS3Storage(IotSiteWiseCustomerManagedDatastoreS3Storage
                                                .builder()
                                                .bucket(TEST_S3_BUCKET)
                                                .keyPrefix(TEST_PREFIX)
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();

        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder()
                .tags(software.amazon.awssdk.services.iotanalytics.model.Tag.builder().key(TEST_KEY1).value(TEST_VALUE1).build(),
                        software.amazon.awssdk.services.iotanalytics.model.Tag.builder().key(TEST_KEY2).value(TEST_VALUE2).build())
                .build();

        when(proxyClient.client().createDatastore(createDatastoreRequestArgumentCaptor.capture()))
                .thenReturn(createDatastoreResponse);
        when(proxyClient.client().describeDatastore(any(DescribeDatastoreRequest.class)))
                .thenReturn(describeDatastoreResponse);
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(listTagsForResourceResponse);

        // WHEN
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        // THEN
        verify(proxyClient.client(), times(1)).createDatastore(any(CreateDatastoreRequest.class));

        final CreateDatastoreRequest createDatastoreRequest = createDatastoreRequestArgumentCaptor.getValue();
        assertThat(createDatastoreRequest.datastoreName()).isEqualTo(TEST_DATASTORE_NAME);
        assertThat(createDatastoreRequest.retentionPeriod().numberOfDays()).isEqualTo(TEST_DAYS);
        assertThat(createDatastoreRequest.datastoreStorage().iotSiteWiseMultiLayerStorage().customerManagedS3Storage().bucket()).isEqualTo(TEST_S3_BUCKET);
        assertThat(createDatastoreRequest.datastoreStorage().iotSiteWiseMultiLayerStorage().customerManagedS3Storage().keyPrefix()).isEqualTo(TEST_PREFIX);

        assertThat(createDatastoreRequest.datastoreStorage().customerManagedS3()).isNull();
        assertThat(createDatastoreRequest.datastoreStorage().serviceManagedS3()).isNull();
        assertThat(createDatastoreRequest.fileFormatConfiguration()).isNull();

        assertThat(createDatastoreRequest.tags().size()).isEqualTo(2);
        assertThat(createDatastoreRequest.tags().get(0).key()).isEqualTo(TEST_KEY1);
        assertThat(createDatastoreRequest.tags().get(0).value()).isEqualTo(TEST_VALUE1);
        assertThat(createDatastoreRequest.tags().get(1).key()).isEqualTo(TEST_KEY2);
        assertThat(createDatastoreRequest.tags().get(1).value()).isEqualTo(TEST_VALUE2);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        assertThat(response.getResourceModel().getDatastoreName()).isEqualTo(request.getDesiredResourceState().getDatastoreName());
        assertThat(response.getResourceModel().getDatastoreStorage()).isEqualTo(request.getDesiredResourceState().getDatastoreStorage());
        assertThat(response.getResourceModel().getRetentionPeriod()).isEqualTo(request.getDesiredResourceState().getRetentionPeriod());
        assertThat(response.getResourceModel().getTags()).isEqualTo(request.getDesiredResourceState().getTags());
        assertThat(response.getResourceModel().getId()).isEqualTo(TEST_DATASTORE_ID);
    }

    @Test
    public void GIVEN_request_with_id_WHEN_call_handleRequest_THEN_return_InvalidRequest() {
        // GIVEN
        final ResourceModel model = ResourceModel.builder().id(TEST_DATASTORE_ID).build();

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
        final ResourceModel model = ResourceModel.builder().datastoreName(TEST_DATASTORE_NAME).build();

        when(proxyClient.client().createDatastore(createDatastoreRequestArgumentCaptor.capture()))
                .thenThrow(ResourceAlreadyExistsException.builder().message("already exist").build());

        // WHEN / THEN
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        assertThrows(CfnAlreadyExistsException.class,
                () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));
        assertThat(createDatastoreRequestArgumentCaptor.getValue().datastoreName()).isEqualTo(TEST_DATASTORE_NAME);
    }
}
