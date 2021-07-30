package com.amazonaws.iotanalytics.datastore;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.junit.jupiter.MockitoExtension;

import software.amazon.awssdk.services.iotanalytics.model.Datastore;
import software.amazon.awssdk.services.iotanalytics.model.DescribeDatastoreRequest;
import software.amazon.awssdk.services.iotanalytics.model.DescribeDatastoreResponse;
import software.amazon.awssdk.services.iotanalytics.model.InvalidRequestException;
import software.amazon.awssdk.services.iotanalytics.model.JsonConfiguration;
import software.amazon.awssdk.services.iotanalytics.model.LimitExceededException;
import software.amazon.awssdk.services.iotanalytics.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.iotanalytics.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.iotanalytics.model.ServiceManagedDatastoreS3Storage;
import software.amazon.awssdk.services.iotanalytics.model.ServiceUnavailableException;
import software.amazon.awssdk.services.iotanalytics.model.TagResourceRequest;
import software.amazon.awssdk.services.iotanalytics.model.TagResourceResponse;
import software.amazon.awssdk.services.iotanalytics.model.UntagResourceRequest;
import software.amazon.awssdk.services.iotanalytics.model.UntagResourceResponse;

import software.amazon.awssdk.services.iotanalytics.model.UpdateDatastoreRequest;
import software.amazon.awssdk.services.iotanalytics.model.UpdateDatastoreResponse;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnNotUpdatableException;
import software.amazon.cloudformation.exceptions.CfnServiceLimitExceededException;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class UpdateHandlerTest extends AbstractTestBase {
    private static final String TEST_DATASTORE_ARN = "test_datastore_arn";
    private static final String TEST_DATASTORE_ID = TEST_DATASTORE_ARN;
    private static final String TEST_DATASTORE_NAME = "test_datastore_name";
    private static final int TEST_DAYS = 10;
    private static final String TEST_KEY1 = "key1";
    private static final String TEST_VALUE1 = "value1";
    private static final String TEST_KEY2 = "key2";
    private static final String TEST_VALUE2 = "value2";
    private static final String TEST_VALUE22 = "value22";
    private static final String TEST_KEY3 = "key3";
    private static final String TEST_VALUE3 = "value3";

    @Captor
    private ArgumentCaptor<UpdateDatastoreRequest> updateDatastoreRequestArgumentCaptor;

    @Captor
    private ArgumentCaptor<TagResourceRequest> tagResourceRequestArgumentCaptor;

    @Captor
    private ArgumentCaptor<UntagResourceRequest> untagResourceRequestArgumentCaptor;

    @Captor
    private ArgumentCaptor<DescribeDatastoreRequest> describeDatastoreRequestArgumentCaptor;

    @Captor
    private ArgumentCaptor<ListTagsForResourceRequest> listTagsForResourceRequestArgumentCaptor;

    private UpdateHandler handler;
    private ResourceModel preModel;
    private ResourceModel newModel;
    private DescribeDatastoreResponse describeDatastoreResponseFull;
    private ListTagsForResourceResponse listTagsForResourceResponseFull;
    private DescribeDatastoreResponse describeDatastoreResponseSimple;
    private ListTagsForResourceResponse listTagsForResourceResponseSimple;

    @BeforeEach
    public void setup() {
        super.setUp();
        handler = new UpdateHandler();

        preModel = ResourceModel.builder()
                .datastoreName(TEST_DATASTORE_NAME)
                .id(TEST_DATASTORE_ID)
                .retentionPeriod(RetentionPeriod.builder().numberOfDays(TEST_DAYS).build())
                .tags(Arrays.asList(Tag.builder().key(TEST_KEY1).value(TEST_VALUE1).build(),
                        Tag.builder().key(TEST_KEY2).value(TEST_VALUE2).build()))
                .datastorePartitions(DatastorePartitions
                        .builder()
                        .partitions(Arrays.asList(
                                DatastorePartition.builder()
                                        .partition(Partition.builder()
                                                .attributeName("myAttribute")
                                                .build())
                                        .build()))
                        .build())
                .build();

        newModel = ResourceModel.builder()
                .datastoreName(TEST_DATASTORE_NAME)
                .datastoreStorage(DatastoreStorage
                        .builder()
                        .serviceManagedS3(new HashMap<>())
                        .build())
                .fileFormatConfiguration(FileFormatConfiguration.builder().jsonConfiguration(new HashMap<>()).build())
                .tags(Arrays.asList(Tag.builder().key(TEST_KEY3).value(TEST_VALUE3).build(),
                        Tag.builder().key(TEST_KEY2).value(TEST_VALUE22).build()))
                .build();

        describeDatastoreResponseFull = DescribeDatastoreResponse.builder().datastore(
                Datastore.builder()
                        .name(TEST_DATASTORE_NAME)
                        .arn(TEST_DATASTORE_ARN)
                        .storage(software.amazon.awssdk.services.iotanalytics.model.DatastoreStorage
                                .builder()
                                .serviceManagedS3(ServiceManagedDatastoreS3Storage.builder().build())
                                .build())
                        .retentionPeriod(software.amazon.awssdk.services.iotanalytics.model.RetentionPeriod
                                .builder()
                                .numberOfDays(TEST_DAYS)
                                .build())
                        .fileFormatConfiguration(software.amazon.awssdk.services.iotanalytics.model.FileFormatConfiguration
                                .builder()
                                .jsonConfiguration(JsonConfiguration.builder().build())
                                .build())
                        .build())
                .build();

        listTagsForResourceResponseFull = ListTagsForResourceResponse.builder()
                .tags(Arrays.asList(software.amazon.awssdk.services.iotanalytics.model.Tag.builder().key(TEST_KEY1).value(TEST_VALUE1).build(),
                        software.amazon.awssdk.services.iotanalytics.model.Tag.builder().key(TEST_KEY2).value(TEST_VALUE22).build(),
                        software.amazon.awssdk.services.iotanalytics.model.Tag.builder().key(TEST_KEY3).value(TEST_VALUE3).build()))
                .build();

        describeDatastoreResponseSimple = DescribeDatastoreResponse.builder()
                .datastore(Datastore.builder().name(TEST_DATASTORE_NAME).build())
                .build();

        listTagsForResourceResponseSimple = ListTagsForResourceResponse.builder().build();
    }

    @Test
    public void GIVEN_request_WHEN_call_handleRequest_THEN_return_success() {

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(newModel)
                .previousResourceState(preModel)
                .build();

        when(proxyClient.client().updateDatastore(updateDatastoreRequestArgumentCaptor.capture())).thenReturn(UpdateDatastoreResponse.builder().build());

        when(proxyClient.client().untagResource(untagResourceRequestArgumentCaptor.capture())).thenReturn(UntagResourceResponse.builder().build());

        when(proxyClient.client().tagResource(tagResourceRequestArgumentCaptor.capture())).thenReturn(TagResourceResponse.builder().build());

        when(proxyClient.client().describeDatastore(describeDatastoreRequestArgumentCaptor.capture())).thenReturn(describeDatastoreResponseFull);

        when(proxyClient.client().listTagsForResource(listTagsForResourceRequestArgumentCaptor.capture())).thenReturn(listTagsForResourceResponseFull);

        // WHEN
        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        // THEN
        final UpdateDatastoreRequest updateDatastoreRequest = updateDatastoreRequestArgumentCaptor.getValue();
        assertThat(updateDatastoreRequest.datastoreName()).isEqualTo(TEST_DATASTORE_NAME);
        assertThat(updateDatastoreRequest.datastoreStorage().serviceManagedS3()).isNotNull();
        assertThat(updateDatastoreRequest.datastoreStorage().customerManagedS3()).isNull();
        assertThat(updateDatastoreRequest.retentionPeriod()).isNull();
        assertThat(updateDatastoreRequest.fileFormatConfiguration().parquetConfiguration()).isNull();
        assertThat(updateDatastoreRequest.fileFormatConfiguration().jsonConfiguration()).isNotNull();

        final TagResourceRequest tagResourceRequest = tagResourceRequestArgumentCaptor.getValue();
        assertThat(tagResourceRequest.resourceArn()).isEqualTo(TEST_DATASTORE_ARN);
        assertThat(tagResourceRequest.tags().size()).isEqualTo(2);
        final List<software.amazon.awssdk.services.iotanalytics.model.Tag> tagsToAdd = new ArrayList<>(tagResourceRequest.tags());
        tagsToAdd.sort(Comparator.comparing(software.amazon.awssdk.services.iotanalytics.model.Tag::key));
        assertThat(tagsToAdd.get(0).key()).isEqualTo(TEST_KEY2);
        assertThat(tagsToAdd.get(0).value()).isEqualTo(TEST_VALUE22);
        assertThat(tagsToAdd.get(1).key()).isEqualTo(TEST_KEY3);
        assertThat(tagsToAdd.get(1).key()).isEqualTo(TEST_KEY3);

        final UntagResourceRequest untagResourceRequest = untagResourceRequestArgumentCaptor.getValue();
        assertThat(untagResourceRequest.resourceArn()).isEqualTo(TEST_DATASTORE_ARN);
        assertThat(untagResourceRequest.tagKeys().size()).isEqualTo(1);
        assertThat(untagResourceRequest.tagKeys().contains(TEST_KEY1)).isTrue();

        assertThat(describeDatastoreRequestArgumentCaptor.getValue().datastoreName()).isEqualTo(TEST_DATASTORE_NAME);

        assertThat(listTagsForResourceRequestArgumentCaptor.getValue().resourceArn()).isEqualTo(TEST_DATASTORE_ARN);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        assertThat(response.getResourceModel().getDatastoreName()).isEqualTo(TEST_DATASTORE_NAME);
        assertThat(response.getResourceModel().getId()).isEqualTo(TEST_DATASTORE_ID);
        assertThat(response.getResourceModel().getDatastoreStorage().getCustomerManagedS3()).isNull();
        assertThat(response.getResourceModel().getDatastoreStorage().getServiceManagedS3()).isNotNull();
        assertThat(response.getResourceModel().getRetentionPeriod().getNumberOfDays()).isEqualTo(TEST_DAYS);
        assertThat(response.getResourceModel().getFileFormatConfiguration().getParquetConfiguration()).isNull();
        assertThat(response.getResourceModel().getFileFormatConfiguration().getJsonConfiguration()).isNotNull();

        assertThat(response.getResourceModel().getTags().size()).isEqualTo(3);
        assertThat(response.getResourceModel().getTags().get(0).getKey()).isEqualTo(TEST_KEY1);
        assertThat(response.getResourceModel().getTags().get(0).getValue()).isEqualTo(TEST_VALUE1);
        assertThat(response.getResourceModel().getTags().get(1).getKey()).isEqualTo(TEST_KEY2);
        assertThat(response.getResourceModel().getTags().get(1).getValue()).isEqualTo(TEST_VALUE22);
        assertThat(response.getResourceModel().getTags().get(2).getKey()).isEqualTo(TEST_KEY3);
        assertThat(response.getResourceModel().getTags().get(2).getValue()).isEqualTo(TEST_VALUE3);
    }

    @Test
    public void GIVEN_update_diff_datastore_name_WHEN_call_handleRequest_THEN_throw_CfnNotUpdatableException() {
        // GIVEN
        final ResourceModel preModel = ResourceModel.builder().datastoreName("name1").build();
        final ResourceModel newModel = ResourceModel.builder().datastoreName("name2").build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(newModel)
                .previousResourceState(preModel)
                .build();

        // WHEN / THEN
        assertThrows(CfnNotUpdatableException.class,
                () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));

        verify(proxyClient.client(), never()).updateDatastore(any(UpdateDatastoreRequest.class));
        verify(proxyClient.client(), never()).untagResource(any(UntagResourceRequest.class));
        verify(proxyClient.client(), never()).tagResource(any(TagResourceRequest.class));
        verify(proxyClient.client(), never()).describeDatastore(any(DescribeDatastoreRequest.class));
        verify(proxyClient.client(), never()).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void GIVEN_update_datastore_arn_WHEN_call_handleRequest_THEN_throw_CfnNotUpdatableException() {
        // GIVEN
        final ResourceModel preModel = ResourceModel.builder().datastoreName("name1").id("id1").build();
        final ResourceModel newModel = ResourceModel.builder().datastoreName("name1").id("id2").build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(newModel)
                .previousResourceState(preModel)
                .build();

        // WHEN / THEN
        assertThrows(CfnNotUpdatableException.class,
                () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));

        verify(proxyClient.client(), never()).updateDatastore(any(UpdateDatastoreRequest.class));
        verify(proxyClient.client(), never()).untagResource(any(UntagResourceRequest.class));
        verify(proxyClient.client(), never()).tagResource(any(TagResourceRequest.class));
        verify(proxyClient.client(), never()).describeDatastore(any(DescribeDatastoreRequest.class));
        verify(proxyClient.client(), never()).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void GIVEN_update_partitions_same_array_size_different_items_WHEN_call_handleRequest_THEN_return_failed_invalid_request() {
        // GIVEN
        final ResourceModel preModel = ResourceModel.builder().datastoreName("name1")
                .datastorePartitions(DatastorePartitions
                        .builder()
                        .partitions(Arrays.asList(
                                DatastorePartition.builder()
                                        .partition(Partition.builder()
                                                .attributeName("Attribute1")
                                                .build())
                                        .build(),
                                DatastorePartition.builder()
                                        .timestampPartition(TimestampPartition.builder()
                                                .attributeName("timeAttribute")
                                                .timestampFormat("yyyy-MM-dd HH:mm:ss")
                                                .build())
                                        .build()))
                        .build()).build();
        final ResourceModel newModel = ResourceModel.builder().datastoreName("name1")
                .datastorePartitions(DatastorePartitions
                        .builder()
                        .partitions(Arrays.asList(
                                DatastorePartition.builder()
                                        .partition(Partition.builder()
                                                .attributeName("DifferentAttribute")
                                                .build())
                                        .build(),
                                DatastorePartition.builder()
                                        .timestampPartition(TimestampPartition.builder()
                                                .attributeName("timeAttribute")
                                                .timestampFormat("yyyy-MM-dd HH:mm:ss")
                                                .build())
                                        .build()))
                        .build()).build();

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

        verify(proxyClient.client(), never()).updateDatastore(any(UpdateDatastoreRequest.class));
        verify(proxyClient.client(), never()).untagResource(any(UntagResourceRequest.class));
        verify(proxyClient.client(), never()).tagResource(any(TagResourceRequest.class));
        verify(proxyClient.client(), never()).describeDatastore(any(DescribeDatastoreRequest.class));
        verify(proxyClient.client(), never()).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void GIVEN_update_partitions_different_array_size_WHEN_call_handleRequest_THEN_throw_CfnNotUpdatableException() {
        // GIVEN
        final ResourceModel preModel = ResourceModel.builder().datastoreName("name1")
                .datastorePartitions(DatastorePartitions
                        .builder()
                        .partitions(Arrays.asList(
                                DatastorePartition.builder()
                                        .partition(Partition.builder()
                                                .attributeName("myAttribute")
                                                .build())
                                        .build(),
                                DatastorePartition.builder()
                                        .partition(Partition.builder()
                                                .attributeName("myAttribute2")
                                                .build())
                                        .build()))
                        .build()).build();
        final ResourceModel newModel = ResourceModel.builder().datastoreName("name1")
                .datastorePartitions(DatastorePartitions
                        .builder()
                        .partitions(Arrays.asList(
                                DatastorePartition.builder()
                                        .partition(Partition.builder()
                                                .attributeName("myAttribute")
                                                .build())
                                        .build()))
                        .build()).build();

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

        verify(proxyClient.client(), never()).updateDatastore(any(UpdateDatastoreRequest.class));
        verify(proxyClient.client(), never()).untagResource(any(UntagResourceRequest.class));
        verify(proxyClient.client(), never()).tagResource(any(TagResourceRequest.class));
        verify(proxyClient.client(), never()).describeDatastore(any(DescribeDatastoreRequest.class));
        verify(proxyClient.client(), never()).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void GIVEN_update_same_datastore_arn_WHEN_call_handleRequest_THEN_return_success() {
        // GIVEN
        final ResourceModel preModel = ResourceModel.builder().datastoreName(TEST_DATASTORE_NAME).id(TEST_DATASTORE_ID).build();
        final ResourceModel newModel = ResourceModel.builder().datastoreName(TEST_DATASTORE_NAME).id(TEST_DATASTORE_ID).build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(newModel)
                .previousResourceState(preModel)
                .build();

        when(proxyClient.client().describeDatastore(any(DescribeDatastoreRequest.class))).thenReturn(describeDatastoreResponseSimple);
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class))).thenReturn(listTagsForResourceResponseSimple);

        // WHEN
        final ProgressEvent<ResourceModel, CallbackContext> response =
                handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        // THEN
        verify(proxyClient.client(), times(1)).updateDatastore(any(UpdateDatastoreRequest.class));
        verify(proxyClient.client(), never()).untagResource(any(UntagResourceRequest.class));
        verify(proxyClient.client(), never()).tagResource(any(TagResourceRequest.class));
        verify(proxyClient.client(), times(2)).describeDatastore(any(DescribeDatastoreRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
        assertThat(response.getResourceModel().getTags()).isNull();
        assertThat(response.getResourceModel().getDatastoreName()).isEqualTo(TEST_DATASTORE_NAME);
        assertThat(response.getResourceModel().getDatastoreStorage().getCustomerManagedS3()).isNull();
        assertThat(response.getResourceModel().getDatastoreStorage().getServiceManagedS3()).isNotNull();
        assertThat(response.getResourceModel().getRetentionPeriod()).isNull();
        assertThat(response.getResourceModel().getFileFormatConfiguration().getJsonConfiguration()).isNotNull();
        assertThat(response.getResourceModel().getFileFormatConfiguration().getParquetConfiguration()).isNull();

    }

    @Test
    public void GIVEN_iota_update_exception_WHEN_call_handleRequest_THEN_throw_Exception() {
        // GIVEN
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(newModel)
                .previousResourceState(preModel)
                .build();

        when(proxyClient.client().updateDatastore(updateDatastoreRequestArgumentCaptor.capture())).thenThrow(InvalidRequestException.builder().build());

        // WHEN / THEN
        assertThrows(CfnInvalidRequestException.class,
                () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));

        assertThat(updateDatastoreRequestArgumentCaptor.getValue().datastoreName()).isEqualTo(TEST_DATASTORE_NAME);
        verify(proxyClient.client(), times(1)).updateDatastore(any(UpdateDatastoreRequest.class));
        verify(proxyClient.client(), never()).untagResource(any(UntagResourceRequest.class));
        verify(proxyClient.client(), never()).tagResource(any(TagResourceRequest.class));
        verify(proxyClient.client(), never()).describeDatastore(any(DescribeDatastoreRequest.class));
        verify(proxyClient.client(), never()).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void GIVEN_iota_untag_exception_WHEN_call_handleRequest_THEN_throw_Exception() {
        // GIVEN
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(newModel)
                .previousResourceState(preModel)
                .build();

        when(proxyClient.client().updateDatastore(any(UpdateDatastoreRequest.class))).thenReturn(UpdateDatastoreResponse.builder().build());
        when(proxyClient.client().untagResource(any(UntagResourceRequest.class))).thenThrow(LimitExceededException.builder().build());
        when(proxyClient.client().describeDatastore(any(DescribeDatastoreRequest.class))).thenReturn(describeDatastoreResponseSimple);

        // WHEN / THEN
        assertThrows(CfnServiceLimitExceededException.class,
                () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));

        verify(proxyClient.client(), times(1)).updateDatastore(any(UpdateDatastoreRequest.class));
        verify(proxyClient.client(), times(1)).untagResource(any(UntagResourceRequest.class));
        verify(proxyClient.client(), times(1)).describeDatastore(any(DescribeDatastoreRequest.class));
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

        when(proxyClient.client().updateDatastore(any(UpdateDatastoreRequest.class))).thenReturn(UpdateDatastoreResponse.builder().build());
        when(proxyClient.client().untagResource(any(UntagResourceRequest.class))).thenReturn(UntagResourceResponse.builder().build());
        when(proxyClient.client().tagResource(any(TagResourceRequest.class))).thenThrow(ServiceUnavailableException.builder().build());
        when(proxyClient.client().describeDatastore(any(DescribeDatastoreRequest.class))).thenReturn(describeDatastoreResponseSimple);

        // WHEN / THEN
        assertThrows(CfnGeneralServiceException.class,
                () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));

        verify(proxyClient.client(), times(1)).updateDatastore(any(UpdateDatastoreRequest.class));
        verify(proxyClient.client(), times(1)).untagResource(any(UntagResourceRequest.class));
        verify(proxyClient.client(), times(1)).tagResource(any(TagResourceRequest.class));
        verify(proxyClient.client(), times(1)).describeDatastore(any(DescribeDatastoreRequest.class));
        verify(proxyClient.client(), never()).listTagsForResource(any(ListTagsForResourceRequest.class));
    }
}
