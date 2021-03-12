package com.amazonaws.iotanalytics.channel;

import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import software.amazon.awssdk.services.iotanalytics.model.Channel;
import software.amazon.awssdk.services.iotanalytics.model.CustomerManagedChannelS3Storage;
import software.amazon.awssdk.services.iotanalytics.model.DescribeChannelRequest;
import software.amazon.awssdk.services.iotanalytics.model.DescribeChannelResponse;
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


import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ReadHandlerTest extends AbstractTestBase {
    private static final String TEST_CHANNEL_ARN = "test_channel_arn";
    private static final String TEST_CHANNEL_ID = TEST_CHANNEL_ARN;
    private static final String TEST_CHANNEL_NAME = "test_channel_name";
    private static final String TEST_S3_BUCKET = "test_s3_bucket";
    private static final String TEST_PREFIX = "test_prefix";
    private static final String TEST_ROLE = "test_role";
    private static final String TEST_KEY1 = "key1";
    private static final String TEST_VALUE1 = "value1";
    private static final String TEST_KEY2 = "key2";
    private static final String TEST_VALUE2 = "value2";

    @Captor
    private ArgumentCaptor<DescribeChannelRequest> describeChannelRequestArgumentCaptor;

    @Captor
    private ArgumentCaptor<ListTagsForResourceRequest> listTagsForResourceRequestArgumentCaptor;

    private ReadHandler handler;

    private DescribeChannelResponse describeChannelResponse;

    @BeforeEach
    public void setup() {
        super.setUp();
        handler = new ReadHandler();

        describeChannelResponse = DescribeChannelResponse
                .builder()
                .channel(
                        Channel
                                .builder()
                                .arn(TEST_CHANNEL_ARN)
                                .retentionPeriod(software.amazon.awssdk.services.iotanalytics.model.RetentionPeriod
                                        .builder()
                                        .unlimited(true)
                                        .build())
                                .storage(software.amazon.awssdk.services.iotanalytics.model.ChannelStorage
                                        .builder()
                                        .customerManagedS3(
                                                CustomerManagedChannelS3Storage
                                                        .builder()
                                                        .bucket(TEST_S3_BUCKET)
                                                        .keyPrefix(TEST_PREFIX)
                                                        .roleArn(TEST_ROLE)
                                                        .build())
                                        .build())
                                .name(TEST_CHANNEL_NAME)
                                .build())
                .build();
    }

    @Test
    public void GIVEN_iota_good_WHEN_call_handleRequest_THEN_return_success() {
        // GIVEN
        final ResourceModel model = ResourceModel.builder().channelName(TEST_CHANNEL_NAME).build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder()
                .tags(software.amazon.awssdk.services.iotanalytics.model.Tag.builder().key(TEST_KEY1).value(TEST_VALUE1).build(),
                        software.amazon.awssdk.services.iotanalytics.model.Tag.builder().key(TEST_KEY2).value(TEST_VALUE2).build())
                .build();

        when(proxyClient.client().describeChannel(describeChannelRequestArgumentCaptor.capture())).thenReturn(describeChannelResponse);
        when(proxyClient.client().listTagsForResource(listTagsForResourceRequestArgumentCaptor.capture())).thenReturn(listTagsForResourceResponse);

        // WHEN
        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        // THEN
        assertThat(describeChannelRequestArgumentCaptor.getValue().channelName()).isEqualTo(TEST_CHANNEL_NAME);
        assertThat(listTagsForResourceRequestArgumentCaptor.getValue().resourceArn()).isEqualTo(TEST_CHANNEL_ARN);
        verify(proxyClient.client(), times(1)).describeChannel(any(DescribeChannelRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        assertThat(response.getResourceModel().getChannelName()).isEqualTo(TEST_CHANNEL_NAME);
        assertThat(response.getResourceModel().getId()).isEqualTo(TEST_CHANNEL_ID);
        assertThat(response.getResourceModel().getRetentionPeriod().getUnlimited()).isTrue();
        assertThat(response.getResourceModel().getRetentionPeriod().getNumberOfDays()).isNull();
        assertThat(response.getResourceModel().getChannelStorage().getServiceManagedS3()).isNull();
        assertThat(response.getResourceModel().getChannelStorage().getCustomerManagedS3().getBucket()).isEqualTo(TEST_S3_BUCKET);
        assertThat(response.getResourceModel().getChannelStorage().getCustomerManagedS3().getKeyPrefix()).isEqualTo(TEST_PREFIX);
        assertThat(response.getResourceModel().getChannelStorage().getCustomerManagedS3().getRoleArn()).isEqualTo(TEST_ROLE);
        assertThat(response.getResourceModel().getTags().size()).isEqualTo(2);
        assertThat(response.getResourceModel().getTags().get(0).getKey()).isEqualTo(TEST_KEY1);
        assertThat(response.getResourceModel().getTags().get(0).getValue()).isEqualTo(TEST_VALUE1);
        assertThat(response.getResourceModel().getTags().get(1).getKey()).isEqualTo(TEST_KEY2);
        assertThat(response.getResourceModel().getTags().get(1).getValue()).isEqualTo(TEST_VALUE2);
    }

    @Test
    public void GIVEN_iota_describe_exception_WHEN_call_handleRequest_THEN_throw_exception() {
        // GIVEN
        final ResourceModel model = ResourceModel.builder().channelName(TEST_CHANNEL_NAME).build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final ResourceNotFoundException resourceNotFoundException = ResourceNotFoundException.builder().build();
        when(proxyClient.client().describeChannel(describeChannelRequestArgumentCaptor.capture())).thenThrow(resourceNotFoundException);

        // WHEN / THEN
        assertThrows(CfnNotFoundException.class,
                () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));
        assertThat(describeChannelRequestArgumentCaptor.getValue().channelName()).isEqualTo(TEST_CHANNEL_NAME);
        verify(proxyClient.client(), never()).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void GIVEN_iota_list_tags_exception_WHEN_call_handleRequest_THEN_throw_exception() {
        // GIVEN
        final ResourceModel model = ResourceModel.builder().channelName(TEST_CHANNEL_NAME).build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        when(proxyClient.client().describeChannel(describeChannelRequestArgumentCaptor.capture())).thenReturn(describeChannelResponse);

        when(proxyClient.client().listTagsForResource(listTagsForResourceRequestArgumentCaptor.capture())).thenThrow(InternalFailureException.builder().build());

        // WHEN / THEN
        assertThrows(CfnServiceInternalErrorException.class,
                () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));
        assertThat(describeChannelRequestArgumentCaptor.getValue().channelName()).isEqualTo(TEST_CHANNEL_NAME);
        assertThat(listTagsForResourceRequestArgumentCaptor.getValue().resourceArn()).isEqualTo(TEST_CHANNEL_ARN);

        verify(proxyClient.client(), times(1)).describeChannel(any(DescribeChannelRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
    }
}
