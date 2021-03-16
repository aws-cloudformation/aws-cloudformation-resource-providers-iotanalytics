package com.amazonaws.iotanalytics.channel;

import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import software.amazon.awssdk.services.iotanalytics.model.Channel;
import software.amazon.awssdk.services.iotanalytics.model.CreateChannelRequest;
import software.amazon.awssdk.services.iotanalytics.model.CreateChannelResponse;
import software.amazon.awssdk.services.iotanalytics.model.CustomerManagedChannelS3Storage;
import software.amazon.awssdk.services.iotanalytics.model.DescribeChannelRequest;
import software.amazon.awssdk.services.iotanalytics.model.DescribeChannelResponse;
import software.amazon.awssdk.services.iotanalytics.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.iotanalytics.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.iotanalytics.model.ResourceAlreadyExistsException;
import software.amazon.cloudformation.exceptions.CfnAlreadyExistsException;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CreateHandlerTest extends AbstractTestBase {

    private static final String TEST_CHANNEL_ARN = "test_channel_arn";
    private static final String TEST_CHANNEL_ID = TEST_CHANNEL_ARN;
    private static final String TEST_CHANNEL_NAME = "test_channel_name";
    private static final int TEST_DAYS = 10;
    private static final String TEST_S3_BUCKET = "test_s3_bucket";
    private static final String TEST_PREFIX = "test_prefix/";
    private static final String TEST_ROLE = "arn:aws:iam::1234567890:role/Test-Role";
    private static final String TEST_KEY1 = "key1";
    private static final String TEST_VALUE1 = "value1";
    private static final String TEST_KEY2 = "key2";
    private static final String TEST_VALUE2 = "value2";

    private CreateHandler handler;

    @Captor
    private ArgumentCaptor<CreateChannelRequest> createChannelRequestCaptor;

    @BeforeEach
    public void setup() {
        super.setUp();
        handler = new CreateHandler();
    }

    @Test
    public void GIVEN_request_WHEN_call_handleRequest_THEN_return_success() {
        // GIVEN
        final ResourceModel model = ResourceModel.builder()
                .channelName(TEST_CHANNEL_NAME)
                .channelStorage(ChannelStorage
                        .builder()
                        .customerManagedS3(CustomerManagedS3
                                .builder()
                                .bucket(TEST_S3_BUCKET)
                                .keyPrefix(TEST_PREFIX)
                                .roleArn(TEST_ROLE)
                                .build())
                        .build())
                .retentionPeriod(RetentionPeriod.builder().numberOfDays(TEST_DAYS).build())
                .tags(Arrays.asList(Tag.builder().key(TEST_KEY1).value(TEST_VALUE1).build(),
                        Tag.builder().key(TEST_KEY2).value(TEST_VALUE2).build()))
                .build();

        final CreateChannelResponse createChannelResponse = CreateChannelResponse.builder().build();

        final DescribeChannelResponse describeChannelResponse = DescribeChannelResponse.builder()
                .channel(Channel
                        .builder()
                        .arn(TEST_CHANNEL_ARN)
                        .name(TEST_CHANNEL_NAME)
                        .retentionPeriod(software.amazon.awssdk.services.iotanalytics.model.RetentionPeriod
                                .builder()
                                .numberOfDays(TEST_DAYS)
                                .build())
                        .storage(software.amazon.awssdk.services.iotanalytics.model.ChannelStorage
                                .builder()
                                .customerManagedS3(CustomerManagedChannelS3Storage
                                        .builder()
                                        .bucket(TEST_S3_BUCKET)
                                        .keyPrefix(TEST_PREFIX)
                                        .roleArn(TEST_ROLE)
                                        .build())
                                .build())
                        .build())
                .build();

        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder()
                .tags(software.amazon.awssdk.services.iotanalytics.model.Tag.builder().key(TEST_KEY1).value(TEST_VALUE1).build(),
                        software.amazon.awssdk.services.iotanalytics.model.Tag.builder().key(TEST_KEY2).value(TEST_VALUE2).build())
                .build();

        when(proxyClient.client().createChannel(createChannelRequestCaptor.capture())).thenReturn(createChannelResponse);
        when(proxyClient.client().describeChannel(any(DescribeChannelRequest.class))).thenReturn(describeChannelResponse);
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class))).thenReturn(listTagsForResourceResponse);

        // WHEN
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        // THEN
        verify(proxyClient.client(), times(1)).createChannel(any(CreateChannelRequest.class));

        final CreateChannelRequest createChannelRequest = createChannelRequestCaptor.getValue();
        assertThat(createChannelRequest.channelName()).isEqualTo(TEST_CHANNEL_NAME);
        assertThat(createChannelRequest.retentionPeriod().numberOfDays()).isEqualTo(TEST_DAYS);
        assertThat(createChannelRequest.channelStorage().customerManagedS3().bucket()).isEqualTo(TEST_S3_BUCKET);
        assertThat(createChannelRequest.channelStorage().customerManagedS3().keyPrefix()).isEqualTo(TEST_PREFIX);
        assertThat(createChannelRequest.channelStorage().customerManagedS3().roleArn()).isEqualTo(TEST_ROLE);
        assertThat(createChannelRequest.channelStorage().serviceManagedS3()).isNull();
        assertThat(createChannelRequest.tags().size()).isEqualTo(2);
        assertThat(createChannelRequest.tags().get(0).key()).isEqualTo(TEST_KEY1);
        assertThat(createChannelRequest.tags().get(0).value()).isEqualTo(TEST_VALUE1);
        assertThat(createChannelRequest.tags().get(1).key()).isEqualTo(TEST_KEY2);
        assertThat(createChannelRequest.tags().get(1).value()).isEqualTo(TEST_VALUE2);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        assertThat(response.getResourceModel().getChannelName()).isEqualTo(request.getDesiredResourceState().getChannelName());
        assertThat(response.getResourceModel().getChannelStorage()).isEqualTo(request.getDesiredResourceState().getChannelStorage());
        assertThat(response.getResourceModel().getRetentionPeriod()).isEqualTo(request.getDesiredResourceState().getRetentionPeriod());
        assertThat(response.getResourceModel().getTags()).isEqualTo(request.getDesiredResourceState().getTags());
        assertThat(response.getResourceModel().getId()).isEqualTo(TEST_CHANNEL_ID);
    }


    @Test
    public void GIVEN_request_with_id_WHEN_call_handleRequest_THEN_return_InvalidRequest() {
        // GIVEN
        final ResourceModel model = ResourceModel.builder().id(TEST_CHANNEL_ID).build();

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
        final ResourceModel model = ResourceModel.builder().channelName(TEST_CHANNEL_NAME).build();

        when(proxyClient.client().createChannel(createChannelRequestCaptor.capture()))
                .thenThrow(ResourceAlreadyExistsException.builder().message("already exist").build());

        // WHEN / THEN
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        assertThrows(CfnAlreadyExistsException.class,
                () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));
        assertThat(createChannelRequestCaptor.getValue().channelName()).isEqualTo(TEST_CHANNEL_NAME);
    }
}
