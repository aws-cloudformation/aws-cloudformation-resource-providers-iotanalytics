package com.amazonaws.iotanalytics.channel;

import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import software.amazon.awssdk.services.iotanalytics.model.DeleteChannelRequest;
import software.amazon.awssdk.services.iotanalytics.model.DeleteChannelResponse;
import software.amazon.awssdk.services.iotanalytics.model.DescribeChannelRequest;
import software.amazon.awssdk.services.iotanalytics.model.DescribeChannelResponse;
import software.amazon.awssdk.services.iotanalytics.model.ResourceNotFoundException;
import software.amazon.awssdk.services.iotanalytics.model.ServiceUnavailableException;
import software.amazon.awssdk.services.iotanalytics.model.ThrottlingException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnThrottlingException;
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
public class DeleteHandlerTest extends AbstractTestBase {
    private static final String TEST_CHANNEL_NAME = "test_channel_name";

    private DeleteHandler handler;

    @Captor
    private ArgumentCaptor<DeleteChannelRequest> deleteChannelRequestArgumentCaptor;

    @Captor
    private ArgumentCaptor<DescribeChannelRequest> describeChannelRequestArgumentCaptor;

    @BeforeEach
    public void setup() {
        super.setUp();
        handler =  new DeleteHandler();
    }

    @Test
    public void GIVEN_delete_success_WHEN_call_handleRequest_THEN_return_success() {
        // GIVEN
        final ResourceModel model = ResourceModel.builder().channelName(TEST_CHANNEL_NAME).build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        final DeleteChannelResponse deleteChannelResponse = DeleteChannelResponse.builder().build();
        when(proxyClient.client().deleteChannel(deleteChannelRequestArgumentCaptor.capture())).thenReturn(deleteChannelResponse);

        final ResourceNotFoundException resourceNotFoundException = ResourceNotFoundException.builder().build();
        when(proxyClient.client().describeChannel(describeChannelRequestArgumentCaptor.capture())).thenThrow(resourceNotFoundException);

        // WHEN
        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        // THEN

        assertThat(deleteChannelRequestArgumentCaptor.getValue().channelName()).isEqualTo(TEST_CHANNEL_NAME);
        assertThat(describeChannelRequestArgumentCaptor.getValue().channelName()).isEqualTo(TEST_CHANNEL_NAME);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(1)).deleteChannel(any(DeleteChannelRequest.class));
        verify(proxyClient.client(), times(1)).describeChannel(any(DescribeChannelRequest.class));
    }

    @Test
    public void GIVEN_delete_stabilize_WHEN_call_handleRequest_THEN_return_success() {
        // GIVEN
        final ResourceModel model = ResourceModel.builder().channelName(TEST_CHANNEL_NAME).build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final DeleteChannelResponse deleteChannelResponse = DeleteChannelResponse.builder().build();
        when(proxyClient.client().deleteChannel(deleteChannelRequestArgumentCaptor.capture())).thenReturn(deleteChannelResponse);

        final DescribeChannelResponse describeChannelResponse = DescribeChannelResponse.builder().build();
        final ResourceNotFoundException resourceNotFoundException = ResourceNotFoundException.builder().build();
        when(proxyClient.client().describeChannel(describeChannelRequestArgumentCaptor.capture()))
                .thenReturn(describeChannelResponse)
                .thenThrow(resourceNotFoundException);

        // WHEN
        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        // THEN
        assertThat(deleteChannelRequestArgumentCaptor.getValue().channelName()).isEqualTo(TEST_CHANNEL_NAME);
        assertThat(describeChannelRequestArgumentCaptor.getValue().channelName()).isEqualTo(TEST_CHANNEL_NAME);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(1)).deleteChannel(any(DeleteChannelRequest.class));
        verify(proxyClient.client(), times(2)).describeChannel(any(DescribeChannelRequest.class));

    }

    @Test
    public void GIVEN_delete_stabilize_exceed_limit_WHEN_call_handleRequest_THEN_throw_CfnThrottlingException() {
        // GIVEN
        final ResourceModel model = ResourceModel.builder().channelName(TEST_CHANNEL_NAME).build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final DeleteChannelResponse deleteChannelResponse = DeleteChannelResponse.builder().build();
        when(proxyClient.client().deleteChannel(deleteChannelRequestArgumentCaptor.capture())).thenReturn(deleteChannelResponse);

        final ThrottlingException resourceNotFoundException = ThrottlingException.builder().build();
        when(proxyClient.client().describeChannel(describeChannelRequestArgumentCaptor.capture()))
                .thenThrow(resourceNotFoundException);

        // WHEN / THEN
        assertThrows(CfnThrottlingException.class,
                () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));
        assertThat(deleteChannelRequestArgumentCaptor.getValue().channelName()).isEqualTo(TEST_CHANNEL_NAME);
        assertThat(describeChannelRequestArgumentCaptor.getValue().channelName()).isEqualTo(TEST_CHANNEL_NAME);

        verify(proxyClient.client(), times(1)).deleteChannel(any(DeleteChannelRequest.class));
        verify(proxyClient.client(), times(1)).describeChannel(any(DescribeChannelRequest.class));
    }

    @Test
    public void GIVE_delete_exception_WHEN_call_handleRequest_THEN_throw_Exception() {
        // GIVEN
        final ResourceModel model = ResourceModel.builder().channelName(TEST_CHANNEL_NAME).build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final ServiceUnavailableException serviceUnavailableException = ServiceUnavailableException.builder().build();
        when(proxyClient.client().deleteChannel(deleteChannelRequestArgumentCaptor.capture())).thenThrow(serviceUnavailableException);

        // WHEN / THEN
        assertThrows(CfnGeneralServiceException.class,
                () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));
        assertThat(deleteChannelRequestArgumentCaptor.getValue().channelName()).isEqualTo(TEST_CHANNEL_NAME);

        verify(proxyClient.client(), times(1)).deleteChannel(any(DeleteChannelRequest.class));
        verify(proxyClient.client(), never()).describeChannel(any(DescribeChannelRequest.class));
    }
}
