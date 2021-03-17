package com.amazonaws.iotanalytics.pipeline;

import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import software.amazon.awssdk.services.iotanalytics.model.DeletePipelineRequest;
import software.amazon.awssdk.services.iotanalytics.model.DeletePipelineResponse;
import software.amazon.awssdk.services.iotanalytics.model.DescribePipelineRequest;
import software.amazon.awssdk.services.iotanalytics.model.DescribePipelineResponse;
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

import static com.amazonaws.iotanalytics.pipeline.TestConstants.TEST_PIPELINE_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DeleteHandlerTest extends AbstractTestBase {

    private DeleteHandler handler;

    @Captor
    private ArgumentCaptor<DeletePipelineRequest> deletePipelineRequestArgumentCaptor;

    @Captor
    private ArgumentCaptor<DescribePipelineRequest> describePipelineRequestArgumentCaptor;

    @BeforeEach
    public void setup() {
        super.setUp();
        handler = new DeleteHandler();
    }

    @Test
    public void GIVEN_delete_success_WHEN_call_handleRequest_THEN_return_success() {
        // GIVEN
        final ResourceModel model = ResourceModel.builder().pipelineName(TEST_PIPELINE_NAME).build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final DeletePipelineResponse deletePipelineResponse = DeletePipelineResponse.builder().build();
        when(proxyClient.client().deletePipeline(deletePipelineRequestArgumentCaptor.capture()))
                .thenReturn(deletePipelineResponse);

        final ResourceNotFoundException resourceNotFoundException = ResourceNotFoundException.builder().build();
        when(proxyClient.client().describePipeline(describePipelineRequestArgumentCaptor.capture()))
                .thenThrow(resourceNotFoundException);

        // WHEN
        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        // THEN

        assertThat(deletePipelineRequestArgumentCaptor.getValue().pipelineName())
                .isEqualTo(TEST_PIPELINE_NAME);
        assertThat(describePipelineRequestArgumentCaptor.getValue().pipelineName())
                .isEqualTo(TEST_PIPELINE_NAME);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(1)).deletePipeline(any(DeletePipelineRequest.class));
        verify(proxyClient.client(), times(1)).describePipeline(any(DescribePipelineRequest.class));
    }

    @Test
    public void GIVEN_delete_stabilize_WHEN_call_handleRequest_THEN_return_success() {
        // GIVEN
        final ResourceModel model = ResourceModel.builder().pipelineName(TEST_PIPELINE_NAME).build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final DeletePipelineResponse deletePipelineResponse = DeletePipelineResponse.builder().build();
        when(proxyClient.client().deletePipeline(deletePipelineRequestArgumentCaptor.capture()))
                .thenReturn(deletePipelineResponse);

        final DescribePipelineResponse describePipelineResponse = DescribePipelineResponse.builder().build();
        final ResourceNotFoundException resourceNotFoundException = ResourceNotFoundException.builder().build();
        when(proxyClient.client().describePipeline(describePipelineRequestArgumentCaptor.capture()))
                .thenReturn(describePipelineResponse)
                .thenThrow(resourceNotFoundException);

        // WHEN
        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        // THEN
        assertThat(deletePipelineRequestArgumentCaptor.getValue().pipelineName())
                .isEqualTo(TEST_PIPELINE_NAME);
        assertThat(describePipelineRequestArgumentCaptor.getValue().pipelineName())
                .isEqualTo(TEST_PIPELINE_NAME);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(1)).deletePipeline(any(DeletePipelineRequest.class));
        verify(proxyClient.client(), times(2)).describePipeline(any(DescribePipelineRequest.class));

    }

    @Test
    public void GIVEN_delete_stabilize_exceed_limit_WHEN_call_handleRequest_THEN_throw_CfnThrottlingException() {
        // GIVEN
        final ResourceModel model = ResourceModel.builder().pipelineName(TEST_PIPELINE_NAME).build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final DeletePipelineResponse deletePipelineResponse = DeletePipelineResponse.builder().build();
        when(proxyClient.client().deletePipeline(deletePipelineRequestArgumentCaptor.capture()))
                .thenReturn(deletePipelineResponse);

        final ThrottlingException resourceNotFoundException = ThrottlingException.builder().build();
        when(proxyClient.client().describePipeline(describePipelineRequestArgumentCaptor.capture()))
                .thenThrow(resourceNotFoundException);

        // WHEN / THEN
        assertThrows(CfnThrottlingException.class,
                () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));
        assertThat(deletePipelineRequestArgumentCaptor.getValue().pipelineName())
                .isEqualTo(TEST_PIPELINE_NAME);
        assertThat(describePipelineRequestArgumentCaptor.getValue().pipelineName())
                .isEqualTo(TEST_PIPELINE_NAME);

        verify(proxyClient.client(), times(1)).deletePipeline(any(DeletePipelineRequest.class));
        verify(proxyClient.client(), times(1)).describePipeline(any(DescribePipelineRequest.class));
    }

    @Test
    public void GIVE_delete_exception_WHEN_call_handleRequest_THEN_throw_Exception() {
        // GIVEN
        final ResourceModel model = ResourceModel.builder().pipelineName(TEST_PIPELINE_NAME).build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final ServiceUnavailableException serviceUnavailableException = ServiceUnavailableException.builder().build();
        when(proxyClient.client().deletePipeline(deletePipelineRequestArgumentCaptor.capture()))
                .thenThrow(serviceUnavailableException);

        // WHEN / THEN
        assertThrows(CfnGeneralServiceException.class,
                () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));
        assertThat(deletePipelineRequestArgumentCaptor.getValue().pipelineName())
                .isEqualTo(TEST_PIPELINE_NAME);

        verify(proxyClient.client(), times(1)).deletePipeline(any(DeletePipelineRequest.class));
        verify(proxyClient.client(), never()).describePipeline(any(DescribePipelineRequest.class));
    }
}
