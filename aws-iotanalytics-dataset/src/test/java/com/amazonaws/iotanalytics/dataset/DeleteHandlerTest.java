package com.amazonaws.iotanalytics.dataset;

import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import software.amazon.awssdk.services.iotanalytics.model.DeleteDatasetRequest;
import software.amazon.awssdk.services.iotanalytics.model.DeleteDatasetResponse;
import software.amazon.awssdk.services.iotanalytics.model.DescribeDatasetRequest;
import software.amazon.awssdk.services.iotanalytics.model.DescribeDatasetResponse;
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

import static com.amazonaws.iotanalytics.dataset.TestConstants.TEST_DATASET_NAME;
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
    private ArgumentCaptor<DeleteDatasetRequest> deleteDatasetRequestArgumentCaptor;

    @Captor
    private ArgumentCaptor<DescribeDatasetRequest> describeDatasetRequestArgumentCaptor;

    @BeforeEach
    public void setup() {
        super.setUp();
        handler =  new DeleteHandler();
    }

    @Test
    public void GIVEN_delete_success_WHEN_call_handleRequest_THEN_return_success() {
        // GIVEN
        final ResourceModel model = ResourceModel.builder().datasetName(TEST_DATASET_NAME).build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final DeleteDatasetResponse deleteDatasetResponse = DeleteDatasetResponse.builder().build();
        when(proxyClient.client().deleteDataset(deleteDatasetRequestArgumentCaptor.capture())).thenReturn(deleteDatasetResponse);

        final ResourceNotFoundException resourceNotFoundException = ResourceNotFoundException.builder().build();
        when(proxyClient.client().describeDataset(describeDatasetRequestArgumentCaptor.capture())).thenThrow(resourceNotFoundException);

        // WHEN
        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        // THEN

        assertThat(deleteDatasetRequestArgumentCaptor.getValue().datasetName()).isEqualTo(TEST_DATASET_NAME);
        assertThat(describeDatasetRequestArgumentCaptor.getValue().datasetName()).isEqualTo(TEST_DATASET_NAME);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(1)).deleteDataset(any(DeleteDatasetRequest.class));
        verify(proxyClient.client(), times(1)).describeDataset(any(DescribeDatasetRequest.class));
    }

    @Test
    public void GIVEN_delete_stabilize_WHEN_call_handleRequest_THEN_return_success() {
        // GIVEN
        final ResourceModel model = ResourceModel.builder().datasetName(TEST_DATASET_NAME).build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final DeleteDatasetResponse deleteDatasetResponse = DeleteDatasetResponse.builder().build();
        when(proxyClient.client().deleteDataset(deleteDatasetRequestArgumentCaptor.capture()))
                .thenReturn(deleteDatasetResponse);

        final DescribeDatasetResponse describeDatasetResponse = DescribeDatasetResponse.builder().build();
        final ResourceNotFoundException resourceNotFoundException = ResourceNotFoundException.builder().build();
        when(proxyClient.client().describeDataset(describeDatasetRequestArgumentCaptor.capture()))
                .thenReturn(describeDatasetResponse)
                .thenThrow(resourceNotFoundException);

        // WHEN
        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        // THEN
        assertThat(deleteDatasetRequestArgumentCaptor.getValue().datasetName()).isEqualTo(TEST_DATASET_NAME);
        assertThat(describeDatasetRequestArgumentCaptor.getValue().datasetName()).isEqualTo(TEST_DATASET_NAME);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(1)).deleteDataset(any(DeleteDatasetRequest.class));
        verify(proxyClient.client(), times(2)).describeDataset(any(DescribeDatasetRequest.class));
    }

    @Test
    public void GIVEN_delete_stabilize_exceed_limit_WHEN_call_handleRequest_THEN_throw_CfnThrottlingException() {
        // GIVEN
        final ResourceModel model = ResourceModel.builder().datasetName(TEST_DATASET_NAME).build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final DeleteDatasetResponse deleteDatasetResponse = DeleteDatasetResponse.builder().build();
        when(proxyClient.client().deleteDataset(deleteDatasetRequestArgumentCaptor.capture()))
                .thenReturn(deleteDatasetResponse);

        final ThrottlingException resourceNotFoundException = ThrottlingException.builder().build();
        when(proxyClient.client().describeDataset(describeDatasetRequestArgumentCaptor.capture()))
                .thenThrow(resourceNotFoundException);

        // WHEN / THEN
        assertThrows(CfnThrottlingException.class,
                () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));
        assertThat(deleteDatasetRequestArgumentCaptor.getValue().datasetName()).isEqualTo(TEST_DATASET_NAME);
        assertThat(describeDatasetRequestArgumentCaptor.getValue().datasetName()).isEqualTo(TEST_DATASET_NAME);

        verify(proxyClient.client(), times(1))
                .deleteDataset(any(DeleteDatasetRequest.class));
        verify(proxyClient.client(), times(1))
                .describeDataset(any(DescribeDatasetRequest.class));
    }

    @Test
    public void GIVE_delete_exception_WHEN_call_handleRequest_THEN_throw_Exception() {
        // GIVEN
        final ResourceModel model = ResourceModel.builder().datasetName(TEST_DATASET_NAME).build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final ServiceUnavailableException serviceUnavailableException = ServiceUnavailableException.builder().build();
        when(proxyClient.client().deleteDataset(deleteDatasetRequestArgumentCaptor.capture()))
                .thenThrow(serviceUnavailableException);

        // WHEN / THEN
        assertThrows(CfnGeneralServiceException.class,
                () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));
        assertThat(deleteDatasetRequestArgumentCaptor.getValue().datasetName()).isEqualTo(TEST_DATASET_NAME);

        verify(proxyClient.client(), times(1)).deleteDataset(any(DeleteDatasetRequest.class));
        verify(proxyClient.client(), never()).describeDataset(any(DescribeDatasetRequest.class));
    }
}
