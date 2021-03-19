package com.amazonaws.iotanalytics.dataset;

import software.amazon.awssdk.services.iotanalytics.model.ListDatasetsRequest;
import software.amazon.awssdk.services.iotanalytics.model.ListDatasetsResponse;
import software.amazon.awssdk.services.iotanalytics.model.DatasetSummary;
import software.amazon.awssdk.services.iotanalytics.model.ServiceUnavailableException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ListHandlerTest extends AbstractTestBase {

    private static final String TEST_NEXT_TOKEN = "test_next_token";
    private static final String TEST_DATASET_NAME_1 = "test_dataset_name_1";
    private static final String TEST_DATASET_NAME_2 = "test_dataset_name_2";
    private static final String TEST_DATASET_NAME_3 = "test_dataset_name_3";
    private static final String TEST_DATASET_NAME_4 = "test_dataset_name_4";

    private ResourceHandlerRequest<ResourceModel> request;
    private ListHandler handler;

    @BeforeEach
    public void setup() {
        super.setUp();
        handler = new ListHandler();

        final ResourceModel model = ResourceModel.builder().build();
        request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();
    }

    @Test
    public void GIVEN_iota_good_WHEN_call_handleRequest_THEN_return_success() {
        // GIVEN
        final ListDatasetsResponse listDatasetsResponse = ListDatasetsResponse.builder()
                .nextToken(TEST_NEXT_TOKEN)
                .datasetSummaries(
                        DatasetSummary.builder().datasetName(TEST_DATASET_NAME_1).build(),
                        DatasetSummary.builder().datasetName(TEST_DATASET_NAME_2).build(),
                        DatasetSummary.builder().datasetName(TEST_DATASET_NAME_3).build(),
                        DatasetSummary.builder().datasetName(TEST_DATASET_NAME_4).build()
                )
                .build();

        when(proxyClient.client().listDatasets(any(ListDatasetsRequest.class)))
                .thenReturn(listDatasetsResponse);

        // WHEN
        final ProgressEvent<ResourceModel, CallbackContext> response =
                handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        // THEN
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNotNull();
        assertThat(response.getNextToken()).isEqualTo(TEST_NEXT_TOKEN);
        assertThat(response.getResourceModels().size()).isEqualTo(4);
        assertThat(response.getResourceModels().get(0).getDatasetName()).isEqualTo(TEST_DATASET_NAME_1);
        assertThat(response.getResourceModels().get(1).getDatasetName()).isEqualTo(TEST_DATASET_NAME_2);
        assertThat(response.getResourceModels().get(2).getDatasetName()).isEqualTo(TEST_DATASET_NAME_3);
        assertThat(response.getResourceModels().get(3).getDatasetName()).isEqualTo(TEST_DATASET_NAME_4);
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(1)).listDatasets(any(ListDatasetsRequest.class));
    }

    @Test
    public void GIVEN_iota_not_found_WHEN_call_handleRequest_THEN_return_ResourceNotFound() {
        // GIVEN
        when(proxyClient.client().listDatasets(any(ListDatasetsRequest.class)))
                .thenThrow(ServiceUnavailableException.builder().build());

        // WHEN / THEN
        assertThrows(CfnGeneralServiceException.class,
                () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));
    }
}
