package com.amazonaws.iotanalytics.datastore;

import software.amazon.awssdk.services.iotanalytics.model.DatastoreSummary;
import software.amazon.awssdk.services.iotanalytics.model.ListDatastoresRequest;
import software.amazon.awssdk.services.iotanalytics.model.ListDatastoresResponse;
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
    private static final String TEST_DATASTORE_NAME_1 = "test_datastore_name_1";
    private static final String TEST_DATASTORE_NAME_2 = "test_datastore_name_2";
    private static final String TEST_DATASTORE_NAME_3 = "test_datastore_name_3";
    private static final String TEST_DATASTORE_NAME_4 = "test_datastore_name_4";


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
        final ListDatastoresResponse listDatastoresResponse = ListDatastoresResponse.builder()
                .nextToken(TEST_NEXT_TOKEN)
                .datastoreSummaries(
                        DatastoreSummary.builder().datastoreName(TEST_DATASTORE_NAME_1).build(),
                        DatastoreSummary.builder().datastoreName(TEST_DATASTORE_NAME_2).build(),
                        DatastoreSummary.builder().datastoreName(TEST_DATASTORE_NAME_3).build(),
                        DatastoreSummary.builder().datastoreName(TEST_DATASTORE_NAME_4).build()
                )
                .build();

        when(proxyClient.client().listDatastores(any(ListDatastoresRequest.class)))
                .thenReturn(listDatastoresResponse);

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
        assertThat(response.getResourceModels().get(0).getDatastoreName()).isEqualTo(TEST_DATASTORE_NAME_1);
        assertThat(response.getResourceModels().get(1).getDatastoreName()).isEqualTo(TEST_DATASTORE_NAME_2);
        assertThat(response.getResourceModels().get(2).getDatastoreName()).isEqualTo(TEST_DATASTORE_NAME_3);
        assertThat(response.getResourceModels().get(3).getDatastoreName()).isEqualTo(TEST_DATASTORE_NAME_4);
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(1)).listDatastores(any(ListDatastoresRequest.class));
    }

    @Test
    public void GIVEN_iota_not_found_WHEN_call_handleRequest_THEN_return_ResourceNotFound() {
        // GIVEN
        when(proxyClient.client().listDatastores(any(ListDatastoresRequest.class)))
                .thenThrow(ServiceUnavailableException.builder().build());

        // WHEN / THEN
        assertThrows(CfnGeneralServiceException.class,
                () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));
    }
}