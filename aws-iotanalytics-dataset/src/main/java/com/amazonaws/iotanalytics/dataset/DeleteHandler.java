package com.amazonaws.iotanalytics.dataset;

import software.amazon.awssdk.services.iotanalytics.IoTAnalyticsClient;
import software.amazon.awssdk.services.iotanalytics.model.DeleteDatasetRequest;
import software.amazon.awssdk.services.iotanalytics.model.DeleteDatasetResponse;
import software.amazon.awssdk.services.iotanalytics.model.DescribeDatasetRequest;
import software.amazon.awssdk.services.iotanalytics.model.IoTAnalyticsException;
import software.amazon.awssdk.services.iotanalytics.model.ResourceNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class DeleteHandler extends BaseIoTAnalyticsHandler {
    private static final String CALL_GRAPH = "AWS-IoTAnalytics-Dataset::Delete";
    private static final String OPERATION = "DeleteDataset";
    private static final String OPERATION_READ = "DeleteDataset_Read";

    private Logger logger;

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<IoTAnalyticsClient> proxyClient,
            final Logger logger) {

        this.logger = logger;

        final ResourceModel model = request.getDesiredResourceState();

        return proxy.initiate(CALL_GRAPH, proxyClient, model, callbackContext)
                .translateToServiceRequest(Translator::translateToDeleteDatasetRequest)
                .backoffDelay(DELAY_CONSTANT)
                .makeServiceCall(this::deleteDataset)
                .stabilize(this::stabilizedOnDelete)
                .done(response -> ProgressEvent.defaultSuccessHandler(null));
    }

    private DeleteDatasetResponse deleteDataset(final DeleteDatasetRequest request,
                                                final ProxyClient<IoTAnalyticsClient> proxyClient) {
        try {
            final DeleteDatasetResponse response = proxyClient.injectCredentialsAndInvokeV2(
                    request,
                    proxyClient.client()::deleteDataset);
            logger.log(String.format("%s [%s] successfully deleted", ResourceModel.TYPE_NAME, request.datasetName()));
            return response;
        } catch (final IoTAnalyticsException e) {
            logger.log(String.format("ERROR %s [%s] fail to be deleted: %s", ResourceModel.TYPE_NAME, request.datasetName(), e.toString()));
            throw Translator.translateExceptionToHandlerException(
                    e,
                    OPERATION,
                    request.datasetName());
        }
    }

    private boolean stabilizedOnDelete(
            final DeleteDatasetRequest request,
            final DeleteDatasetResponse result,
            final ProxyClient<IoTAnalyticsClient> proxyClient,
            final ResourceModel model,
            final CallbackContext callbackContext) {
        try {
            final DescribeDatasetRequest describeDatasetRequest =
                    DescribeDatasetRequest.builder().datasetName(model.getDatasetName()).build();
            proxyClient.injectCredentialsAndInvokeV2(describeDatasetRequest, proxyClient.client()::describeDataset);
            logger.log(String.format("ERROR %s [%s] still exists after deleting", ResourceModel.TYPE_NAME, request.datasetName()));
            return false;
        } catch (final ResourceNotFoundException e) {
            return true;
        } catch (final IoTAnalyticsException e) {
            throw Translator.translateExceptionToHandlerException(e, OPERATION_READ, model.getDatasetName());
        }
    }
}
