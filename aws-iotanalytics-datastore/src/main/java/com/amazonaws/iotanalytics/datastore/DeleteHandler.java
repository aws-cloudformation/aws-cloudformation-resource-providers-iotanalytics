package com.amazonaws.iotanalytics.datastore;

import software.amazon.awssdk.services.iotanalytics.IoTAnalyticsClient;
import software.amazon.awssdk.services.iotanalytics.model.DeleteDatastoreRequest;
import software.amazon.awssdk.services.iotanalytics.model.DeleteDatastoreResponse;
import software.amazon.awssdk.services.iotanalytics.model.DescribeDatastoreRequest;
import software.amazon.awssdk.services.iotanalytics.model.IoTAnalyticsException;
import software.amazon.awssdk.services.iotanalytics.model.ResourceNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class DeleteHandler extends BaseIoTAnalyticsHandler{
    private static final String CALL_GRAPH = "AWS-IoTAnalytics-Datastore::Delete";
    private static final String OPERATION = "DeleteDatastore";
    private static final String OPERATION_READ = "DeleteDatastore_Read";

    private Logger logger;
    private AmazonWebServicesClientProxy proxy;

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<IoTAnalyticsClient> proxyClient,
            final Logger logger) {

        this.logger = logger;
        this.proxy = proxy;
        final ResourceModel model = request.getDesiredResourceState();

        return proxy.initiate(CALL_GRAPH, proxyClient, model, callbackContext)
                .translateToServiceRequest(Translator::translateToDeleteDatastoreRequest)
                .backoffDelay(DELAY_CONSTANT)
                .makeServiceCall(this::deleteDatastore)
                .stabilize(this::stabilizedOnDelete)
                .done(response -> ProgressEvent.defaultSuccessHandler(null));
    }

    private DeleteDatastoreResponse deleteDatastore(final DeleteDatastoreRequest request,
                                                    final ProxyClient<IoTAnalyticsClient> proxyClient) {
        try {
            DeleteDatastoreResponse response = proxy.injectCredentialsAndInvokeV2(
                    request,
                    proxyClient.client()::deleteDatastore);
            logger.log(String.format("%s [%s] successfully deleted", ResourceModel.TYPE_NAME, request.datastoreName()));
            return response;
        } catch (final IoTAnalyticsException e) {
            logger.log(String.format("ERROR %s [%s] fail to be deleted: %s", ResourceModel.TYPE_NAME, request.datastoreName(), e.toString()));
            throw Translator.translateExceptionToHandlerException(
                    e,
                    OPERATION,
                    request.datastoreName());
        }
    }

    private boolean stabilizedOnDelete(
            final DeleteDatastoreRequest request,
            final DeleteDatastoreResponse result,
            final ProxyClient<IoTAnalyticsClient> proxyClient,
            final ResourceModel model,
            final CallbackContext callbackContext) {
        try {
            final DescribeDatastoreRequest describeDatastoreRequest = DescribeDatastoreRequest.builder().datastoreName(model.getDatastoreName()).build();
            proxy.injectCredentialsAndInvokeV2(describeDatastoreRequest, proxyClient.client()::describeDatastore);
            logger.log(String.format("ERROR %s [%s] still exists after deleting", ResourceModel.TYPE_NAME, request.datastoreName()));
            return false;
        } catch (final ResourceNotFoundException e) {
            return true;
        } catch (final IoTAnalyticsException e) {
            throw Translator.translateExceptionToHandlerException(e, OPERATION_READ, model.getDatastoreName());
        }
    }
}
