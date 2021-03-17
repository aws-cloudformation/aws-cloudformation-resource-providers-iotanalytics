package com.amazonaws.iotanalytics.datastore;

import software.amazon.awssdk.services.iotanalytics.IoTAnalyticsClient;
import software.amazon.awssdk.services.iotanalytics.model.IoTAnalyticsException;
import software.amazon.awssdk.services.iotanalytics.model.ListDatastoresRequest;
import software.amazon.awssdk.services.iotanalytics.model.ListDatastoresResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.List;
import java.util.stream.Collectors;

public class ListHandler extends BaseIoTAnalyticsHandler {
    private static final String OPERATION = "ListDatastores";

    private Logger logger;


    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<IoTAnalyticsClient> proxyClient,
            final Logger logger) {
        this.logger = logger;

        final ListDatastoresRequest listDatastoresRequest = ListDatastoresRequest
                .builder()
                .nextToken(request.getNextToken())
                .build();

        try {
            final ListDatastoresResponse listDatastoresResponse
                    = proxy.injectCredentialsAndInvokeV2(
                    listDatastoresRequest, proxyClient.client()::listDatastores);

            final List<ResourceModel> models = listDatastoresResponse.datastoreSummaries()
                    .stream()
                    .map(datastoreSummary -> ResourceModel
                            .builder()
                            .datastoreName(datastoreSummary.datastoreName())
                            .build()).collect(Collectors.toList());
            logger.log(String.format("%s successfully list datastores", ResourceModel.TYPE_NAME));
            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                    .resourceModels(models)
                    .status(OperationStatus.SUCCESS)
                    .nextToken(listDatastoresResponse.nextToken())
                    .build();
        } catch (final IoTAnalyticsException e) {
            logger.log(String.format("ERROR %s listing datastores %s.", ResourceModel.TYPE_NAME, e.toString()));
            throw Translator.translateExceptionToHandlerException(
                    e,
                    OPERATION,
                    null);
        }
    }
}
