package com.amazonaws.iotanalytics.dataset;

import software.amazon.awssdk.services.iotanalytics.IoTAnalyticsClient;
import software.amazon.awssdk.services.iotanalytics.model.IoTAnalyticsException;
import software.amazon.awssdk.services.iotanalytics.model.ListDatasetsRequest;
import software.amazon.awssdk.services.iotanalytics.model.ListDatasetsResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.List;
import java.util.stream.Collectors;

public class ListHandler extends BaseIoTAnalyticsHandler {
    private static final String OPERATION = "ListDatasets";

    private Logger logger;

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<IoTAnalyticsClient> proxyClient,
            final Logger logger) {
        this.logger = logger;
        final ListDatasetsRequest listDatasetsRequest = ListDatasetsRequest
                .builder()
                .nextToken(request.getNextToken())
                .build();

        try {
            final ListDatasetsResponse listDatasetsResponse
                    = proxy.injectCredentialsAndInvokeV2(
                    listDatasetsRequest, proxyClient.client()::listDatasets);

            final List<ResourceModel> models = listDatasetsResponse.datasetSummaries()
                    .stream()
                    .map(datasetSummary -> ResourceModel
                            .builder()
                            .datasetName(datasetSummary.datasetName())
                            .build()).collect(Collectors.toList());
            logger.log(String.format("%s successfully list datasets", ResourceModel.TYPE_NAME));
            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                    .resourceModels(models)
                    .status(OperationStatus.SUCCESS)
                    .nextToken(listDatasetsResponse.nextToken())
                    .build();
        } catch (final IoTAnalyticsException e) {
            logger.log(String.format("ERROR %s listing datasets %s.", ResourceModel.TYPE_NAME, e.toString()));
            throw Translator.translateExceptionToHandlerException(
                    e,
                    OPERATION,
                    null);
        }
    }
}
