package com.amazonaws.iotanalytics.pipeline;

import software.amazon.awssdk.services.iotanalytics.IoTAnalyticsClient;
import software.amazon.awssdk.services.iotanalytics.model.IoTAnalyticsException;
import software.amazon.awssdk.services.iotanalytics.model.ListPipelinesRequest;
import software.amazon.awssdk.services.iotanalytics.model.ListPipelinesResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.List;
import java.util.stream.Collectors;

public class ListHandler extends BaseIoTAnalyticsHandler {

    private static final String OPERATION = "ListPipelines";

    private Logger logger;

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<IoTAnalyticsClient> proxyClient,
            final Logger logger) {

        this.logger = logger;
        final ListPipelinesRequest listPipelinesRequest = ListPipelinesRequest
                .builder()
                .nextToken(request.getNextToken())
                .build();

        try {
            final ListPipelinesResponse listPipelinesResponse
                    = proxy.injectCredentialsAndInvokeV2(
                    listPipelinesRequest, proxyClient.client()::listPipelines);

            final List<ResourceModel> models = listPipelinesResponse.pipelineSummaries()
                    .stream()
                    .map(pipelineSummary -> ResourceModel
                            .builder()
                            .pipelineName(pipelineSummary.pipelineName())
                            .build()).collect(Collectors.toList());
            logger.log(String.format("%s successfully list pipelines", ResourceModel.TYPE_NAME));
            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                    .resourceModels(models)
                    .status(OperationStatus.SUCCESS)
                    .nextToken(listPipelinesResponse.nextToken())
                    .build();
        } catch (final IoTAnalyticsException e) {
            logger.log(String.format("ERROR %s listing pipelines %s.", ResourceModel.TYPE_NAME, e.toString()));
            throw Translator.translateExceptionToHandlerException(
                    e,
                    OPERATION,
                    null);
        }
    }
}
