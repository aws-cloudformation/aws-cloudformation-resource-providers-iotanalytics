package com.amazonaws.iotanalytics.channel;

import software.amazon.awssdk.services.iotanalytics.IoTAnalyticsClient;
import software.amazon.awssdk.services.iotanalytics.model.ListChannelsRequest;
import software.amazon.awssdk.services.iotanalytics.model.ListChannelsResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.List;
import java.util.stream.Collectors;

public class ListHandler extends BaseIoTAnalyticsHandler {
    private static final String OPERATION = "ListChannels";

    private Logger logger;

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<IoTAnalyticsClient> proxyClient,
            final Logger logger) {
        this.logger = logger;
        final ListChannelsRequest listChannelsRequest = ListChannelsRequest
                .builder()
                .nextToken(request.getNextToken())
                .build();

        try {
            final ListChannelsResponse listChannelsResponse
                    = proxy.injectCredentialsAndInvokeV2(
                            listChannelsRequest, proxyClient.client()::listChannels);

            final List<ResourceModel> models = listChannelsResponse.channelSummaries()
                    .stream()
                    .map(channelSummary -> ResourceModel
                            .builder()
                            .channelName(channelSummary.channelName())
                            .build()).collect(Collectors.toList());
            logger.log(String.format("%s successfully list channels", ResourceModel.TYPE_NAME));
            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                    .resourceModels(models)
                    .status(OperationStatus.SUCCESS)
                    .nextToken(listChannelsResponse.nextToken())
                    .build();
        } catch (final Exception e) {
            logger.log(String.format("ERROR %s listing channel %s.", ResourceModel.TYPE_NAME, e.toString()));
            throw Translator.translateExceptionToHandlerException(
                    e,
                    OPERATION,
                    null);
        }
    }
}
