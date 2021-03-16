package com.amazonaws.iotanalytics.channel;

import software.amazon.awssdk.services.iotanalytics.IoTAnalyticsClient;
import software.amazon.awssdk.services.iotanalytics.model.CreateChannelRequest;
import software.amazon.awssdk.services.iotanalytics.model.CreateChannelResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import org.apache.commons.lang3.StringUtils;

public class CreateHandler extends BaseIoTAnalyticsHandler {
    private static final String CALL_GRAPH = "AWS-IoTAnalytics-Channel::Create";
    private static final String OPERATION = "CreateChannel";

    private Logger logger;

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<IoTAnalyticsClient> proxyClient,
            final Logger logger) {

        this.logger = logger;

        final ResourceModel model = request.getDesiredResourceState();

        if (!StringUtils.isEmpty(model.getId())) {
            logger.log(String.format("%s [%s] id is read-only", ResourceModel.TYPE_NAME, model.getId()));
            return ProgressEvent.failed(model, null, HandlerErrorCode.InvalidRequest,
                    "Id is a read-only property and cannot be set.");
        }

        return ProgressEvent.progress(model, callbackContext)
                .then(progress ->
                        proxy.initiate(CALL_GRAPH, proxyClient, model, callbackContext)
                            .translateToServiceRequest(Translator::translateToCreateChannelRequest)
                            .backoffDelay(DELAY_CONSTANT)
                            .makeServiceCall(this::createChannel)
                            .progress())
                .then(progress ->
                        new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));

    }

    private CreateChannelResponse createChannel(final CreateChannelRequest request, final ProxyClient<IoTAnalyticsClient> proxyClient) {
        try {
            final CreateChannelResponse response = proxyClient.injectCredentialsAndInvokeV2(request,
                    proxyClient.client()::createChannel);
            logger.log(String.format("%s [%s] successfully created", ResourceModel.TYPE_NAME, request.channelName()));
            return response;
        } catch (final Exception e) {
            logger.log(String.format("ERROR %s [%s] fail to be created: %s", ResourceModel.TYPE_NAME, request.channelName(), e.toString()));
            throw Translator.translateExceptionToHandlerException(
                    e,
                    OPERATION,
                    request.channelName()
            );
        }
    }
}
