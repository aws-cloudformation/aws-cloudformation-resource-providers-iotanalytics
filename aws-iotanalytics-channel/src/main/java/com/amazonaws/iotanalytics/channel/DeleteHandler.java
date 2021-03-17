package com.amazonaws.iotanalytics.channel;

import software.amazon.awssdk.services.iotanalytics.IoTAnalyticsClient;
import software.amazon.awssdk.services.iotanalytics.model.DeleteChannelRequest;
import software.amazon.awssdk.services.iotanalytics.model.DeleteChannelResponse;
import software.amazon.awssdk.services.iotanalytics.model.DescribeChannelRequest;
import software.amazon.awssdk.services.iotanalytics.model.IoTAnalyticsException;
import software.amazon.awssdk.services.iotanalytics.model.ResourceNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class DeleteHandler extends BaseIoTAnalyticsHandler {
    private static final String CALL_GRAPH = "AWS-IoTAnalytics-Channel::Delete";
    private static final String OPERATION = "DeleteChannel";
    private static final String OPERATION_READ = "DeleteChannel_Read";

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
                .translateToServiceRequest(Translator::translateToDeleteChannelRequest)
                .backoffDelay(DELAY_CONSTANT)
                .makeServiceCall(this::deleteChannel)
                .stabilize(this::stabilizedOnDelete)
                .done(response -> ProgressEvent.defaultSuccessHandler(null));
    }

    private DeleteChannelResponse deleteChannel(final DeleteChannelRequest request,
                                                final ProxyClient<IoTAnalyticsClient> proxyClient) {
        try {
            final DeleteChannelResponse response = proxyClient.injectCredentialsAndInvokeV2(
                    request,
                    proxyClient.client()::deleteChannel);
            logger.log(String.format("%s [%s] successfully deleted", ResourceModel.TYPE_NAME, request.channelName()));
            return response;
        } catch (final IoTAnalyticsException e) {
            logger.log(String.format("ERROR %s [%s] fail to be deleted: %s", ResourceModel.TYPE_NAME, request.channelName(), e.toString()));
            throw Translator.translateExceptionToHandlerException(
                    e,
                    OPERATION,
                    request.channelName());
        }
    }

    private boolean stabilizedOnDelete(
            final DeleteChannelRequest request,
            final DeleteChannelResponse result,
            final ProxyClient<IoTAnalyticsClient> proxyClient,
            final ResourceModel model,
            final CallbackContext callbackContext) {
        try {
            final DescribeChannelRequest describeChannelRequest = DescribeChannelRequest.builder().channelName(model.getChannelName()).build();
            proxyClient.injectCredentialsAndInvokeV2(describeChannelRequest, proxyClient.client()::describeChannel);
            logger.log(String.format("ERROR %s [%s] still exists after deleting", ResourceModel.TYPE_NAME, request.channelName()));
            return false;
        } catch (final ResourceNotFoundException e) {
            return true;
        } catch (final IoTAnalyticsException e) {
            throw Translator.translateExceptionToHandlerException(e, OPERATION_READ, model.getChannelName());
        }
    }
}
