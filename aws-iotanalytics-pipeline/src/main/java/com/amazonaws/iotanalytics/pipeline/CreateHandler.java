package com.amazonaws.iotanalytics.pipeline;

import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.services.iotanalytics.IoTAnalyticsClient;
import software.amazon.awssdk.services.iotanalytics.model.CreatePipelineRequest;
import software.amazon.awssdk.services.iotanalytics.model.CreatePipelineResponse;
import software.amazon.awssdk.services.iotanalytics.model.IoTAnalyticsException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.resource.IdentifierUtils;

public class CreateHandler extends BaseIoTAnalyticsHandler {
    private static final String CALL_GRAPH = "AWS-IoTAnalytics-Pipeline::Create";
    private static final String OPERATION = "CreatePipeline";
    private static final int MAX_NAME_LENGTH = 128;

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

        if (StringUtils.isBlank(model.getPipelineName())) {
            final String pipelineName = IdentifierUtils.generateResourceIdentifier(
                    request.getLogicalResourceIdentifier(),
                    request.getClientRequestToken(),
                    MAX_NAME_LENGTH
            );
            logger.log(String.format("Missing channelName. Generated pipelineName for %s: %s", ResourceModel.TYPE_NAME, pipelineName));
            model.setPipelineName(pipelineName);
        }

        return ProgressEvent.progress(model, callbackContext)
                .then(progress ->
                        proxy.initiate(CALL_GRAPH, proxyClient, model, callbackContext)
                                .translateToServiceRequest(Translator::translateToCreatePipelineRequest)
                                .backoffDelay(DELAY_CONSTANT)
                                .makeServiceCall(this::createPipeline)
                                .progress())
                .then(progress ->
                        new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));

    }

    private CreatePipelineResponse createPipeline(final CreatePipelineRequest request,
                                                  final ProxyClient<IoTAnalyticsClient> proxyClient) {
        try {
            final CreatePipelineResponse response = proxyClient.injectCredentialsAndInvokeV2(request,
                    proxyClient.client()::createPipeline);
            logger.log(String.format("%s [%s] successfully created", ResourceModel.TYPE_NAME, request.pipelineName()));
            return response;
        } catch (final IoTAnalyticsException e) {
            logger.log(String.format("ERROR %s [%s] fail to be created: %s", ResourceModel.TYPE_NAME, request.pipelineName(), e.toString()));
            throw Translator.translateExceptionToHandlerException(
                    e,
                    OPERATION,
                    request.pipelineName()
            );
        }
    }
}
