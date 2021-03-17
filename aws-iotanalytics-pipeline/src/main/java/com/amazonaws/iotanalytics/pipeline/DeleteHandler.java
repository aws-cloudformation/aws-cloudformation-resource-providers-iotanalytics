package com.amazonaws.iotanalytics.pipeline;

import software.amazon.awssdk.services.iotanalytics.IoTAnalyticsClient;
import software.amazon.awssdk.services.iotanalytics.model.DeletePipelineRequest;
import software.amazon.awssdk.services.iotanalytics.model.DeletePipelineResponse;
import software.amazon.awssdk.services.iotanalytics.model.DescribePipelineRequest;
import software.amazon.awssdk.services.iotanalytics.model.IoTAnalyticsException;
import software.amazon.awssdk.services.iotanalytics.model.ResourceNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class DeleteHandler extends BaseIoTAnalyticsHandler {
    private static final String CALL_GRAPH = "AWS-IoTAnalytics-Pipeline::Delete";
    private static final String OPERATION = "DeletePipeline";
    private static final String OPERATION_READ = "DeletePipeline_Read";
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

        return proxy.initiate(CALL_GRAPH, proxyClient, model, callbackContext)
                .translateToServiceRequest(Translator::translateToDeletePipelineRequest)
                .backoffDelay(DELAY_CONSTANT)
                .makeServiceCall(this::deletePipeline)
                .stabilize(this::stabilizedOnDelete)
                .done(response -> ProgressEvent.defaultSuccessHandler(null));
    }

    private DeletePipelineResponse deletePipeline(final DeletePipelineRequest request,
                                                 final ProxyClient<IoTAnalyticsClient> proxyClient) {
        try {
            DeletePipelineResponse response = proxyClient.injectCredentialsAndInvokeV2(
                    request,
                    proxyClient.client()::deletePipeline);
            logger.log(String.format("%s [%s] successfully deleted", ResourceModel.TYPE_NAME, request.pipelineName()));
            return response;
        } catch (final IoTAnalyticsException e) {
            logger.log(String.format("ERROR %s [%s] fail to be deleted: %s", ResourceModel.TYPE_NAME, request.pipelineName(), e.toString()));
            throw Translator.translateExceptionToHandlerException(
                    e,
                    OPERATION,
                    request.pipelineName());
        }
    }

    private boolean stabilizedOnDelete(
            final DeletePipelineRequest request,
            final DeletePipelineResponse result,
            final ProxyClient<IoTAnalyticsClient> proxyClient,
            final ResourceModel model,
            final CallbackContext callbackContext) {
        try {
            final DescribePipelineRequest describePipelineRequest = DescribePipelineRequest.builder().pipelineName(model.getPipelineName()).build();
            proxyClient.injectCredentialsAndInvokeV2(describePipelineRequest, proxyClient.client()::describePipeline);
            logger.log(String.format("ERROR %s [%s] still exists after deleting", ResourceModel.TYPE_NAME, request.pipelineName()));
            return false;
        } catch (final ResourceNotFoundException e) {
            return true;
        } catch (final IoTAnalyticsException e) {
            throw Translator.translateExceptionToHandlerException(e, OPERATION_READ, model.getPipelineName());
        }
    }
}
