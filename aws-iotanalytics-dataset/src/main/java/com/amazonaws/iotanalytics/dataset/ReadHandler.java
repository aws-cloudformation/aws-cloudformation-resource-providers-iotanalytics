package com.amazonaws.iotanalytics.dataset;

import software.amazon.awssdk.services.iotanalytics.IoTAnalyticsClient;
import software.amazon.awssdk.services.iotanalytics.model.DescribeDatasetRequest;
import software.amazon.awssdk.services.iotanalytics.model.DescribeDatasetResponse;
import software.amazon.awssdk.services.iotanalytics.model.IoTAnalyticsException;
import software.amazon.awssdk.services.iotanalytics.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.iotanalytics.model.ListTagsForResourceResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class ReadHandler extends BaseIoTAnalyticsHandler {
    private static final String OPERATION_DESCRIBE = "DescribeDataset";
    private static final String OPERATION_LIST_TAGS = "DescribeDataset_ListTags";
    private static final String CALL_GRAPH = "AWS-IoTAnalytics-Dataset::Read";

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
                .translateToServiceRequest(Translator::translateToDescribeDatasetRequest)
                .backoffDelay(DELAY_CONSTANT)
                .makeServiceCall(this::readDataset)
                .done((describeRequest, describeResponse, sdkProxyClient, resourceModel, context) -> {
                    try {
                        final ListTagsForResourceResponse listTagsForResourceResponse =
                                sdkProxyClient.injectCredentialsAndInvokeV2(ListTagsForResourceRequest.builder()
                                                .resourceArn(describeResponse.dataset().arn())
                                                .build(),
                                        sdkProxyClient.client()::listTagsForResource);
                        logger.log(String.format("%s [%s] has successfully been listed tags", ResourceModel.TYPE_NAME, describeResponse.dataset().arn()));
                        return ProgressEvent.defaultSuccessHandler(Translator.translateFromDescribeResponse(describeResponse, listTagsForResourceResponse));
                    } catch (final IoTAnalyticsException e) {
                        logger.log(String.format("ERROR %s [%s] fail to be listed tags: %s", ResourceModel.TYPE_NAME, describeResponse.dataset().arn(), e.toString()));
                        throw Translator.translateExceptionToHandlerException(
                                e,
                                OPERATION_LIST_TAGS,
                                describeRequest.datasetName());
                    }
                });
    }

    private DescribeDatasetResponse readDataset(final DescribeDatasetRequest request,
                                                  final ProxyClient<IoTAnalyticsClient> proxyClient) {
        try {
            final DescribeDatasetResponse response
                    = proxyClient.injectCredentialsAndInvokeV2(request, proxyClient.client()::describeDataset);
            logger.log(String.format("%s [%s] has successfully been read", ResourceModel.TYPE_NAME, request.datasetName()));
            return response;
        } catch (final IoTAnalyticsException e) {
            logger.log(String.format("ERROR %s [%s] fail to be read: %s", ResourceModel.TYPE_NAME, request.datasetName(), e.toString()));
            throw Translator.translateExceptionToHandlerException(
                    e,
                    OPERATION_DESCRIBE,
                    request.datasetName());
        }
    }
}
