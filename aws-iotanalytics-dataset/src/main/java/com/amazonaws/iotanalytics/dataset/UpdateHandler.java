package com.amazonaws.iotanalytics.dataset;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.services.iotanalytics.IoTAnalyticsClient;
import software.amazon.awssdk.services.iotanalytics.model.DescribeDatasetRequest;
import software.amazon.awssdk.services.iotanalytics.model.DescribeDatasetResponse;
import software.amazon.awssdk.services.iotanalytics.model.IoTAnalyticsException;
import software.amazon.awssdk.services.iotanalytics.model.TagResourceRequest;
import software.amazon.awssdk.services.iotanalytics.model.TagResourceResponse;
import software.amazon.awssdk.services.iotanalytics.model.UntagResourceRequest;
import software.amazon.awssdk.services.iotanalytics.model.UntagResourceResponse;
import software.amazon.awssdk.services.iotanalytics.model.UpdateDatasetRequest;
import software.amazon.awssdk.services.iotanalytics.model.UpdateDatasetResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class UpdateHandler extends BaseIoTAnalyticsHandler {

    private static final String OPERATION_DATASET = "UpdateDataset";
    private static final String OPERATION_DELETE_TAG = "UpdateDataset_DeleteTags";
    private static final String OPERATION_ADD_TAG = "UpdateDataset_AddTags";
    private static final String OPERATION_DESCRIBE = "DescribeDataset";

    private static final String CALL_GRAPH_DATASET = "AWS-IoTAnalytics-Dataset::Update";
    private static final String CALL_GRAPH_DELETE_TAG = "AWS-IoTAnalytics-Dataset::Update-Delete-Tag";
    private static final String CALL_GRAPH_ADD_TAG = "AWS-IoTAnalytics-Dataset::Update-Add-Tag";

    private Logger logger;

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<IoTAnalyticsClient> proxyClient,
        final Logger logger) {
        this.logger = logger;

        final ResourceModel prevModel = request.getPreviousResourceState();
        final ResourceModel newModel = request.getDesiredResourceState();

        if (!StringUtils.equals(newModel.getDatasetName(), prevModel.getDatasetName())) {
            return updateFailedProgressEvent("DatasetName", null, callbackContext, HandlerErrorCode.InvalidRequest);
        } else if (!StringUtils.isEmpty(newModel.getId())
                && !StringUtils.equals(newModel.getId(), prevModel.getId())) {
            return updateFailedProgressEvent("Id", null, callbackContext, HandlerErrorCode.InvalidRequest);
        }

        return ProgressEvent.progress(newModel, callbackContext)
                .then(progress ->
                        proxy.initiate(CALL_GRAPH_DATASET, proxyClient, newModel, callbackContext)
                                .translateToServiceRequest(Translator::translateToUpdateDatasetRequest)
                                .backoffDelay(DELAY_CONSTANT)
                                .makeServiceCall(this::updateDataset)
                                .progress())
                .then(progress -> updateTags(proxy, proxyClient, newModel, prevModel, progress))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    private UpdateDatasetResponse updateDataset(final UpdateDatasetRequest updateDatasetRequest,
                                                final ProxyClient<IoTAnalyticsClient> proxyClient) {
        try {
            final UpdateDatasetResponse updateDatasetResponse
                    = proxyClient.injectCredentialsAndInvokeV2(
                    updateDatasetRequest,
                    proxyClient.client()::updateDataset);
            logger.log(String.format("%s [%s] has successfully been updated", ResourceModel.TYPE_NAME, updateDatasetRequest.datasetName()));
            return updateDatasetResponse;
        } catch (final IoTAnalyticsException e) {
            logger.log(String.format("ERROR %s [%s] fail to be updated: %s", ResourceModel.TYPE_NAME, updateDatasetRequest.datasetName(), e.toString()));
            throw Translator.translateExceptionToHandlerException(
                    e,
                    OPERATION_DATASET,
                    updateDatasetRequest.datasetName());
        }
    }

    private ProgressEvent<ResourceModel, CallbackContext> updateFailedProgressEvent(final String propertyName,
                                                                                    final ResourceModel model,
                                                                                    final CallbackContext callbackContext,
                                                                                    final HandlerErrorCode errorCode) {
        logger.log(String.format("ERROR %s [%s] is not updatable", ResourceModel.TYPE_NAME, propertyName));
        return ProgressEvent.failed(model, callbackContext, errorCode,
                String.format("%s cannot be updated", propertyName));
    }

    private String getDatasetArn(final DescribeDatasetRequest request,
                                 final ProxyClient<IoTAnalyticsClient> proxyClient) {
        try {
            final DescribeDatasetResponse response = proxyClient.injectCredentialsAndInvokeV2(request, proxyClient.client()::describeDataset);
            final String datasetArn = response.dataset().arn();
            logger.log(String.format("Successfully read arn for %s [%s].", ResourceModel.TYPE_NAME, request.datasetName()));
            return datasetArn;
        } catch (final IoTAnalyticsException e) {
            logger.log(String.format("ERROR %s [%s] failed to read arn: %s", ResourceModel.TYPE_NAME, request.datasetName(), e.toString()));
            throw com.amazonaws.iotanalytics.dataset.Translator.translateExceptionToHandlerException(
                e,
                OPERATION_DESCRIBE,
                request.datasetName());
        }
    }

    private ProgressEvent<ResourceModel, CallbackContext> updateTags(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<IoTAnalyticsClient> proxyClient,
            final ResourceModel model,
            final ResourceModel preModel,
            ProgressEvent<ResourceModel, CallbackContext> progress) {
        final Map<String, String> oldTagsMap = tagListToMap(preModel.getTags());
        final Map<String, String> newTagsMap = tagListToMap(model.getTags());

        final Map<String, String> tagsToDelete = getTagsToDelete(oldTagsMap, newTagsMap);
        final Map<String, String> tagsToCreate = getTagsToCreate(oldTagsMap, newTagsMap);

        String datasetArn = getDatasetArn(
            Translator.translateToDescribeDatasetRequest(model),
            proxyClient);

        return progress
                .then(progress1 -> {
                    if (!tagsToDelete.isEmpty()) {
                        return proxy.initiate(CALL_GRAPH_DELETE_TAG, proxyClient, model, progress.getCallbackContext())
                                .translateToServiceRequest(resourceModel ->
                                        UntagResourceRequest
                                                .builder()
                                                .resourceArn(datasetArn)
                                                .tagKeys(tagsToDelete.keySet())
                                                .build())
                                .makeServiceCall(this::deleteTags)
                                .progress();
                    }
                    return progress1;
                })
                .then(progress2 -> {
                    if (!tagsToCreate.isEmpty()) {
                        return proxy.initiate(CALL_GRAPH_ADD_TAG, proxyClient, model, progress.getCallbackContext())
                                .translateToServiceRequest(resourceModel ->
                                        TagResourceRequest
                                                .builder()
                                                .resourceArn(datasetArn)
                                                .tags(tagsMapToList(tagsToCreate))
                                                .build())
                                .makeServiceCall(this::addTags)
                                .progress();
                    }
                    return progress2;
                });
    }

    private Map<String, String> tagListToMap(@Nullable final List<Tag> tagList) {
        if (tagList != null) {
            final Map<String, String> tagsMap = new HashMap<>();
            tagList.forEach(tag -> tagsMap.put(tag.getKey(), tag.getValue()));
            return tagsMap;
        }
        return new HashMap<>();
    }

    private List<software.amazon.awssdk.services.iotanalytics.model.Tag> tagsMapToList(
            @Nonnull final Map<String, String> tagsMap) {
        return tagsMap.entrySet().stream()
                .map(tag ->
                        software.amazon.awssdk.services.iotanalytics.model.Tag.builder()
                                .key(tag.getKey())
                                .value(tag.getValue())
                                .build())
                .collect(Collectors.toList());
    }

    private Map<String, String> getTagsToDelete(final Map<String, String> oldTags,
                                                final Map<String, String> newTags) {
        final Map<String, String> tags = new HashMap<>();
        // Get all the tag keys are contained by oldTags and not contained by newTags.
        // Ex, old {a: 1}, {b: 2}, new {a: 2}
        // return {b: 2}
        final Set<String> removedKeys = Sets.difference(oldTags.keySet(), newTags.keySet());

        for (final String key : removedKeys) {
            tags.put(key, oldTags.get(key));
        }
        return tags;
    }

    private Map<String, String> getTagsToCreate(final Map<String, String> oldTags,
                                                final Map<String, String> newTags) {
        final Map<String, String> tags = new HashMap<>();
        // Get all tag elements that are contained by newTags and not by oldTags.
        // Ex, old {a: 1}, {b: 2}, new {a: 1}, {b: 3}
        // return {b: 3}
        final Set<Map.Entry<String, String>> entriesToCreate =
                Sets.difference(newTags.entrySet(), oldTags.entrySet());

        for (final Map.Entry<String, String> entry : entriesToCreate) {
            tags.put(entry.getKey(), entry.getValue());
        }
        return tags;
    }

    private UntagResourceResponse deleteTags(final UntagResourceRequest untagResourceRequest,
                                             final ProxyClient<IoTAnalyticsClient> proxyClient) {
        try {
            final UntagResourceResponse untagResourceResponse
                    = proxyClient.injectCredentialsAndInvokeV2(
                    untagResourceRequest, proxyClient.client()::untagResource);
            logger.log(String.format("%s [%s] has successfully been removed tags", ResourceModel.TYPE_NAME, untagResourceRequest.resourceArn()));
            return untagResourceResponse;
        } catch (final IoTAnalyticsException e) {
            logger.log(String.format("ERROR %s [%s] fail to be removed tags: %s", ResourceModel.TYPE_NAME, untagResourceRequest.resourceArn(), e.toString()));
            throw Translator.translateExceptionToHandlerException(
                    e,
                    OPERATION_DELETE_TAG,
                    untagResourceRequest.resourceArn());
        }
    }

    private TagResourceResponse addTags(final TagResourceRequest tagResourceRequest,
                                        final ProxyClient<IoTAnalyticsClient> proxyClient) {
        try {
            final TagResourceResponse tagResourceResponse
                    = proxyClient.injectCredentialsAndInvokeV2(
                    tagResourceRequest, proxyClient.client()::tagResource);
            logger.log(String.format("%s [%s] has successfully been added tags", ResourceModel.TYPE_NAME, tagResourceRequest.resourceArn()));
            return tagResourceResponse;
        } catch (final IoTAnalyticsException e) {
            logger.log(String.format("ERROR %s [%s] fail to be added tags: %s", ResourceModel.TYPE_NAME, tagResourceRequest.resourceArn(), e.toString()));
            throw Translator.translateExceptionToHandlerException(
                    e,
                    OPERATION_ADD_TAG,
                    tagResourceRequest.resourceArn());
        }
    }
}
