package com.amazonaws.iotanalytics.pipeline;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.iotanalytics.model.AddAttributesActivity;
import software.amazon.awssdk.services.iotanalytics.model.ChannelActivity;
import software.amazon.awssdk.services.iotanalytics.model.CreatePipelineRequest;
import software.amazon.awssdk.services.iotanalytics.model.DatastoreActivity;
import software.amazon.awssdk.services.iotanalytics.model.DeletePipelineRequest;
import software.amazon.awssdk.services.iotanalytics.model.DeviceRegistryEnrichActivity;
import software.amazon.awssdk.services.iotanalytics.model.DeviceShadowEnrichActivity;
import software.amazon.awssdk.services.iotanalytics.model.FilterActivity;
import software.amazon.awssdk.services.iotanalytics.model.InvalidRequestException;
import software.amazon.awssdk.services.iotanalytics.model.IoTAnalyticsException;
import software.amazon.awssdk.services.iotanalytics.model.LambdaActivity;
import software.amazon.awssdk.services.iotanalytics.model.LimitExceededException;
import software.amazon.awssdk.services.iotanalytics.model.MathActivity;
import software.amazon.awssdk.services.iotanalytics.model.Pipeline;
import software.amazon.awssdk.services.iotanalytics.model.DescribePipelineResponse;
import software.amazon.awssdk.services.iotanalytics.model.DescribePipelineRequest;
import software.amazon.awssdk.services.iotanalytics.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.iotanalytics.model.PipelineActivity;
import software.amazon.awssdk.services.iotanalytics.model.RemoveAttributesActivity;
import software.amazon.awssdk.services.iotanalytics.model.ResourceAlreadyExistsException;
import software.amazon.awssdk.services.iotanalytics.model.ResourceNotFoundException;
import software.amazon.awssdk.services.iotanalytics.model.SelectAttributesActivity;
import software.amazon.awssdk.services.iotanalytics.model.ServiceUnavailableException;
import software.amazon.awssdk.services.iotanalytics.model.Tag;
import software.amazon.awssdk.services.iotanalytics.model.ThrottlingException;
import software.amazon.awssdk.services.iotanalytics.model.UpdatePipelineRequest;
import software.amazon.cloudformation.exceptions.BaseHandlerException;
import software.amazon.cloudformation.exceptions.CfnAccessDeniedException;
import software.amazon.cloudformation.exceptions.CfnAlreadyExistsException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.exceptions.CfnServiceInternalErrorException;
import software.amazon.cloudformation.exceptions.CfnServiceLimitExceededException;
import software.amazon.cloudformation.exceptions.CfnThrottlingException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

class Translator {
    private Translator() {
    }

    static DescribePipelineRequest translateToDescribePipelineRequest(final ResourceModel model) {
        return DescribePipelineRequest.builder().pipelineName(model.getPipelineName()).build();
    }

    static ResourceModel translateFromDescribeResponse(final DescribePipelineResponse describePipelineResponse,
                                                       final ListTagsForResourceResponse listTagsForResourceResponse) {
        final Pipeline pipeline = describePipelineResponse.pipeline();
        final List<Tag> tagList = listTagsForResourceResponse.tags();
        return ResourceModel.builder()
                .pipelineName(pipeline.name())
                .id(pipeline.arn())
                .pipelineActivities(translateActivitiesToCfn(pipeline.activities()))
                .tags(translateTagsToCfn(tagList))
                .build();
    }

    static CreatePipelineRequest translateToCreatePipelineRequest(final ResourceModel model) {
        return CreatePipelineRequest.builder()
                .pipelineName(model.getPipelineName())
                .pipelineActivities(translatePipelineActivitiesFromCfn(model.getPipelineActivities()))
                .tags(translateTagListsFromCfn(model.getTags()))
                .build();
    }

    static UpdatePipelineRequest translateToUpdatePipelineRequest(final ResourceModel model) {
        return UpdatePipelineRequest.builder()
                .pipelineName(model.getPipelineName())
                .pipelineActivities(translatePipelineActivitiesFromCfn(model.getPipelineActivities()))
                .build();
    }

    static DeletePipelineRequest translateToDeletePipelineRequest(final ResourceModel model) {
        return DeletePipelineRequest.builder().pipelineName(model.getPipelineName()).build();
    }

    static BaseHandlerException translateExceptionToHandlerException(
            final IoTAnalyticsException e,
            final String operation,
            @Nullable final String name
    ) {
        if (e instanceof ResourceAlreadyExistsException) {
            return new CfnAlreadyExistsException(e);
        } else if (e instanceof ResourceNotFoundException) {
            if(StringUtils.isNullOrEmpty(name)) return new CfnNotFoundException(e);
            return new CfnNotFoundException(ResourceModel.TYPE_NAME, name);
        } else if (e instanceof InvalidRequestException) {
            return new CfnInvalidRequestException(e.getMessage(), e);
        } else if (e instanceof ThrottlingException) {
            return new CfnThrottlingException(operation, e);
        } else if (e instanceof ServiceUnavailableException) {
            return new CfnGeneralServiceException(operation, e);
        } else if (e instanceof LimitExceededException) {
            return new CfnServiceLimitExceededException(ResourceModel.TYPE_NAME, e.getMessage());
        } else {
            if (e.awsErrorDetails() != null
                    && "AccessDeniedException".equalsIgnoreCase(e.awsErrorDetails().errorCode())) {
                return new CfnAccessDeniedException(operation, e);
            }
            return new CfnServiceInternalErrorException(operation, e);
        }
    }

    private static List<Activity> translateActivitiesToCfn(
            @Nonnull final List<PipelineActivity> pipelineActivities) {
        return pipelineActivities.stream().map(pipelineActivity -> {
            if (pipelineActivity.channel() != null) {
                return translateChannelActivityToCfn(pipelineActivity.channel());
            } else if (pipelineActivity.addAttributes() != null) {
                return translateAddAttributesActivityToCfn(pipelineActivity.addAttributes());
            } else if (pipelineActivity.removeAttributes() != null) {
                return translateRemoveAttributesActivityToCfn(pipelineActivity.removeAttributes());
            } else if (pipelineActivity.selectAttributes() != null) {
                return translateSelectAttributesActivityToCfn(pipelineActivity.selectAttributes());
            } else if (pipelineActivity.deviceRegistryEnrich() != null) {
                return translateDeviceRegistryEnrichActivityToCfn(pipelineActivity.deviceRegistryEnrich());
            } else if (pipelineActivity.deviceShadowEnrich() != null) {
                return translateDeviceShadowEnrichActivityToCfn(pipelineActivity.deviceShadowEnrich());
            } else if (pipelineActivity.filter() != null) {
                return translateFilterActivityToCfn(pipelineActivity.filter());
            } else if (pipelineActivity.math() != null) {
                return translateMathActivityToCfn(pipelineActivity.math());
            } else if (pipelineActivity.lambda() != null) {
                return translateLambdaActivityToCfn(pipelineActivity.lambda());
            } else if (pipelineActivity.datastore() != null) {
                return translateDatastoreActivityToCfn(pipelineActivity.datastore());
            } else {
                return null;
            }
        }).filter(Objects::nonNull).collect(Collectors.toList());
    }

    private static List<PipelineActivity> translatePipelineActivitiesFromCfn(
            @Nonnull List<Activity> cfnActivityList) {
        return cfnActivityList.stream().map(cfnActivity -> {
            if (cfnActivity.getChannel() != null) {
                return translateChannelActivityFromCfn(cfnActivity.getChannel());
            } else if (cfnActivity.getAddAttributes() != null) {
                return translateAddAttributesActivityFromCfn(cfnActivity.getAddAttributes());
            } else if (cfnActivity.getRemoveAttributes() != null) {
                return translateRemoveAttributesActivityFromCfn(cfnActivity.getRemoveAttributes());
            } else if (cfnActivity.getSelectAttributes() != null) {
                return translateSelectAttributesActivityFromCfn(cfnActivity.getSelectAttributes());
            } else if (cfnActivity.getDeviceRegistryEnrich() != null) {
                return translateDeviceRegistryEnrichActivityFromCfn(cfnActivity.getDeviceRegistryEnrich());
            } else if (cfnActivity.getDeviceShadowEnrich() != null) {
                return translateDeviceShadowEnrichActivityFromCfn(cfnActivity.getDeviceShadowEnrich());
            } else if (cfnActivity.getFilter() != null) {
                return translateFilterActivityFromCfn(cfnActivity.getFilter());
            } else if (cfnActivity.getMath() != null) {
                return translateMathActivityFromCfn(cfnActivity.getMath());
            } else if (cfnActivity.getLambda() != null) {
                return translateLambdaActivityFromCfn(cfnActivity.getLambda());
            } else if (cfnActivity.getDatastore() != null) {
                return translateDatastoreActivityFromCfn(cfnActivity.getDatastore());
            } else {
                return null;
            }
        }).filter(Objects::nonNull).collect(Collectors.toList());
    }

    private static Activity translateChannelActivityToCfn(
            @Nonnull final ChannelActivity channelActivity) {
        return Activity.builder()
                .channel(Channel
                        .builder()
                        .channelName(channelActivity.channelName())
                        .name(channelActivity.name())
                        .next(channelActivity.next())
                        .build()).build();
    }

    private static PipelineActivity translateChannelActivityFromCfn(
            @Nonnull final Channel cfnChannelActivity) {
        return PipelineActivity.builder()
                .channel(ChannelActivity
                        .builder()
                        .channelName(cfnChannelActivity.getChannelName())
                        .name(cfnChannelActivity.getName())
                        .next(cfnChannelActivity.getNext())
                        .build()).build();
    }

    private static Activity translateAddAttributesActivityToCfn(
            @Nonnull final AddAttributesActivity addAttributesActivity) {
        return Activity.builder()
                .addAttributes(AddAttributes
                        .builder()
                        .attributes(addAttributesActivity.attributes())
                        .name(addAttributesActivity.name())
                        .next(addAttributesActivity.next())
                        .build()).build();
    }

    private static PipelineActivity translateAddAttributesActivityFromCfn(
            @Nonnull final AddAttributes cfnAddAttributesActivity) {
        return PipelineActivity.builder()
                .addAttributes(AddAttributesActivity
                        .builder()
                        .attributes(cfnAddAttributesActivity.getAttributes())
                        .name(cfnAddAttributesActivity.getName())
                        .next(cfnAddAttributesActivity.getNext())
                        .build()).build();
    }

    private static Activity translateRemoveAttributesActivityToCfn(
            @Nonnull final RemoveAttributesActivity removeAttributesActivity) {
        return Activity.builder()
                .removeAttributes(RemoveAttributes.builder()
                        .attributes(removeAttributesActivity.attributes())
                        .name(removeAttributesActivity.name())
                        .next(removeAttributesActivity.next())
                        .build()).build();
    }

    private static PipelineActivity translateRemoveAttributesActivityFromCfn(
            @Nonnull final RemoveAttributes cfnRemoveAttributesActivity) {
        return PipelineActivity.builder()
                .removeAttributes(RemoveAttributesActivity.builder()
                        .attributes(cfnRemoveAttributesActivity.getAttributes())
                        .name(cfnRemoveAttributesActivity.getName())
                        .next(cfnRemoveAttributesActivity.getNext())
                        .build()).build();
    }

    private static Activity translateSelectAttributesActivityToCfn(
            @Nonnull final SelectAttributesActivity selectAttributesActivity) {
        return Activity.builder()
                .selectAttributes(SelectAttributes.builder()
                        .attributes(selectAttributesActivity.attributes())
                        .name(selectAttributesActivity.name())
                        .next(selectAttributesActivity.next())
                        .build()).build();
    }

    private static PipelineActivity translateSelectAttributesActivityFromCfn(
            @Nonnull final SelectAttributes cfnSelectAttributesActivity) {
        return PipelineActivity.builder()
                .selectAttributes(SelectAttributesActivity.builder()
                        .attributes(cfnSelectAttributesActivity.getAttributes())
                        .name(cfnSelectAttributesActivity.getName())
                        .next(cfnSelectAttributesActivity.getNext())
                        .build()).build();
    }

    private static Activity translateDeviceRegistryEnrichActivityToCfn(
            @Nonnull final DeviceRegistryEnrichActivity deviceRegistryEnrichActivity) {
        return Activity.builder()
                .deviceRegistryEnrich(DeviceRegistryEnrich.builder()
                        .roleArn(deviceRegistryEnrichActivity.roleArn())
                        .name(deviceRegistryEnrichActivity.name())
                        .next(deviceRegistryEnrichActivity.next())
                        .attribute(deviceRegistryEnrichActivity.attribute())
                        .thingName(deviceRegistryEnrichActivity.thingName())
                        .build()).build();
    }

    private static PipelineActivity translateDeviceRegistryEnrichActivityFromCfn(
            @Nonnull final DeviceRegistryEnrich cfnDeviceRegistryEnrichActivity) {
        return PipelineActivity.builder()
                .deviceRegistryEnrich(DeviceRegistryEnrichActivity.builder()
                        .roleArn(cfnDeviceRegistryEnrichActivity.getRoleArn())
                        .name(cfnDeviceRegistryEnrichActivity.getName())
                        .next(cfnDeviceRegistryEnrichActivity.getNext())
                        .attribute(cfnDeviceRegistryEnrichActivity.getAttribute())
                        .thingName(cfnDeviceRegistryEnrichActivity.getThingName())
                        .build()).build();
    }

    private static Activity translateDeviceShadowEnrichActivityToCfn(
            @Nonnull final DeviceShadowEnrichActivity deviceShadowEnrichActivity) {
        return Activity.builder()
                .deviceShadowEnrich(DeviceShadowEnrich.builder()
                        .roleArn(deviceShadowEnrichActivity.roleArn())
                        .attribute(deviceShadowEnrichActivity.attribute())
                        .name(deviceShadowEnrichActivity.name())
                        .thingName(deviceShadowEnrichActivity.thingName())
                        .next(deviceShadowEnrichActivity.next())
                        .build()).build();
    }

    private static PipelineActivity translateDeviceShadowEnrichActivityFromCfn(
            @Nonnull final DeviceShadowEnrich cfnDeviceShadowEnrichActivity) {
        return PipelineActivity.builder()
                .deviceShadowEnrich(DeviceShadowEnrichActivity.builder()
                        .roleArn(cfnDeviceShadowEnrichActivity.getRoleArn())
                        .attribute(cfnDeviceShadowEnrichActivity.getAttribute())
                        .name(cfnDeviceShadowEnrichActivity.getName())
                        .thingName(cfnDeviceShadowEnrichActivity.getThingName())
                        .next(cfnDeviceShadowEnrichActivity.getNext())
                        .build()).build();
    }

    private static Activity translateFilterActivityToCfn(
            @Nonnull final FilterActivity filterActivity) {
        return Activity.builder()
                .filter(Filter.builder()
                        .filter(filterActivity.filter())
                        .name(filterActivity.name())
                        .next(filterActivity.next())
                        .build()).build();
    }

    private static PipelineActivity translateFilterActivityFromCfn(
            @Nonnull final Filter cfnFilterActivity) {
        return PipelineActivity.builder()
                .filter(FilterActivity.builder()
                        .filter(cfnFilterActivity.getFilter())
                        .name(cfnFilterActivity.getName())
                        .next(cfnFilterActivity.getNext())
                        .build()).build();
    }

    private static Activity translateMathActivityToCfn(
            @Nonnull final MathActivity mathActivity) {
        return Activity.builder()
                .math(Math.builder()
                        .math(mathActivity.math())
                        .attribute(mathActivity.attribute())
                        .name(mathActivity.name())
                        .next(mathActivity.next())
                        .build()).build();
    }

    private static PipelineActivity translateMathActivityFromCfn(
            @Nonnull final Math cfnMathActivity) {
        return PipelineActivity.builder()
                .math(MathActivity.builder()
                        .math(cfnMathActivity.getMath())
                        .attribute(cfnMathActivity.getAttribute())
                        .name(cfnMathActivity.getName())
                        .next(cfnMathActivity.getNext())
                        .build()).build();
    }

    private static Activity translateLambdaActivityToCfn(
            @Nonnull final LambdaActivity lambdaActivity) {
        return Activity.builder()
                .lambda(Lambda.builder()
                        .lambdaName(lambdaActivity.lambdaName())
                        .name(lambdaActivity.name())
                        .next(lambdaActivity.next())
                        .batchSize(lambdaActivity.batchSize())
                        .build()).build();
    }

    private static PipelineActivity translateLambdaActivityFromCfn(
            @Nonnull final Lambda cfnLambdaActivity) {
        return PipelineActivity.builder()
                .lambda(LambdaActivity.builder()
                        .lambdaName(cfnLambdaActivity.getLambdaName())
                        .name(cfnLambdaActivity.getName())
                        .next(cfnLambdaActivity.getNext())
                        .batchSize(cfnLambdaActivity.getBatchSize())
                        .build()).build();
    }

    private static Activity translateDatastoreActivityToCfn(
            @Nonnull final DatastoreActivity datastoreActivity) {
        return Activity.builder()
                .datastore(Datastore.builder()
                        .datastoreName(datastoreActivity.datastoreName())
                        .name(datastoreActivity.name())
                        .build()).build();
    }

    private static PipelineActivity translateDatastoreActivityFromCfn(
            @Nonnull final Datastore cfnDatastoreActivity) {
        return PipelineActivity.builder()
                .datastore(DatastoreActivity.builder()
                        .datastoreName(cfnDatastoreActivity.getDatastoreName())
                        .name(cfnDatastoreActivity.getName())
                        .build()).build();
    }

    private static List<com.amazonaws.iotanalytics.pipeline.Tag> translateTagsToCfn(
            @Nullable final List<software.amazon.awssdk.services.iotanalytics.model.Tag> tags) {
        if (tags == null || tags.isEmpty()) {
            return null;
        }
        return tags.stream().map((tag ->
                com.amazonaws.iotanalytics.pipeline.Tag.builder()
                        .key(tag.key())
                        .value(tag.value())
                        .build()))
                .collect(Collectors.toList());
    }

    private static List<software.amazon.awssdk.services.iotanalytics.model.Tag> translateTagListsFromCfn(
            @Nullable final List<com.amazonaws.iotanalytics.pipeline.Tag> cfnTagList
    ) {
        if (cfnTagList == null) {
            return null;
        } else {
            return cfnTagList.stream().map(tag ->
                    Tag.builder()
                            .key(tag.getKey())
                            .value(tag.getValue())
                            .build()
            ).collect(Collectors.toList());
        }
    }
}
