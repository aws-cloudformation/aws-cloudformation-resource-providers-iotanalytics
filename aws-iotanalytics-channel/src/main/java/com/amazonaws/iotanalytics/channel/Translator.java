package com.amazonaws.iotanalytics.channel;

import com.amazonaws.util.StringUtils;
import software.amazon.awssdk.services.iotanalytics.model.Channel;
import software.amazon.awssdk.services.iotanalytics.model.ChannelStorage;
import software.amazon.awssdk.services.iotanalytics.model.CreateChannelRequest;
import software.amazon.awssdk.services.iotanalytics.model.CustomerManagedChannelS3Storage;
import software.amazon.awssdk.services.iotanalytics.model.DeleteChannelRequest;
import software.amazon.awssdk.services.iotanalytics.model.RetentionPeriod;

import software.amazon.awssdk.services.iotanalytics.model.DescribeChannelRequest;
import software.amazon.awssdk.services.iotanalytics.model.DescribeChannelResponse;
import software.amazon.awssdk.services.iotanalytics.model.InvalidRequestException;
import software.amazon.awssdk.services.iotanalytics.model.LimitExceededException;
import software.amazon.awssdk.services.iotanalytics.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.iotanalytics.model.ResourceAlreadyExistsException;
import software.amazon.awssdk.services.iotanalytics.model.ResourceNotFoundException;
import software.amazon.awssdk.services.iotanalytics.model.ServiceManagedChannelS3Storage;
import software.amazon.awssdk.services.iotanalytics.model.ServiceUnavailableException;
import software.amazon.awssdk.services.iotanalytics.model.Tag;
import software.amazon.awssdk.services.iotanalytics.model.ThrottlingException;
import software.amazon.awssdk.services.iotanalytics.model.UpdateChannelRequest;
import software.amazon.cloudformation.exceptions.BaseHandlerException;
import software.amazon.cloudformation.exceptions.CfnAlreadyExistsException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.exceptions.CfnServiceInternalErrorException;
import software.amazon.cloudformation.exceptions.CfnServiceLimitExceededException;
import software.amazon.cloudformation.exceptions.CfnThrottlingException;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class Translator {

    static DescribeChannelRequest translateToDescribeChannelRequest(final ResourceModel model) {
        return DescribeChannelRequest.builder().channelName(model.getChannelName()).build();
    }

    static ResourceModel translateFromDescribeResponse(final DescribeChannelResponse describeChannelResponse,
                                                       final ListTagsForResourceResponse listTagsForResourceResponse) {
        final Channel channel = describeChannelResponse.channel();
        final List<Tag> tagList = listTagsForResourceResponse.tags();
        return ResourceModel.builder()
                .channelName(channel.name())
                .id(channel.arn())
                .retentionPeriod(translateRetentionPeriodToCfn(channel.retentionPeriod()))
                .channelStorage(translateChannelStorageToCfn(channel.storage()))
                .tags(translateTagsToCfn(tagList))
                .build();
    }

    static CreateChannelRequest translateToCreateChannelRequest(final ResourceModel model) {
        return CreateChannelRequest.builder()
                .channelName(model.getChannelName())
                .channelStorage(translateChannelStorageFromCfn(model.getChannelStorage()))
                .retentionPeriod(translateRetentionPeriodFromCfn(model.getRetentionPeriod()))
                .tags(translateTagListsFromCfn( model.getTags()))
                .build();
    }

    static UpdateChannelRequest translateToUpdateChannelRequest(final ResourceModel model) {
        return UpdateChannelRequest.builder()
                .channelName(model.getChannelName())
                .channelStorage(translateChannelStorageFromCfn(model.getChannelStorage()))
                .retentionPeriod(translateRetentionPeriodFromCfn(model.getRetentionPeriod()))
                .build();
    }

    static DeleteChannelRequest translateToDeleteChannelRequest(final ResourceModel model) {
        return DeleteChannelRequest.builder().channelName(model.getChannelName()).build();
    }

    static BaseHandlerException translateExceptionToHandlerException(
            final Exception e,
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
            throw new CfnGeneralServiceException(operation, e);
        } else if (e instanceof LimitExceededException) {
            throw new CfnServiceLimitExceededException(ResourceModel.TYPE_NAME, e.getMessage());
        } else {
            return new CfnServiceInternalErrorException(operation, e);
        }
    }

    private static com.amazonaws.iotanalytics.channel.RetentionPeriod translateRetentionPeriodToCfn(
            @Nullable final RetentionPeriod retentionPeriod) {
        if (retentionPeriod == null) {
            return null;
        }
        return com.amazonaws.iotanalytics.channel.RetentionPeriod.builder()
                .numberOfDays(retentionPeriod.numberOfDays())
                .unlimited(retentionPeriod.unlimited())
                .build();
    }

    private static RetentionPeriod translateRetentionPeriodFromCfn(
            @Nullable final com.amazonaws.iotanalytics.channel.RetentionPeriod cfnRetentionPeriod
    ) {
        if (cfnRetentionPeriod == null) {
            return null;
        }
        return RetentionPeriod.builder()
                .numberOfDays(cfnRetentionPeriod.getNumberOfDays())
                .unlimited(cfnRetentionPeriod.getUnlimited())
                .build();
    }

    private static com.amazonaws.iotanalytics.channel.ChannelStorage translateChannelStorageToCfn(
            @Nullable final ChannelStorage channelStorage) {
        if (channelStorage != null && channelStorage.customerManagedS3() != null) {
            return com.amazonaws.iotanalytics.channel.ChannelStorage.builder().customerManagedS3(
                    com.amazonaws.iotanalytics.channel.CustomerManagedS3.builder()
                            .bucket(channelStorage.customerManagedS3().bucket())
                            .keyPrefix(channelStorage.customerManagedS3().keyPrefix())
                            .roleArn(channelStorage.customerManagedS3().roleArn())
                            .build())
                    .build();
        }
        return com.amazonaws.iotanalytics.channel.ChannelStorage
                .builder()
                .serviceManagedS3(new HashMap<>())
                .build();
    }

    private static ChannelStorage translateChannelStorageFromCfn(
            @Nullable final com.amazonaws.iotanalytics.channel.ChannelStorage cfnChannelStorage
    ) {
        if (cfnChannelStorage == null) {
            return null;
        }
        final ChannelStorage.Builder builder =
                ChannelStorage.builder();
        if (cfnChannelStorage.getCustomerManagedS3() != null) {
            builder.customerManagedS3(CustomerManagedChannelS3Storage
                    .builder()
                    .bucket(cfnChannelStorage.getCustomerManagedS3().getBucket())
                    .keyPrefix(cfnChannelStorage.getCustomerManagedS3().getKeyPrefix())
                    .roleArn(cfnChannelStorage.getCustomerManagedS3().getRoleArn())
                    .build()
            );
        } else {
            builder.serviceManagedS3(ServiceManagedChannelS3Storage.builder().build());
        }
        return builder.build();
    }

    private static List<com.amazonaws.iotanalytics.channel.Tag> translateTagsToCfn(final List<Tag> tags) {
        if (tags == null || tags.isEmpty()) {
            return null;
        }
        return tags.stream().map((tag ->
                com.amazonaws.iotanalytics.channel.Tag.builder()
                        .key(tag.key())
                        .value(tag.value())
                        .build()))
                .collect(Collectors.toList());
    }

    private static List<Tag> translateTagListsFromCfn(
            @Nullable final List<com.amazonaws.iotanalytics.channel.Tag> tagList
    ) {
        if (tagList == null) {
            return null;
        } else {
            return tagList.stream().map(tag ->
                    Tag.builder()
                            .key(tag.getKey())
                            .value(tag.getValue())
                            .build()
            ).collect(Collectors.toList());
        }
    }
}
