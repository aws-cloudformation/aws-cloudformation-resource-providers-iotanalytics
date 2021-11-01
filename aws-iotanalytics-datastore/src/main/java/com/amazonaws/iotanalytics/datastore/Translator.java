package com.amazonaws.iotanalytics.datastore;

import com.amazonaws.util.StringUtils;
import com.google.common.annotations.VisibleForTesting;
import software.amazon.awssdk.services.iotanalytics.model.Column;
import software.amazon.awssdk.services.iotanalytics.model.Datastore;
import software.amazon.awssdk.services.iotanalytics.model.DatastoreIotSiteWiseMultiLayerStorage;
import software.amazon.awssdk.services.iotanalytics.model.DatastorePartition;
import software.amazon.awssdk.services.iotanalytics.model.DatastorePartitions;
import software.amazon.awssdk.services.iotanalytics.model.DatastoreStorage;
import software.amazon.awssdk.services.iotanalytics.model.CreateDatastoreRequest;
import software.amazon.awssdk.services.iotanalytics.model.CustomerManagedDatastoreS3Storage;
import software.amazon.awssdk.services.iotanalytics.model.DeleteDatastoreRequest;
import software.amazon.awssdk.services.iotanalytics.model.DescribeDatastoreRequest;
import software.amazon.awssdk.services.iotanalytics.model.DescribeDatastoreResponse;
import software.amazon.awssdk.services.iotanalytics.model.FileFormatConfiguration;
import software.amazon.awssdk.services.iotanalytics.model.InvalidRequestException;
import software.amazon.awssdk.services.iotanalytics.model.IoTAnalyticsException;
import software.amazon.awssdk.services.iotanalytics.model.IotSiteWiseCustomerManagedDatastoreS3Storage;
import software.amazon.awssdk.services.iotanalytics.model.JsonConfiguration;
import software.amazon.awssdk.services.iotanalytics.model.LimitExceededException;
import software.amazon.awssdk.services.iotanalytics.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.iotanalytics.model.ParquetConfiguration;
import software.amazon.awssdk.services.iotanalytics.model.Partition;
import software.amazon.awssdk.services.iotanalytics.model.ResourceAlreadyExistsException;
import software.amazon.awssdk.services.iotanalytics.model.ResourceNotFoundException;
import software.amazon.awssdk.services.iotanalytics.model.RetentionPeriod;
import software.amazon.awssdk.services.iotanalytics.model.ServiceManagedDatastoreS3Storage;
import software.amazon.awssdk.services.iotanalytics.model.SchemaDefinition;
import software.amazon.awssdk.services.iotanalytics.model.ServiceUnavailableException;
import software.amazon.awssdk.services.iotanalytics.model.Tag;
import software.amazon.awssdk.services.iotanalytics.model.ThrottlingException;
import software.amazon.awssdk.services.iotanalytics.model.TimestampPartition;
import software.amazon.awssdk.services.iotanalytics.model.UpdateDatastoreRequest;
import software.amazon.cloudformation.exceptions.BaseHandlerException;
import software.amazon.cloudformation.exceptions.CfnAccessDeniedException;
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

class Translator {
    private Translator() {}
    static ResourceModel translateFromDescribeResponse(final DescribeDatastoreResponse describeDatastoreResponse,
                                                       final ListTagsForResourceResponse listTagsForResourceResponse) {
        final Datastore datastore = describeDatastoreResponse.datastore();
        final List<Tag> tagList = listTagsForResourceResponse.tags();
        return ResourceModel.builder()
                .datastoreName(datastore.name())
                .id(datastore.arn())
                .retentionPeriod(translateRetentionPeriodToCfn(datastore.retentionPeriod()))
                .datastoreStorage(translateDatastoreStorageToCfn(datastore.storage()))
                .fileFormatConfiguration(translateFileFormatConfigurationToCfn(datastore.fileFormatConfiguration()))
                .tags(translateTagsToCfn(tagList))
                .datastorePartitions(translateDatastorePartitionsToCfn(datastore.datastorePartitions()))
                .build();
    }
    static UpdateDatastoreRequest translateToUpdateDatastoreRequest(final ResourceModel model) {
        return UpdateDatastoreRequest.builder()
                .datastoreName(model.getDatastoreName())
                .datastoreStorage(translateDatastoreStorageFromCfn(model.getDatastoreStorage()))
                .retentionPeriod(translateRetentionPeriodFromCfn(model.getRetentionPeriod()))
                .fileFormatConfiguration(translateFileFormatConfigurationFromCfn(model.getFileFormatConfiguration()))
                .build();
    }

    static DescribeDatastoreRequest translateToDescribeDatastoreRequest(final ResourceModel model) {
        return DescribeDatastoreRequest.builder().datastoreName(model.getDatastoreName()).build();
    }

    static DeleteDatastoreRequest translateToDeleteDatastoreRequest(final ResourceModel model) {
        return DeleteDatastoreRequest.builder().datastoreName(model.getDatastoreName()).build();
    }


    static CreateDatastoreRequest translateToCreateDatastoreRequest(final ResourceModel model) {
        return CreateDatastoreRequest.builder()
                .datastoreName(model.getDatastoreName())
                .datastoreStorage(translateDatastoreStorageFromCfn(model.getDatastoreStorage()))
                .retentionPeriod(translateRetentionPeriodFromCfn(model.getRetentionPeriod()))
                .fileFormatConfiguration(translateFileFormatConfigurationFromCfn(model.getFileFormatConfiguration()))
                .tags(translateTagListsFromCfn(model.getTags()))
                .datastorePartitions(translateDatastorePartitionsFromCfn(model.getDatastorePartitions()))
                .build();
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

    private static com.amazonaws.iotanalytics.datastore.DatastoreStorage translateDatastoreStorageToCfn(
            @Nullable final DatastoreStorage datastoreStorage
    ) {
        if (datastoreStorage != null) {
            if (datastoreStorage.customerManagedS3() != null) {
                return com.amazonaws.iotanalytics.datastore.DatastoreStorage.builder().customerManagedS3(
                        com.amazonaws.iotanalytics.datastore.CustomerManagedS3.builder()
                                .bucket(datastoreStorage.customerManagedS3().bucket())
                                .keyPrefix(datastoreStorage.customerManagedS3().keyPrefix())
                                .roleArn(datastoreStorage.customerManagedS3().roleArn())
                                .build())
                        .build();
            }

            if (datastoreStorage.iotSiteWiseMultiLayerStorage() != null) {
                return com.amazonaws.iotanalytics.datastore.DatastoreStorage.builder().iotSiteWiseMultiLayerStorage(
                        com.amazonaws.iotanalytics.datastore.IotSiteWiseMultiLayerStorage.builder()
                                .customerManagedS3Storage(
                                        com.amazonaws.iotanalytics.datastore.CustomerManagedS3Storage.builder()
                                                .bucket(datastoreStorage.iotSiteWiseMultiLayerStorage().customerManagedS3Storage().bucket())
                                                .keyPrefix(datastoreStorage.iotSiteWiseMultiLayerStorage().customerManagedS3Storage().keyPrefix())
                                                .build()
                                )
                                .build())
                        .build();
            }
        }
        return com.amazonaws.iotanalytics.datastore.DatastoreStorage
                .builder()
                .serviceManagedS3(new HashMap<>())
                .build();
    }

    private static DatastoreStorage translateDatastoreStorageFromCfn(
            @Nullable final com.amazonaws.iotanalytics.datastore.DatastoreStorage cfnDatastoreStorage
    ) {
        if (cfnDatastoreStorage == null) {
            return null;
        }
        final DatastoreStorage.Builder builder = DatastoreStorage.builder();
        if (cfnDatastoreStorage.getIotSiteWiseMultiLayerStorage() != null) {
            builder.iotSiteWiseMultiLayerStorage(DatastoreIotSiteWiseMultiLayerStorage.builder()
                    .customerManagedS3Storage(IotSiteWiseCustomerManagedDatastoreS3Storage.builder()
                            .bucket(cfnDatastoreStorage.getIotSiteWiseMultiLayerStorage().getCustomerManagedS3Storage().getBucket())
                            .keyPrefix(cfnDatastoreStorage.getIotSiteWiseMultiLayerStorage().getCustomerManagedS3Storage().getKeyPrefix())
                            .build()
                    )
                    .build()
            );
        }
        if (cfnDatastoreStorage.getCustomerManagedS3() != null) {
            builder.customerManagedS3(CustomerManagedDatastoreS3Storage
                    .builder()
                    .bucket(cfnDatastoreStorage.getCustomerManagedS3().getBucket())
                    .keyPrefix(cfnDatastoreStorage.getCustomerManagedS3().getKeyPrefix())
                    .roleArn(cfnDatastoreStorage.getCustomerManagedS3().getRoleArn())
                    .build()
            );
        }
        if (cfnDatastoreStorage.getServiceManagedS3() != null) {
            builder.serviceManagedS3(ServiceManagedDatastoreS3Storage.builder().build());
        }
        return builder.build();
    }

    private static com.amazonaws.iotanalytics.datastore.RetentionPeriod translateRetentionPeriodToCfn(
            @Nullable final RetentionPeriod retentionPeriod
    ) {
        if (retentionPeriod == null) {
            return null;
        }
        return com.amazonaws.iotanalytics.datastore.RetentionPeriod.builder()
                .numberOfDays(retentionPeriod.numberOfDays())
                .unlimited(retentionPeriod.unlimited())
                .build();
    }

    private static RetentionPeriod translateRetentionPeriodFromCfn(
            @Nullable final com.amazonaws.iotanalytics.datastore.RetentionPeriod cfnRetentionPeriod
    ) {
        if (cfnRetentionPeriod == null) {
            return null;
        }
        return RetentionPeriod.builder()
                .numberOfDays(cfnRetentionPeriod.getNumberOfDays())
                .unlimited(cfnRetentionPeriod.getUnlimited())
                .build();
    }

    private static com.amazonaws.iotanalytics.datastore.FileFormatConfiguration translateFileFormatConfigurationToCfn(
            @Nullable final FileFormatConfiguration fileFormatConfiguration
    ) {
        if (fileFormatConfiguration != null && fileFormatConfiguration.parquetConfiguration() != null) {
            return com.amazonaws.iotanalytics.datastore.FileFormatConfiguration.builder()
                    .parquetConfiguration(com.amazonaws.iotanalytics.datastore.ParquetConfiguration
                            .builder()
                            .schemaDefinition(translateSchemaDefinitionToCfn(fileFormatConfiguration
                                    .parquetConfiguration()
                                    .schemaDefinition()))
                            .build())
                    .build();
        }
        return com.amazonaws.iotanalytics.datastore.FileFormatConfiguration.builder()
                .jsonConfiguration(new HashMap<>())
                .build();
    }

    private static FileFormatConfiguration translateFileFormatConfigurationFromCfn(
            @Nullable final com.amazonaws.iotanalytics.datastore.FileFormatConfiguration cfnFileFormatConfiguration
    ) {
        if (cfnFileFormatConfiguration == null) {
            return null;
        }
        final FileFormatConfiguration.Builder builder = FileFormatConfiguration.builder();
        if (cfnFileFormatConfiguration.getParquetConfiguration() != null) {
            final com.amazonaws.iotanalytics.datastore.SchemaDefinition cfnSchemaDefinition
                    = cfnFileFormatConfiguration.getParquetConfiguration().getSchemaDefinition();
            builder.parquetConfiguration(ParquetConfiguration
                    .builder()
                    .schemaDefinition(translateSchemaDefinitionFromCfn(cfnSchemaDefinition))
                    .build());
        }
        if (cfnFileFormatConfiguration.getJsonConfiguration() != null) {
            builder.jsonConfiguration(JsonConfiguration.builder().build());
        }
        return builder.build();
    }

    private static com.amazonaws.iotanalytics.datastore.DatastorePartitions translateDatastorePartitionsToCfn(
            @Nullable final DatastorePartitions datastorePartitions
    ) {
        if (datastorePartitions == null) {
            return null;
        }
        final com.amazonaws.iotanalytics.datastore.DatastorePartitions.DatastorePartitionsBuilder builder =
                com.amazonaws.iotanalytics.datastore.DatastorePartitions.builder();
        if (datastorePartitions.partitions() != null) {
            builder.partitions(datastorePartitions.partitions().stream().map(par -> {
                final com.amazonaws.iotanalytics.datastore.DatastorePartition.DatastorePartitionBuilder dpBuilder =
                        com.amazonaws.iotanalytics.datastore.DatastorePartition.builder();
                if (par.attributePartition() != null) {
                    dpBuilder.partition(com.amazonaws.iotanalytics.datastore.Partition.builder()
                            .attributeName(par.attributePartition().attributeName())
                            .build());
                }
                if (par.timestampPartition() != null) {
                    dpBuilder.timestampPartition(com.amazonaws.iotanalytics.datastore.TimestampPartition.builder()
                            .attributeName(par.timestampPartition().attributeName())
                            .timestampFormat(par.timestampPartition().timestampFormat())
                            .build());
                }
                return dpBuilder.build();
            })
                    .collect(Collectors.toList()));
        }
        return builder.build();
    }

    private static DatastorePartitions translateDatastorePartitionsFromCfn(
            @Nullable final com.amazonaws.iotanalytics.datastore.DatastorePartitions cfnDatastorePartitions
    ) {
        if (cfnDatastorePartitions == null) {
            return null;
        }
        final DatastorePartitions.Builder builder = DatastorePartitions.builder();
        if (cfnDatastorePartitions.getPartitions() != null) {
            builder.partitions(cfnDatastorePartitions.getPartitions().stream().map(par -> {
                final DatastorePartition.Builder dpBuilder = DatastorePartition.builder();
                if (par.getPartition() != null) {
                    dpBuilder.attributePartition(Partition.builder()
                            .attributeName(par.getPartition().getAttributeName())
                            .build());
                }
                if (par.getTimestampPartition() != null) {
                    dpBuilder.timestampPartition(TimestampPartition.builder()
                            .attributeName(par.getTimestampPartition().getAttributeName())
                            .timestampFormat(par.getTimestampPartition().getTimestampFormat())
                            .build());
                }
                return dpBuilder.build();
            })
                    .collect(Collectors.toList()));
        }
        return builder.build();
    }

    @VisibleForTesting
    static com.amazonaws.iotanalytics.datastore.SchemaDefinition translateSchemaDefinitionToCfn(
            @Nullable final SchemaDefinition schemaDefinition
    ) {
        if (schemaDefinition == null) {
            return null;
        }
        final com.amazonaws.iotanalytics.datastore.SchemaDefinition.SchemaDefinitionBuilder builder =
                com.amazonaws.iotanalytics.datastore.SchemaDefinition.builder();
        if (schemaDefinition.columns() != null) {
            builder.columns(schemaDefinition.columns().stream().map(column ->
                    com.amazonaws.iotanalytics.datastore.Column.builder()
                            .name(column.name())
                            .type(column.type())
                            .build())
                    .collect(Collectors.toList()));
        }
        return builder.build();
    }

    @VisibleForTesting
    static SchemaDefinition translateSchemaDefinitionFromCfn(
            @Nullable final com.amazonaws.iotanalytics.datastore.SchemaDefinition cfnSchemaDefinition
    ) {
        if (cfnSchemaDefinition == null) {
            return null;
        }
        final SchemaDefinition.Builder builder = SchemaDefinition.builder();
        if (cfnSchemaDefinition.getColumns() != null) {
            builder.columns(cfnSchemaDefinition.getColumns().stream().map(column ->
                    Column.builder()
                            .name(column.getName())
                            .type(column.getType())
                            .build())
                    .collect(Collectors.toList()));
        }
        return builder.build();
    }

    private static List<com.amazonaws.iotanalytics.datastore.Tag> translateTagsToCfn(final List<Tag> tags) {
        if (tags == null || tags.isEmpty()) {
            return null;
        }
        return tags.stream().map((tag ->
                com.amazonaws.iotanalytics.datastore.Tag.builder()
                        .key(tag.key())
                        .value(tag.value())
                        .build()))
                .collect(Collectors.toList());
    }

    private static List<Tag> translateTagListsFromCfn(
            @Nullable final List<com.amazonaws.iotanalytics.datastore.Tag> cfnTagList
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
