package com.amazonaws.iotanalytics.dataset;

import com.amazonaws.util.StringUtils;
import com.google.common.annotations.VisibleForTesting;
import software.amazon.awssdk.services.iotanalytics.model.ContainerDatasetAction;
import software.amazon.awssdk.services.iotanalytics.model.CreateDatasetRequest;
import software.amazon.awssdk.services.iotanalytics.model.DatasetAction;
import software.amazon.awssdk.services.iotanalytics.model.DatasetTrigger;
import software.amazon.awssdk.services.iotanalytics.model.DeleteDatasetRequest;
import software.amazon.awssdk.services.iotanalytics.model.DescribeDatasetRequest;
import software.amazon.awssdk.services.iotanalytics.model.DescribeDatasetResponse;
import software.amazon.awssdk.services.iotanalytics.model.InvalidRequestException;
import software.amazon.awssdk.services.iotanalytics.model.IoTAnalyticsException;
import software.amazon.awssdk.services.iotanalytics.model.LimitExceededException;
import software.amazon.awssdk.services.iotanalytics.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.iotanalytics.model.Dataset;
import software.amazon.awssdk.services.iotanalytics.model.QueryFilter;
import software.amazon.awssdk.services.iotanalytics.model.ResourceAlreadyExistsException;
import software.amazon.awssdk.services.iotanalytics.model.ResourceNotFoundException;
import software.amazon.awssdk.services.iotanalytics.model.ServiceUnavailableException;
import software.amazon.awssdk.services.iotanalytics.model.SqlQueryDatasetAction;
import software.amazon.awssdk.services.iotanalytics.model.Tag;
import software.amazon.awssdk.services.iotanalytics.model.ThrottlingException;
import software.amazon.awssdk.services.iotanalytics.model.UpdateDatasetRequest;
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

public class Translator {
    private Translator() {}
    static DescribeDatasetRequest translateToDescribeDatasetRequest(final ResourceModel model) {
        return DescribeDatasetRequest.builder().datasetName(model.getDatasetName()).build();
    }

    static DeleteDatasetRequest translateToDeleteDatasetRequest(final ResourceModel model) {
        return DeleteDatasetRequest.builder().datasetName(model.getDatasetName()).build();
    }

    static UpdateDatasetRequest translateToUpdateDatasetRequest(final ResourceModel model) {
        return UpdateDatasetRequest.builder()
                .datasetName(model.getDatasetName())
                .actions(translateDatasetActionsFromCfn(model.getActions()))
                .lateDataRules(translateLateDateRulesFromCfn(model.getLateDataRules()))
                .contentDeliveryRules(translateDatasetContentDeliveryRuleFromCfn(model.getContentDeliveryRules()))
                .triggers(translateTriggersFromCfn(model.getTriggers()))
                .versioningConfiguration(translateVersioningConfigurationFromCfn(model.getVersioningConfiguration()))
                .retentionPeriod(translateRetentionPeriodFromCfn(model.getRetentionPeriod()))
                .build();
    }

    static CreateDatasetRequest translateToCreateDatasetRequest(final ResourceModel model) {
        return CreateDatasetRequest.builder()
                .datasetName(model.getDatasetName())
                .actions(translateDatasetActionsFromCfn(model.getActions()))
                .lateDataRules(translateLateDateRulesFromCfn(model.getLateDataRules()))
                .contentDeliveryRules(translateDatasetContentDeliveryRuleFromCfn(model.getContentDeliveryRules()))
                .triggers(translateTriggersFromCfn(model.getTriggers()))
                .versioningConfiguration(translateVersioningConfigurationFromCfn(model.getVersioningConfiguration()))
                .retentionPeriod(translateRetentionPeriodFromCfn(model.getRetentionPeriod()))
                .tags(translateTagListsFromCfn(model.getTags()))
                .build();
    }

    static ResourceModel translateFromDescribeResponse(final DescribeDatasetResponse describeDatasetResponse,
                                                       final ListTagsForResourceResponse listTagsForResourceResponse) {
        final Dataset dataset = describeDatasetResponse.dataset();
        final List<Tag> tagList = listTagsForResourceResponse.tags();
        return ResourceModel.builder()
                .datasetName(dataset.name())
                .id(dataset.arn())
                .actions(translateDatasetActionsToCfn(dataset.actions()))
                .contentDeliveryRules(translateDatasetContentDeliveryRuleToCfn(dataset.contentDeliveryRules()))
                .lateDataRules(translateLateDateRulesToCfn(dataset.lateDataRules()))
                .retentionPeriod(translateRetentionPeriodToCfn(dataset.retentionPeriod()))
                .triggers(translateTriggersToCfn(dataset.triggers()))
                .versioningConfiguration(translateVersioningConfigurationToCfn(dataset.versioningConfiguration()))
                .tags(translateTagsToCfn(tagList))
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
            if (StringUtils.isNullOrEmpty(name)) return new CfnNotFoundException(e);
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

    private static List<Action> translateDatasetActionsToCfn(
            @Nonnull final List<DatasetAction> actions) { // dataset actions are required and not null
        return actions
                .stream()
                .filter(Objects::nonNull)
                .map(action ->
                        Action.builder()
                                .actionName(action.actionName())
                                .containerAction(translateContainerActionToCfn(action.containerAction()))
                                .queryAction(translateSqlQueryActionToCfn(action.queryAction()))
                                .build()
                ).collect(Collectors.toList());
    }

    private static List<DatasetAction> translateDatasetActionsFromCfn(
            @Nonnull final List<Action> cfnActions) { // dataset actions are required and not null
        return cfnActions
                .stream()
                .filter(Objects::nonNull)
                .map(cfnAction ->
                        DatasetAction.builder()
                                .actionName(cfnAction.getActionName())
                                .containerAction(translateContainerActionFromCfn(cfnAction.getContainerAction()))
                                .queryAction(translateSqlQueryActionFromCfn(cfnAction.getQueryAction()))
                                .build())
                .collect(Collectors.toList());
    }

    private static QueryAction translateSqlQueryActionToCfn(
            @Nullable final SqlQueryDatasetAction sqlQueryDatasetAction) {
        if (sqlQueryDatasetAction == null) {
            return null;
        }
        return QueryAction.builder()
                .sqlQuery(sqlQueryDatasetAction.sqlQuery())
                .filters(translateFiltersToCfn(sqlQueryDatasetAction.filters()))
                .build();
    }

    private static SqlQueryDatasetAction translateSqlQueryActionFromCfn(
            @Nullable final QueryAction cfnQueryAction) {
        if (cfnQueryAction == null) {
            return null;
        }
        return SqlQueryDatasetAction.builder()
                .sqlQuery(cfnQueryAction.getSqlQuery())
                .filters(translateFiltersFromCfn(cfnQueryAction.getFilters()))
                .build();
    }

    @VisibleForTesting
    static List<Filter> translateFiltersToCfn(
            @Nullable final List<QueryFilter> filters) {
        if (filters == null) {
            return null;
        }
        return filters.stream().filter(Objects::nonNull).map(
                queryFilter ->
                        Filter.builder()
                                .deltaTime(queryFilter.deltaTime() == null ? null :
                                        DeltaTime.builder()
                                                .offsetSeconds(queryFilter.deltaTime().offsetSeconds())
                                                .timeExpression(queryFilter.deltaTime().timeExpression())
                                                .build())
                                .build())
                .collect(Collectors.toList());
    }

    @VisibleForTesting
    static List<QueryFilter> translateFiltersFromCfn(
            @Nullable final List<Filter> cfnFilters) {
        if (cfnFilters == null) {
            return null;
        }
        return cfnFilters.stream().filter(Objects::nonNull).map(
                cfnFilter ->
                        QueryFilter.builder()
                                .deltaTime(cfnFilter.getDeltaTime() == null ? null :
                                        software.amazon.awssdk.services.iotanalytics.model.DeltaTime
                                                .builder()
                                                .offsetSeconds(cfnFilter.getDeltaTime().getOffsetSeconds())
                                                .timeExpression(cfnFilter.getDeltaTime().getTimeExpression())
                                                .build())
                                .build()
        ).collect(Collectors.toList());
    }

    static ContainerAction translateContainerActionToCfn(
            @Nullable final ContainerDatasetAction containerDatasetAction) {
        if (containerDatasetAction == null) {
            return null;
        }
        return ContainerAction.builder()
                .executionRoleArn(containerDatasetAction.executionRoleArn())
                .image(containerDatasetAction.image())
                .resourceConfiguration(ResourceConfiguration
                        .builder()
                        .computeType(containerDatasetAction.resourceConfiguration().computeType().toString())
                        .volumeSizeInGB(containerDatasetAction.resourceConfiguration().volumeSizeInGB())
                        .build()) // resourceConfiguration is require, not null.
                .variables(translateVariablesToCfn(containerDatasetAction.variables()))
                .build();
    }

    private static ContainerDatasetAction translateContainerActionFromCfn(
            @Nullable final ContainerAction cfnContainerAction) {
        if (cfnContainerAction == null) {
            return null;
        }
        return ContainerDatasetAction.builder()
                .executionRoleArn(cfnContainerAction.getExecutionRoleArn())
                .image(cfnContainerAction.getImage())
                .resourceConfiguration(software.amazon.awssdk.services.iotanalytics.model.ResourceConfiguration
                        .builder()
                        .computeType(cfnContainerAction.getResourceConfiguration().getComputeType())
                        .volumeSizeInGB(cfnContainerAction.getResourceConfiguration().getVolumeSizeInGB())
                        .build()) // // resourceConfiguration is require, not null.
                .variables(translateVariablesFromCfn(cfnContainerAction.getVariables()))
                .build();
    }

    @VisibleForTesting
    static List<Variable> translateVariablesToCfn(
            @Nullable final List<software.amazon.awssdk.services.iotanalytics.model.Variable> variableList) {
        if (variableList == null) {
            return null;
        }
        return variableList.stream().filter(Objects::nonNull).map(variable ->
                Variable.builder()
                        .variableName(variable.name())
                        .doubleValue(variable.doubleValue())
                        .stringValue(variable.stringValue())
                        .datasetContentVersionValue(variable.datasetContentVersionValue() == null
                                ? null
                                : DatasetContentVersionValue
                                .builder()
                                .datasetName(variable.datasetContentVersionValue().datasetName())
                                .build())
                        .outputFileUriValue(variable.outputFileUriValue() == null
                                ? null
                                : OutputFileUriValue
                                .builder()
                                .fileName(variable.outputFileUriValue().fileName())
                                .build()
                        )
                        .build()).collect(Collectors.toList());
    }

    @VisibleForTesting
    static List<software.amazon.awssdk.services.iotanalytics.model.Variable> translateVariablesFromCfn(
            @Nullable final List<Variable> cfnVariableList) {
        if (cfnVariableList == null) {
            return null;
        }
        return cfnVariableList.stream().filter(Objects::nonNull).map(cfnVariable ->
                software.amazon.awssdk.services.iotanalytics.model.Variable.builder()
                        .name(cfnVariable.getVariableName())
                        .doubleValue(cfnVariable.getDoubleValue())
                        .stringValue(cfnVariable.getStringValue())
                        .datasetContentVersionValue(cfnVariable.getDatasetContentVersionValue() == null
                                ? null
                                : software.amazon.awssdk.services.iotanalytics.model.DatasetContentVersionValue
                                .builder()
                                .datasetName(cfnVariable.getDatasetContentVersionValue().getDatasetName())
                                .build())
                        .outputFileUriValue(cfnVariable.getOutputFileUriValue() == null
                                ? null
                                : software.amazon.awssdk.services.iotanalytics.model.OutputFileUriValue
                                .builder()
                                .fileName(cfnVariable.getOutputFileUriValue().getFileName())
                                .build()
                        )
                        .build()).collect(Collectors.toList());
    }

    @VisibleForTesting
    static List<DatasetContentDeliveryRule> translateDatasetContentDeliveryRuleToCfn(
            @Nullable final List<software.amazon.awssdk.services.iotanalytics.model.DatasetContentDeliveryRule> datasetContentDeliveryRules) {
        if (datasetContentDeliveryRules == null) {
            return null;
        }
        return datasetContentDeliveryRules
                .stream()
                .filter(Objects::nonNull)
                .map(datasetContentDeliveryRule ->
                        DatasetContentDeliveryRule
                                .builder()
                                .entryName(datasetContentDeliveryRule.entryName())
                                .destination(DatasetContentDeliveryRuleDestination
                                        .builder()
                                        .iotEventsDestinationConfiguration(translateIotEventsDestinationConfigurationToCfn(
                                                datasetContentDeliveryRule
                                                        .destination()
                                                        .iotEventsDestinationConfiguration())) // Destination is required and not null
                                        .s3DestinationConfiguration(translateS3DestinationConfigurationToCfn(
                                                datasetContentDeliveryRule
                                                        .destination()
                                                        .s3DestinationConfiguration()
                                        ))
                                        .build())
                                .build())
                .collect(Collectors.toList());
    }

    @VisibleForTesting
    static List<software.amazon.awssdk.services.iotanalytics.model.DatasetContentDeliveryRule> translateDatasetContentDeliveryRuleFromCfn(
            @Nullable final List<DatasetContentDeliveryRule> cfnDatasetContentDeliveryRules) {
        if (cfnDatasetContentDeliveryRules == null) {
            return null;
        }
        return cfnDatasetContentDeliveryRules
                .stream()
                .filter(Objects::nonNull)
                .map(cfnDatasetContentDeliveryRule ->
                        software.amazon.awssdk.services.iotanalytics.model.DatasetContentDeliveryRule
                                .builder()
                                .entryName(cfnDatasetContentDeliveryRule.getEntryName())
                                .destination(software.amazon.awssdk.services.iotanalytics.model.DatasetContentDeliveryDestination
                                        .builder()
                                        .iotEventsDestinationConfiguration(translateIotEventsDestinationConfigurationFromCfn(
                                                cfnDatasetContentDeliveryRule
                                                        // Destination is required and not null
                                                        .getDestination()
                                                        .getIotEventsDestinationConfiguration()))
                                        .s3DestinationConfiguration(translateS3DestinationConfigurationFromCfn(
                                                cfnDatasetContentDeliveryRule
                                                        // Destination is required and not null
                                                        .getDestination()
                                                        .getS3DestinationConfiguration()
                                        ))
                                        .build())
                                .build())
                .collect(Collectors.toList());
    }

    private static S3DestinationConfiguration translateS3DestinationConfigurationToCfn(
            @Nullable final software.amazon.awssdk.services.iotanalytics.model.S3DestinationConfiguration
            s3DestinationConfiguration) {
        if (s3DestinationConfiguration == null) {
            return null;
        }
        return S3DestinationConfiguration.builder()
                .key(s3DestinationConfiguration.key())
                .roleArn(s3DestinationConfiguration.roleArn())
                .bucket(s3DestinationConfiguration.bucket())
                .glueConfiguration(s3DestinationConfiguration.glueConfiguration() == null ? null :
                        GlueConfiguration.builder()
                                .databaseName(s3DestinationConfiguration.glueConfiguration().databaseName())
                                .tableName(s3DestinationConfiguration.glueConfiguration().tableName())
                                .build())
                .build();
    }

    private static software.amazon.awssdk.services.iotanalytics.model.S3DestinationConfiguration translateS3DestinationConfigurationFromCfn(
            @Nullable final S3DestinationConfiguration
                    cfnS3DestinationConfiguration) {
        if (cfnS3DestinationConfiguration == null) {
            return null;
        }
        return software.amazon.awssdk.services.iotanalytics.model.S3DestinationConfiguration
                .builder()
                .key(cfnS3DestinationConfiguration.getKey())
                .roleArn(cfnS3DestinationConfiguration.getRoleArn())
                .bucket(cfnS3DestinationConfiguration.getBucket())
                .glueConfiguration(cfnS3DestinationConfiguration.getGlueConfiguration() == null ? null :
                        software.amazon.awssdk.services.iotanalytics.model.GlueConfiguration
                                .builder()
                                .databaseName(cfnS3DestinationConfiguration.getGlueConfiguration().getDatabaseName())
                                .tableName(cfnS3DestinationConfiguration.getGlueConfiguration().getTableName())
                                .build())
                .build();
    }

    private static IotEventsDestinationConfiguration translateIotEventsDestinationConfigurationToCfn(
            @Nullable final software.amazon.awssdk.services.iotanalytics.model.IotEventsDestinationConfiguration
                    iotEventsDestinationConfiguration) {
        if (iotEventsDestinationConfiguration == null) {
            return null;
        }
        return IotEventsDestinationConfiguration.builder()
                .inputName(iotEventsDestinationConfiguration.inputName())
                .roleArn(iotEventsDestinationConfiguration.roleArn())
                .build();
    }

    private static software.amazon.awssdk.services.iotanalytics.model.IotEventsDestinationConfiguration translateIotEventsDestinationConfigurationFromCfn(
            @Nullable final IotEventsDestinationConfiguration
                    cfnIotEventsDestinationConfiguration) {
        if (cfnIotEventsDestinationConfiguration == null) {
            return null;
        }
        return software.amazon.awssdk.services.iotanalytics.model.IotEventsDestinationConfiguration
                .builder()
                .inputName(cfnIotEventsDestinationConfiguration.getInputName())
                .roleArn(cfnIotEventsDestinationConfiguration.getRoleArn())
                .build();
    }

    @VisibleForTesting
    static List<LateDataRule> translateLateDateRulesToCfn(
            @Nullable final List<software.amazon.awssdk.services.iotanalytics.model.LateDataRule> lateDataRules) {
        if (lateDataRules == null) {
            return null;
        }
        return lateDataRules
                .stream()
                .filter(Objects::nonNull)
                .map(lateDataRule ->
                        LateDataRule
                                .builder()
                                .ruleName(lateDataRule.ruleName())
                                .ruleConfiguration(LateDataRuleConfiguration.builder()
                                        .deltaTimeSessionWindowConfiguration(
                                                lateDataRule.ruleConfiguration().deltaTimeSessionWindowConfiguration() == null
                                                        ? null
                                                        : DeltaTimeSessionWindowConfiguration
                                                        .builder()
                                                        .timeoutInMinutes(lateDataRule
                                                                .ruleConfiguration()
                                                                .deltaTimeSessionWindowConfiguration()
                                                                .timeoutInMinutes())
                                                        .build())
                                        .build()) // ruleConfiguration is required and not null
                                .build())
                .collect(Collectors.toList());
    }

    @VisibleForTesting
    static List<software.amazon.awssdk.services.iotanalytics.model.LateDataRule> translateLateDateRulesFromCfn(
            @Nullable final List<LateDataRule> cfnLateDataRules) {
        if (cfnLateDataRules == null) {
            return null;
        }
        return cfnLateDataRules.stream().filter(Objects::nonNull)
                .map(cfnLateDataRule ->
                        software.amazon.awssdk.services.iotanalytics.model.LateDataRule.builder()
                                .ruleName(cfnLateDataRule.getRuleName())
                                .ruleConfiguration(software.amazon.awssdk.services.iotanalytics.model.LateDataRuleConfiguration
                                        .builder()
                                        .deltaTimeSessionWindowConfiguration(
                                                // ruleConfiguration is required and not null
                                                cfnLateDataRule.getRuleConfiguration().getDeltaTimeSessionWindowConfiguration() == null
                                                        ? null
                                                        : software.amazon.awssdk.services.iotanalytics.model.DeltaTimeSessionWindowConfiguration
                                                        .builder()
                                                        .timeoutInMinutes(cfnLateDataRule
                                                                .getRuleConfiguration()
                                                                .getDeltaTimeSessionWindowConfiguration()
                                                                .getTimeoutInMinutes())
                                                        .build()
                                        )
                                        .build())
                                .build()
                )
                .collect(Collectors.toList());
    }

    private static RetentionPeriod translateRetentionPeriodToCfn(
            @Nullable final software.amazon.awssdk.services.iotanalytics.model.RetentionPeriod retentionPeriod) {
        if (retentionPeriod == null) {
            return null;
        }
        return RetentionPeriod.builder()
                .numberOfDays(retentionPeriod.numberOfDays())
                .unlimited(retentionPeriod.unlimited())
                .build();
    }

    private static software.amazon.awssdk.services.iotanalytics.model.RetentionPeriod translateRetentionPeriodFromCfn(
            @Nullable final RetentionPeriod cfnRetentionPeriod
    ) {
        if (cfnRetentionPeriod == null) {
            return null;
        }
        return software.amazon.awssdk.services.iotanalytics.model.RetentionPeriod.builder()
                .numberOfDays(cfnRetentionPeriod.getNumberOfDays())
                .unlimited(cfnRetentionPeriod.getUnlimited())
                .build();
    }

    @VisibleForTesting
    static List<Trigger> translateTriggersToCfn(@Nullable final List<DatasetTrigger> datasetTriggers) {
        if (datasetTriggers == null) {
            return null;
        }
        return datasetTriggers.stream().filter(Objects::nonNull)
                .map(trigger ->
                    Trigger.builder()
                            .triggeringDataset(translateTriggeringDatasetToCfn(trigger.dataset()))
                            .schedule(translateScheduleToCfn(trigger.schedule()))
                            .build()
                ).collect(Collectors.toList());
    }

    @VisibleForTesting
    static List<DatasetTrigger> translateTriggersFromCfn(@Nullable final List<Trigger> cfnTriggers) {
        if (cfnTriggers == null) {
            return null;
        }
        return cfnTriggers.stream().filter(Objects::nonNull)
                .map(cfnTrigger ->
                        DatasetTrigger.builder()
                                .dataset(translateTriggeringDatasetFromCfn(cfnTrigger.getTriggeringDataset()))
                                .schedule(translateScheduleFromCfn(cfnTrigger.getSchedule()))
                                .build())
                .collect(Collectors.toList());
    }

    private static TriggeringDataset translateTriggeringDatasetToCfn(
            @Nullable final software.amazon.awssdk.services.iotanalytics.model.TriggeringDataset triggeringDataset) {
        if (triggeringDataset == null) {
            return null;
        }
        return TriggeringDataset.builder().datasetName(triggeringDataset.name()).build();
    }

    private static software.amazon.awssdk.services.iotanalytics.model.TriggeringDataset translateTriggeringDatasetFromCfn(
            @Nullable final TriggeringDataset cfnTriggeringDataset) {
        if (cfnTriggeringDataset == null) {
            return null;
        }
        return software.amazon.awssdk.services.iotanalytics.model.TriggeringDataset
                .builder().name(cfnTriggeringDataset.getDatasetName()).build();
    }

    private static Schedule translateScheduleToCfn(
            @Nullable final software.amazon.awssdk.services.iotanalytics.model.Schedule schedule) {
        if (schedule == null) {
            return null;
        }
        return Schedule.builder().scheduleExpression(schedule.expression()).build();
    }

    private static software.amazon.awssdk.services.iotanalytics.model.Schedule translateScheduleFromCfn(
            @Nullable final Schedule cfnSchedule) {
        if (cfnSchedule == null) {
            return null;
        }
        return software.amazon.awssdk.services.iotanalytics.model.Schedule
                .builder().expression(cfnSchedule.getScheduleExpression()).build();
    }

    private static VersioningConfiguration translateVersioningConfigurationToCfn(
            @Nullable final software.amazon.awssdk.services.iotanalytics.model.VersioningConfiguration versioningConfiguration) {
        if (versioningConfiguration == null) {
            return null;
        }
        return VersioningConfiguration.builder()
                .maxVersions(versioningConfiguration.maxVersions())
                .unlimited(versioningConfiguration.unlimited())
                .build();
    }

    private static software.amazon.awssdk.services.iotanalytics.model.VersioningConfiguration translateVersioningConfigurationFromCfn(
            @Nullable final VersioningConfiguration cfnVersioningConfiguration) {
        if (cfnVersioningConfiguration == null) {
            return null;
        }
        return software.amazon.awssdk.services.iotanalytics.model.VersioningConfiguration.builder()
                .maxVersions(cfnVersioningConfiguration.getMaxVersions())
                .unlimited(cfnVersioningConfiguration.getUnlimited())
                .build();
    }

    @VisibleForTesting
    static List<com.amazonaws.iotanalytics.dataset.Tag> translateTagsToCfn(
            @Nullable final List<Tag> tags) {
        if (tags == null || tags.isEmpty()) {
            return null;
        }
        return tags.stream().filter(Objects::nonNull).map((tag ->
                com.amazonaws.iotanalytics.dataset.Tag.builder()
                        .key(tag.key())
                        .value(tag.value())
                        .build()))
                .collect(Collectors.toList());
    }

    @VisibleForTesting
    static List<Tag> translateTagListsFromCfn(
            @Nullable final List<com.amazonaws.iotanalytics.dataset.Tag> cfnTagList
    ) {
        if (cfnTagList == null) {
            return null;
        } else {
            return cfnTagList.stream().filter(Objects::nonNull).map(tag ->
                    Tag.builder()
                            .key(tag.getKey())
                            .value(tag.getValue())
                            .build()
            ).collect(Collectors.toList());
        }
    }
}
