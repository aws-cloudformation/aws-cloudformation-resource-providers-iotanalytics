package com.amazonaws.iotanalytics.dataset;

import software.amazon.awssdk.services.iotanalytics.model.ContainerDatasetAction;
import software.amazon.awssdk.services.iotanalytics.model.DatasetAction;
import software.amazon.awssdk.services.iotanalytics.model.DatasetContentDeliveryDestination;
import software.amazon.awssdk.services.iotanalytics.model.DatasetTrigger;
import software.amazon.awssdk.services.iotanalytics.model.QueryFilter;
import software.amazon.awssdk.services.iotanalytics.model.SqlQueryDatasetAction;

import java.util.Arrays;
import java.util.Collections;

public class TestConstants {
    static final String TEST_DATASET_NAME = "test_dataset_name";
    static final String TEST_DATASET_ARN = "test_dataset_arn";
    static final String TEST_DATASET_ID = TEST_DATASET_ARN;

    // Tags
    static final String TEST_KEY1 = "key1";
    static final String TEST_VALUE1 = "value1";
    static final String TEST_KEY2 = "key2";
    static final String TEST_VALUE2 = "value2";
    static final String TEST_VALUE22 = "value22";
    static final String TEST_KEY3 = "key3";
    static final String TEST_VALUE3 = "value3";

    // Actions
    static final String TEST_ACTION_NAME_CONTAINER = "test_action_name_container";
    static final String TEST_ACTION_NAME_SQL = "test_action_name_sql";

    static final String TEST_CONTAINER_ACTION_ROLE = "test_container_action_role";
    static final String TEST_CONTAINER_IMAGE = "test_container_image";
    static final String TEST_CONTAINER_RES_CONFIG_COMPUTE_TYPE = "ACU_1";
    static final int TEST_CONTAINER_RES_CONFIG_VOL_SIZE = 1;
    static final String TEST_CONTAINER_VAR_NAME_1 = "test_container_var_name_1";
    static final String TEST_CONTAINER_VAR_NAME_2 = "test_container_var_name_2";
    static final Double TEST_CONTAINER_VAR_DOUBLE_VAL = 9.9;
    static final String TEST_CONTAINER_VAR_STRING_VAL = "test_container_var_string_val";
    static final String TEST_CONTAINER_VAR_CONTENT_VERSION_DATASET_NAME = "test_container_var_content_vs_ds_name";
    static final String TEST_CONTAINER_VAR_CONTENT_FILE_URI_NAME = "test_container_var_content_file_uri_name";

    static final String TEST_SQL_QUERY = "test_sql_query";
    static final Integer TEST_FILTER_DELTA_OFFSET_SEC = 10;
    static final String TEST_FILTER_DELTA_TIME_EXPRESSION = "test_filter_delta_time_expression";

    static final Action CFN_CONTAINER_ACTION = Action.builder()
            .actionName(TEST_ACTION_NAME_CONTAINER)
            .containerAction(ContainerAction.builder()
                    .image(TEST_CONTAINER_IMAGE)
                    .executionRoleArn(TEST_CONTAINER_ACTION_ROLE)
                    .resourceConfiguration(ResourceConfiguration.builder()
                            .volumeSizeInGB(TEST_CONTAINER_RES_CONFIG_VOL_SIZE)
                            .computeType(TEST_CONTAINER_RES_CONFIG_COMPUTE_TYPE)
                            .build())
                    .variables(Arrays.asList(
                            Variable.builder()
                                    .variableName(TEST_CONTAINER_VAR_NAME_1)
                                    .stringValue(TEST_CONTAINER_VAR_STRING_VAL)
                                    .outputFileUriValue(OutputFileUriValue.builder()
                                            .fileName(TEST_CONTAINER_VAR_CONTENT_FILE_URI_NAME)
                                            .build())
                                    .datasetContentVersionValue(DatasetContentVersionValue
                                            .builder()
                                            .datasetName(TEST_CONTAINER_VAR_CONTENT_VERSION_DATASET_NAME)
                                            .build())
                                    .build(),
                            Variable.builder()
                                    .variableName(TEST_CONTAINER_VAR_NAME_2)
                                    .doubleValue(TEST_CONTAINER_VAR_DOUBLE_VAL)
                                    .build()
                    ))
                    .build())
            .build();

    static final DatasetAction IOTA_CONTAINER_ACTION = DatasetAction.builder()
            .actionName(TEST_ACTION_NAME_CONTAINER)
            .containerAction(ContainerDatasetAction.builder()
                    .image(TEST_CONTAINER_IMAGE)
                    .executionRoleArn(TEST_CONTAINER_ACTION_ROLE)
                    .resourceConfiguration(software.amazon.awssdk.services.iotanalytics.model.ResourceConfiguration
                            .builder()
                            .computeType(TEST_CONTAINER_RES_CONFIG_COMPUTE_TYPE)
                            .volumeSizeInGB(TEST_CONTAINER_RES_CONFIG_VOL_SIZE)
                            .build())
                    .variables(Arrays.asList(
                            software.amazon.awssdk.services.iotanalytics.model.Variable.builder()
                                    .name(TEST_CONTAINER_VAR_NAME_1)
                                    .stringValue(TEST_CONTAINER_VAR_STRING_VAL)
                                    .datasetContentVersionValue(software.amazon.awssdk.services.iotanalytics.model.DatasetContentVersionValue
                                            .builder()
                                            .datasetName(TEST_CONTAINER_VAR_CONTENT_VERSION_DATASET_NAME)
                                            .build())
                                    .outputFileUriValue(software.amazon.awssdk.services.iotanalytics.model.OutputFileUriValue.builder()
                                            .fileName(TEST_CONTAINER_VAR_CONTENT_FILE_URI_NAME)
                                            .build())
                                    .build(),
                            software.amazon.awssdk.services.iotanalytics.model.Variable.builder()
                                    .name(TEST_CONTAINER_VAR_NAME_2)
                                    .doubleValue(TEST_CONTAINER_VAR_DOUBLE_VAL)
                                    .build()
                    ))
                    .build())
            .build();

    static final Action CFN_SQL_ACTION = Action.builder()
            .actionName(TEST_ACTION_NAME_SQL)
            .queryAction(
                    QueryAction.builder()
                            .sqlQuery(TEST_SQL_QUERY)
                            .filters(Collections.singletonList(Filter
                                    .builder()
                                    .deltaTime(DeltaTime.builder()
                                            .timeExpression(TEST_FILTER_DELTA_TIME_EXPRESSION)
                                            .offsetSeconds(TEST_FILTER_DELTA_OFFSET_SEC)
                                            .build())
                                    .build()))
                            .build())
            .build();
    static final DatasetAction IOTA_SQL_ACTION = DatasetAction.builder()
            .actionName(TEST_ACTION_NAME_SQL)
            .queryAction(
                    SqlQueryDatasetAction.builder()
                            .sqlQuery(TEST_SQL_QUERY)
                            .filters(Collections.singletonList(
                                    QueryFilter
                                            .builder()
                                            .deltaTime(
                                                    software.amazon.awssdk.services.iotanalytics.model.DeltaTime.builder()
                                                            .timeExpression(TEST_FILTER_DELTA_TIME_EXPRESSION)
                                                            .offsetSeconds(TEST_FILTER_DELTA_OFFSET_SEC)
                                                            .build())
                                            .build()))
                            .build())
            .build();


    // Late data rule
    static final String TEST_LATE_DATA_RULE_NAME = "test_late_data_rule_name";
    static final Integer TEST_DELTA_TIME_SESSION_WIN_CONFIG_TIMEOUT = 2;

    static final LateDataRule CFN_LATE_DATE_RULE = LateDataRule.builder().ruleName(TEST_LATE_DATA_RULE_NAME)
            .ruleConfiguration(LateDataRuleConfiguration.builder()
                    .deltaTimeSessionWindowConfiguration(DeltaTimeSessionWindowConfiguration.builder()
                            .timeoutInMinutes(TEST_DELTA_TIME_SESSION_WIN_CONFIG_TIMEOUT)
                            .build())
                    .build())
            .build();
    static final software.amazon.awssdk.services.iotanalytics.model.LateDataRule IOTA_LATE_DATE_RULE
            = software.amazon.awssdk.services.iotanalytics.model.LateDataRule.builder().ruleName(TEST_LATE_DATA_RULE_NAME)
            .ruleConfiguration
                    (software.amazon.awssdk.services.iotanalytics.model.LateDataRuleConfiguration.builder()
                            .deltaTimeSessionWindowConfiguration(
                                    software.amazon.awssdk.services.iotanalytics.model.DeltaTimeSessionWindowConfiguration.builder()
                                            .timeoutInMinutes(TEST_DELTA_TIME_SESSION_WIN_CONFIG_TIMEOUT)
                                            .build())
                            .build())
            .build();

    // Content Delivery Rule
    static final String TEST_CONTENT_DELIVERY_ENTRY_NAME_IOT = "test_content_delivery_rule_entry_name_iot";
    static final String TEST_CONTENT_DELIVERY_ENTRY_NAME_S3 = "test_content_delivery_rule_entry_name_s3";

    static final String TEST_CONTENT_DELIVERY_IOT_EVENT_INPUT_NAME = "test_content_delivery_iot_event_input_name";
    static final String TEST_CONTENT_DELIVERY_IOT_EVENT_ROLE = "test_content_delivery_iot_event_role";

    static final String TEST_CONTENT_DELIVERY_S3_KEY = "test_content_delivery_s3_key";
    static final String TEST_CONTENT_DELIVERY_S3_ROLE = "test_content_delivery_s3_role";
    static final String TEST_CONTENT_DELIVERY_S3_BUCKET = "test_content_delivery_s3_bucket";
    static final String TEST_CONTENT_DELIVERY_S3_GLUE_TABLE = "test_content_delivery_s3_glue_table";
    static final String TEST_CONTENT_DELIVERY_S3_GLUE_DB = "test_content_delivery_s3_glue_db";

    static final DatasetContentDeliveryRule CFN_CONTENT_DELIVERY_RULE_IOT_EVENT = DatasetContentDeliveryRule
            .builder().destination(DatasetContentDeliveryRuleDestination
                    .builder()
                    .iotEventsDestinationConfiguration(IotEventsDestinationConfiguration.builder()
                            .roleArn(TEST_CONTENT_DELIVERY_IOT_EVENT_ROLE)
                            .inputName(TEST_CONTENT_DELIVERY_IOT_EVENT_INPUT_NAME)
                            .build())
                    .build())
            .entryName(TEST_CONTENT_DELIVERY_ENTRY_NAME_IOT).build();
    static final DatasetContentDeliveryRule CFN_CONTENT_DELIVERY_RULE_S3 = DatasetContentDeliveryRule
            .builder().destination(DatasetContentDeliveryRuleDestination
                    .builder()
                    .s3DestinationConfiguration(S3DestinationConfiguration.builder()
                            .roleArn(TEST_CONTENT_DELIVERY_S3_ROLE)
                            .key(TEST_CONTENT_DELIVERY_S3_KEY)
                            .bucket(TEST_CONTENT_DELIVERY_S3_BUCKET)
                            .glueConfiguration(GlueConfiguration.builder()
                                    .tableName(TEST_CONTENT_DELIVERY_S3_GLUE_TABLE)
                                    .databaseName(TEST_CONTENT_DELIVERY_S3_GLUE_DB)
                                    .build())
                            .build())
                    .build())
            .entryName(TEST_CONTENT_DELIVERY_ENTRY_NAME_S3).build();

    static final software.amazon.awssdk.services.iotanalytics.model.DatasetContentDeliveryRule IOTA_CONTENT_DELIVERY_RULE_IOT_EVENT =
            software.amazon.awssdk.services.iotanalytics.model.DatasetContentDeliveryRule
                    .builder().destination(DatasetContentDeliveryDestination
                    .builder()
                    .iotEventsDestinationConfiguration(
                            software.amazon.awssdk.services.iotanalytics.model.IotEventsDestinationConfiguration.builder()
                                    .roleArn(TEST_CONTENT_DELIVERY_IOT_EVENT_ROLE)
                                    .inputName(TEST_CONTENT_DELIVERY_IOT_EVENT_INPUT_NAME)
                                    .build())
                    .build())
                    .entryName(TEST_CONTENT_DELIVERY_ENTRY_NAME_IOT).build();
    static final software.amazon.awssdk.services.iotanalytics.model.DatasetContentDeliveryRule IOTA_CONTENT_DELIVERY_RULE_S3 =
            software.amazon.awssdk.services.iotanalytics.model.DatasetContentDeliveryRule
                    .builder().destination(DatasetContentDeliveryDestination
                    .builder()
                    .s3DestinationConfiguration(
                            software.amazon.awssdk.services.iotanalytics.model.S3DestinationConfiguration.builder()
                                    .roleArn(TEST_CONTENT_DELIVERY_S3_ROLE)
                                    .key(TEST_CONTENT_DELIVERY_S3_KEY)
                                    .bucket(TEST_CONTENT_DELIVERY_S3_BUCKET)
                                    .glueConfiguration(
                                            software.amazon.awssdk.services.iotanalytics.model.GlueConfiguration.builder()
                                                    .tableName(TEST_CONTENT_DELIVERY_S3_GLUE_TABLE)
                                                    .databaseName(TEST_CONTENT_DELIVERY_S3_GLUE_DB)
                                                    .build())
                                    .build())
                    .build())
                    .entryName(TEST_CONTENT_DELIVERY_ENTRY_NAME_S3).build();

    // Retention
    static final int TEST_DAYS = 10;
    static final RetentionPeriod CFN_RETENTION_UNLIMITED = RetentionPeriod.builder().unlimited(true).build();
    static final RetentionPeriod CFN_RETENTION_DAYS = RetentionPeriod.builder().numberOfDays(TEST_DAYS).build();
    static final software.amazon.awssdk.services.iotanalytics.model.RetentionPeriod IOTA_RETENTION_UNLIMITED
            = software.amazon.awssdk.services.iotanalytics.model.RetentionPeriod.builder().unlimited(true).build();
    static final software.amazon.awssdk.services.iotanalytics.model.RetentionPeriod IOTA_RETENTION_DAYS =
            software.amazon.awssdk.services.iotanalytics.model.RetentionPeriod.builder().numberOfDays(TEST_DAYS).build();

    // Triggers
    static final String TEST_TRIGGER_DATASET_NAME = "test_trigger_dataset_name";
    static final String TEST_TRIGGER_DATASET_SCHEDULE_EXPRESSION = "test_trigger_dataset_schedule_expression";

    static final Trigger CFN_TRIGGER_BY_DATASET = Trigger.builder()
            .triggeringDataset(TriggeringDataset
                    .builder()
                    .datasetName(TEST_TRIGGER_DATASET_NAME)
                    .build()).build();
    static final Trigger CFN_TRIGGER_BY_SCHEDULE = Trigger.builder()
            .schedule(Schedule
                    .builder()
                    .scheduleExpression(TEST_TRIGGER_DATASET_SCHEDULE_EXPRESSION)
                    .build()).build();
    static final DatasetTrigger IOTA_TRIGGER_BY_DATASET = DatasetTrigger.builder()
            .dataset(software.amazon.awssdk.services.iotanalytics.model.TriggeringDataset
                    .builder()
                    .name(TEST_TRIGGER_DATASET_NAME)
                    .build()).build();
    static final DatasetTrigger IOTA_TRIGGER_BY_SCHEDULE = DatasetTrigger.builder()
            .schedule(software.amazon.awssdk.services.iotanalytics.model.Schedule
                    .builder()
                    .expression(TEST_TRIGGER_DATASET_SCHEDULE_EXPRESSION)
                    .build()).build();

    // Version Config
    static final int TEST_VERSION_CONFIG_MAX_VERSION = 10;

    static final VersioningConfiguration CFN_VERSION_CONFIG = VersioningConfiguration.builder()
            .maxVersions(TEST_VERSION_CONFIG_MAX_VERSION).unlimited(false).build();
    static final software.amazon.awssdk.services.iotanalytics.model.VersioningConfiguration IOTA_VERSION_CONFIG =
            software.amazon.awssdk.services.iotanalytics.model.VersioningConfiguration.builder()
                    .maxVersions(TEST_VERSION_CONFIG_MAX_VERSION).unlimited(false).build();
}
