package com.amazonaws.iotanalytics.pipeline;

import software.amazon.awssdk.services.iotanalytics.model.AddAttributesActivity;
import software.amazon.awssdk.services.iotanalytics.model.ChannelActivity;
import software.amazon.awssdk.services.iotanalytics.model.DatastoreActivity;
import software.amazon.awssdk.services.iotanalytics.model.DeviceRegistryEnrichActivity;
import software.amazon.awssdk.services.iotanalytics.model.DeviceShadowEnrichActivity;
import software.amazon.awssdk.services.iotanalytics.model.FilterActivity;
import software.amazon.awssdk.services.iotanalytics.model.LambdaActivity;
import software.amazon.awssdk.services.iotanalytics.model.MathActivity;
import software.amazon.awssdk.services.iotanalytics.model.PipelineActivity;
import software.amazon.awssdk.services.iotanalytics.model.RemoveAttributesActivity;
import software.amazon.awssdk.services.iotanalytics.model.SelectAttributesActivity;

import java.util.Collections;

public class TestConstants {
    static final String TEST_PIPELINE_ARN = "test_pipeline_arn";
    static final String TEST_PIPELINE_ID = TEST_PIPELINE_ARN;
    static final String TEST_PIPELINE_NAME = "test_pipeline_name";

    static final String TEST_KEY1 = "key1";
    static final String TEST_VALUE1 = "value1";
    static final String TEST_KEY2 = "key2";
    static final String TEST_VALUE2 = "value2";
    static final String TEST_VALUE22 = "value22";
    static final String TEST_KEY3 = "key3";
    static final String TEST_VALUE3 = "value3";

    static final String TEST_CHANNEL_NAME = "test_channel_name";
    static final String TEST_CHANNEL_ACTIVITY_NAME = "test_channel_activity_name";

    static final String TEST_ADD_ATTR_ACTIVITY_NAME = "test_add_attr_activity_name";
    private static final String TEST_ADD_ATTR_KEY = "test_add_attr_key";
    static final String TEST_ADD_ATTR_VALUE = "test_add_attr_VALUE";

    static final String TEST_RM_ATTR_ACTIVITY_NAME = "test_rm_attr_activity_name";
    static final String TEST_RM_ATTR_KEY = "test_rm_attr_key";

    static final String TEST_SELECT_ATTR_ACTIVITY_NAME = "test_select_attr_activity_name";
    static final String TEST_SELECT_ATTR_KEY = "test_select_attr_key";

    static final String TEST_DEVICE_REGISTRY_ENRICH_ACTIVITY_NAME = "test_device_registry_enrich_activity_name";
    static final String TEST_DEVICE_REGISTRY_ENRICH_ROLE = "arn:aws:iam::1234567890:role:MyEnrichRole";
    static final String TEST_DEVICE_REGISTRY_ENRICH_ATTR = "test_device_registry_enrich_attr";
    static final String TEST_DEVICE_REGISTRY_ENRICH_THING_NAME = "test_device_registry_enrich_thing_name";

    static final String TEST_DEVICE_SHADOW_ENRICH_ACTIVITY_NAME = "test_device_shadow_enrich_activity_name";
    static final String TEST_DEVICE_SHADOW_ENRICH_ROLE = "arn:aws:iam::1234567890:role:MyEnrichRole";
    static final String TEST_DEVICE_SHADOW_ENRICH_ATTR = "test_device_shadow_enrich_attr";
    static final String TEST_DEVICE_SHADOW_ENRICH_THING_NAME = "test_device_shadow_enrich_thing_name";

    static final String TEST_FILTER_ACTIVITY_NAME = "test_filter_activity_name";
    static final String TEST_FILTER = "test_filter";

    static final String TEST_MATH_ACTIVITY_NAME = "test_math_activity_name";
    static final String TEST_MATH_ATTR = "test_math_attr";
    static final String TEST_MATH = "test_math";

    static final String TEST_LAMBDA_ACTIVITY_NAME = "test_lambda_activity_name";
    static final String TEST_LAMBDA_NAME = "test_lambda_name";
    static final int TEST_LAMBDA_BATCH_SIZE = 5;

    static final String TEST_DATASTORE_NAME = "test_datastore_name";
    static final String TEST_DATASTORE_ACTIVITY_NAME = "test_datastore_activity_name";

    static final Activity CFN_CHANNEL_ACTIVITY = Activity.builder()
            .channel(com.amazonaws.iotanalytics.pipeline.Channel.builder()
                    .channelName(TEST_CHANNEL_NAME)
                    .name(TEST_CHANNEL_ACTIVITY_NAME)
                    .next(TEST_ADD_ATTR_ACTIVITY_NAME)
                    .build())
            .build();
    static final Activity CFN_CHANNEL_TO_DATASTORE_ACTIVITY = Activity.builder()
            .channel(com.amazonaws.iotanalytics.pipeline.Channel.builder()
                    .channelName(TEST_CHANNEL_NAME)
                    .name(TEST_CHANNEL_ACTIVITY_NAME)
                    .next(TEST_DATASTORE_ACTIVITY_NAME)
                    .build())
            .build();
    static final PipelineActivity IOTA_CHANNEL_ACTIVITY = PipelineActivity.builder()
            .channel(ChannelActivity.builder()
                    .channelName(TEST_CHANNEL_NAME)
                    .name(TEST_CHANNEL_ACTIVITY_NAME)
                    .next(TEST_ADD_ATTR_ACTIVITY_NAME)
                    .build())
            .build();
    static final PipelineActivity IOTA_CHANNEL_TO_DATASTORE_ACTIVITY = PipelineActivity.builder()
            .channel(ChannelActivity.builder()
                    .channelName(TEST_CHANNEL_NAME)
                    .name(TEST_CHANNEL_ACTIVITY_NAME)
                    .next(TEST_DATASTORE_ACTIVITY_NAME)
                    .build())
            .build();

    static final Activity CFN_ADD_ATTR_ACTIVITY = Activity.builder()
            .addAttributes(com.amazonaws.iotanalytics.pipeline.AddAttributes.builder()
                    .attributes(Collections.singletonMap(TEST_ADD_ATTR_KEY,
                            TEST_ADD_ATTR_VALUE))
                    .name(TEST_ADD_ATTR_ACTIVITY_NAME)
                    .next(TEST_RM_ATTR_ACTIVITY_NAME)
                    .build())
            .build();
    static final PipelineActivity IOTA_ADD_ATTR_ACTIVITY = PipelineActivity.builder()
            .addAttributes(AddAttributesActivity.builder()
                    .attributes(Collections.singletonMap(TEST_ADD_ATTR_KEY,
                            TEST_ADD_ATTR_VALUE))
                    .name(TEST_ADD_ATTR_ACTIVITY_NAME)
                    .next(TEST_RM_ATTR_ACTIVITY_NAME)
                    .build())
            .build();

    static final Activity CFN_RM_ATTR_ACTIVITY = Activity.builder()
            .removeAttributes(com.amazonaws.iotanalytics.pipeline.RemoveAttributes.builder()
                    .attributes(Collections.singletonList(TEST_RM_ATTR_KEY))
                    .name(TEST_RM_ATTR_ACTIVITY_NAME)
                    .next(TEST_SELECT_ATTR_ACTIVITY_NAME)
                    .build())
            .build();
    static final PipelineActivity IOTA_RM_ATTR_ACTIVITY = PipelineActivity.builder()
            .removeAttributes(RemoveAttributesActivity.builder()
                    .attributes(Collections.singletonList(TEST_RM_ATTR_KEY))
                    .name(TEST_RM_ATTR_ACTIVITY_NAME)
                    .next(TEST_SELECT_ATTR_ACTIVITY_NAME)
                    .build())
            .build();

    static final Activity CFN_SELECT_ATTR_ACTIVITY = Activity.builder()
            .selectAttributes(com.amazonaws.iotanalytics.pipeline.SelectAttributes.builder()
                    .attributes(Collections.singletonList(TEST_SELECT_ATTR_KEY))
                    .name(TEST_SELECT_ATTR_ACTIVITY_NAME)
                    .next(TEST_DEVICE_REGISTRY_ENRICH_ACTIVITY_NAME)
                    .build())
            .build();
    static final PipelineActivity IOTA_SELECT_ATTR_ACTIVITY = PipelineActivity.builder()
            .selectAttributes(SelectAttributesActivity.builder()
                    .attributes(Collections.singletonList(TEST_SELECT_ATTR_KEY))
                    .name(TEST_SELECT_ATTR_ACTIVITY_NAME)
                    .next(TEST_DEVICE_REGISTRY_ENRICH_ACTIVITY_NAME)
                    .build())
            .build();

    static final Activity CFN_DEVICE_REGISTRY_ENRICH_ACTIVITY = Activity.builder()
            .deviceRegistryEnrich(com.amazonaws.iotanalytics.pipeline.DeviceRegistryEnrich.builder()
                    .roleArn(TEST_DEVICE_REGISTRY_ENRICH_ROLE)
                    .name(TEST_DEVICE_REGISTRY_ENRICH_ACTIVITY_NAME)
                    .thingName(TEST_DEVICE_REGISTRY_ENRICH_THING_NAME)
                    .attribute(TEST_DEVICE_REGISTRY_ENRICH_ATTR)
                    .next(TEST_DEVICE_SHADOW_ENRICH_ACTIVITY_NAME)
                    .build())
            .build();
    static final PipelineActivity IOTA_DEVICE_REGISTRY_ENRICH_ACTIVITY = PipelineActivity.builder()
            .deviceRegistryEnrich(DeviceRegistryEnrichActivity.builder()
                    .roleArn(TEST_DEVICE_REGISTRY_ENRICH_ROLE)
                    .name(TEST_DEVICE_REGISTRY_ENRICH_ACTIVITY_NAME)
                    .thingName(TEST_DEVICE_REGISTRY_ENRICH_THING_NAME)
                    .attribute(TEST_DEVICE_REGISTRY_ENRICH_ATTR)
                    .next(TEST_DEVICE_SHADOW_ENRICH_ACTIVITY_NAME)
                    .build())
            .build();

    static final Activity CFN_DEVICE_SHADOW_ACTIVITY = Activity.builder()
            .deviceShadowEnrich(com.amazonaws.iotanalytics.pipeline.DeviceShadowEnrich.builder()
                    .roleArn(TEST_DEVICE_SHADOW_ENRICH_ROLE)
                    .name(TEST_DEVICE_SHADOW_ENRICH_ACTIVITY_NAME)
                    .thingName(TEST_DEVICE_SHADOW_ENRICH_THING_NAME)
                    .attribute(TEST_DEVICE_SHADOW_ENRICH_ATTR)
                    .next(TEST_FILTER_ACTIVITY_NAME)
                    .build())
            .build();
    static final PipelineActivity IOTA_DEVICE_SHADOW_ACTIVITY = PipelineActivity.builder()
            .deviceShadowEnrich(DeviceShadowEnrichActivity.builder()
                    .roleArn(TEST_DEVICE_SHADOW_ENRICH_ROLE)
                    .name(TEST_DEVICE_SHADOW_ENRICH_ACTIVITY_NAME)
                    .thingName(TEST_DEVICE_SHADOW_ENRICH_THING_NAME)
                    .attribute(TEST_DEVICE_SHADOW_ENRICH_ATTR)
                    .next(TEST_FILTER_ACTIVITY_NAME)
                    .build())
            .build();

    static final Activity CFN_FILTER_ACTIVITY = Activity.builder()
            .filter(com.amazonaws.iotanalytics.pipeline.Filter.builder()
                    .name(TEST_FILTER_ACTIVITY_NAME)
                    .filter(TEST_FILTER)
                    .next(TEST_MATH_ACTIVITY_NAME)
                    .build())
            .build();
    static final PipelineActivity IOTA_FILTER_ACTIVITY = PipelineActivity.builder()
            .filter(FilterActivity.builder()
                    .name(TEST_FILTER_ACTIVITY_NAME)
                    .filter(TEST_FILTER)
                    .next(TEST_MATH_ACTIVITY_NAME)
                    .build())
            .build();

    static final Activity CFN_MATH_ACTIVITY = Activity.builder()
            .math(com.amazonaws.iotanalytics.pipeline.Math.builder()
                    .name(TEST_MATH_ACTIVITY_NAME)
                    .math(TEST_MATH)
                    .attribute(TEST_MATH_ATTR)
                    .next(TEST_MATH_ACTIVITY_NAME)
                    .build())
            .build();
    static final PipelineActivity IOTA_MATH_ACTIVITY = PipelineActivity.builder()
            .math(MathActivity.builder()
                    .name(TEST_MATH_ACTIVITY_NAME)
                    .math(TEST_MATH)
                    .attribute(TEST_MATH_ATTR)
                    .next(TEST_MATH_ACTIVITY_NAME)
                    .build())
            .build();

    static final Activity CFN_LAMBDA_ACTIVITY = Activity.builder()
            .lambda(com.amazonaws.iotanalytics.pipeline.Lambda.builder()
                    .name(TEST_LAMBDA_ACTIVITY_NAME)
                    .lambdaName(TEST_LAMBDA_NAME)
                    .batchSize(TEST_LAMBDA_BATCH_SIZE)
                    .next(TEST_DATASTORE_ACTIVITY_NAME)
                    .build())
            .build();
    static final PipelineActivity IOTA_LAMBDA_ACTIVITY = PipelineActivity.builder()
            .lambda(LambdaActivity.builder()
                    .name(TEST_LAMBDA_ACTIVITY_NAME)
                    .lambdaName(TEST_LAMBDA_NAME)
                    .batchSize(TEST_LAMBDA_BATCH_SIZE)
                    .next(TEST_DATASTORE_ACTIVITY_NAME)
                    .build())
            .build();

    static final Activity CFN_DATASTORE_ACTIVITY = Activity.builder()
            .datastore(com.amazonaws.iotanalytics.pipeline.Datastore.builder()
                    .datastoreName(TEST_DATASTORE_NAME)
                    .name(TEST_DATASTORE_ACTIVITY_NAME)
                    .build())
            .build();
    static final PipelineActivity IOTA_DATASTORE_ACTIVITY = PipelineActivity.builder()
            .datastore(DatastoreActivity.builder()
                    .datastoreName(TEST_DATASTORE_NAME)
                    .name(TEST_DATASTORE_ACTIVITY_NAME)
                    .build())
            .build();

}
