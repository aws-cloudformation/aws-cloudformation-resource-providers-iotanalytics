## aws-cloudformation-resource-providers-iotanalytics

The CloudFormation Resource Provider Package For AWS IoT Analytics

## Testing Note
In order to perform contract tests with `cfn-test`, you will need to:
* Change `<account_id>` in input files to your account ID
* Create these resources in your account (in us-east-1 whenever applicable)
    * IAM Role: `cfn-testing-role` (with permissions for all the resources below, ideally full permissions for `iotanalytics` and read permissions for others)
    * IoTAnalytics Channel: `cfn_testing_channel`
    * IoTAnalytics Datastore: `cfn_testing_datastore1`, `cfn_testing_datastore2`
    * S3 Bucket: `cfn-testing-bucket`
    * ECR Container: `cfn-testing-container`
    
In order to register your resources with `cfn submit`, you will need to:
* Perform all steps as if you were going to run `cfn test`
* If you create a specific role for this, it should allow these endpoints in trust policy:
    * `cloudformation.amazonaws.com`
    * `resources.cloudformation.amazonaws.com`
* Run `cfn submit --set-default --role-arn arn:aws:iam::<accound_id>:role/<role>`, replacing `account_id` with your account id and `role` with the role you just created or you can reuse `cfn-testing-role`
    
## License

This project is licensed under the Apache-2.0 License.

