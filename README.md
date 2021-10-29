## aws-cloudformation-resource-providers-iotanalytics

The CloudFormation Resource Provider Package For AWS IoT Analytics

## Testing Note
Run contract tests with `cfn test`

Test registration with `cfn submit -set-default --role-arn arn:aws:iam::<dev account id>:role/cfn-testing-role`

You will have to create these resources with a CFN template and export the necessary values in the account you wish to use before you can run the above commands:
```
Resources:
  CFNTestingRole:
    Type: 'AWS::IAM::Role'
    Properties:
      RoleName: "cfn-testing-role"
      Policies:
        # Attach necessary policies here, ideally all iotanalytics and read permission for other resources
      AssumeRolePolicyDocument:
        Version: 2012-10-17
        Statement:
          - Action: "sts:AssumeRole"
            Effect: Allow
            Principal:
              Service:
                - "iotanalytics.aws.internal"
                - "iotanalytics.amazonaws.com"
                - "resources.cloudformation.amazonaws.com"
                - "cloudformation.amazonaws.com"
  CFNTestingChannel:
    Type: 'AWS::IoTAnalytics::Channel'
    Properties:
      ChannelName: "cfn_testing_channel"
  CFNTestingDatastoreOne:
    Type: 'AWS::IoTAnalytics::Datastore'
    Properties:
      DatastoreName: "cfn_testing_datastore1"
  CFNTestingDatastoreTwo:
    Type: 'AWS::IoTAnalytics::Datastore'
    Properties:
      DatastoreName: "cfn_testing_datastore2"
  CFNTestingECRRepo:
    Type: 'AWS::ECR::Repository'
    Properties:
      RepositoryName: !Sub "cfn-testing-repo"
  CFNTestingS3Bucket:
    Type: 'AWS::S3::Bucket'
    Properties:
      BucketName: !Sub "cfn-testing-bucket-${AWS::Region}-${AWS::AccountId}"
  CFNTestingS3BucketPolicy:
    Type: 'AWS::S3::BucketPolicy'
    Properties:
      Bucket: !Ref CFNTestingS3Bucket
      PolicyDocument:
        Statement:
          - Action:
              - "s3:GetBucketLocation"
              - "s3:GetObject"
              - "s3:ListBucket"
            Effect: Allow
            Resource:
              - !GetAtt CFNTestingS3Bucket.Arn
              - !Sub "${CFNTestingS3Bucket.Arn}/*"
            Principal:
              Service:
                - "iotanalytics.aws.internal"
                - "iotanalytics.amazonaws.com"
          - Action:
              - "s3:*"
            Effect: Deny
            Principal: "*"
            Resource:
              - !GetAtt CFNTestingS3Bucket.Arn
              - !Sub "${CFNTestingS3Bucket.Arn}/*"
            Condition:
              Bool:
                aws:SecureTransport: false
Outputs:
  TestRegion:
    Value: !Ref "AWS::Region"
    Export:
      Name: TestRegion
  TestPartition:
    Value: !Ref "AWS::Partition"
    Export:
      Name: TestPartition
  TestAccountId:
    Value: !Ref "AWS::AccountId"
    Export:
      Name: TestAccountId
  CFNTestingRole:
    Value: !GetAtt CFNTestingRole.Arn
    Export:
      Name: CFNTestingRole
  CFNTestingS3Bucket:
    Value: !GetAtt CFNTestingS3Bucket.Arn
    Export:
      Name: CFNTestingS3Bucket
  CFNTestingECRRepo:
    Value: !GetAtt CFNTestingECRRepo.RepositoryUri
    Export:
      Name: CFNTestingECRRepo
```
    
## License

This project is licensed under the Apache-2.0 License.

