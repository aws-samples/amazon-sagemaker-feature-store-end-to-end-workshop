# Copyright 2021 Amazon.com and its affiliates; all rights reserved. 
# This file is AWS Content and may not be duplicated or distributed without permission
"""This module contains helper methods related to Amazon SageMaker Feature Store"""
from __future__ import print_function, absolute_import

import boto3
import time
import json
import uuid
import pandas as pd
import numpy as np
import os
from typing import List, Dict, Union, Tuple

from multiprocessing import Pool

import sagemaker
from sagemaker.feature_store.feature_group import FeatureGroup
from sagemaker.session import Session
from sagemaker import get_execution_role
from sagemaker.feature_store.feature_group import FeatureGroup

from sagemaker.processing import ProcessingInput, ProcessingOutput
from sagemaker.workflow.steps import ProcessingStep
from sagemaker.spark.processing import PySparkProcessor
from sagemaker.sklearn.processing import SKLearnProcessor
from sagemaker.workflow.pipeline import Pipeline

TEMP_TIME_TRAVEL_ID_NAME = 'TEMP_TIME_TRAVEL_ID'

class FeatureStore:
    """Provides an easy to use wrapper for Amazon SageMaker Feature Store.
    
    This helper class offers a number of convenient methods on top of the Feature Store
    API to simplify interacting with both the online store and the offline store.
    
    The offline store wrapper provides a simple-to-use row-level time travel 
    method that cuts across multiple feature groups and ensures no feature leakage,
    with individual timestamps for each event for a desired training dataset.
    An As-of event time method is also provided, giving you the set of feature values
    as of a single specific point in time. The helper class also provides methods 
    for getting record counts, sample records, min and max write times and event times.
    
    The online store methods provide convenience on top of GetRecord and BatchGetRecord.
    They return values in native Python data types. The methods also permit specifying
    a simple list of features using a fully-qualified name (e.g., 'customers:salary').
    
    A set of methods is also provided for creating a form of feature pipeline using
    SageMaker Pipelines, SageMaker Processing jobs, and Amazon EventBridge.
    """
    
    _iam_pipeline_start_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sagemaker:StartPipelineExecution"
                ],
                "Resource": ["TBD"]
            }
        ]
    }
    _iam_trust_policy = {
        'Version': '2012-10-17',
        'Statement': [
            {
                'Effect': 'Allow',
                'Principal': {
                  'Service': 'events.amazonaws.com'
                },
                'Action': 'sts:AssumeRole'
            }
        ]
    }
    _script_dir = 'sm'
    _pipeline_script_tmp_name = './batch_ingest.py'
    _role = get_execution_role()

    def __init__(self):
        """Constructs a FeatureStore instance."""

        self._boto_session = boto3.Session()
        self._region = self._boto_session.region_name
        self._account_id = boto3.client("sts").get_caller_identity()["Account"]
        self._s3_client = boto3.client('s3', region_name=self._region),
        self._sm_client = self._boto_session.client(service_name='sagemaker', 
                                                    region_name=self._region)
        self._featurestore_runtime = self._boto_session.client(service_name='sagemaker-featurestore-runtime', 
                                                               region_name=self._region)
        self._feature_store_session = Session(
            boto_session=self._boto_session,
            sagemaker_client=self._sm_client,
            sagemaker_featurestore_runtime_client=self._featurestore_runtime
        )
        self._sm_sess = sagemaker.Session()
        self._default_bucket = self._sm_sess.default_bucket() 

    def list_feature_groups(self, name_contains: str=None) -> List[str]:
        """Returns a list of the names of all existing feature groups in this account.
        
        Optionally, you can use the name_contains parameter to filter the list. Takes care of
        iterator tokens for environments with many feature groups.
        
        Args:
            name_contains (str): An optional substring to use for filtering results whose name contains this string.
        
        Returns:
            list: a list of feature group names.
        """
        matching_fgs = []
        next_token = None
        sub_name = None

        if name_contains is None:
            response = self._sm_client.list_feature_groups()
        else:
            sub_name = name_contains
            response = self._sm_client.list_feature_groups(NameContains=sub_name)

        if 'NextToken' in list(response.keys()):
            next_token = response['NextToken']
        else:
            fg_summaries = response['FeatureGroupSummaries']
            for summary in fg_summaries:
                matching_fgs.append(summary)

        while next_token is not None:
            if name_contains is None:
                response = self._sm_client.list_feature_groups(NextToken=next_token)
            else:
                response = self._sm_client.list_feature_groups(NameContains=sub_name, NextToken=next_token)
            if 'NextToken' in list(response.keys()):
                next_token = response['NextToken']
                fg_summaries = response['FeatureGroupSummaries']
                for summary in fg_summaries:
                    matching_fgs.append(summary['FeatureGroupName'])
            else:
                fg_summaries = response['FeatureGroupSummaries']
                for summary in fg_summaries:
                    matching_fgs.append(summary['FeatureGroupName'])
                break

        return matching_fgs
    
###
### To use all of the feature pipeline capabilities (create, enable, disable, update, remove) in this package of utilities, 
### you will need your IAM execution role to include the following.
###
#
# {
#     "Version": "2012-10-17",
#     "Statement": [
#         {
#             "Action": [
#                 "iam:AttachRolePolicy",
#                 "iam:CreateRole",
#                 "iam:CreatePolicy",
#                 "events:PutRule",
#                 "iam:PassRole",
#                 "events:PutTargets",
#                 "events:EnableRule",
#                 "events:DisableRule",
#                 "events:ListTargetsByRule",
#                 "events:RemoveTargets",
#                 "events:DeleteRule"
#             ],
#             "Effect": "Allow",
#             "Resource": "*"
#         }
#     ]
# }
#
### And the trust relationship for Amazon EventBridge
#
# {
#   "Version": "2012-10-17",
#   "Statement": [
#     {
#       "Effect": "Allow",
#       "Principal": {
#         "Service": [
#           "events.amazonaws.com",
#           "sagemaker.amazonaws.com"
#         ]
#       },
#       "Action": "sts:AssumeRole"
#     }
#   ]
# }

    def enable_feature_pipeline(self, fg_name: str) -> None:
        """Enables a feature pipeline.
        
        The feature pipeline must already exist. Once a feature pipeline is enabled,
        it will be executed according to the schedule that was supplied when it
        was created.
        
        Args:
            fg_name (str): Name of the feature group associated with the pipeline to enable.
        """
        events_client = boto3.client('events')

        # get name of pipeline, which is a known EventBridge rule name, with a 'pipeline-' prefix and FG suffix
        pipeline_name = f'scheduled-sm-pipeline-{fg_name}'

        # disable the EventBridge rule
        events_client.enable_rule(Name=pipeline_name)
        return

    def disable_feature_pipeline(self, fg_name: str) -> None:
        """Disables a feature pipeline. 
        
        The feature pipeline must already exist. Once a feature pipeline is disabled,
        it will no longer be triggered on a schedule until it is once again enabled.
        
        Args:
            fg_name (str): Name of the feature group associated with the pipeline to disable.
        """
        events_client = boto3.client('events')

        # get name of pipeline, which is a known EventBridge rule name, with a 'pipeline-' prefix and FG suffix
        pipeline_name = f'scheduled-sm-pipeline-{fg_name}'

        # disable the EventBridge rule
        events_client.disable_rule(Name=pipeline_name)
        return

    def _create_script(self, out_script: str, user_defined_script: str=None, script_type: str='python') -> None:
        """Creates the processing job script file.
        
        Assembles a processing job script given the user defined script as well as
        the common top portion and bottom portion of the script that is pre-defined.
        If no user defined script is provided, the script will only perform feature
        ingestion of the input data, without any transformations.
        
        Args:
            out_script (str): Filename used for resulting script.
            user_defined_script (str): Optional filename containing user provided transform or SQL query.
            script_type (str): 'python' or 'pyspark_sql' indicating type of script to be created.
        """
        filenames = []

        if script_type == 'python':
            if user_defined_script is None:
                _processing_script = f'{self._script_dir}/batch_ingest_sm_sklearn_empty.py'
            else:
                _processing_script = user_defined_script

            filenames = [f'{self._script_dir}/batch_ingest_sm_sklearn_top.py', _processing_script, 
                         f'{self._script_dir}/batch_ingest_sm_sklearn_bottom.py']
        else:
            if user_defined_script is None:
                _processing_script = f'{self._script_dir}/batch_ingest_sm_pyspark_empty.py'
            else:
                _processing_script = user_defined_script

            filenames = [f'{self._script_dir}/batch_ingest_sm_pyspark_top.py', _processing_script, 
                         f'{self._script_dir}/batch_ingest_sm_pyspark_bottom.py']
        with open(out_script, 'w') as outfile:
            for fname in filenames:
                with open(fname) as infile:
                    outfile.write(infile.read())
        return

    def schedule_feature_pipeline(self, s3_uri_prefix: str, fg_name: str, 
                                  user_defined_script: str=None, 
                                  schedule: str='rate(1 day)',
                                  script_type: str='python',
                                  enable: bool=True, 
                                  instance_type: str='ml.m5.xlarge', instance_count: int=1,
                                  max_processes: int=16, max_workers: int=4,
                                  max_runtime_in_seconds: int=3600) -> None:
        """Creates a brand new feature pipeline for an existing feature group. 
        
        Creates a SageMaker Pipeline containing a single step representing a SageMaker Processing job. 
        Uses SKLearn processor or PySpark processor depending on the script type. Creates a new IAM policy 
        to allow Amazon EventBridge to start the SageMaker Pipeline. Creates an EventBridge rule based 
        on the schedule provided. Associates the schedule rule with the Pipeline. Pipeline will be started 
        each time the schedule is triggered. Pipeline triggering can be enabled or disabled.
        
        For script_type='python', the user_defined_script must contain an apply_transforms function that
        takes in a Pandas dataframe and returns a transformed Pandas dataframe containing the exact set of
        columns needed for ingesting to the target feature group.
        
        For script_type='pyspark_sql', the user_defined_script must contain an transform_query function that
        takes in a feature group name, and returns a SQL query that uses that feature group name in its FROM
        clause to return the exact set of columns needed for ingesting to the target feature group. 
        The processing script will prepare a Spark dataframe with data from the data source, and will run
        the SQL query that is returned by transform_query. The resulting Spark dataframe will be ingested.
        
        If no user_defined_script is provided, the feature pipeline will simply ingest the features provided
        from the data source (s3 in this case). Otherwise, the transformations will be performed on the input
        data, and the resulting features will be ingested.
        
        Args:
            s3_uri_prefix (str): Raw data source, can be prefix for multiple files, currently only CSV supported.
            fg_name (str): Name of the feature group this pipeline is associated with.
            user_defined_script (str): Optional, filename for processing script.
            schedule (str): Optional, cron scheduling expression compatible with Amazon EventBridge.
            script_type (str): Optional, 'python' or 'pyspark_sql' indicating type of script to be created.
            enable (bool): Optional, whether or not to immediately enable the pipeline.
            instance_type (str): Optional, instance type for the processing job.
            instance_count (int): Optional, number of instances to use for the processing job.
            max_processes (int): Optional, max number of processes to use for feature ingestion.
            max_workers (int): Optional, max number of workers to use in each process.
            max_runtime_in_seconds (int): Optional, max number of seconds permitted for this processing job to run, defaults to one hour.
        """

        self._create_script(self._pipeline_script_tmp_name, user_defined_script, script_type)

        # Create a new SageMaker Pipeline
        pipeline_name = f'sm-pipeline-{fg_name}'

        # 1/ Create a Processing Step to run the SageMaker Processing job
        processor = None
        if script_type == 'python':
            processor = SKLearnProcessor(framework_version='0.20.0',
                                             role=self._role,
                                             instance_type=instance_type,
                                             instance_count=instance_count,
                                             base_job_name=pipeline_name)
            proc_inputs = [ProcessingInput(s3_data_type='S3Prefix', 
                                source=s3_uri_prefix, 
                                s3_data_distribution_type='ShardedByS3Key', 
                                destination="/opt/ml/processing/input")]
            proc_outputs = []
            job_args = ['--num_processes', str(max_processes), 
                           '--num_workers', str(max_workers),
                           '--feature_group_name', fg_name,
                           '--region_name', sagemaker.Session().boto_region_name]
        else:
            processor = PySparkProcessor(framework_version='3.0',
                                role=self._role,
                                instance_type=instance_type,  
                                instance_count=instance_count,
                                base_job_name=pipeline_name,
                                env={'AWS_DEFAULT_REGION': self._region,
                                     'mode': 'python'},
                                max_runtime_in_seconds=max_runtime_in_seconds)
            proc_inputs = []
            job_args = ['--feature_group_name', fg_name,
                        '--region_name', self._region,
                        '--s3_uri_prefix', s3_uri_prefix]
            proc_outputs = [ProcessingOutput(output_name='output-1',
                                             source='/opt/ml/processing/spark-events/',
                                             destination=f's3://{default_bucket}/spark-logs',
                                             s3_upload_mode='Continuous')]

        step_process = ProcessingStep(
            name='FeaturePipelineStep',
            processor=processor,
            inputs=proc_inputs,
            outputs=proc_outputs,
            job_arguments=job_args,
            code=self._pipeline_script_tmp_name 
            #,spark_event_logs_s3_uri=f's3://{default_bucket}/spark-logs'
        )

        # 2/ Create a simple pipeline containing that one step
        pipeline = Pipeline(
            name=pipeline_name,
            parameters=[],
            steps=[step_process],
        )

        # 3/ Save the new pipeline
        pipeline.upsert(role_arn=self._role)

        # 4/ Get the pipeline arn
        pipeline_descr = pipeline.describe()
        pipeline_arn = pipeline_descr['PipelineArn']

        # Create an IAM policy to enable EventBridge to start the SageMaker Pipeline
        timestamp = int(time.time())
        iam_client = boto3.client('iam')
        role_name = f'Amazon_EventBridge_Invoke_SageMaker_Pipeline_{timestamp}'
        FeatureStore._iam_pipeline_start_policy['Statement'][0]['Resource'] = pipeline_arn

        event_bridge_role = iam_client.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(FeatureStore._iam_trust_policy),
                Description='Policy to allow Amazon EventBridge to start a specific SageMaker Pipeline'
                )

        policy_res = iam_client.create_policy(
            PolicyName=f'Amazon_EventBridge_Invoke_SageMaker_Pipeline_Policy_{timestamp}',
            PolicyDocument=json.dumps(FeatureStore._iam_pipeline_start_policy)
        )
        policy_arn = policy_res['Policy']['Arn']

        policy_attach_res = iam_client.attach_role_policy(
            RoleName=event_bridge_role['Role']['RoleName'],
            PolicyArn=policy_arn
        )

        # Create the new EventBridge rule to run the pipeline on a schedule
        events_client = boto3.client('events')

        rule_name = f'scheduled-{pipeline_name}'
        events_client.put_rule(
            Name=rule_name,
            ScheduleExpression=schedule,
            State='DISABLED',
            Description='Automated creation of scheduled pipeline',
            RoleArn=self._role,
            EventBusName='default'
        )

        # Add the SageMaker Pipeline as a target to the EventBridge scheduled rule
        tmp_id = str(uuid.uuid4())

        events_client.put_targets(
            Rule=rule_name,
            Targets=[{'Id': tmp_id,
                      'Arn': pipeline_arn,
                      'RoleArn': event_bridge_role['Role']['Arn']
                     }]
        )

        if enable:
            self.enable_feature_pipeline(fg_name)
        return 

    def update_feature_pipeline(self, s3_uri_prefix: str, fg_name: str, 
                                user_defined_script: str=None,
                                script_type: str='python',
                                enable: bool=True, 
                                instance_type: str='ml.m5.xlarge', instance_count: int=1,
                                max_processes: int=16, max_workers: int=4,
                                max_runtime_in_seconds: int=3600) -> None:
        """Updates an existing feature pipeline to use a new script.
        
        See schedule_feature_pipeline for details. Note that update_feature_pipeline takes
        advantage of the existing schedule. To change the schedule, you must remove and 
        re-create the feature pipeline.
        
        Args:
            s3_uri_prefix (str): Raw data source, can be prefix for multiple files, currently only CSV supported.
            fg_name (str): Name of the feature group this pipeline is associated with.
            user_defined_script (str): Optional, filename for processing script.
            script_type (str): Optional, 'python' or 'pyspark_sql' indicating type of script to be created.
            enable (bool): Optional, whether or not to immediately enable the pipeline.
            instance_type (str): Optional, instance type for the processing job.
            instance_count (int): Optional, number of instances to use for the processing job.
            max_processes (int): Optional, max number of processes to use for feature ingestion.
            max_workers (int): Optional, max number of workers to use in each process.
            max_runtime_in_seconds (int): Optional, max number of seconds permitted for this processing job to run, defaults to one hour.
        """

        self._create_script(self._pipeline_script_tmp_name, user_defined_script, script_type)

        # Create a new SageMaker Pipeline
        pipeline_name = f'sm-pipeline-{fg_name}'

        # 1/ Create a Processing Step to run the SageMaker Processing job
        processor = None
        if script_type == 'python':
            processor = SKLearnProcessor(framework_version='0.20.0',
                                             role=self._role,
                                             instance_type=instance_type,
                                             instance_count=instance_count,
                                             base_job_name=pipeline_name)
            proc_inputs = [ProcessingInput(s3_data_type='S3Prefix', 
                                source=s3_uri_prefix, 
                                s3_data_distribution_type='ShardedByS3Key', 
                                destination="/opt/ml/processing/input")]
            proc_outputs = []
            job_args = ['--num_processes', str(max_processes), 
                           '--num_workers', str(max_workers),
                           '--feature_group_name', fg_name,
                           '--region_name', self._region]
        else:
            processor = PySparkProcessor(framework_version='3.0',
                                role=self._role,
                                instance_type=instance_type,  
                                instance_count=instance_count,
                                base_job_name=pipeline_name,
                                env={'AWS_DEFAULT_REGION': self._region,
                                     'mode': 'python'},
                                max_runtime_in_seconds=max_runtime_in_seconds)
            proc_inputs = []
            job_args = ['--feature_group_name', fg_name,
                        '--region_name', self._region,
                        '--s3_uri_prefix', s3_uri_prefix]
            proc_outputs = [ProcessingOutput(output_name='output-1',
                                             source='/opt/ml/processing/spark-events/',
                                             destination=f's3://{default_bucket}/spark-logs',
                                             s3_upload_mode='Continuous')]

        step_process = ProcessingStep(
            name='FeaturePipelineStep',
            processor=processor,
            inputs=proc_inputs,
            outputs=proc_outputs,
            job_arguments=job_args,
            code=self._pipeline_script_tmp_name 
            #,spark_event_logs_s3_uri=f's3://{default_bucket}/spark-logs'
        )

        # 2/ Create a simple pipeline containing that one step
        pipeline = Pipeline(
            name=pipeline_name,
            parameters=[],
            steps=[step_process],
        )

        # 3/ Save the new pipeline
        pipeline.upsert(role_arn=self._role)

        # 4/ To ensure the pipeline gets triggered immediately, disable and re-enable after short wait
        if enable:
            self.disable_feature_pipeline(fg_name)
            time.sleep(3)
            self.enable_feature_pipeline(fg_name)
        return 

    ## TODO: handle the case where there are already-running executions. Currently ends up 
    ## deleting the EventBridge rule, but fails to delete the pipeline itself. Need to handle
    ## delete_pipeline error and retry.

    def remove_feature_pipeline(self, fg_name: str) -> None:
        """Removes an existing feature pipeline.
        
        Also removes the underlying EventBridge rule that was used for scheduling / triggering,
        as well as the underlying SageMaker Pipeline.
        
        Args:
            fg_name (str): Name of the feature group this pipeline is associated with.
        """
        # get name of pipeline, which is a known EventBridge rule name, with a 'pipeline-' prefix and FG suffix
        rule_name = f'scheduled-sm-pipeline-{fg_name}'

        events_client = boto3.client('events')

        try:
            # get Id of pipeline
            pipe_id = events_client.list_targets_by_rule(Rule=rule_name)['Targets'][0]['Id']

            # remove EventBridge rule targets
            events_client.remove_targets(Rule=rule_name, Ids=[pipe_id])

            # disable the EventBridge rule
            events_client.disable_rule(Name=rule_name)

            # remove the EventBridge rule
            events_client.delete_rule(Name=rule_name)
        except:
            pass

        # delete the SageMaker Pipeline
        pipeline_name = f'sm-pipeline-{fg_name}'
        resp = self._sm_client.list_pipeline_executions(PipelineName=pipeline_name, MaxResults=100)

        for e in resp['PipelineExecutionSummaries']:
            if e['PipelineExecutionStatus'] == 'Executing':
                self._sm_client.stop_pipeline_execution(PipelineExecutionArn=e['PipelineExecutionArn'])

        self._sm_client.delete_pipeline(PipelineName=pipeline_name)    
        return

    ## TODO: add caching of feature group descriptions
    def describe_feature_group(self, fg_name: str) -> Dict:
        """Returns a dictionary describing an existing feature group.
        
        See boto3 describe_feature_group for details.
        
        Args:
            fg_name (str): Name of the feature group to describe.
            
        Returns:
            Feature Group description dictionary.
        """
        _tmp_desc = self._sm_client.describe_feature_group(FeatureGroupName=fg_name)
        return _tmp_desc

    def delete_feature_group(self, fg_name: str, delete_s3=True) -> None:
        """Deletes an existing feature group.
        
        Note that this method does not delete the Glue table that may have been created when
        the feature group was created.
        
        Args:
            fg_name (str): Name of the feature group to delete.
            delete_s3 (bool): Whether or not to also delete the s3 offline storage for this feature group.
        """
        ## TODO: properly handle situation when fg does not exist
        ## TODO: Delete Glue table if one was created automatically

        has_offline_store = True
        try:
            self.describe_feature_group(fg_name)['OfflineStoreConfig']
        except:
            has_offline_store = False
            pass

        if has_offline_store:
            offline_store_config = self.describe_feature_group(fg_name)['OfflineStoreConfig']
            if not offline_store_config['DisableGlueTableCreation']:
                table_name = offline_store_config['DataCatalogConfig']['TableName']
                catalog_id = offline_store_config['DataCatalogConfig']['Catalog']
                database_name = offline_store_config['DataCatalogConfig']['Database']

    #         glue_client = boto3.client('glue')

    #         try:
    #             glue_client.delete_table(
    #                 CatalogId=catalog_id,
    #                 DatabaseName=database_name,
    #                 Name=table_name
    #             )    
    #         except:
    #             # An error occurred (AccessDeniedException) when calling the DeleteTable operation: 
    #             # Cross account access is not supported for account that hasn't imported Athena catalog to Glue.
    #             print('Failed to delete Glue table.')
    #             print('See https://docs.aws.amazon.com/athena/latest/ug/glue-upgrade.html')


        # Delete s3 objects from offline store for this FG
        if delete_s3 and has_offline_store:
            s3_uri = self.describe_feature_group(fg_name)['OfflineStoreConfig']['S3StorageConfig']['S3Uri']
            base_offline_prefix = '/'.join(s3_uri.split('/')[3:])
            offline_prefix = f'{base_offline_prefix}/{self._account_id}/sagemaker/' +\
                             f'{self._region}/offline-store/{fg_name}/'
            s3_bucket_name = s3_uri.split('/')[2]
            s3 = boto3.resource('s3')
            bucket = s3.Bucket(s3_bucket_name)
            coll = bucket.objects.filter(Prefix=offline_prefix)
            print(f'Deleting all s3 objects in prefix: {offline_prefix} in bucket {s3_bucket_name}')
            resp = coll.delete()

        resp = None
        try:
            resp = self._sm_client.delete_feature_group(FeatureGroupName=fg_name)
        except:
            pass

        self._wait_for_feature_group_deletion_complete(fg_name)
        return 

    def _wait_for_feature_group_deletion_complete(self, feature_group_name: str) -> None:
        """Wait for feature group deletion to complete.
        
        Args:
            feature_group_name (str): Name of the feature group to wait for.
        """
        while True:
            try:
                status = self.describe_feature_group(feature_group_name)['FeatureGroupStatus']
                print('Waiting for Feature Group deletion...')
                time.sleep(5)
            except:
                break
        return

    def _wait_for_feature_group_creation_complete(self, feature_group_name: str) -> None:
        """Wait for feature group creation to complete.
        
        Args:
            feature_group_name (str): Name of the feature group to wait for.
        """
        status = self.describe_feature_group(feature_group_name)['FeatureGroupStatus']
        while status == 'Creating':
            print('Waiting for Feature Group creation...')
            time.sleep(5)
            status = self.describe_feature_group(feature_group_name)['FeatureGroupStatus']
        if status != 'Created':
            raise RuntimeError(f'Failed to create feature group {feature_group_name}')
        print(f'FeatureGroup {feature_group_name} successfully created.')

    def _df_to_feature_defs(self, df: pd.DataFrame) -> List[Dict]:
        """Maps the columns from a Pandas to a list of feature definitions compatible with CreateFeatureGroup.
        
        Args:
            df (Dataframe): Dataframe containing set of columns that represent features for a target feature group.
        """
        feature_definitions = []

        for col, col_type in zip(df.columns, df.dtypes):
            feature = {'FeatureName': col}

            if col_type == 'float64':
                feature['FeatureType'] = 'Fractional'
            elif col_type == 'int64':
                feature['FeatureType'] = 'Integral'
            else:
                feature['FeatureType'] = 'String'

            feature_definitions.append(feature)    
        return feature_definitions

    def _escape_tag_chars(self, in_str: str) -> str:
        escaped_str = in_str.replace('$', '_D_')
        escaped_str = escaped_str.replace('?', '_Q_')
        return escaped_str

    def _unescape_tag_chars(self, in_str: str) -> str:
        unescaped_str = in_str.replace('_D_', '$')
        unescaped_str = unescaped_str.replace('_Q_', '?')
        return unescaped_str

    def create_fg_from_df(self, fg_name: str, df: pd.DataFrame, description: str=None,
                          id_name: str='Id', event_time_name: str='UpdateTime', 
                          tags: Dict=None, online: bool=True, s3_uri: str=None) -> None:
        """Creates a new feature group based on a Pandas dataframe.
        
        This method creates a new feature group using feature definitions derived from 
        the column names and column data types of an existing Pandas dataframe. This is
        a convenience method to avoid having to know the details of FeatureDefinition 
        syntax and data types.
        
        Args:
            fg_name (str): Name of the feature group to create.
            df (pd.DataFrame): Pandas dataframe.
            description (str): Text description that should be associated with this new feature group.
            id_name (str): Name of feature that will be used as the record identifier. Must exist in the dataframe.
            even_time_name (str): Name of feature that will be used as the event time. Must exist in the dataframe.
            tags (dict): Dictionary of tags to associate with the new feature group.
            online (bool): Whether or not this feature group will have an online store.
            s3_uri (str): Base s3 uri indicating where the offline storage should be kept for this feature group.
        """

        if not id_name in df.columns:
            print(f'invalid id column name: {id_name}')
            return
        if not event_time_name in df.columns:
            print(f'invalid event time column name: {event_time_name}')
            return

        if s3_uri is None:
            s3_uri = f's3://{self._default_bucket}/offline-store'

        other_args = {}
        if s3_uri is not None:
            other_args['OfflineStoreConfig'] = {'S3StorageConfig': {'S3Uri': s3_uri}}

        if tags is not None:
            tags_as_kv_array = []
            for k, v in tags.items():
                curr_kv = {'Key': k, 'Value': self._escape_tag_chars(v)}
                tags_as_kv_array.append(curr_kv)
            other_args['Tags'] = tags_as_kv_array
        
        if description is not None:
            other_args['Description'] = description

        resp = self._sm_client.create_feature_group(
                FeatureGroupName = fg_name,
                RecordIdentifierFeatureName = id_name,
                EventTimeFeatureName = event_time_name,
                FeatureDefinitions = self._df_to_feature_defs(df),
                OnlineStoreConfig = {'EnableOnlineStore': online},
                RoleArn = self._role,
                **other_args)

        self._wait_for_feature_group_creation_complete(fg_name)
        return 

    def get_tags(self, fg_name: str) -> Dict[str, str]:
        """Returns the set of tags associated with a given feature group.
        
        Args:
            fg_name (str): Name of the feature group whose tags should be retrieved.
            
        Return:
            Python dictionary of tags that are associated with the feature group.
        """
        fg_arn = self.describe_feature_group(fg_name)['FeatureGroupArn']
        resp = self._sm_client.list_tags(ResourceArn=fg_arn)
        tags_kv_array = resp['Tags']
        tags = {}
        for kv in tags_kv_array:
            k = kv['Key']
            v = kv['Value']
            tags[k] = self._unescape_tag_chars(v)

        return tags

    def ingest_from_df(self, fg_name: str, df: pd.DataFrame, max_processes: int=4, max_workers: int=4,
                      wait: bool=True) -> None:
        """Ingests all the rows of a Pandas dataframe as new feature records into the given feature group.
        
        Args:
            fg_name (str): Name of the feature group in which to ingest new feature records.
            df (DataFrame): Pandas dataframe containing feature records.
            max_processes (int): Max number of processes to use when ingesting.
            max_workers (int): Max number of workers to use in each process when ingesting.
            wait (bool): Whether or not to wait for ingestion to complete.
        """
        
        # TODO: handle ingestion errors returned by underlying ingest method.
        
        fg = FeatureGroup(name=fg_name, sagemaker_session=self._feature_store_session)
        fg.ingest(data_frame=df, max_processes=max_processes, max_workers=max_workers, wait=wait)
        return
    
    def delete_record(self, fg_name: str, record_id: Union[str, int], event_time: Union[str, int]) -> None:
        """Deletes a specific record from a feature group.
        
        Args:
            fg_name (str): Name of the feature group from which to delete the record.
            record_id (Union[str,int]): Identifier of record to delete. Can be int or str record identifier.
            event_time (Union[str,int]): Event time in ISO 8601 format, identifying the event time timestamp of the record to delete.
        """
        
        # TODO: test with int-based event time
        
        results = []

        resp = self._featurestore_runtime.delete_record(FeatureGroupName=fg_name, 
                                                  RecordIdentifierValueAsString=str(record_id),
                                                  EventTime=event_time)
        return

    def _record_to_dict(self, feature_records: List[Dict], feature_types: List[str]) -> Dict:
        tmp_dict = {}
        for f in feature_records:
            feature_name = f['FeatureName']
            string_feature_val = f['ValueAsString']
            feature_type = feature_types[feature_name]

            if feature_type == 'Integral':
                tmp_dict[f['FeatureName']] = int(string_feature_val)
            elif feature_type == 'Fractional':
                tmp_dict[f['FeatureName']] = float(string_feature_val)
            else:
                tmp_dict[f['FeatureName']] = string_feature_val

        return tmp_dict

    def get_latest_feature_values(self, fg_name: str, identifiers: List[Union[str,int]], features: List[str]=None) -> Dict[str, Union[str, int, float]]:
        """Retrieves a set of features of identified records from the online store.
        
        Convenience wrapper on top of GetRecord API to allow record identifiers to be passed as either strings
        or integers, and to get native Python return types for feature values instead of only strings.
        
        Args:
            fg_name (str): Name of the feature group from which to retrieve the records.
            identifiers (List[Union[str,int]]): List record identifiers whose records are to be retrieved.
            features (List[str]): List of named features to retrieve, or None for all features.
            
        Returns:
            Dict[str, Union[str, int, float]]: Dictionary of named feature values with native Python types
        """
        feature_defs = self.describe_feature_group(fg_name)['FeatureDefinitions']
        feature_types = {}
        for fd in feature_defs:
            feature_types[fd['FeatureName']] = fd['FeatureType']

        results = []
        resp = None

        for curr_id in identifiers:
            record_identifier_value = str(curr_id)
            if features is None:
                resp = self._featurestore_runtime.get_record(FeatureGroupName=fg_name, 
                                                       RecordIdentifierValueAsString=record_identifier_value)
            else:
                resp = self._featurestore_runtime.get_record(FeatureGroupName=fg_name, 
                                                       RecordIdentifierValueAsString=record_identifier_value,
                                                       FeatureNames=features)
            try:
                curr_record = self._record_to_dict(resp['Record'], feature_types)
                results.append(curr_record)
            except:
                pass

        return results

    def _get_core_features(self, fg_name: str) -> List[str]:
        _fg_desc = self.describe_feature_group(fg_name)
        _feat_defs = _fg_desc['FeatureDefinitions']
        _feature_list = []
        for _f in _feat_defs:
            _feature_list.append(_f['FeatureName'])

        # remove the id feature and the time feature, since they'll already be covered
        _id_feature_name = _fg_desc['RecordIdentifierFeatureName']
        _time_feature_name = _fg_desc['EventTimeFeatureName']

        _exclude_list = [_id_feature_name, _time_feature_name]
        _cleaned_list = [ x for x in _feature_list if not x in _exclude_list ]

        return _cleaned_list

    def _feature_list_to_df(self, features: List[str]) -> pd.DataFrame:
        _groups_and_features = []
        _fg_number = 0
        for _f in features:
            _parts = _f.split(':')
            _fg = _parts[0]
            _fn = _parts[1]
            _feature_number = 0

            if _fn == '*':
                _all_core_features = self._get_core_features(_fg)
                for _core_feature in _all_core_features:
                    _groups_and_features.append([_fg, _core_feature, _fg_number, _feature_number])
                    _feature_number += 1
            else:
                _groups_and_features.append([_fg, _fn, _fg_number, _feature_number])

            _fg_number += 1

        return pd.DataFrame(_groups_and_features, columns=['fg_name', 'feature_name', 'fg_number', 'feature_number'])

    def get_latest_featureset_values(self, id_dict: Dict[str,Union[str,int]], features: List[str], 
                                     verbose: bool=False) -> Dict[str, Union[str, int, float]]:
        """Retrieves a set of features of identified records from one or more online feature groups.
        
        This convenience method lets the caller specify a "feature set" to be retrieved. A feature set is an
        ordered list of fully-qualified feature names with a feature group name as well as a feature name. 
        The feature name can be a wildcard to indicate that all features from that feature
        group should be returned. Since multiple feature groups are involved, an identifier dictionary must
        also be specified. For each unique identifier name across the feature set, a single record identifier
        value is specified for the lookup. That same identifier value will be used for each feature group with
        a matching record identifier feature name. For example, 'customer_id' may be the identifier used for 
        a 'customer' feature group and a 'customer-demographics' feature group. The 'customer_id' lookup identifier
        would be specified once in the 'id_dict' input argument.
        
        Args:
            fg_name (str): Name of the feature group from which to retrieve the records.
            id_dict (Dict[str,Union[str,int]]): Dictionary of record identifiers whose records are to be retrieved, key is the identifier feature
              name (can be different for each feature group), and value is the actual record identifier.
            features (List[str]): List of named features to retrieve. Features are fully-qualified as 'fg-name:feature-name',
              or 'fg-name:*' for all features.

        Returns:
            Dict[str, Union[str, int, float]]: Dictionary of named feature values with native Python types
        """

        ## TODO: BatchGetRecord does not honor the order of the features requested, so this function
        ##       should enforce reordering of results to match the requested order. This is important when mapping
        ##       to a feature vector to feed a model prediction.

        _feature_types = {}
        _resp = None
        _features_df = self._feature_list_to_df(features)
        if verbose:
            print(_features_df.head())

        _gb = _features_df.groupby('fg_name')

        _fg_requests = []

        for _g in _gb:
            _curr_features = []
            _fg_name = _g[0]
            for _f in _g[1].iterrows():
                _curr_features.append(_f[1]['feature_name'])
            _feature_defs = self.describe_feature_group(_fg_name)['FeatureDefinitions']
            for _fd in _feature_defs:
                _feature_types[_fd['FeatureName']] = _fd['FeatureType']

            _id_feature_name = self.describe_feature_group(_fg_name)['RecordIdentifierFeatureName']
            _id_value = id_dict[_id_feature_name]

            if verbose:
                print(f'fg name: {_fg_name}, id: {_id_feature_name}, id val: {_id_value}, features: {_curr_features}')

            _fg_requests.append({'FeatureGroupName': _fg_name,
                                 'RecordIdentifiersValueAsString': [str(_id_value)],
                                 'FeatureNames': _curr_features})

        if verbose:
            print(_fg_requests)
            print(_feature_types)

        _resp = self._featurestore_runtime.batch_get_record(Identifiers=_fg_requests)

        if verbose:
            _num_recs = len(_resp['Records'])
            print(f'got back {_num_recs} records')

        _results_dict = []

        _all_records = _resp['Records']
        for _req in _fg_requests:
            _fg_name = _req['FeatureGroupName']

            # enforce the original order of feature groups
            _curr_record = next(_item for _item in _all_records if _item['FeatureGroupName'] == _fg_name)['Record']
            if verbose:
                print(_curr_record)

            ## TODO: extend _record_to_dict to take a feature name list and enforce that order in the return value
            _curr_record_dict = self._record_to_dict(_curr_record, _feature_types)

            if verbose:
                print(_results_dict)
                print(_curr_record_dict)

            _results_dict = dict(_results_dict, **_curr_record_dict)

        return _results_dict

    def _run_query(self, query_string: str, tmp_uri: str, database: str, verbose: bool=True) -> pd.DataFrame:
        athena = boto3.client('athena')

        # submit the Athena query
        if verbose:
            print('Running query:\n ' + query_string)
            
        query_execution = athena.start_query_execution(
            QueryString=query_string,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': tmp_uri}
        )

        # wait for the Athena query to complete
        query_execution_id = query_execution['QueryExecutionId']
        query_state = athena.get_query_execution(QueryExecutionId=query_execution_id)['QueryExecution']['Status']['State']
        while (query_state != 'SUCCEEDED' and query_state != 'FAILED'):
            time.sleep(2)
            query_state = athena.get_query_execution(QueryExecutionId=query_execution_id)['QueryExecution']['Status']['State']

        if query_state == 'FAILED':
            print(athena.get_query_execution(QueryExecutionId=query_execution_id))
            failure_reason = athena.get_query_execution(QueryExecutionId=query_execution_id)['QueryExecution']['Status']['StateChangeReason']
            print(failure_reason)
            df = pd.DataFrame()
            return df
        else:
            ## TODO: fix this to allow user-defined prefix
            results_file_prefix = f'offline-store/query_results/{query_execution_id}.csv'

            # Prepare query results for training.
            filename = f'query_results_{query_execution_id}.csv'
            results_bucket = (tmp_uri.split('//')[1]).split('/')[0]
            try:
                if verbose:
                    print(f'results_bucket: {results_bucket}')
                    print(f'results_file_prefix: {results_file_prefix}')
                    print(f'filename: {filename}')
                    
                s3_client = boto3.client('s3', region_name=self._region)
                s3_client.download_file(results_bucket, results_file_prefix, filename)
                df = pd.read_csv(filename)
                if verbose:
                    print(f'Query results shape: {df.shape}')
                os.remove(filename)

                s3_client.delete_object(Bucket=results_bucket, Key=results_file_prefix)
                s3_client.delete_object(Bucket=results_bucket, Key=results_file_prefix + '.metadata')
                return df
            except Exception as inst:
                if verbose:
                    print(f'Failed download')
                    print(f'Exception: {inst}')
                df = None
                pass

    def _drop_temp_table(self, tmp_table_name: str, verbose: bool=False) -> pd.DataFrame:
        _drop_string = f'DROP TABLE {tmp_table_name}'
        _database = 'sagemaker_featurestore'
        _s3_uri = f's3://{self._default_bucket}/offline-store'
        _tmp_uri = f'{_s3_uri}/query_results/'
        return self._run_query(_drop_string, _tmp_uri, _database, verbose=verbose)

    def _get_offline_details(self, fg_name: str, s3_uri: str=None) -> Tuple[str, str, str]:
        _data_catalog_config = self.describe_feature_group(fg_name)['OfflineStoreConfig']['DataCatalogConfig']
        _table = _data_catalog_config['TableName']
        _database = _data_catalog_config['Database']

        if s3_uri is None:
            s3_uri = f's3://{self._default_bucket}/offline-store'
        _tmp_uri = f'{s3_uri}/query_results/'
        return _table, _database, _tmp_uri

    def sample(self, fg_name: str, n: int=10, sample_pct: int=25, s3_tmp_uri: int=None) -> pd.DataFrame:
        """Retrieves a sample of records from the offline store for a given feature group.
        
        Args:
            fg_name (str): Name of the feature group from which to retrieve the records.
            n (int): Optional, number of records to sample.
            sample_pct (int): Optional, percentage of records to sample.
            s3_tmp_uri (str): Optional, temporary storage area for query results.
            
        Returns:
            pd.DataFrame: Dataframe containing sample records
        """
        
        # TODO: clarify use of sample_pct vs n
        
        _table, _database, _tmp_uri = self._get_offline_details(fg_name, s3_tmp_uri)
        _query_string = f'SELECT * FROM "' +_table+ f'"'  + f' tablesample bernoulli({sample_pct}) limit {n}'
        return self._run_query(_query_string, _tmp_uri, _database, verbose=False)

    def get_minmax_timestamps(self, fg_name: str, s3_tmp_uri: str=None) -> pd.DataFrame:
        """Retrieves min and max event times and write times for a given feature group.
        
        Useful for understanding the age of the offline store as well as how fresh the data is. Can 
        also be useful for understanding the use of event time, which can be different for any given
        feature group.
        
        Args:
            fg_name (str): Name of the feature group from which to retrieve the timestamps.
            s3_tmp_uri (str): Optional, temporary storage area for query results.
            
        Returns:
            pd.DataFrame: Dataframe containing min_write_time, max_write_time, min_event_time, and max_event_time columns
        """
        _table, _database, _tmp_uri = self._get_offline_details(fg_name, s3_tmp_uri)
        
        _event_time = self.describe_feature_group(fg_name)['EventTimeFeatureName']

        _query_string = f'SELECT min(write_time) as min_write_time, max(write_time) as max_write_time, ' +\
                        f'min({_event_time}) as min_event_time, max({_event_time}) as max_event_time FROM "' +_table+ f'"'
        _tmp_df = self._run_query(_query_string, _tmp_uri, _database, verbose=False)
        return _tmp_df

    def get_historical_record_count(self, fg_name: str, s3_tmp_uri: str=None) -> int:
        """Returns the number of records that exist in the offline store for a given feature group.
        
        Useful for understanding the size of an offline store.
        
        Args:
            fg_name (str): Name of the feature group from which to get the record count.
            s3_tmp_uri (str): Optional, temporary storage area for query results.
            
        Returns:
            int: Number of records in the offline store for the given feature group
        """
        _table, _database, _tmp_uri = self._get_offline_details(fg_name, s3_tmp_uri)
        _query_string = f'SELECT COUNT(*) FROM "' +_table+ f'"'
        _tmp_df = self._run_query(_query_string, _tmp_uri, _database, verbose=False)
        return _tmp_df.iat[0, 0]
    
    def get_offline_store_url(self, fg_name: str) -> str:
        """Returns a URL to navigate to the S3 offline storage of a given feature group.
        
        Args:
            fg_name (str): Name of the feature group whose URL is returned.
            s3_tmp_uri (str): Optional, temporary storage area for query results.
            
        Returns:
            str: URL for navigating to the S3 offline storage area
        """
        fg_s3_uri = ''
        has_offline_store = True
        offline_store_config = {}
        try:
            offline_store_config = self.describe_feature_group(fg_name)['OfflineStoreConfig']
        except:
            has_offline_store = False
            return fg_s3_uri

        table_name, _, _ = self._get_offline_details(fg_name)

        base_s3_uri = offline_store_config['S3StorageConfig']['ResolvedOutputS3Uri']
        base_offline_prefix = '/'.join(base_s3_uri.split('/')[3:])
        s3_bucket_name = base_s3_uri.split('/')[2]

        return f'https://s3.console.aws.amazon.com/s3/buckets/{s3_bucket_name}?region={self._region}&prefix={base_offline_prefix}/'

    def get_glue_table_url(self, fg_name: str) -> str:
        """Returns a URL to navigate to the Glue table of a given feature group.
        
        Args:
            fg_name (str): Name of the feature group whose URL is returned.
            s3_tmp_uri (str): Optional, temporary storage area for query results.
            
        Returns:
            str: URL for navigating to the Glue table
        """
        _data_catalog_config = self.describe_feature_group(fg_name)['OfflineStoreConfig']['DataCatalogConfig']
        _table = _data_catalog_config['TableName']
        _database = _data_catalog_config['Database']

        return f'https://console.aws.amazon.com/glue/home?region={self._region}#table:catalog={self._account_id};name={_table};namespace={_database}'

    def download_sample_offline_file(self, fg_name: str) -> str:
        """Downloads a sample parquet file from the offline store and returns its local filename
        
        Args:
            fg_name (str): Name of the feature group whose sample file should be returned.
            
        Returns:
            str: name of local file that was created by downloading from the offline store
        """
        # TODO: change the implementation to try to grab the most recent parquet file.
        #       otherwise, may be confusing.

        fg_s3_uri = ''
        has_offline_store = True
        offline_store_config = {}
        try:
            offline_store_config = self.describe_feature_group(fg_name)['OfflineStoreConfig']
        except:
            has_offline_store = False
            return fg_s3_uri

        base_s3_uri = offline_store_config['S3StorageConfig']['ResolvedOutputS3Uri']
        bucket = base_s3_uri.split('s3://')[1].split('/')[0]
        prefix = base_s3_uri.replace(f's3://{bucket}/', '')

        s3_client = boto3.client('s3')
        resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        key_to_download = ''
        for obj_key in resp['Contents']:
            curr_key = obj_key['Key']
            if '.parquet' in curr_key:
                key_to_download = curr_key
                break

        filename = key_to_download.split('/')[-1]
        s3_client.download_file(self._default_bucket, key_to_download, filename)
        return filename
    
    def _id_list_as_string(self, fg_name: str, record_ids: List[Union[str,int]]) -> List[str]:
        fg_descr = self.describe_feature_group(fg_name)
        id_feature_name = fg_descr['RecordIdentifierFeatureName']

        feature_defs = fg_descr['FeatureDefinitions']

        id_feature_def = [dictionary for dictionary in feature_defs if dictionary['FeatureName'] == id_feature_name]
        id_feature_type = id_feature_def[0]['FeatureType']

        if id_feature_type == 'String':
            _string_id_list = ','.join("'" + x + "'" for x in record_ids)
        else:
            _string_id_list = ','.join(str(x) for x in record_ids)

        return _string_id_list

    def _get_other_cols(self, events_df: pd.DataFrame, events_timestamp_col: str, 
                        fg_desc: Dict, features: List[str]) -> List[str]:
        # start with the event column names
        _col_list = []
        for _c in events_df.columns:
            if _c.lower() in features or _c.lower() == events_timestamp_col:
                _col_list.append(_c.lower())

        # add in all the requested feature group feature names
        for _f in features:
            _col_list.append(_f.lower())

        # remove the id feature and the time feature, since they'll already be covered
        _id_feature_name = fg_desc['RecordIdentifierFeatureName']
        _time_feature_name = fg_desc['EventTimeFeatureName']

        _exclude_list = [_id_feature_name, _time_feature_name, events_timestamp_col]
        _cleaned_list = [ x for x in _col_list if not x in _exclude_list ]
        
        # de-duplicate the list, as a feature group wildcard may have pulled in some keys
        # that are also in the event dataframe.
        _cleaned_list = list(dict.fromkeys(_cleaned_list))

        _col_string = ','.join(_cleaned_list)
        return _col_string

    def _get_athena_col_defs(self, df: pd.DataFrame) -> List[str]:
        _col_num = 0
        _num_cols = len(df.columns)
        _col_string = ''
        for c in df.columns:
            _curr_type = df.dtypes[_col_num]
            if _curr_type == 'object':
                _col_type = 'STRING'
            elif _curr_type == 'int64':
                _col_type = 'INT'
            elif _curr_type == 'float64':
                _col_type = 'DOUBLE'
            elif _curr_type == 'bool':
                _col_type = 'BOOLEAN'
            ## TODO: handle 'datetime64'

            _col_string += f'{c} {_col_type}'
            if _col_num < (_num_cols - 1):
                _col_string += ', '
            _col_num += 1
        return _col_string

    def _make_temp_table(self, tmp_table_name: str, csv_location: str, events_df: pd.DataFrame, 
                         verbose: bool=False) -> str:
        athena_cols = self._get_athena_col_defs(events_df)
        serde_properties =   '"separatorChar" = ",", "quoteChar"     = "`", "escapeChar"    = "\\\\"'

        _create_string = f'CREATE EXTERNAL TABLE {tmp_table_name} ({athena_cols}) ' +\
            "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' " +\
            f'WITH SERDEPROPERTIES ({serde_properties}) ' +\
            f"LOCATION '{csv_location}';"

        _database = 'sagemaker_featurestore'
        _s3_uri = f's3://{self._default_bucket}/offline-store'
        _tmp_uri = f'{_s3_uri}/query_results/'

        self._run_query(_create_string, _tmp_uri, _database, verbose=verbose)
        return _create_string
    
    def _qualify_cols(self, letter, col_list_string):
        _cols = col_list_string.split(',')
        _qual_col_list_string = f', {letter}.'.join(_cols)
        return f'{letter}.' + _qual_col_list_string

    def _row_level_time_travel(self, events_df: pd.DataFrame, fg_name: str, 
                               events_table_name: str, events_timestamp_col: str, 
                               features: List[str], fg_table_name: str, verbose: bool=False) -> pd.DataFrame:
        _fg_desc = self.describe_feature_group(fg_name)
        if verbose:
            print(f'** in _row_level_time_travel, fg name: {fg_name}, desc: {_fg_desc}')

        _id_feature_name = _fg_desc['RecordIdentifierFeatureName']
        _time_feature_name = _fg_desc['EventTimeFeatureName']
        _feat_defs = _fg_desc['FeatureDefinitions']

        _other_cols = self._get_other_cols(events_df, events_timestamp_col, _fg_desc, features)
        
        
        if verbose:
            print(f'fg: {fg_name}, features: {features}, _other_cols: {_other_cols}')

        _f_cols = self._qualify_cols('f', _other_cols)
        _t_cols = self._qualify_cols('t', _other_cols)
        
        _sub_query = f'select f.{_id_feature_name}, {_f_cols}, ' +\
                     f'{events_timestamp_col}, {_time_feature_name}, e.{TEMP_TIME_TRAVEL_ID_NAME}, is_deleted, write_time, row_number() ' +\
                     f'over (partition by f.{_id_feature_name}, {events_timestamp_col} ' +\
                     f'order by {_time_feature_name} desc, write_time desc) as row_number ' +\
                     f'from "{fg_table_name}" f, "{events_table_name}" e ' +\
                     f'where {_time_feature_name} < {events_timestamp_col} ' +\
                     f'and f.{_id_feature_name} = e.{_id_feature_name}'

        _travel_string = f'with temp as (select {_id_feature_name}, {_other_cols}, {events_timestamp_col},' +\
                         f' {_time_feature_name}, write_time ' +\
                         f'from ({_sub_query}) ' +\
                         'where row_number = 1 and NOT is_deleted ' +\
                         f'order by {_id_feature_name} desc, {events_timestamp_col} desc, {_time_feature_name} desc) ' +\
                         f'select e.{events_timestamp_col}, e.{_id_feature_name}, {_t_cols}, e.{TEMP_TIME_TRAVEL_ID_NAME} ' +\
                         f'from "{events_table_name}" e left outer join temp t ' +\
                         f'on e.{events_timestamp_col} = t.{events_timestamp_col} and ' +\
                         f'e.{_id_feature_name} = t.{_id_feature_name} ' +\
                         f'order by e.{TEMP_TIME_TRAVEL_ID_NAME} asc'

        _database = 'sagemaker_featurestore'
        _s3_uri = f's3://{self._default_bucket}/offline-store'
        _tmp_uri = f'{_s3_uri}/query_results/'

        df = self._run_query(_travel_string, _tmp_uri, _database, verbose=verbose)
        if verbose:
            print(f'== Completed row-level travel on {fg_table_name}, result shape: {0 if df is None else df.shape}')
        return df

    def _mp_row_level_time_travel_wrapper(request: List) -> pd.DataFrame:
        """Class-level method to wrap around lower level row-level time travel for use with multiprocessing."""
        _tmp_fs = FeatureStore()
        return _tmp_fs._row_level_time_travel(request[0], request[1], request[2], request[3], 
                                      request[4], request[5], request[6])

    def _row_level_time_travel_wrapper(self, request: List) -> pd.DataFrame:
        """Wrapper around lower level row-level time travel method, for sequential usage per feature group."""
        return self._row_level_time_travel(request[0], request[1], request[2], request[3], 
                                      request[4], request[5], request[6])

    def get_features(self, 
                     events_df: pd.DataFrame, 
                     timestamp_col: str, 
                     features: List[str], 
                     parallel: bool=True, 
                     verbose: bool=False) -> pd.DataFrame:
        """Performs row-level time travel, returning a point-in-time accurate dataset.
        
        This convenience method lets the caller specify a "feature set" to be retrieved, with feature values
        that are free from feature leakage. The events_df specifies a set of event timestamps and corresponding
        record identifiers to drive the row-level time travel. The features parameter identifies the feature set 
        to retrieve. It is an ordered list of fully-qualified feature names with a feature group name as well 
        as a feature name. The feature name can be a wildcard to indicate that all features from that feature
        group should be returned. Since multiple feature groups may be involved, the events dataframe must
        have a column for each unique identifier name across the feature set. That same identifier value will 
        be used for each feature group with a matching record identifier feature name. For example, 'customer_id' 
        may be the identifier used for a 'customer' feature group and a 'customer-demographics' feature group. 
        The 'customer_id' lookup identifier would be a column in the events_df dataframe.
        
        Depending on the requested timestamp and the event times of the feature values found, the row-level time
        travel will return null values for feature values that would not yet have been available. For feature values
        that are available, the time travel will return the most recent value, without allowing any values that are
        newer than the requested timestamp.
        
        Args:
            fg_name (str): Name of the feature group from which to retrieve the records.
            events_df (pd.DataFrame): Dataframe with an event timestamp and one or more identifier columns.
            timestamp_col (str): Name used in the events_df dataframe to indicate the desired event timestamp.
            features (List[str]): List of named features to retrieve. Features are fully-qualified as 'fg-name:feature-name',
              or 'fg-name:*' for all features.
            parallel (bool): Whether to perform subsets of the time travel in parallel. This saves time, but there are cases where parallel execution will fail.
            verbose (bool): Whether to print out details of the execution along the way.

        Returns:
            pd.DataFrame: Dataframe containing the event time stamp, the record identifiers, followed by the features
              requested in the feature set
        """        
        
        # Copy the entities dataframe to S3 as a CSV file with no header, making it suitable for
        # an Athena temporary table.
        
        _base_tmp_prefix = 'fs-query-input'
        _tmp_prefix = int(f'{time.time():<19}'.replace(' ', '0')[:18].replace('.', '')) 

        _obj_name = f'{_base_tmp_prefix}/{_tmp_prefix}/{_tmp_prefix}.csv'
        _csv_uri = f's3://{self._default_bucket}/{_obj_name}'

        if verbose:
            print(f'\nUploading events as a csv file to be used as a temp table...\n{_csv_uri}')

        events_df[TEMP_TIME_TRAVEL_ID_NAME] = np.arange(len(events_df))
        events_df.to_csv(_csv_uri, index=False, header=None)

        if verbose:
            print('...upload completed')

        _csv_location = f's3://{self._default_bucket}/{_base_tmp_prefix}/{_tmp_prefix}/'
        _events_table_name = f'tmp_events_{_tmp_prefix}'

        # Create a temporary table on top of the entities CSV in S3
        self._make_temp_table(_events_table_name, _csv_location, 
                              events_df, verbose=verbose)

        # Perform row-level time travel

        # first parse out the names of the FG's and features
        _features_df = self._feature_list_to_df(features)

        # now establish the final column names in the resulting df; take care of lower case
        _feature_columns = [c.lower() for c in _features_df['feature_name'].values]
        _final_columns = events_df.columns.tolist()
        _final_columns.extend(_feature_columns)
        
        # de-duplicate the list, as a feature group wildcard may have pulled in some keys
        # that are also in the event dataframe.
        _final_columns = list(dict.fromkeys(_final_columns))
        
        if verbose:
            print(f'  feature columns: {_feature_columns}')
            print(f'  event df columns: {events_df.columns.tolist()}')
            print(f'  final columns: {_final_columns}')

        _gb = _features_df.groupby('fg_name')

        _cumulative_features_df = None
        _requests = []

        # capture a list of offline row-level time travel requests, one for each feature group

        for _g in _gb:
            _curr_features = []
            fg_name = _g[0]
            for _f in _g[1].iterrows():
                _curr_features.append(_f[1]['feature_name'])

            if verbose:
                print(f'fg name: {fg_name}, features: {_curr_features}')

            _offlinestore_table, _database, _tmp_uri = self._get_offline_details(fg_name)

            _requests.append([events_df, fg_name, _events_table_name, timestamp_col, 
                              _curr_features, _offlinestore_table, verbose])

        if verbose:
            print(f'\nTime travel requests list:\n{_requests}')

        # perform row-level time travel for each feature group in parallel or in series

        _return_data = []
        if parallel:
            _p = Pool(processes=len(_requests))
            _return_data = _p.map(FeatureStore._mp_row_level_time_travel_wrapper, _requests)
        else:
            for _r in _requests:
                _curr_return = self._row_level_time_travel_wrapper(_r)
                _return_data.append(_curr_return)

        # clean up the result columns to eliminate extras, concatenate subsets of columns
        # returned from pieces of the time travel. column subsets are returned in the 
        # user-specified order, and they get appended in the right order. this helps with
        # usability of the resulting dataframe, eliminating confusion for the caller.
        
        _which_return = 0
        for _curr_results_df in _return_data:
            # ensure each set of results is sorted exactly the same way, using the temp ID
            # row number column we added to the events dataframe at the start
            _curr_results_df.sort_values(by=[TEMP_TIME_TRAVEL_ID_NAME], ascending=True, inplace=True)
            
            if verbose:
                if _curr_results_df is None:
                    print(f'Got back None from row level time travel for {_which_return} query')
                else:
                    print(f'\nGot back these results for query {_which_return}')
                    print(_curr_results_df.head())

            # drop the event time and ID columns, and the tmp ID, just focus on the features
            _curr_results_df = _curr_results_df.drop([_curr_results_df.columns[0],
                                                      _curr_results_df.columns[1],
                                                      TEMP_TIME_TRAVEL_ID_NAME], axis=1)
            if _cumulative_features_df is None:
                _cumulative_features_df = _curr_results_df.copy()
            else:                
                # now append the remaining current result columns at the end of the cumulative columns
                _cumulative_features_df = pd.concat([_cumulative_features_df, 
                                                     _curr_results_df], axis=1)
                if verbose:
                    print(f'Cumulative results:')
                    print(_cumulative_features_df.head())

            _which_return += 1


        # Clean up the temporary Athena table
        _drop_df = self._drop_temp_table(_events_table_name, verbose=verbose)
        if verbose:
            if _drop_df is None:
                print('drop_temp returned None')
            else:
                print(f'drop_temp returned {_drop_df.head()}')

        # Clean up the temporary CSV from S3
        # TODO: Athena seems to leave a .txt file hanging around as well, should delete that
        if verbose:
            print(f'deleting the temp csv from s3: {_obj_name}')
        _s3 = boto3.resource('s3')
        _obj = _s3.Object(self._default_bucket, _obj_name)
        _obj.delete()
        
        # drop the tmp ID
        events_df = events_df.drop(TEMP_TIME_TRAVEL_ID_NAME, axis=1)

        # Ensure that the final dataframe columns and column order matches the request.
        _final_df = pd.concat([events_df, _cumulative_features_df], axis=1)
        _final_columns.remove(TEMP_TIME_TRAVEL_ID_NAME)
        
        if verbose:
            print(f'  final df columns before reorder: {_final_df.columns.tolist()}')
            print(f'  final column names in order: {_final_columns}')
        return _final_df[_final_columns]

    def get_historical_offline_feature_values(self, fg_name: str, 
                                              record_ids: List[Union[str,int]]=None, 
                                              feature_names: List[str]=None, 
                                              s3_uri: str=None, 
                                              verbose: bool=False) -> pd.DataFrame:
        """Returns a dataframe with all historical rows from a given feature group offline store.
        
        For large offline stores, specify a list of record identifiers, or 
        consider a direct Athena SQL query with custom filters instead.
        
        Args:
            fg_name (str): Name of the feature group from which to retrieve the records.
            record_ids (List[Union[str,int]]): List of record identifiers to retrieve, or all if not specified.
            feature_names (List[str]): List of named features to retrieve.
            s3_uri (str): Location in s3 where temporary files will be stored.
            verbose (bool): Whether to print out details of the execution along the way.

        Returns:
            pd.DataFrame: Dataframe containing the exact features requested, or the entire set of features if a feature list was not specified
        """        
        
        _table, _database, _tmp_uri = self._get_offline_details(fg_name, s3_uri)

        # construct an Athena query

        id_feature_name = self.describe_feature_group(fg_name)['RecordIdentifierFeatureName']
        time_feature_name = self.describe_feature_group(fg_name)['EventTimeFeatureName']

        if feature_names is None:
            feature_name_string = '*'
        else:
            feature_name_string = ','.join(feature_names)

        if record_ids is None:
            where_clause = ''
        else:
            id_list_string = self._id_list_as_string(fg_name, record_ids)
    #         id_list_string = ','.join(str(x) for x in record_ids)
            where_clause = f' WHERE {id_feature_name} IN ({id_list_string})'

        _query_string = f'SELECT {feature_name_string} FROM "' +_table+ f'" {where_clause}'

        return self._run_query(_query_string, _tmp_uri, _database, verbose=verbose)

    def get_latest_offline_feature_values(self, fg_name: str, 
                                          record_ids: List[Union[str,int]]=None, 
                                          feature_names: List[str]=None, 
                                          s3_uri: str=None, 
                                          verbose: bool=False) -> pd.DataFrame:
        """Returns a dataframe with the most recent rows from a given feature group offline store.
        
        Often, an offline store will contain multiple records for any given record identifier.
        The offline store is append-only. This method will return only the most recent record
        for any given identifier, based on the record event time.
        
        For large offline stores, specify a list of record identifiers, or 
        consider a direct Athena SQL query with custom filters instead.
        
        Args:
            fg_name (str): Name of the feature group from which to retrieve the records.
            record_ids (List[Union[str,int]]): List of record identifiers to retrieve, or all if not specified.
            feature_names (List[str]): List of named features to retrieve.
            s3_uri (str): Location in s3 where temporary files will be stored.
            verbose (bool): Whether to print out details of the execution along the way.

        Returns:
            pd.DataFrame: Dataframe containing the latest set of records, with the exact features requested, 
              or the entire set of features if a feature list was not specified
        """        

        _table, _database, _tmp_uri = self._get_offline_details(fg_name, s3_uri)
        _fg_desc = self.describe_feature_group(fg_name)

        # construct an Athena query

        id_feature_name = _fg_desc['RecordIdentifierFeatureName']
        time_feature_name = _fg_desc['EventTimeFeatureName']

        if feature_names is None:
            feature_name_string = '*'
        else:
            feature_name_string = ','.join(feature_names)

        if record_ids is None:
            where_clause = ''
        else:
            id_list_string = self._id_list_as_string(fg_name, record_ids)
            where_clause = f' WHERE {id_feature_name} IN ({id_list_string})'

        _subquery = f'SELECT *, ROW_NUMBER() OVER (PARTITION BY {id_feature_name} ' + \
                    f'ORDER BY {time_feature_name} DESC, api_invocation_Time DESC, write_time DESC) AS row_num ' + \
                    f'FROM "' +_table+ f'" {where_clause}'
        _query_string = f'SELECT {feature_name_string} FROM ({_subquery}) WHERE row_num = 1 AND NOT is_deleted'

        return self._run_query(_query_string, _tmp_uri, _database, verbose)

    def get_latest_offline_feature_values_as_of(self, fg_name: str, 
                                                as_of: str, 
                                                record_ids: List[Union[str,int]]=None, 
                                                feature_names: List[str]=None, 
                                                s3_uri: str=None, 
                                                verbose: bool=False):
        """Performs an as-of time travel, returning records as of a specific event time timestamp.
        
        Note that this is not row-level time travel. The timestamp is used uniformly across all returned
        records. The feature names specified are all associated with the one specific feature group targeted.
        Depending on the requested as-of timestamp and the event times of the feature values found, the 
        query will return null values for feature values that would not yet have been available. For feature values
        that are available, the query will return the most recent value, without allowing any values that are
        newer than the requested as-of event time timestamp.
        
        Args:
            fg_name (str): Name of the feature group from which to retrieve the records.
            as_of (str): Event time for which all returned records should be older than.
            record_ids (List[Union[str,int]]): List of record identifiers to retrieve, or all if not specified.
            feature_names (List[str]): List of named features to retrieve.
            s3_uri (str): Location in s3 where temporary files will be stored.
            verbose (bool): Whether to print out details of the execution along the way.

        Returns:
            pd.DataFrame: Dataframe containing the records that existed as of the specified event time, 
              with the exact features requested, or the entire set of features if a feature list was not specified
        """
        _fg_desc = self.describe_feature_group(fg_name)

        _table = _fg_desc['OfflineStoreConfig']['DataCatalogConfig']['TableName']
        _database = _fg_desc['OfflineStoreConfig']['DataCatalogConfig']['Database']
        if s3_uri is None:
            s3_uri = f's3://{self._default_bucket}/offline-store'

        _tmp_uri = f'{s3_uri}/query_results/'

        # construct an Athena query

        id_feature_name = _fg_desc['RecordIdentifierFeatureName']
        time_feature_name = _fg_desc['EventTimeFeatureName']

        if feature_names is None:
            feature_name_string = '*'
        else:
            feature_name_string = ','.join(feature_names)

        if record_ids is None:
            where_clause = f" WHERE {time_feature_name} <= '{as_of.upper()}'"
        else:
            id_list_string = _id_list_as_string(fg_name, record_ids)
            where_clause = f" WHERE {id_feature_name} IN ({id_list_string}) AND {time_feature_name} <= '{as_of.upper()}'"

        ## TODO: resolve issue with Presto and iso 8601 timestamps. partial solution provided by from_iso8601_timestamp
        ##  https://aws.amazon.com/premiumsupport/knowledge-center/query-table-athena-timestamp-empty/
        _subquery = f'SELECT *, ROW_NUMBER() OVER (PARTITION BY {id_feature_name} ' + \
                    f'ORDER BY {time_feature_name} DESC, api_invocation_Time DESC, write_time DESC) AS row_num ' + \
                    f'FROM "' +_table+ f'" {where_clause}'
    ##                f"WHERE {time_feature_name} <= TIMESTAMP '{as_of.upper()}'"
        _query_string = f'SELECT {feature_name_string} FROM ({_subquery}) WHERE row_num = 1 AND NOT is_deleted'

        return self._run_query(_query_string, _tmp_uri, _database, verbose)

    
    def _update_flow(self, s3_file_to_ingest, bucket, flow_location):
        flow_json = {'metadata': {'version': 1},
                     'nodes': [
                         {'node_id': '7f6515d7-7ea4-48ba-98ce-5b32c73306e6',
                               'type': 'SOURCE',
                               'operator': 'sagemaker.s3_source_0.1',
                               'parameters': {'dataset_definition': {'__typename': 'S3CreateDatasetDefinitionOutput',
                                 'datasetSourceType': 'S3',
                                 'name': s3_file_to_ingest.split('/')[-1],
                                 'description': None,
                                 's3ExecutionContext': {'__typename': 'S3ExecutionContext',
                                  's3Uri': s3_file_to_ingest,
                                  's3ContentType': 'csv',
                                  's3HasHeader': True}}},
                               'inputs': [],
                               'outputs': [{'name': 'default'}]
                         },
                         {'node_id': 'e6a71ea2-dd1e-477f-964a-03238f974a35',
                               'type': 'TRANSFORM',
                               'operator': 'sagemaker.spark.infer_and_cast_type_0.1',
                               'parameters': {},
                               'trained_parameters': {},
                               'inputs': [{'name': 'default',
                                 'node_id': '7f6515d7-7ea4-48ba-98ce-5b32c73306e6',
                                 'output_name': 'default'}],
                               'outputs': [{'name': 'default'}]
                         }]
                    }

        with open('tmp.flow', 'w') as f:
            json.dump(flow_json, f)

        s3_client = boto3.client('s3')
        s3_client.upload_file('tmp.flow', bucket, flow_location)
        os.remove('tmp.flow')
        return flow_json

    def _create_flow_notebook_processing_input(self, base_dir, flow_s3_uri):
        return {
            "InputName": "flow",
            "S3Input": {
                "LocalPath": f"{base_dir}/flow",
                "S3Uri": flow_s3_uri,
                "S3DataType": "S3Prefix",
                "S3InputMode": "File",
            },
        }

    def _create_s3_processing_input(self, base_dir, name, dataset_definition):
        return {
            "InputName": name,
            "S3Input": {
                "LocalPath": f"{base_dir}/{name}",
                "S3Uri": dataset_definition["s3ExecutionContext"]["s3Uri"],
                "S3DataType": "S3Prefix",
                "S3InputMode": "File",
            },
        }

    def _create_processing_inputs(self, processing_dir, flow, flow_uri):
        """Helper function for creating processing inputs
        :param flow: loaded data wrangler flow notebook
        :param flow_uri: S3 URI of the data wrangler flow notebook
        """
        processing_inputs = []
        flow_processing_input = _create_flow_notebook_processing_input(processing_dir, flow_uri)
        processing_inputs.append(flow_processing_input)

        for node in flow["nodes"]:
            if "dataset_definition" in node["parameters"]:
                data_def = node["parameters"]["dataset_definition"]
                name = data_def["name"]
                source_type = data_def["datasetSourceType"]

                if source_type == "S3":
                    s3_processing_input = _create_s3_processing_input(
                        processing_dir, name, data_def)
                    processing_inputs.append(s3_processing_input)
                else:
                    raise ValueError(f"{source_type} is not supported for Data Wrangler Processing.")
        return processing_inputs

    def ingest_with_dw(self, new_file_to_ingest, feature_group_name, 
                       instance_count=1, instance_type='ml.m5.4xlarge', prefix='data_wrangler_flows',
                       bucket=None, iam_role=None, processing_job_name=None):
        if bucket is None:
            bucket = self._default_bucket
        if iam_role is None:
            iam_role = self._role
        if processing_job_name is None:
            curr_timestamp = int(datetime.now().timestamp())    
            processing_job_name = f'dw-ingest-{curr_timestamp}'

        if region == 'us-east-1':
            container_uri = "663277389841.dkr.ecr.us-east-1.amazonaws.com/sagemaker-data-wrangler-container:1.3.1"
        elif region == 'us-east-2':
            container_uri = "415577184552.dkr.ecr.us-east-2.amazonaws.com/sagemaker-data-wrangler-container:1.3.0"
        processing_dir = "/opt/ml/processing"

        flow_id = f"{time.strftime('%d-%H-%M-%S', time.gmtime())}-{str(uuid.uuid4())[:8]}"
        flow_name = f'flow-{flow_id}'
        flow_location = f'{prefix}/{flow_name}.flow'
        flow_uri = f's3://{bucket}/{flow_location}'

        flow = self._update_flow(new_file_to_ingest, bucket, flow_location)
        processingResources = {
                'ClusterConfig': {
                    'InstanceCount': instance_count,
                    'InstanceType': instance_type,
                    'VolumeSizeInGB': 30
                }
            }

        appSpecification = {'ImageUri': container_uri}

        self._sm_client.create_processing_job(
                ProcessingInputs=_create_processing_inputs(processing_dir, flow, flow_uri),
                ProcessingOutputConfig={
                    'Outputs': [
                        {
                            'OutputName': 'e6a71ea2-dd1e-477f-964a-03238f974a35.default',
                            'FeatureStoreOutput': {
                                'FeatureGroupName': feature_group_name
                            },
                            'AppManaged': True
                        }
                    ],
                },
                ProcessingJobName=processing_job_name,
                ProcessingResources=processingResources,
                AppSpecification=appSpecification,
                RoleArn=iam_role
            )
        return processing_job_name
