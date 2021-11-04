import uuid
import time
from datetime import datetime
import json
from time import sleep
import pandas as pd
import os
import boto3
#import Utils
import sagemaker
from sagemaker.session import Session
from sagemaker import get_execution_role
from sagemaker.feature_store.feature_group import FeatureGroup




region = os.environ.get('AWS_REGION')
print(region)
# def get_session(region):
#     """Gets the sagemaker session based on the region.
#     Args:
#         region: the aws region to start the session
#         default_bucket: the bucket to use for storing the artifacts
#     Returns:
#         `sagemaker.session.Session instance
#     """

#     boto_session = boto3.Session(region_name=region)

#     sagemaker_client = boto_session.client("sagemaker")
#     runtime_client = boto_session.client("sagemaker-runtime")
#     return sagemaker.session.Session(
#         boto_session=boto_session,
#         sagemaker_client=sagemaker_client,
#         sagemaker_runtime_client=runtime_client,
#     )

# sagemaker_session = get_session(region)
# role = sagemaker_session.get_caller_identity_arn()
# print(role)

boto_session = boto3.Session()
region = os.environ.get('AWS_REGION')
print(region)

account_id = boto3.client("sts").get_caller_identity()["Account"]

s3_client = boto3.client('s3', region_name=region)

sagemaker_client = boto_session.client(service_name='sagemaker', region_name=region)
featurestore_runtime = boto_session.client(service_name='sagemaker-featurestore-runtime', region_name=region)

feature_store_session = Session(
    boto_session=boto_session,
    sagemaker_client=sagemaker_client,
    sagemaker_featurestore_runtime_client=featurestore_runtime
)

sm_sess = sagemaker.Session()
default_bucket = sm_sess.default_bucket() 

# def get_session(region):
#     """Gets the sagemaker session based on the region.
#     Args:
#         region: the aws region to start the session
#         default_bucket: the bucket to use for storing the artifacts
#     Returns:
#         `sagemaker.session.Session instance
#     """

#     boto_session = boto3.Session(region_name=region)

#     sagemaker_client = boto_session.client("sagemaker")
#     runtime_client = boto_session.client("sagemaker-runtime")
#     return sagemaker.session.Session(
#         boto_session=boto_session,
#         sagemaker_client=sagemaker_client,
#         sagemaker_runtime_client=runtime_client,
#     )

# sagemaker_session = get_session(region)
# role = sagemaker_session.get_caller_identity_arn()
# print(role)

def describe_feature_group(fg_name):
    _tmp_client = boto_session.client(service_name='sagemaker', region_name=region)
    return _tmp_client.describe_feature_group(FeatureGroupName=fg_name)

def get_offline_store_url(fg_name):
    fg_s3_uri = ''
    has_offline_store = True
    offline_store_config = {}
    try:
        offline_store_config = describe_feature_group(fg_name)['OfflineStoreConfig']
    except:
        has_offline_store = False
        return fg_s3_uri
    
    table_name, _, _ = _get_offline_details(fg_name)

    base_s3_uri = offline_store_config['S3StorageConfig']['ResolvedOutputS3Uri']
    base_offline_prefix = '/'.join(base_s3_uri.split('/')[3:])
    s3_bucket_name = base_s3_uri.split('/')[2]
    
    return f'https://s3.console.aws.amazon.com/s3/buckets/{s3_bucket_name}?region={region}&prefix={base_offline_prefix}/'

def _get_core_features(fg_name):
    _fg_desc = describe_feature_group(fg_name)
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


def _feature_list_to_df(features):
    _groups_and_features = []
    _fg_number = 0
    for _f in features:
        _parts = _f.split(':')
        _fg = _parts[0]
        _fn = _parts[1]
        _feature_number = 0
        
        if _fn == '*':
            _all_core_features = _get_core_features(_fg)
            for _core_feature in _all_core_features:
                _groups_and_features.append([_fg, _core_feature, _fg_number, _feature_number])
                _feature_number += 1
        else:
            _groups_and_features.append([_fg, _fn, _fg_number, _feature_number])
            
        _fg_number += 1
        
    return pd.DataFrame(_groups_and_features, columns=['fg_name', 'feature_name', 'fg_number', 'feature_number'])


def list_feature_groups(name_contains=None):
    matching_fgs = []
    next_token = None
    sub_name = None
    
    if name_contains is None:
        response = sagemaker_client.list_feature_groups()
    else:
        sub_name = name_contains
        response = sagemaker_client.list_feature_groups(NameContains=sub_name)

    if 'NextToken' in list(response.keys()):
        next_token = response['NextToken']
    else:
        fg_summaries = response['FeatureGroupSummaries']
        for summary in fg_summaries:
            matching_fgs.append(summary)

    while next_token is not None:
        if name_contains is None:
            response = sagemaker_client.list_feature_groups(NextToken=next_token)
        else:
            response = sagemaker_client.list_feature_groups(NameContains=sub_name, NextToken=next_token)
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

## TODO: add caching of feature group descriptions
def describe_feature_group(fg_name):
    _tmp_client = boto_session.client(service_name='sagemaker', region_name=region)
    return _tmp_client.describe_feature_group(FeatureGroupName=fg_name)

def get_offline_store_url(fg_name):
    fg_s3_uri = ''
    has_offline_store = True
    offline_store_config = {}
    try:
        offline_store_config = describe_feature_group(fg_name)['OfflineStoreConfig']
    except:
        has_offline_store = False
        return fg_s3_uri
    
    table_name, _, _ = _get_offline_details(fg_name)

    base_s3_uri = offline_store_config['S3StorageConfig']['ResolvedOutputS3Uri']
    base_offline_prefix = '/'.join(base_s3_uri.split('/')[3:])
    s3_bucket_name = base_s3_uri.split('/')[2]
    
    return f'https://s3.console.aws.amazon.com/s3/buckets/{s3_bucket_name}?region={region}&prefix={base_offline_prefix}/'

def get_glue_table_url(fg_name):
    _data_catalog_config = describe_feature_group(fg_name)['OfflineStoreConfig']['DataCatalogConfig']
    _table = _data_catalog_config['TableName']
    _database = _data_catalog_config['Database']

    return f'https://console.aws.amazon.com/glue/home?region={region}#table:catalog={account_id};name={_table};namespace={_database}'

def download_sample_offline_file(fg_name):
    ## TODO: change the implementation to try to grab the very most recent parquet file.
    ##.      otherwise, can be confusing in the hello world notebook when looking at most recent data.
    
    fg_s3_uri = ''
    has_offline_store = True
    offline_store_config = {}
    try:
        offline_store_config = describe_feature_group(fg_name)['OfflineStoreConfig']
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
    s3_client.download_file(default_bucket, key_to_download, filename)
    return filename

def delete_feature_group(fg_name, delete_s3=True):
    ## TODO: wait until it is fully deleted b4 returning
    ## TODO: properly handle situation when fg is already gone
    ## TODO: Delete Glue table if one was created automatically

    has_offline_store = True
    try:
        describe_feature_group(fg_name)['OfflineStoreConfig']
    except:
        has_offline_store = False
        pass

    if has_offline_store:
        offline_store_config = describe_feature_group(fg_name)['OfflineStoreConfig']
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
        s3_uri = describe_feature_group(fg_name)['OfflineStoreConfig']['S3StorageConfig']['S3Uri']
        base_offline_prefix = '/'.join(s3_uri.split('/')[3:])
        offline_prefix = f'{base_offline_prefix}/{account_id}/sagemaker/{region}/offline-store/{fg_name}/'
        s3_bucket_name = s3_uri.split('/')[2]
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(s3_bucket_name)
        coll = bucket.objects.filter(Prefix=offline_prefix)
        print(f'Deleting all s3 objects in prefix: {offline_prefix} in bucket {s3_bucket_name}')
        resp = coll.delete()
    
    resp = None
    try:
        resp = sagemaker_client.delete_feature_group(FeatureGroupName=fg_name)
    except:
        pass
    
    _wait_for_feature_group_deletion_complete(fg_name)
    return 

def ingest_from_df(fg_name, df, max_processes=4, max_workers=4):
    fg = FeatureGroup(name=fg_name, sagemaker_session=feature_store_session)
    fg.ingest(data_frame=df, max_processes=max_processes, max_workers=max_workers, wait=True)
    
def _record_to_dict(rec, feature_types):
    tmp_dict = {}
    for f in rec:
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

def delete_record(fg_name, record_id, event_time):
    results = []
    
    resp = featurestore_runtime.delete_record(FeatureGroupName=fg_name, 
                                              RecordIdentifierValueAsString=str(record_id),
                                              EventTime=event_time)
    return

def get_latest_feature_values(fg_name, id_value_list, features=None):
    feature_defs = describe_feature_group(fg_name)['FeatureDefinitions']
    feature_types = {}
    for fd in feature_defs:
        feature_types[fd['FeatureName']] = fd['FeatureType']
        
    results = []
    resp = None
    
    for curr_id in id_value_list:
        record_identifier_value = str(curr_id)
        if features is None:
            resp = featurestore_runtime.get_record(FeatureGroupName=fg_name, 
                                                   RecordIdentifierValueAsString=record_identifier_value)
        else:
            resp = featurestore_runtime.get_record(FeatureGroupName=fg_name, 
                                                   RecordIdentifierValueAsString=record_identifier_value,
                                                   FeatureNames=features)
        try:
            curr_record = _record_to_dict(resp['Record'], feature_types)
            results.append(curr_record)
        except:
            pass

    return results

def get_latest_featureset_values(id_dict, features, verbose=False):

    ## TODO: BatchGetRecord does not honor the order of the features requested, so this function
    ##       should enforce reordering of results to match the requested order. This is important when mapping
    ##       to a feature vector to feed a model prediction.
    
    _feature_types = {}
    _resp = None
    _features_df = _feature_list_to_df(features)
    if verbose:
        print(_features_df.head())
    
    _gb = _features_df.groupby('fg_name')

    _fg_requests = []
    
    for _g in _gb:
        _curr_features = []
        _fg_name = _g[0]
        for _f in _g[1].iterrows():
            _curr_features.append(_f[1]['feature_name'])
        _feature_defs = describe_feature_group(_fg_name)['FeatureDefinitions']
        for _fd in _feature_defs:
            _feature_types[_fd['FeatureName']] = _fd['FeatureType']
            
        _id_feature_name = describe_feature_group(_fg_name)['RecordIdentifierFeatureName']
        _id_value = id_dict[_id_feature_name]
        
        if verbose:
            print(f'fg name: {_fg_name}, id: {_id_feature_name}, id val: {_id_value}, features: {_curr_features}')
            
        _fg_requests.append({'FeatureGroupName': _fg_name,
                             'RecordIdentifiersValueAsString': [str(_id_value)],
                             'FeatureNames': _curr_features})
            
    if verbose:
        print(_fg_requests)
        print(_feature_types)
    
    _resp = featurestore_runtime.batch_get_record(Identifiers=_fg_requests)
    
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
        _curr_record_dict = _record_to_dict(_curr_record, _feature_types)
        
        if verbose:
            print(_results_dict)
            print(_curr_record_dict)
            
        _results_dict = dict(_results_dict, **_curr_record_dict)
      
    return _results_dict

def _run_query(query_string, tmp_uri, database, verbose=True):
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
        sleep(2)
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
            s3_client.download_file(results_bucket, results_file_prefix, filename)
            df = pd.read_csv(filename)
            if verbose:
                print(f'Query results shape: {df.shape}')
            os.remove(filename)

            s3_client.delete_object(Bucket=results_bucket, Key=results_file_prefix)
            s3_client.delete_object(Bucket=results_bucket, Key=results_file_prefix + '.metadata')
            return df
        except:
            df = None
            pass
        
        
def get_latest_featureset_values_e(id_dict, features, verbose=False):

    ## TODO: BatchGetRecord does not honor the order of the features requested, so this function
    ##       should enforce reordering of results to match the requested order. This is important when mapping
    ##       to a feature vector to feed a model prediction.
    
    _feature_types = {}
    _resp = None
    _features_df = _feature_list_to_df(features)
    if verbose:
        print(_features_df.head())
    
    _gb = _features_df.groupby('fg_name')

    _fg_requests = []
    
    for _g in _gb:
        _curr_features = []
        _fg_name = _g[0]
        for _f in _g[1].iterrows():
            _curr_features.append(_f[1]['feature_name'])
        _feature_defs = describe_feature_group(_fg_name)['FeatureDefinitions']
        for _fd in _feature_defs:
            _feature_types[_fd['FeatureName']] = _fd['FeatureType']
            
        _id_feature_name = describe_feature_group(_fg_name)['RecordIdentifierFeatureName']
        _id_value = id_dict[_id_feature_name]
        
        if verbose:
            print(f'fg name: {_fg_name}, id: {_id_feature_name}, id val: {_id_value}, features: {_curr_features}')
            
        _fg_requests.append({'FeatureGroupName': _fg_name,
                             'RecordIdentifiersValueAsString': [str(_id_value)],
                             'FeatureNames': _curr_features})
            
    if verbose:
        print(_fg_requests)
        print(_feature_types)
    
    _resp = featurestore_runtime.batch_get_record(Identifiers=_fg_requests)
    
    if verbose:
        _num_recs = len(_resp['Records'])
        print(f'got back {_num_recs} records')

    _results_dict = []

    _all_records = _resp['Records']
    for item in _all_records:
        #print(item)
        for _req in _fg_requests:
            _fg_name = _req['FeatureGroupName']

            # enforce the original order of feature groups
            _curr_record = [_item for _item in item['Record'] if item['FeatureGroupName'] == _fg_name]
            #print(_curr_record)
            #print(type(_curr_record))
            if verbose:
                print(_curr_record)

            ## TODO: extend _record_to_dict to take a feature name list and enforce that order in the return value
            _curr_record_dict = _record_to_dict(_curr_record, _feature_types)

            if verbose:
                print(_results_dict)
                print(_curr_record_dict)

            _results_dict = dict(_results_dict, **_curr_record_dict)

    return _results_dict
        