import pandas as pd
import os
import boto3

import sagemaker
from sagemaker.session import Session
from sagemaker import get_execution_role
from sagemaker.feature_store.feature_group import FeatureGroup

boto_session = boto3.Session()
region = os.environ.get('AWS_REGION')
print(region)

sagemaker_client = boto_session.client(service_name='sagemaker', region_name=region)
featurestore_runtime = boto_session.client(service_name='sagemaker-featurestore-runtime', region_name=region)

feature_store_session = Session(
    boto_session=boto_session,
    sagemaker_client=sagemaker_client,
    sagemaker_featurestore_runtime_client=featurestore_runtime
)

FG_DESC_CACHE = {}

def describe_feature_group(fg_name):
    if fg_name in FG_DESC_CACHE:
        print('using cached desc')
        return FG_DESC_CACHE[fg_name]
    else:
        print('populating cache with latest version from boto3')
        _desc = sagemaker_client.describe_feature_group(FeatureGroupName=fg_name)
        FG_DESC_CACHE[fg_name] = _desc
        return _desc

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
    
    if verbose:
        print(f'After calling batch_get_record on {id_dict}, here are the {len(_all_records)} records returned')
        print('=== start records ===')
        print(_all_records)
        print('=== end records ===')

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
