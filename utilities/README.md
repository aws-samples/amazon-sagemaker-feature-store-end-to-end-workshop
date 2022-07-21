# Amazon SageMaker Feature Store Helper

This project is for helper utilities for using [Amazon SageMaker Feature Store](https://aws.amazon.com/sagemaker/feature-store/), with a goal of improving the usability of Feature Store for data scientists.

The FeatureStore helper class  offers a number of convenient methods on top of the Feature Store API to simplify interacting with both the online store and the offline store.

The offline store wrapper provides a simple-to-use row-level time travel method that cuts across multiple feature groups and ensures no feature leakage, with individual timestamps for each event for a desired training dataset. An As-of event time method is also provided, giving you the set of feature values as of a single specific point in time. The helper class also provides methods for getting record counts, sample records, min and max write times and event times.

The online store methods provide convenience on top of GetRecord and BatchGetRecord. They return values in native Python data types. The methods also permit specifying a simple list of features using a fully-qualified name (e.g., 'customers:salary').

A set of methods is also provided for creating a form of feature pipeline using SageMaker Pipelines, SageMaker Processing jobs, and Amazon EventBridge.

## Usage

```python
from utilities.feature_store_helper import FeatureStore
fs = FeatureStore()
```
## Offline store dataset retrievals

### Row-level time travel
First create an events dataframe with desired timestamps and identifiers for various feature groups.
```python
multi_id_events = [['2020-02-01T08:30:00Z', 6, 450],
                  ['2020-02-02T10:15:30Z', 5, 5000],
                  ['2020-02-03T13:20:59Z', 1, 1999],
                  ['2021-01-01T00:00:00Z', 1, 2001]]
multi_id_df = pd.DataFrame(multi_id_events, columns=['my_event_time', 'Id', 'HOUSE_ID'])
multi_id_df.head()
```
Output:
```
           my_event_time 	Id 	HOUSE_ID
0 	2020-02-01T08:30:00Z 	6 	450
1 	2020-02-02T10:15:30Z 	5 	5000
2 	2020-02-03T13:20:59Z 	1 	1999
3 	2021-01-01T00:00:00Z 	1 	2001
```
Now use `get_features` to do row-level time travel and specify the feature set to include in my resulting training dataset.
```python
fs.get_features(multi_id_df, 'my_event_time', 
                   features=['customers:ZipCode', 
                             'payments:avg_amount', 
                             'payments:avg_days_late',
                             'housing:SQUARE_FEET',
                             'housing:PRICE'],
               parallel=True)
```
Output:
```
           my_event_time 	Id 	HOUSE_ID 	zipcode 	avg_amount 	avg_days_late 	square_feet 	price
0 	2020-02-01T08:30:00Z 	6 	450 	      11111 	     1100      	        5 	       2620 	483576
1 	2020-02-02T10:15:30Z 	5 	5000 	      22222 	     1600 	            6          3262 	272467
2 	2020-02-03T13:20:59Z 	1 	1999 	      33333 	     1500 	            5 	       4128 	713124
3 	2021-01-01T00:00:00Z 	1 	2001 	      33333 	     1100 	            5          3126 	473071
```

### Time travel as of a single timestamp

```python
full_df = fs.get_latest_offline_feature_values_as_of('customers', 
                            '2020-02-03T08:30:00Z', 
                            feature_names=['ZipCode','Churn'])
full_df
```
Output:
```
 	ZipCode 	Churn
0 	33333 	    0
1 	33333 	    1
2 	33333 	    0
3 	33333 	    0
4 	33333 	    1
5 	33333 	    1
```

### Other utility functions 

#### Sample records

This method is handy when exploring feature groups, let's you browse some sample records without having to write any queries.

```python
fs.sample('housing', 5)
```
Output:
```
    price 	year_built 	square_feet 	num_bedrooms 	num_bathrooms 	lot_acres 	garage_spaces 	location 	house_id 	event_time 	write_time 	api_invocation_time 	is_deleted
0 	379392 	1995 	2286 	6 	3 	0.97 	3 	Phoenix_AZ 	27 	2021-01-01T00:00:00.000Z 	2021-10-06 03:49:23.586 	2021-10-06 03:44:26.000 	False
1 	586072 	2019 	3010 	6 	2 	1.20 	2 	Phoenix_AZ 	6295 	2021-01-01T00:00:00.000Z 	2021-10-06 03:49:23.586 	2021-10-06 03:44:26.000 	False
2 	296210 	1944 	3771 	4 	3 	0.85 	1 	Houston_TX 	7559 	2021-01-01T00:00:00.000Z 	2021-10-06 03:49:23.586 	2021-10-06 03:44:27.000 	False
3 	167758 	1949 	2776 	4 	1 	1.01 	2 	LosAngeles_CA 	8813 	2021-01-01T00:00:00.000Z 	2021-10-06 03:49:23.586 	2021-10-06 03:44:27.000 	False
4 	664146 	2019 	3960 	3 	1 	0.64 	2 	Philadelphia_PA 	8813 	2021-01-01T00:00:00.000Z 	2021-10-06 03:49:23.586 	2021-10-06 03:44:27.000 	False
```

### Get min and max event times and write times 

This method is helpful when trying to understand the scope of historical feature value coverage. The `write_time` minimum and 
maximum timestamps tell you how long feature ingestion has been happening to this feature group. Likewise, the `event_time` 
minimum and maximum gives you an indication of how event time is being used.

```python
fs.get_minmax_timestamps('housing')
```
Output:
```
    min_write_time 	max_write_time 	min_event_time 	max_event_time
0 	2021-10-06 03:32:59.329 	2021-10-11 17:52:06.075 	2020-01-01T00:00:00.000Z 	2021-06-01T00:00:00.000Z
```
### Download a sample offline store file

```python
tmp_filename = fs.download_sample_offline_file('customers')
p_df = pd.read_parquet(tmp_filename)
p_df
```
Output:
```
 	Id 	UpdateTime 	ZipCode 	Churn 	write_time 	api_invocation_time 	is_deleted
0 	1 	2020-02-01T00:00:00Z 	11111 	1 	2021-10-11 16:20:38.040000+00:00 	2021-10-11 16:15:39+00:00 	False
```

## Online store feature retrieval

### Get latest features for a feature set 

This method provides a convenience wrapper around the BatchGetRecord API to simplify access to a feature set that
is a list of features from N different feature groups.

```python
fs_dict = fs.get_latest_featureset_values({'Id': 2}, ['customers:ZipCode'])
print(f'Feature set as dictionary: {fs_dict}')
print(f'Feature set as vector: {list(fs_dict.values())}')
```
Output:
```
Feature set as dictionary: {'ZipCode': 11111}
Feature set as vector: [11111]
```

### Get latest feature values for a list of record identifiers and list of features
```python
fs.get_latest_feature_values('customers', [4], features=['ZipCode'])
```
Output:
```
[{'ZipCode': 11111}]
```

### Get all features for a specific set of identifiers
```python
fs.get_latest_feature_values('customers', [4])
```
Output:
```
[{'Id': 4, 'UpdateTime': '2020-02-01T00:00:00Z', 'ZipCode': 11111, 'Churn': 0}]
```

## Feature ingestion

### Ingest entire Pandas DataFrame

```python
fs.ingest_from_df('customers', df)
```

## Create, describe, delete, and list feature groups

### Create feature group 

```python
df = pd.read_csv('./customers.csv')
ORIGINAL_RECORD_COUNT = df.shape[0]
df.head()

tags = {'Environment': 'DEV', 
        'CostCenter': 'C20', 
        'Maintainer': 'John Smith', 
        'DocURL': 'https://www.google.com'}
fs.create_fg_from_df('customers', df, 'this is my new fg', tags=tags, id_name='Id')
```
Output:
```
Waiting for Feature Group creation...
Waiting for Feature Group creation...
Waiting for Feature Group creation...
Waiting for Feature Group creation...
FeatureGroup tmp-fg successfully created.
```

### List feature groups 
```python
fs.list_feature_groups(name_contains='housing')
```
Output:
```
['housing']
```

### Describe feature group 

```python
fs.describe_feature_group('tmp-fg')
```
Output:
```
{'FeatureGroupArn': 'arn:aws:sagemaker:us-east-1:355151823911:feature-group/tmp-fg',
 'FeatureGroupName': 'tmp-fg',
 'RecordIdentifierFeatureName': 'Id',
 'EventTimeFeatureName': 'UpdateTime',
 'FeatureDefinitions': [{'FeatureName': 'Id', 'FeatureType': 'Integral'},
  {'FeatureName': 'UpdateTime', 'FeatureType': 'String'},
  {'FeatureName': 'ZipCode', 'FeatureType': 'Integral'},
  {'FeatureName': 'Churn', 'FeatureType': 'Integral'}],
 'CreationTime': datetime.datetime(2021, 10, 20, 15, 21, 43, 743000, tzinfo=tzlocal()),
 'OnlineStoreConfig': {'EnableOnlineStore': True},
 'OfflineStoreConfig': {'S3StorageConfig': {'S3Uri': 's3://sagemaker-us-east-1-355151823911/offline-store',
   'ResolvedOutputS3Uri': 's3://sagemaker-us-east-1-355151823911/offline-store/355151823911/sagemaker/us-east-1/offline-store/tmp-fg-1634743303/data'},
  'DisableGlueTableCreation': False,
  'DataCatalogConfig': {'TableName': 'tmp-fg-1634743303',
   'Catalog': 'AwsDataCatalog',
   'Database': 'sagemaker_featurestore'}},
 'RoleArn': 'arn:aws:iam::355151823911:role/MySagemakerRole',
 'FeatureGroupStatus': 'Created',
 'OfflineStoreStatus': {'Status': 'Active'},
 'Description': 'this is my new fg',
 'ResponseMetadata': {'RequestId': '8b55b7ca-1dce-49fa-99d4-65a9041f2e59',
  'HTTPStatusCode': 200,
  'HTTPHeaders': {'x-amzn-requestid': '8b55b7ca-1dce-49fa-99d4-65a9041f2e59',
   'content-type': 'application/x-amz-json-1.1',
   'content-length': '1493',
   'date': 'Wed, 20 Oct 2021 15:27:30 GMT'},
  'RetryAttempts': 0}}
  ```

### Get tags

```python
fs.get_tags('customers')
```
Output:
```
{'Maintainer': 'John Smith',
 'Environment': 'DEV',
 'DocURL': 'https://www.google.com',
 'CostCenter': 'C20'}
 ```

### Delete feature group 

```python
fs.delete_feature_group('tmp-fg')
```

## Feature pipelines 

### Schedule a feature pipeline to directly ingested featurized records from S3 

```python
import sagemaker
default_bucket = sagemaker.Session().default_bucket()
data_source = f's3://{default_bucket}/sagemaker-feature-store/hello-data/'

fs.schedule_feature_pipeline(data_source, 'customers')
```

### Schedule a feature pipeline with SQL-based feature transformations

```python
%%writefile ./customer_sql.py

def transform_query(fg_name: str) -> str:
    return f'''
        SELECT *, 
            IF (Id > 3, 0, 1) as Persona, 
            (zipcode * 2) + RAND() as NewFeature1 
        FROM {fg_name} 
        '''
```
Output:
```
Writing ./customer_sql.py
```
```python
schedule_feature_pipeline(data_source, 'customers', 
                                './customer_sql.py', 
                                script_type='pyspark_sql', schedule='rate(1 day)',
                                instance_type='ml.m5.4xlarge', instance_count=1)
```

### Schedule a feature pipeline with Pandas-based feature transformation 

```python
%%writefile ./customer_python.py

import pandas as pd
import numpy as np

def choose_persona(row):
    if row['Id'] > 3:
        return 0
    else:
        return 1

def apply_transforms(df: pd.DataFrame) -> pd.DataFrame:
    df['Persona'] = df.apply(lambda row : choose_persona(row), axis=1) 
    df['NewFeature1'] = df['Persona'] * np.random.rand() + df['ZipCode']
    return df
```
Output:
```
Writing ./customer_python.py
```
```python
Utils.update_feature_pipeline(data_source, 'customers', 
                                './customer_python.py', instance_count=1)
```

### Remove a feature pipeline 

```python
fs.remove_feature_pipeline('customers')
```
