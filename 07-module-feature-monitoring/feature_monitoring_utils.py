# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from sagemaker.feature_store.feature_group import FeatureGroup
from sagemaker import get_execution_role
import sagemaker
import logging
import boto3
import pandas as pd
import numpy as np
import time
import re
import os
import sys
from pyathena import connect
import pandas as pd
import timeit
import json
from IPython.display import display, Markdown
from time import gmtime, strftime, sleep
from datetime import datetime, timezone
import seaborn as sns
#import matplotlib as plt
import matplotlib.pyplot as plt
import json
from time import gmtime, strftime
from sklearn import preprocessing
sys.path.append('..')
from utilities import Utils

#Initialise AWS Clients
role = get_execution_role()
sagemaker_session = sagemaker.Session()
region = sagemaker_session.boto_region_name
featurestore_runtime_client = sagemaker_session.boto_session.client('sagemaker-featurestore-runtime', region_name=region)
default_bucket = sagemaker_session.default_bucket()
prefix = 'sagemaker-feature-store'
s3_uri = f's3://{default_bucket}/{prefix}'
boto_session = boto3.Session(region_name=region)
sagemaker_client = boto_session.client(service_name='sagemaker', region_name=region)

featurestore_runtime = boto_session.client(service_name='sagemaker-featurestore-runtime', region_name=region)

feature_store_session = sagemaker.Session(
    boto_session=boto_session, 
    sagemaker_client=sagemaker_client, 
    sagemaker_featurestore_runtime_client=featurestore_runtime_client
)

s3 = boto3.resource('s3')
s3_client = boto3.client('s3', region_name=region)
account_id = boto3.client("sts").get_caller_identity()["Account"]

glue_crawler_suffix = '-crawler'
databrew_dataset_suffix = '-dataset'
databrew_profilejob_suffix = '-profile-job'
databrew_reports_suffix = '-reports'
databrew_csv_suffix = '-csv'
databrew_ctas_suffix = '-Interim'
ctas_table_suffix = '-ctas-temp'
fg_profileurl_tag = 'ProfileURL'
fg_profiles3link_tag = 'ProfileS3Link'

log = logging.getLogger(__name__)


# Utility function to check if a Athena Table exist
def checkTableExists(tablename):
    athena_result_bucket = f'{s3_uri}/{account_id}/sagemaker/{region}'
    dbcur = connect(s3_staging_dir=athena_result_bucket,
                   region_name=region).cursor()
        
    dbcur.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{0}'
        """.format(tablename.replace('\'', '\'\'')))
    if dbcur.fetchone()[0] == 1:
        dbcur.close()
        return True

    dbcur.close()
    return False


# Utility function to create a filtered (no duplicate, no marked as deleted) table of feature group records 
def create_fg_snapshot_ctas(fg_name, verbose):
    
    fg = FeatureGroup(name=fg_name, sagemaker_session=feature_store_session)
    s3_uri = fg.describe()['OfflineStoreConfig']['S3StorageConfig']['S3Uri']
    database_name = fg.describe()['OfflineStoreConfig']['DataCatalogConfig']['Database']
    table_name_source = fg.describe()['OfflineStoreConfig']['DataCatalogConfig']['TableName']
    fg_s3_url = f'{s3_uri}/{account_id}/sagemaker/{region}/offline-store/{table_name_source}'
    fg_file_name = f'sagemaker-feature-store/{account_id}/sagemaker/{region}/offline-store/{table_name_source}'
    fg_unique_id = fg.describe()['RecordIdentifierFeatureName']
    fg_event_time = fg.describe()['EventTimeFeatureName']    

    print(f"Feature Group S3 URL: {fg_s3_url}")
    print(f"Feature Group Table Name: {table_name_source}")
    
    table_name_target = f'{table_name_source}{ctas_table_suffix}'

    target_table = f'"{database_name}"."{table_name_target}"'
    source_table = f'"{database_name}"."{table_name_source}"'

    query = f' CREATE TABLE {target_table} AS ' \
                f' SELECT * ' \
                f' FROM (SELECT *, row_number() ' \
                f'      OVER (PARTITION BY {fg_unique_id} ' \
                f'      ORDER BY {fg_event_time} desc, api_invocation_time DESC, write_time DESC) AS row_num ' \
                f'      FROM {source_table}) ' \
                f' WHERE row_num = 1 and NOT is_deleted '

    athena_result_bucket = f'{s3_uri}/{account_id}/sagemaker/{region}'

    conn = connect(s3_staging_dir=athena_result_bucket,
                   region_name=region).cursor()

    conn.execute(query)
    if verbose:
        print(conn.fetchall())
        print("Total execution time in millis: ", conn.total_execution_time_in_millis)
        print("Total data scanned in bytes: ", conn.data_scanned_in_bytes)
    print (f"CTAS table created successfully: {table_name_target}")
    
    return table_name_target


# Utility function to retrieve uri from an athena table (in this case ctas) 
def get_s3_uri_from_athena_table(database, table):
    client = boto3.client(service_name='athena', region_name=region)

    response = client.get_table_metadata(
        CatalogName='AwsDataCatalog',
        DatabaseName=database,
        TableName=table
    )
    
    s3_uri_athena_table = response['TableMetadata']['Parameters']['location']
    
    return s3_uri_athena_table 
    
    
# Utility function to create an AWS Glue Crawler - to be used to crawl the snapshot table
def create_crawler(database, table, verbose):
    #Creation of the Crawler
    glue_crawler_name = f'{table}-crawler'
    glue_database_name = database
    glue_table_name = table
    glue_database_description = 'crawler to create new table with partitions'

    #Instantiate AWS Glue client
    glue = boto3.client(service_name='glue', region_name=region,
                  endpoint_url=f'https://glue.{region}.amazonaws.com')

    client = boto3.client('glue')
    response = client.list_crawlers()
    available_crawlers = response["CrawlerNames"]

    for crawler_name in available_crawlers:
        if verbose:
            print(crawler_name)
        #response = client.get_crawler(Name=crawler_name)
        if crawler_name == glue_crawler_name:
            response = client.get_crawler(Name=crawler_name)
            #pprint(response)
            return response, glue_crawler_name
            
    #Create AWS Glue Crawler (more properties can be externalised) - for Orders feature group only
    response = glue.create_crawler(
        Name=glue_crawler_name,
        Role=role,
        Description=glue_database_description,
        Targets={
                'CatalogTargets': [
                {
                    'DatabaseName': glue_database_name,
                    'Tables': [
                        glue_table_name,
                    ]
                },
            ]
        },
        #Schedule='string',  # ON DEMAND by default
        SchemaChangePolicy={
            'UpdateBehavior': 'UPDATE_IN_DATABASE',
            'DeleteBehavior': 'LOG'
        }
    )

    return response, glue_crawler_name


# Utility function to start the AWS Crawler
def run_crawler(crawler: str, verbose: bool, *, timeout_minutes: int = 120, retry_seconds: int = 5) -> None:
    """Run the specified AWS Glue crawler, waiting until completion."""
    # Ref: https://stackoverflow.com/a/66072347/
    timeout_seconds = timeout_minutes * 60
    client = boto3.client("glue")
    start_time = timeit.default_timer()
    abort_time = start_time + timeout_seconds

    def wait_until_ready(verbose: bool) -> None:
        state_previous = None        
        if verbose:
            print(f'state_previous:= {state_previous}')
        while True:
            response_get = client.get_crawler(Name=crawler)
            state = response_get["Crawler"]["State"]
            if verbose:
                print(f'current state= {state}')
            else:
                print(".", end = '')
            if state != state_previous:
                log.info(f"Crawler {crawler} is {state.lower()}.")
                state_previous = state
            if state == "READY":  # Other known states: RUNNING, STOPPING
                print("!\n")
                return
            if timeit.default_timer() > abort_time:
                raise TimeoutError(f"Failed to crawl {crawler}. The allocated time of {timeout_minutes:,} minutes has elapsed.")
            time.sleep(retry_seconds)

    #wait_until_ready(verbose)
    print(f"Start crawling {crawler}.")
    response_start = client.start_crawler(Name=crawler)
    assert response_start["ResponseMetadata"]["HTTPStatusCode"] == 200
    if verbose:
        log.info(f"Crawling {crawler}.")
        print(f"Crawling {crawler}.")
    wait_until_ready(verbose)
    if verbose:
        log.info(f"Crawled {crawler}.")
        print(f"Crawled {crawler}.")


# Utility function to delete the AWS Glue Crawler
def delete_crawler(glue_crawler_name):
    
    #Instantiate AWS Glue client
    glue = boto3.client(service_name='glue', region_name=region,
                  endpoint_url=f'https://glue.{region}.amazonaws.com')
    delete_response = glue.delete_crawler(
        Name=glue_crawler_name
    )
    
    return delete_response


# Utility function to delete snapshot table of feature group records 
def delete_fg_snapshot_ctas(fg_name, verbose, table=""):
    
    # Retreive FG's table name
    fg = FeatureGroup(name=fg_name, sagemaker_session=feature_store_session)
    table_name = fg.describe()['OfflineStoreConfig']['DataCatalogConfig']['TableName']
    database = fg.describe()['OfflineStoreConfig']['DataCatalogConfig']['Database']
    if table == "":
        table = f'{table_name}{ctas_table_suffix}'
        
    # Retrieve property from Athena table to get S3 location of underlying files
    client = boto3.client(service_name='athena', region_name=region)

    response = client.get_table_metadata(
        CatalogName='AwsDataCatalog',
        DatabaseName=database,
        TableName=table
    )
    
    s3_location_ctas_table = response['TableMetadata']['Parameters']['location']
    if verbose:
        print(s3_location_ctas_table)
    
    # Retrieve S3 location from the creation the Temp Snapshot table
    s3_uri=fg.describe()['OfflineStoreConfig']['S3StorageConfig']['S3Uri']
    s3_uri_athena_table = get_s3_uri_from_athena_table(database, table)
    prefix = '/'.join(s3_uri_athena_table.split('/')[3:])
    bucket = s3_uri_athena_table.split('/')[2]
    
    # Drop the Temp Snapshot table
    table = f'`{database}.{table}`'
    query = f' DROP TABLE {table} '

    athena_result_bucket = f'{s3_uri}/{account_id}/sagemaker/{region}'

    conn = connect(s3_staging_dir=athena_result_bucket,
                   region_name=region).cursor()

    conn.execute(query)
    if verbose:
        print(conn.fetchall())
        print(conn.total_execution_time_in_millis)
        print(conn.data_scanned_in_bytes)

    # Delete S3 files
    s3 = boto3.resource('s3')
    objects_to_delete = s3.meta.client.list_objects(Bucket=bucket, Prefix=prefix)

    delete_keys = {'Objects' : []}
    delete_keys['Objects'] = [{'Key' : k} for k in [obj['Key'] for obj in objects_to_delete.get('Contents', [])]]

    s3.meta.client.delete_objects(Bucket=bucket, Delete=delete_keys)
    
    
# Utility function to create a DataBrew dataset from table in AWS Glue Data Catalog (in this case CTAS copy of offline feature group)
def create_databrew_dataset_from_glue_table(fg_name, table_name, glue_databrew_results, results_key, verbose=True):

    # Instantiate an AWS Glue DataBrew Object
    databrew = boto3.client(service_name='databrew', region_name=region)
    fg = FeatureGroup(name=fg_name, sagemaker_session=feature_store_session)
    database = fg.describe()['OfflineStoreConfig']['DataCatalogConfig']['Database']

    # Create AWS Glue Databrew dataset
    databrew_dataset_name = f'{fg_name}{databrew_dataset_suffix}'
    glue_databrew_results_key = f'{results_key}{databrew_ctas_suffix}'
    
    response = databrew.list_datasets()
    available_datasets = response["Datasets"]

    for dataset in available_datasets:
        #print(crawler_name)
        #response = client.get_crawler(Name=crawler_name)
        if dataset["Name"] == databrew_dataset_name:
            #response = client.get_dataset(Name=crawler_name)
            if verbose:
                print(dataset)
            return dataset, databrew_dataset_name

    response = databrew.create_dataset(
        Name=databrew_dataset_name,
        Input={
            'DataCatalogInputDefinition': {
                'CatalogId': account_id,
                'DatabaseName': database,
                'TableName': table_name,
                'TempDirectory': {
                    'Bucket': glue_databrew_results,
                    'Key': glue_databrew_results_key
                }
            }
        }
    )
           
    if verbose:
        print(f'Dataset Created:{response}')
    print("DataBrew Dataset Created: ", databrew_dataset_name)
        
        
# Utility function to wait until job execution ends 
def wait_until_job_ready(profilejob_name, profilejob_runid, verbose, timeout_minutes = 120):
    databrew = boto3.client(service_name='databrew', region_name=region)
    retry_seconds = 5
    timeout_seconds = timeout_minutes * 60
    start_time = timeit.default_timer()
    abort_time = start_time + timeout_seconds

    state_previous = None
    if verbose:
        print(f'state_previous:= {state_previous}')
    while True:
        response_get = databrew.describe_job_run(
            Name = profilejob_name,
            RunId = profilejob_runid)
        state = response_get["State"]
        if verbose:
            print(f'current state= {state}')
        else:
            print(".", end = '')
        if state != state_previous:
            state_previous = state
        if state == "SUCCEEDED":  # Other known states: RUNNING, SUCCEEDED, STOPPING
            time.sleep(retry_seconds)
            print("!\n")
            return
        if timeit.default_timer() > abort_time:
            raise TimeoutError(f"Failed to run {profilejob_name}. The allocated time of {timeout_minutes:,} minutes has elapsed.")
        time.sleep(retry_seconds)
        

        # Utility function for DataBrew Job creation & execution          
def feature_databrew_profile(fg_name, results_bucket, results_key, verbose=True):
    
    fg = FeatureGroup(name=fg_name, sagemaker_session=feature_store_session)
    #Retrieve S3 location to be used for setting up the Crawler (this could be a Utils function)
    s3_uri = fg.describe()['OfflineStoreConfig']['S3StorageConfig']['S3Uri']
    table_name = fg.describe()['OfflineStoreConfig']['DataCatalogConfig']['TableName']
    fg_s3_url = f'{s3_uri}/{account_id}/sagemaker/{region}/offline-store/{table_name}'
    fg_file_name = f'sagemaker-feature-store/{account_id}/sagemaker/{region}/offline-store/{table_name}/data/'
    databrew_dataset_name = f'{fg_name}{databrew_dataset_suffix}'
    databrew_reports_key = f'{results_key}{databrew_reports_suffix}'
    
    
    if verbose:
        print(s3_uri)
        print(fg_s3_url)
        print(fg_file_name)

    # Instantiate an AWS Glue DataBrew Object
    databrew = boto3.client(service_name='databrew', region_name=region)
    
    # CREATE PROFILING JOB
    
    # Profile Job configuration
    databrew_profilejob_name = f'{fg_name}-profile-job'
    
    # Check if Profile Job already exists
    response = databrew.list_jobs()
    available_jobs = response["Jobs"]

    for job in available_jobs:
        #print(crawler_name)
        #response = client.get_crawler(Name=crawler_name)
        if job["Name"] == databrew_profilejob_name:
            #response = client.get_dataset(Name=crawler_name)
            print(job)
            return job, databrew_profilejob_name

    
    databrew_output_location = {
        'Bucket': results_bucket,
        'Key': databrew_reports_key
    }
    databrew_job_configuration = {
        'DatasetStatisticsConfiguration': {
            'Overrides': [{
                'Statistic': 'CORRELATION',
                'Parameters': {'columnNumber': '20'}
            }]
        }
    }
    databrew_job_timeout = 120 # job timeout (minutes)
    
    # Create AWS Glue DataBrew Profile Job
    create_response = databrew.create_profile_job(
        Name = databrew_profilejob_name,
        RoleArn = role,
        DatasetName = databrew_dataset_name,
        OutputLocation = databrew_output_location,
        #Configuration = databrew_job_configuration,
        Timeout = databrew_job_timeout,
        JobSample = {
            'Mode': 'FULL_DATASET'
        }
    )
        
    print(f'AWS Glue DataBrew Profile Job Created: {create_response["Name"]}')

    return create_response
              

# Main function for DataBrew Job execution          
def feature_monitoring_run(fg_name, verbose=True):
    
    # Instantiate an AWS Glue DataBrew Object
    databrew = boto3.client(service_name='databrew', region_name=region)
    fg = FeatureGroup(name=fg_name, sagemaker_session=feature_store_session)
    table_name = fg.describe()['OfflineStoreConfig']['DataCatalogConfig']['TableName']
    table_name_ctas = f'{table_name}{ctas_table_suffix}'
    
    # Variables configuration
    databrew_dataset_name = f'{fg_name}{databrew_dataset_suffix}'
    databrew_profilejob_name = f'{fg_name}{databrew_profilejob_suffix}'

    # Check if the CTAS table exists, then delete it
    if checkTableExists(table_name_ctas):        
        delete_fg_snapshot_ctas(fg_name, verbose)
    # Create the CTAS table
    snapshot_table = create_fg_snapshot_ctas(fg_name, verbose)
    
    print("Running DataBrew Profiling Job")
    #
    # Start first execution of the profile job 
    response_job_start = databrew.start_job_run(
        Name=databrew_profilejob_name)

    # Wait until job completes
    wait_until_job_ready(databrew_profilejob_name, response_job_start["RunId"], verbose)

    assert response_job_start["ResponseMetadata"]["HTTPStatusCode"] == 200
    if verbose:
        print(f"Profiling {databrew_profilejob_name}.")
    
    # Preparing results
    response_get = databrew.describe_job_run(
                    Name=databrew_profilejob_name,
                    RunId=response_job_start["RunId"])
    
    brew_results_bucket = response_get["Outputs"][0]['Location']['Bucket']
    brew_results_key = response_get["Outputs"][0]['Location']['Key']

    output_s3_file_url = f's3://{brew_results_bucket}/{brew_results_key}'
    if verbose:
        print(output_s3_file_url)
    
    databrew_profile_console_url = f'https://{region}.console.aws.amazon.com/databrew/home?region={region}#dataset-details?dataset={databrew_dataset_name}&tab=profile-overview'
    if verbose:
        print(databrew_profile_console_url)
    
    # Load Report JSON file into a Pyton Dict object
    s3 = boto3.resource('s3')
    obj = s3.Object(brew_results_bucket, brew_results_key)
    report_data = json.load(obj.get()['Body']) 

    # Add tags to the FG
    feature_add_tag(fg_name, fg_profileurl_tag, Utils._escape_tag_chars(databrew_profile_console_url))
    feature_add_tag(fg_name, fg_profiles3link_tag, output_s3_file_url)
    
    return brew_results_bucket, brew_results_key, databrew_profile_console_url, report_data, output_s3_file_url


# Create a local folder to store the json files for local trend analysis
def download_json_profiling(brew_results_bucket, results_key, date):
    
    # Create an analysis folder
    current_timestamp = strftime('%m-%d-%H-%M', gmtime())
    analysis_folder_name = f'profiling-analysis-{current_timestamp}'
    os.makedirs(analysis_folder_name)
    databrew_reports_key = f'{results_key}{databrew_reports_suffix}/'

    # Download the profiling JSON analysis file 
    s3_result =  s3_client.list_objects_v2(Bucket=brew_results_bucket, Prefix=databrew_reports_key, Delimiter = "/")
    if 'Contents' in s3_result:
        for item in s3_result['Contents']:            
            if item['LastModified'].strftime('%Y%m%d') > date and item['Key'].endswith(".json"):   
                analysis_file_name = analysis_folder_name + "/" + item['Key'].split('/')[-1]
                try:
                    s3.Bucket(brew_results_bucket).download_file(item['Key'], analysis_file_name)
                except botocore.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == "404":
                        print("The object does not exist.")
                    else:
                        raise
    
    return analysis_folder_name


# LOAD THE FILE
# location of Json file
# load in Pandas Dataframe using pd.read_json(r'.json')
def create_profiling_pd_from_json(directory, feature_group):    
    appended_data = []

    for filename in os.listdir(directory):
        if filename.endswith(".json") and filename.startswith(feature_group):
            location=f'{directory}/{filename}'
            #print(location)
            with open(location) as f:  
                #data = json.load(f)
                json_data = json.load(f)
                df_temp = pd.json_normalize(json_data, record_path='columns', meta=['writtenOn'])
                df_temp['report_file'] = filename
                #df_temp = data_frame.drop(columns="mostCommonValues")
                appended_data.append(df_temp)     
        else:
            continue

    if len(appended_data) > 0:
        analysis_data = pd.concat(appended_data)
        analysis_data = analysis_data.drop(columns="mostCommonValues")
        analysis_data = analysis_data.reset_index(drop=True)
        analysis_data['writtenOn'] = pd.to_datetime(analysis_data['writtenOn'])
        analysis_data = analysis_data.set_index('writtenOn')
        analysis_data.sort_index(inplace=True)
        
        return analysis_data


def plot_profiling_data(pd, feature_name, feature_property, kind='line', 
                        figsize=(11, 4), multiple_plots=False, stacked=False):
    feature_data = pd[pd['name'] == feature_name]
        
    if len(feature_property) > 1:
        # Plot multiple statistics for feature_name 
        if multiple_plots == True:
            # Plot on multiple plots
            feature_data[feature_property].plot(subplots=True, figsize=figsize)
        else:
            # Plot on the same plot with multiple scales
            fig, ax = plt.subplots()
            ax = feature_data[feature_property].plot(figsize=figsize, secondary_y=[feature_property[1]], ax=ax)
            ax.set_xlabel(xlabel="Date")
            ax.set_ylabel(feature_property[0]+" scale")
            ax.right_ax.set_ylabel(feature_property[1]+" scale")
            plt.show()
    else:
        # Plot only feature_property statistic for feature_name 
        fig, ax = plt.subplots()
        feature_data[feature_property[0]].plot(kind=kind, figsize=figsize, stacked=stacked, ax=ax)
        ax.set(xlabel="Date", ylabel=feature_property[0], 
               title=feature_name+"'s "+feature_property[0]+" drift over time")
    
        plt.show()
        
    return feature_data


# Utility function to start the AWS Crawler
def feature_monitoring_prep(fg_name, results_bucket, results_key, verbose = True):

    fg = FeatureGroup(name=fg_name, sagemaker_session=feature_store_session)
    fs_database = fg.describe()['OfflineStoreConfig']['DataCatalogConfig']['Database']
    table_name = fg.describe()['OfflineStoreConfig']['DataCatalogConfig']['TableName']
    table_name_ctas = f'{table_name}{ctas_table_suffix}'
    
    # Check if the CTAS table does not exists, then create it
    if not checkTableExists(table_name_ctas):
        snapshot_table = create_fg_snapshot_ctas(fg_name, verbose)
    
    snapshot_table = table_name_ctas
    # Create AWS glue crawler (if not exist already)
    response_crawler=create_crawler(fs_database, snapshot_table, verbose)
    if verbose:
        print(response_crawler)
    
    # Run the AWS Glue Crawler
    response_run=run_crawler(response_crawler[1], verbose)
    
    #Create AWS DataBrew Dataset
    dataset_response=create_databrew_dataset_from_glue_table(fg_name, snapshot_table, results_bucket, results_key, verbose)
    
    #Create AWS DataBrew Profile Job
    profile_response = feature_databrew_profile(fg_name, results_bucket, results_key, verbose)    

    return snapshot_table, response_crawler, response_run, dataset_response, profile_response


# Utility function to add tag to a feature group
# If the same tag exists it overides previous value
def feature_add_tag(fg_name, tag, value):

    fg = FeatureGroup(name=fg_name, sagemaker_session=feature_store_session)    
    fg_arn = fg.describe()['FeatureGroupArn']
    
    # Add the DataBrew Profile URL tag to FG
    response = sagemaker_client.add_tags(
        ResourceArn= fg_arn,
        Tags=[
            {
                'Key': tag,
                'Value': Utils._escape_tag_chars(value)
            },
        ]
    )
    
    return response


#
# Consolidate monitor reports
# - DataBrew reports Json files stored in S3 are copied into a local create analysis folder
# - Json files are merged into a Pandas dataframe in a flat csv format
# - CSV files are saved into S3
#
def consolidate_monitor_reports(fg_name, results_bucket, results_key, date):

    consolidated_results_key = f'{results_key}{databrew_csv_suffix}'
    
    # DataBrew reports Json files stored in S3 are copied into a local create analysis folder
    analysis_folder_name = download_json_profiling(results_bucket, results_key, date)
    
    # Json files are merged into a Pandas dataframe in a flat csv format
    analysis_data = create_profiling_pd_from_json(analysis_folder_name, fg_name)
    
    if analysis_data is not None:    
        # Generate a shorter DF only with main statistics
        columns_to_drop = ['valueDistribution','zScoreOutliersSample','minimumValues',
                           'maximumValues','zScoreDistribution']
        analysis_data_short = analysis_data.drop(columns_to_drop, axis = 1)

        # Write the DF to CSV files on S3
        current_timestamp = strftime('%m-%d-%H-%M', gmtime())
        file_key=fg_name+'-'+current_timestamp+'.csv'
        file_key_short=fg_name+'-short-'+current_timestamp+'.csv'
        s3 = boto3.client('s3')
        s3.put_object(Bucket=results_bucket, Key=(consolidated_results_key+'/'))

        analysis_data.to_csv('s3://'+results_bucket+'/'+consolidated_results_key+'/'+file_key, sep=';', index=True)
        analysis_data_short.to_csv('s3://'+results_bucket+'/'+consolidated_results_key+'/'+file_key_short, sep=';', index=True)

        s3_file_url = f's3://{results_bucket}/{consolidated_results_key}/{file_key}'
        s3_file_short_url = f's3://{results_bucket}/{consolidated_results_key}/{file_key_short}'

        return analysis_data, analysis_data_short, s3_file_url, s3_file_short_url, analysis_folder_name


# Ingest rows into FG from CSV file and normalize to generate drift
def ingest_rows_fg(fg_name, csv_path, nbrows=1000):
    fg = FeatureGroup(name=fg_name, sagemaker_session=feature_store_session)
    fg_unique_id = fg.describe()['RecordIdentifierFeatureName']
    fg_event_time = fg.describe()['EventTimeFeatureName']    
    
    # Read records from the csv file 
    fg_df = pd.read_csv(csv_path, nrows = nbrows)
    # Do some type conversions
    fg_df[fg_unique_id] = fg_df[fg_unique_id].astype('string')
    fg_df['customer_id'] = fg_df['customer_id'].astype('string')
    fg_df['product_id'] = fg_df['product_id'].astype('string')
    fg_df[fg_event_time] = fg_df[fg_event_time].astype('string')
    
    # Change IDs
    fg_total_record_count = Utils.get_historical_record_count(fg_name)
    id_lst = []
    for i in range(fg_total_record_count, (fg_total_record_count + nbrows)):
        new_id = f'O{i}'
        id_lst.append(new_id)    
    fg_df[fg_unique_id] = np.array(id_lst)
    
    # Normanize the data in order to see feature drift
    scaler = preprocessing.StandardScaler().fit(fg_df['purchase_amount'].values.reshape(-1, 1))
    purchase_amount_scaled = scaler.transform(fg_df['purchase_amount'].values.reshape(-1, 1))
    fg_df['purchase_amount'] = purchase_amount_scaled.reshape(1, -1)[0]
    
    # Ingest into the feature group
    response = fg.ingest(data_frame=fg_df, max_processes=4, max_workers=4, wait=True)    
    assert len(response.failed_rows) == 0
    
    # Additional wait for partitioning folders to be available in the Glue catalog table
    print("Ingesting into the feature group " + fg_name)
    for i in range(30):
        print(".", end = '')
        time.sleep(10)
    print("!")

    
# Utility function to clean up resources from the lab module
def feature_monitoring_cleanup(fg_name, folder_name):
    
    # Variables configuration 
    fg = FeatureGroup(name=fg_name, sagemaker_session=feature_store_session)
    table_name = fg.describe()['OfflineStoreConfig']['DataCatalogConfig']['TableName']
    table_name_ctas = f'{table_name}{ctas_table_suffix}'
    glue_crawler_name = f'{table_name_ctas}{glue_crawler_suffix}'
    databrew_dataset_name = f'{fg_name}{databrew_dataset_suffix}'
    databrew_profilejob_name = f'{fg_name}{databrew_profilejob_suffix}'

    # Delete AWS Glue Crawler
    delete_crawler_response = delete_crawler(glue_crawler_name)
    print (delete_crawler_response)
    
    # Instantiate an AWS Glue DataBrew Object
    databrew = boto3.client(service_name='databrew', region_name=region)
    
    # Delete AWS DataBrew Profile Job
    job_delete_response = databrew.delete_job(
            Name=databrew_profilejob_name
        )
    print(job_delete_response)
    
    # Delete AWS DataBrew dataset
    dataset_delete_response = databrew.delete_dataset(
            Name=databrew_dataset_name
        )
    print(dataset_delete_response)
    
    import shutil
    # delete local analysis folder
    dir_path=f'./{folder_name}'

    try:
        shutil.rmtree(dir_path)
    except OSError as e:
        print("Error: %s : %s" % (dir_path, e.strerror))
    
