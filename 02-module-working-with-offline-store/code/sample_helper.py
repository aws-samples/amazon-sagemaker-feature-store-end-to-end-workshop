# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from time import gmtime, strftime, sleep
from datetime import datetime, timezone
from random import randint

import subprocess
import random
import time
import sys
import os
import re
import sagemaker
import pandas as pd
import numpy as np

import boto3

region = boto3.Session().region_name

# allocate S3 client
s3_client = boto3.client('s3', region_name=region)
print('Created S3 Client for sample_helper.')


def gen_timestamps(ts_start, ts_delta, num=10):
    print(f'> Generating {num} timestamps ...')

    date_start = datetime.fromtimestamp(int(ts_start)).isoformat()
    print(f'Starting time: {date_start}')
    date_end = datetime.fromtimestamp(int(ts_start + ts_delta)).isoformat()
    print(f'Ending time: {date_end}')

    new_timestamps = []
    for i in range(num):
        new_unix_time = ts_start + random.randint(0,ts_delta)
        # Enforce ISO-8601 time format, including 'Z' timezone
        new_datetime = datetime.fromtimestamp(int(new_unix_time)).isoformat()
        new_datetime = str(new_datetime) + 'Z'
        new_timestamps.append(new_datetime)
    
    new_timestamps_df = pd.DataFrame(data=new_timestamps, columns=['Timestamp'])

    return new_timestamps_df


def convert_timestamp_to_iso8601(timestamp):
    # Enforce ISO-8601 time format, including 'Z' timezone
    datetime_iso8601 = datetime.fromtimestamp(int(timestamp)).isoformat()
    datetime_iso8601 = str(datetime_iso8601) + 'Z'
    return datetime_iso8601


def gen_new_records(sample_df, timestamps_df, num=1):
    print(f'> Generating {num} new records for time-series data ...')
    
    sample_record = sample_df.iloc[0]
    sample_rec_id = sample_record['Lead_ProspectID']
    print(f'> New records using record_id: {sample_rec_id}')
    
    # Create new dataframe and drop existing record
    new_records = pd.DataFrame(data=sample_df)
    for irec, rec in new_records.iterrows():
        new_records.drop(irec,axis=0,inplace=True)
    
    for iter in range(num):
        # create copy of sample record
        new_rec = sample_record.copy(deep=True)
        new_rec['Lead_ProspectID'] = sample_rec_id
        # generate new LeadSource value
        new_rec['LeadSource'] = 'NewLeadSource_' + str(iter)
        # Select random timestamp 
        ts_rec = timestamps_df.sample(n=1).iloc[0]
        new_rec['Lead_EventTime'] = ts_rec['Timestamp']
        fields = ['LeadSource', 'Lead_EventTime']
        dump_record(new_rec, fields)
        # Add new sample record to dataframe
        new_records = new_records.append(new_rec, ignore_index=True)
    
    return new_records


def dump_record(record, fields):
    # dump new record items from fields
    dumpstr = '> Record: '
    for field in fields:
        val = record[field]
        fstr = f'{field}: {val}'
        dumpstr = " ".join([dumpstr, fstr])

    print(dumpstr)


def wait_for_feature_group_creation_complete(feature_group):
    status = feature_group.describe().get("FeatureGroupStatus")
    while status == "Creating":
        print("Waiting for Feature Group Creation")
        time.sleep(5)
        status = feature_group.describe().get("FeatureGroupStatus")
    if status != "Created":
        raise RuntimeError(f"Failed to create feature group {feature_group.name}")
    print(f"FeatureGroup {feature_group.name} successfully created.")
    

def wait_for_feature_group_data_ingest(s3_bucket, s3_prefix):
    print(f"Polling S3 location for data: {s3_prefix}")
    offline_store_contents = None
    while True:
        objects_in_bucket = s3_client.list_objects(Bucket=s3_bucket, Prefix=s3_prefix)
        if "Contents" in objects_in_bucket and len(objects_in_bucket["Contents"]) > 1:
            break
        else:
            print("Waiting for data in offline store...")
            time.sleep(60)
    print("Data available.")
