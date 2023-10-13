"""
This PySpark script ingests dummy data into a feature group
using the Feature Store Spark Connector.
"""
from pyspark.sql.functions import udf, datediff, to_date, lit
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql import SparkSession, DataFrame
from argparse import Namespace, ArgumentParser
from datetime import datetime

import argparse
import logging
import boto3
import time
import os

from feature_store_pyspark.FeatureStoreManager import FeatureStoreManager

logger = logging.getLogger('__name__')
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


def read_df(s3_uri_csv_path):
    spark_session = SparkSession.builder.getOrCreate()

    print(f'\nReading CSV file from S3 location: {s3_uri_csv_path}')
    df = spark_session.read.options(Header=True).csv(s3_uri_csv_path)
    count = df.count()
    print(f'\nDone reading dataframe, records: {count}')
    
    df.head(5)
    return df


def run_ingest(feature_group_name, region_name, input_df):
    sm_client = boto3.client("sagemaker", region_name=region_name)
    fg_desc = sm_client.describe_feature_group(FeatureGroupName=feature_group_name)
    print(f'Using Feature Group ARN: {fg_desc["FeatureGroupArn"]}')
    
    feature_store_manager = FeatureStoreManager()
    
    logger.info(f'TIMER-Begin: Feature Store ingest_data: {datetime.now().strftime("%m/%d/%Y, %H:%M:%S")}')
    
    try:
        feature_store_manager.ingest_data(
            input_data_frame=input_df,
            feature_group_arn=fg_desc["FeatureGroupArn"],
            target_stores=["OnlineStore"],
        )
    except Exception:
        failed_records_df = (feature_store_manager.get_failed_stream_ingestion_data_frame())
        logger.warning(f"Records failed to be ingested: {failed_records_df.count()}")

    logger.info(f'TIMER-End: Feature Store ingest_data: {datetime.now().strftime("%m/%d/%Y, %H:%M:%S")}')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--feature_group_name", type=str, help="feature group name")
    parser.add_argument("--region_name", type=str, help="region name")
    parser.add_argument("--s3_uri_csv_path", type=str, help="s3 URI for CSV file")
    args = parser.parse_args()
    
    input_df = read_df(args.s3_uri_csv_path)
    run_ingest(args.feature_group_name, args.region_name, input_df)
