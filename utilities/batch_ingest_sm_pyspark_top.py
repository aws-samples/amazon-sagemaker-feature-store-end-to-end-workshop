from pyspark.ml.feature import VectorAssembler, StringIndexer, MinMaxScaler
from pyspark.sql.functions import udf, datediff, to_date, lit
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql import SparkSession, DataFrame
from argparse import Namespace, ArgumentParser
from pyspark.ml.linalg import Vector
from pyspark.ml import Pipeline
from datetime import datetime
import argparse
import logging
import boto3
import time
import os

logger = logging.getLogger('__name__')
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

curr_region = os.environ['AWS_DEFAULT_REGION']
logger.info(f'***\nRegion environment variable AWS_DEFAULT_REGION is {curr_region}\n')

def transform_row(row) -> list:
    columns = list(row.asDict())
    record = []
    for column in columns:
        feature = {'FeatureName': column, 'ValueAsString': str(row[column])}
        record.append(feature)
    return record

def ingest_to_feature_store(args: argparse.Namespace, rows) -> None:
    feature_group_name = args.feature_group_name
    session = boto3.session.Session()
    featurestore_runtime_client = session.client(service_name='sagemaker-featurestore-runtime',
                                                region_name=args.region_name)
    rows = list(rows)
    logger.info(f'Ingesting {len(rows)} rows into feature group: {feature_group_name}')
    for _, row in enumerate(rows):
        record = transform_row(row)
        response = featurestore_runtime_client.put_record(FeatureGroupName=feature_group_name, Record=record)
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200

def parse_args() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--num_processes', type=int, default=1)
    parser.add_argument('--num_workers', type=int, default=1)
    parser.add_argument('--feature_group_name', type=str)
    parser.add_argument('--region_name', type=str)
    parser.add_argument("--s3_uri_prefix", type=str)
    args, _ = parser.parse_known_args()
    return args