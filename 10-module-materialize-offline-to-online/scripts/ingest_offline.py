"""
This PySpark script ingests dummy data into a feature group
using the Feature Store Spark Connector.
"""
import argparse
import boto3
from pyspark.sql import SparkSession
from feature_store_manager import FeatureStoreManager


def run(feature_group_name, region_name):

    spark = SparkSession.builder.getOrCreate()

    columns = ["RecordIdentifier", "Measure", "EventTime"]
    data = [
        ("1", 0.1, "2023-01-23T00:00:00Z"),
        ("1", 0.2, "2023-01-23T01:00:00Z"),
        ("1", 0.3, "2023-01-23T02:00:00Z"),
        ("2", 0.1, "2023-01-23T00:00:00Z"),
        ("2", 0.2, "2023-01-23T01:00:00Z"),
        ("2", 0.3, "2023-01-23T02:00:00Z"),
        ("3", 0.1, "2023-01-23T00:00:00Z"),
        ("3", 0.2, "2023-01-23T01:00:00Z"),
        ("3", 0.3, "2023-01-23T02:00:00Z"),
    ]

    df = spark.createDataFrame(data).toDF(*columns)

    feature_store_manager = FeatureStoreManager()

    sm_client = boto3.client("sagemaker", region_name=region_name)

    feature_store_manager.ingest_data(
        input_data_frame=df,
        feature_group_arn=sm_client.describe_feature_group(
            FeatureGroupName=feature_group_name
        )["FeatureGroupArn"],
        target_stores=["OfflineStore"],
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--feature_group_name", type=str, help="feature group name")
    parser.add_argument("--region_name", type=str, help="region name")
    args = parser.parse_args()
    run(args.feature_group_name, args.region_name)
