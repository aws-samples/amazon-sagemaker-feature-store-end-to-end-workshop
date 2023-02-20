"""
This PySpark script materializes offline data to the online store.
"""
import argparse
import logging
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from feature_store_manager import FeatureStoreManager

logger = logging.getLogger()


def run(table_format, feature_group_name, region_name):

    sm_client = boto3.client("sagemaker", region_name=region_name)
    fg_desc = sm_client.describe_feature_group(FeatureGroupName=feature_group_name)

    if table_format == "Iceberg":

        s3_uri = fg_desc["OfflineStoreConfig"]["S3StorageConfig"]["S3Uri"]
        glue_table_name = fg_desc["OfflineStoreConfig"]["DataCatalogConfig"][
            "TableName"
        ]

        # configure the SparkSession to read data in Iceberg format
        spark = (
            SparkSession.builder.config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config(
                "spark.sql.catalog.example", "org.apache.iceberg.spark.SparkCatalog"
            )
            .config(
                "spark.sql.catalog.example.catalog-impl",
                "org.apache.iceberg.aws.glue.GlueCatalog",
            )
            .config("spark.sql.catalog.example.warehouse", f"{s3_uri}")
            .config("spark.sql.catalog.example.glue.skip-name-validation", "true")
            .getOrCreate()
        )

        df = spark.sql(
            f"SELECT * FROM example.sagemaker_featurestore.`{glue_table_name}`"
        )
    elif table_format == "Glue":

        resolved_output_s3_uri = fg_desc["OfflineStoreConfig"]["S3StorageConfig"][
            "ResolvedOutputS3Uri"
        ]

        spark = SparkSession.builder.getOrCreate()
        df = spark.read.parquet(resolved_output_s3_uri)

    # get latest records
    df = (
        df.withColumn("event_time", F.to_timestamp("EventTime"))
        .withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("RecordIdentifier").orderBy(
                    F.col("event_time").desc()
                )
            ),
        )
        .filter(F.col("rn") == 1)
        .drop(
            "rn",
            "event_time",
            "api_invocation_time",
            "write_time",
            "is_deleted",
            "year",
            "month",
            "day",
            "hour",
        )
    )

    # ingest latest records to online store
    feature_store_manager = FeatureStoreManager()

    try:
        feature_store_manager.ingest_data(
            input_data_frame=df,
            feature_group_arn=fg_desc["FeatureGroupArn"],
            target_stores=["OnlineStore"],
        )
    except Exception:
        failed_records_df = (
            feature_store_manager.get_failed_stream_ingestion_data_frame()
        )
        logger.warning(
            f"Records failed to be ingested: {failed_records_df.show(truncate=False)}"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--table_format",
        choices=["Iceberg", "Glue"],
        help="table format for offline features",
    )
    parser.add_argument("--feature_group_name", type=str, help="feature group name")
    parser.add_argument("--region_name", type=str, help="region name")
    args = parser.parse_args()

    run(args.table_format, args.feature_group_name, args.region_name)
