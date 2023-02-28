from pyspark.ml.feature import VectorAssembler, StringIndexer, MinMaxScaler
from feature_store_manager import FeatureStoreManager
from pyspark.sql.functions import udf, datediff, to_date, lit, col,isnan, when, count
from pyspark.sql.types import IntegerType, DoubleType, StructType, StructField, StringType, FloatType
from pyspark.sql import SparkSession, DataFrame
from argparse import Namespace, ArgumentParser
from pyspark.ml.linalg import Vector
from pyspark.ml import Pipeline
from datetime import datetime
import argparse
import ast
import logging
import boto3
import time
import os


logger = logging.getLogger('__name__')
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


def parse_args() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--num_processes', type=int, default=1)
    parser.add_argument('--num_workers', type=int, default=1)
    parser.add_argument('--feature_group_name', type=str)
    parser.add_argument('--feature_group_arn', type=str)
    parser.add_argument('--target_feature_store_list', type=str)
    parser.add_argument('--s3_uri_prefix', type=str)
    
    args, _ = parser.parse_known_args()
    return args

def transform_row(row) -> list:
    columns = list(row.asDict())
    record = []
    for column in columns:
        feature = {'FeatureName': column, 'ValueAsString': str(row[column])}
        record.append(feature)
    return record

def batch_ingest_to_feature_store(args: argparse.Namespace, df: DataFrame) -> None:
    feature_group_name = args.feature_group_name
    logger.info(f'Feature Group name supplied is: {feature_group_name}')
    session = boto3.session.Session()

    logger.info(f'Instantiating FeatureStoreManger!')
    feature_store_manager=FeatureStoreManager()

    logger.info(f'trying to load datatypes directly from Dataframe')

    # Load the feature definitions from input schema. The feature definitions can be used to create a feature group
    feature_definitions = feature_store_manager.load_feature_definitions_from_schema(df)
    logger.info(f'Feature definitions loaded successfully!')
    print(feature_definitions)
    feature_group_arn = args.feature_group_arn
    logger.info(f'Feature Group ARN supplied is: {feature_group_arn}')

    # If only OfflineStore is selected, the connector will batch write the data to offline store directly
    args.target_feature_store_list = ast.literal_eval(args.target_feature_store_list)
    logger.info(f'Ingesting into the following stores: {args.target_feature_store_list}')

    feature_store_manager.ingest_data(input_data_frame=df, feature_group_arn=feature_group_arn, target_stores= args.target_feature_store_list) 
    logger.info(f'Feature Ingestions successful!')

def scale_col(df: DataFrame, col_name: str) -> DataFrame:
    unlist = udf(lambda x: round(float(list(x)[0]), 2), DoubleType())
    assembler = VectorAssembler(inputCols=[col_name], outputCol=f'{col_name}_vec')
    # scale an column col_name with minmax scaler and drop the original column

    scaler = MinMaxScaler(inputCol=f'{col_name}_vec', outputCol=f'{col_name}_scaled')
    pipeline = Pipeline(stages=[assembler, scaler])
    df = pipeline.fit(df).transform(df).withColumn(f'{col_name}_scaled', unlist(f'{col_name}_scaled')) \
                                       .drop(f'{col_name}_vec')
    df = df.drop(col_name)
    df = df.withColumnRenamed(f'{col_name}_scaled', col_name)
    return df

def ordinal_encode_col(df: DataFrame, col_name: str) -> DataFrame:
    indexer = StringIndexer(inputCol=col_name, outputCol=f'{col_name}_new')
    df = indexer.fit(df).transform(df)
    df = df.drop(col_name)
    df = df.withColumnRenamed(f'{col_name}_new', col_name)
    return df

def run_spark_job():

    args = parse_args()
   
    spark = SparkSession.builder.getOrCreate()
    
    # set the legacy time parser policy to LEGACY to allow for parsing of dates in the format dd/MM/yyyy HH:mm:ss, which solves backwards compatibility issues to spark 2.4
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    logger.info(f'Using Spark-Version:{spark.version}')

    # get the total number of cores in the Spark cluster; if developing locally, there might be no executor
    try:
        spark_context = spark.sparkContext
        total_cores = int(spark_context._conf.get('spark.executor.instances')) * int(spark_context._conf.get('spark.executor.cores'))
        logger.info(f'Total available cores in the Spark cluster = {total_cores}')
    except:
        total_cores = 1
        logger.error(f'Could not retrieve number of total cores. Setting total cores to 1. Error message: {str(e)}')
       
    logger.info(f'Reading input file from S3. S3 uri is {args.s3_uri_prefix}')

    # define the schema of the input data
    csvSchema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("purchase_amount", FloatType(), False),
        StructField("is_reordered", IntegerType(), False),
        StructField("purchased_on", StringType(), False),
        StructField("event_time", StringType(), False)])

    # read the pyspark dataframe with a schema 
    df = spark.read.option("header", "true").schema(csvSchema).csv(args.s3_uri_prefix)  

    # transform 1 - encode boolean to int
    df = ordinal_encode_col(df, 'is_reordered')
    df = df.withColumn('is_reordered', df['is_reordered'].cast(IntegerType()))

    # transform 2 - min max scale `purchase_amount`
    df = df.withColumn('purchase_amount', df['purchase_amount'].cast(DoubleType()))
    df = scale_col(df, 'purchase_amount')
    
    # transform 3 - derive `n_days_since_last_purchase` column using the `purchased_on` col
    current_date = datetime.today().strftime('%Y-%m-%d')
    df = df.withColumn('n_days_since_last_purchase', datediff(to_date(lit(current_date)), to_date('purchased_on', 'yyyy-MM-dd')))
    df = df.drop('purchased_on')
    df = scale_col(df, 'n_days_since_last_purchase')
    
    
    logger.info(f'Number of partitions = {df.rdd.getNumPartitions()}')
    # Rule of thumb heuristic - rely on the product of #executors by #executor.cores, and then multiply that by 3 or 4
    df = df.repartition(total_cores * 3)
    logger.info(f'Number of partitions after re-partitioning = {df.rdd.getNumPartitions()}')
    logger.info(f'Feature Store ingestion start: {datetime.now().strftime("%m/%d/%Y, %H:%M:%S")}')
    batch_ingest_to_feature_store(args, df)
    logger.info(f'Feature Store ingestion complete: {datetime.now().strftime("%m/%d/%Y, %H:%M:%S")}')

if __name__ == '__main__':
    logger.info('BATCH INGESTION - STARTED')
    run_spark_job()
    logger.info('BATCH INGESTION - COMPLETED')
