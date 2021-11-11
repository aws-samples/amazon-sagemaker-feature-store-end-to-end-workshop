def apply_transforms(spark: SparkSession, df: DataFrame, fg_name: str) -> DataFrame:
    df.createOrReplaceTempView(fg_name)
    query = transform_query(fg_name)
    print(query)
    return spark.sql(query)

def run_spark_job():
    args = parse_args()
    spark_session = SparkSession.builder.appName('PySparkJob').getOrCreate()
    spark_context = spark_session.sparkContext
    total_cores = int(spark_context._conf.get('spark.executor.instances')) * int(spark_context._conf.get('spark.executor.cores'))
    logger.info(f'Total available cores in the Spark cluster = {total_cores}')
    logger.info('Reading input file from S3')
    df = spark_session.read.options(Header=True).csv(args.s3_uri_prefix)
    
    # transform raw features 
    df = apply_transforms(spark_session, df, args.feature_group_name.replace('-', '_'))
    df.show()

    # ingest transformed features
    logger.info(f'Number of partitions = {df.rdd.getNumPartitions()}')
    # Rule of thumb heuristic - rely on the product of #executors by #executor.cores, and then multiply that by 3 or 4
    df = df.repartition(total_cores * 3)
    logger.info(f'Number of partitions after re-partitioning = {df.rdd.getNumPartitions()}')
    logger.info(f'Feature Store ingestion start: {datetime.now().strftime("%m/%d/%Y, %H:%M:%S")}')
    df.foreachPartition(lambda rows: ingest_to_feature_store(args, rows))
    logger.info(f'Feature Store ingestion complete: {datetime.now().strftime("%m/%d/%Y, %H:%M:%S")}')


if __name__ == '__main__':
    logger.info('BATCH INGESTION - STARTED')
    run_spark_job()
    logger.info('BATCH INGESTION - COMPLETED')