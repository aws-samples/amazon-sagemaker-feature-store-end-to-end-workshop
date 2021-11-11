
def get_file_paths(directory):
    file_paths = [] 
    for root, directories, files in os.walk(directory):
        for file_name in files:
            if file_name.endswith('.csv'):
                file_path = os.path.join(root, file_name)
                file_paths.append(file_path)  
    return file_paths

def ingest_data(args: argparse.Namespace) -> None:
    boto_session = boto3.session.Session(region_name=args.region_name)
    sagemaker_session = sagemaker.Session(boto_session=boto_session)

    files = get_file_paths('/opt/ml/processing/input/')
    logger.info(f'Files: {files}')
    if len(files) > 0:
        df = pd.concat([pd.read_csv(file) for file in files], ignore_index=True)
        df = apply_transforms(df)
        logger.info(f'Ingesting a total of [{df.shape[0]}] rows from {len(files)} files')
        logger.info(f'first few rows:\n{df.head()}')
        logger.info(f'Ingesting into feature group [{args.feature_group_name}] using {args.num_processes} processes and {args.num_workers} workers')
        fg = FeatureGroup(name=args.feature_group_name, sagemaker_session=sagemaker_session)
        fg.ingest(data_frame=df, max_processes=args.num_processes, max_workers=args.num_workers, wait=True)
        logger.info('fg.ingest() finished')
    else:
        logger.info(f'No files to ingest. Exiting successfully.')
    return
    
def parse_args() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--num_processes', type=int, default=1)
    parser.add_argument('--num_workers', type=int, default=1)
    parser.add_argument('--feature_group_name', type=str)
    parser.add_argument('--region_name', type=str)
    args, _ = parser.parse_known_args()
    return args


if __name__ == '__main__':
    logger.info('BATCH INGESTION - STARTED')
    args = parse_args()
    ingest_data(args)
    logger.info('BATCH INGESTION - COMPLETED')
