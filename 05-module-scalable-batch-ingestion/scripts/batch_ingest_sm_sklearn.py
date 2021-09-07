import subprocess
import sys
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'sagemaker'])

from sagemaker.feature_store.feature_group import FeatureGroup
from sklearn.preprocessing import MinMaxScaler, LabelEncoder
from datetime import datetime, timezone, date
import pandas as pd
import sagemaker
import argparse
import logging
import boto3
import time
import os


boto_session = boto3.session.Session(region_name='us-east-1')
sagemaker_session = sagemaker.Session(boto_session=boto_session)

logger = logging.getLogger('__name__')
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


label_encoder = LabelEncoder()
min_max_scaler = MinMaxScaler()


def get_file_paths(directory):
    file_paths = [] 
    for root, directories, files in os.walk(directory):
        for file_name in files:
            if file_name.endswith('.csv'):
                file_path = os.path.join(root, file_name)
                file_paths.append(file_path)  
    return file_paths


def get_delta_in_days(date_time) -> int:
    today = date.today()
    delta = today - date_time.date()
    return delta.days


def apply_transforms(df: pd.DataFrame) -> pd.DataFrame:
    df['is_reordered'] = df['is_reordered'].astype(int)
    df['purchased_on'] =  pd.to_datetime(df['purchased_on'], format='%Y-%m-%d %H:%M:%S')
    df['n_days_since_last_purchase'] = df['purchased_on'].apply(lambda x: get_delta_in_days(x))
    df['n_days_since_last_purchase'] = min_max_scaler.fit_transform(df[['n_days_since_last_purchase']])
    df.drop('purchased_on', axis=1, inplace=True)
    return df


def ingest_data(args: argparse.Namespace) -> None:
    files = get_file_paths('/opt/ml/processing/input/')
    logger.info(f'Files: {files}')
    df = pd.concat([pd.read_csv(file) for file in files], ignore_index=True)
    df = apply_transforms(df)
    logger.info(f'Ingesting a total of [{df.shape[0]}] rows from {len(files)} files')
    logger.info(f'Ingesting into feature group [{args.feature_group_name}] using {args.num_processes} processes and {args.num_workers} workers')
    fg = FeatureGroup(name=args.feature_group_name, sagemaker_session=sagemaker_session)
    fg.ingest(data_frame=df, max_processes=args.num_processes, max_workers=args.num_workers, wait=False)
    
    
def parse_args() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--num_processes', type=int, default=1)
    parser.add_argument('--num_workers', type=int, default=1)
    parser.add_argument('--feature_group_name', type=str)
    args, _ = parser.parse_known_args()
    return args


if __name__ == '__main__':
    logger.info('BATCH INGESTION - STARTED')
    args = parse_args()
    ingest_data(args)
    logger.info('BATCH INGESTION - COMPLETED')
