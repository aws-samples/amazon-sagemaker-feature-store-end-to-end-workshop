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

logger = logging.getLogger('__name__')
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


label_encoder = LabelEncoder()
min_max_scaler = MinMaxScaler()
