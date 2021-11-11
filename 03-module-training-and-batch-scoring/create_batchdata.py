import argparse
import uuid
from pathlib import Path

import pandas as pd

# Parse argument variables passed via the CreateDataset processing step
parser = argparse.ArgumentParser()
parser.add_argument("--athena-data", type=str)
args = parser.parse_args()

dataset_path = Path("/opt/ml/processing/output/dataset")
dataset = pd.read_parquet(args.athena_data, engine="pyarrow")

# Write train, test splits to output path
dataset_output_path = Path("/opt/ml/processing/output/dataset")
dataset.to_csv(
    dataset_output_path / f"dataset-{uuid.uuid4()}.csv", index=False, header=False
)
