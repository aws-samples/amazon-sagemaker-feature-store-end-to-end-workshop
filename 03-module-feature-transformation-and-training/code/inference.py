from io import BytesIO
from typing import BinaryIO
import pandas as pd
from botocore.response import StreamingBody
import xgboost as xgb
import numpy as np
import os
import pickle as pkl

PKL_FORMAT = 'pkl_format'
XGB_FORMAT = 'xgb_format'

def get_loaded_booster(model_dir):
    model_files = (data_file for data_file in os.listdir(model_dir)
                   if os.path.isfile(os.path.join(model_dir, data_file)))
    model_file = next(model_files)
    try:
        booster = pkl.load(open(os.path.join(model_dir, model_file), 'rb'))
        format = PKL_FORMAT
    except Exception as exp_pkl:
        try:
            booster = xgb.Booster()
            booster.load_model(os.path.join(model_dir, model_file))
            format = XGB_FORMAT
        except Exception as exp_xgb:
            raise RuntimeError("Model at {} cannot be loaded:\n{}\n{}".format(model_dir, str(exp_pkl), str(exp_xgb)))
    booster.set_param('nthread', 1)
    return booster, format

def model_fn(model_dir):
    """Load a model. For XGBoost Framework, a default function to load a model is not provided.
    Users should provide customized model_fn() in script.
    Args:
        model_dir: a directory where model is saved.
    Returns: A XGBoost model.
    """
    try:
        booster, format = get_loaded_booster(model_dir)
    except Exception as e:
        raise RuntimeError("Unable to load model: {}".format(str(e)))
    return booster, format
            
def input_fn(
  serialized_input_data: StreamingBody,
  content_type: str = "application/x-parquet",
) -> pd.DataFrame:
    """Deserialize inputs"""
    if content_type == "application/x-parquet":
        data = BytesIO(serialized_input_data)
        df = pd.read_parquet(data, engine='pyarrow')
        df = df.to_numpy()
        
        return df
    else:
        raise ValueError(
        "Expected `application/x-parquet`."
        )


def predict_fn(input_data, model):
    """A default predict_fn for XGBooost Framework. Calls a model on data deserialized in input_fn.
    Args:
        input_data: input data (DMatrix) for prediction deserialized by input_fn
        model: XGBoost model loaded in memory by model_fn
    Returns: a prediction
    """
    booster, model_format = model
    dmatrix = xgb.DMatrix(input_data[:, 2:])
    output = booster.predict(dmatrix,
                           ntree_limit=getattr(booster, "best_ntree_limit", 0),
                           validate_features=False)
    final_out = np.append(input_data, [[x] for x in output], axis=1)
    return final_out