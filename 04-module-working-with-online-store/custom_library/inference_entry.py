
import json
from io import StringIO
import os
import pickle as pkl
import joblib
import time
import sys
import subprocess
import numpy as np
from sagemaker_containers.beta.framework import (
    content_types,
    encoders,
    env,
    modules,
    transformer,
    worker,
)


import pandas as pd
import boto3
import sagemaker
import helper
import sagemaker_xgboost_container.encoder as xgb_encoders

boto_session = boto3.Session()
region= boto_session.region_name
print(region)



feature_list=['fscw-products-10-18-00-12:*','fscw-customers-10-18-00-12:*']

import json
import os
import pickle as pkl

import numpy as np
import sagemaker_xgboost_container.encoder as xgb_encoders


def model_fn(model_dir):
    """
    Deserialize and return fitted model.
    """
    model_file = "xgboost-model"
    booster = pkl.load(open(os.path.join(model_dir, model_file), "rb"))
    return booster

def input_fn(request_body, request_content_type):
    print(request_content_type)
    """
    The SageMaker XGBoost model server receives the request data body and the content type,
    and invokes the `input_fn`.
    Return a DMatrix (an object that can be passed to predict_fn).
    """
    print(request_content_type)
    if request_content_type == "text/csv":
        params =request_body.split(',')
        id_dict={'customer_id':params[0], 'product_id':params[1]}
        start = time.time()
        rec=f'{id_dict}, {feature_list}'
        records= helper.get_latest_featureset_values_e(id_dict, feature_list)
        end= time.time()
        duration= end-start
        print ("time to lookup features from two feature stores:", duration)
        records= ",".join([str(e) for e in records.values()])
        return xgb_encoders.csv_to_dmatrix(records)
    else:
        raise ValueError("{} not supported by script!".format(request_content_type))
        
    
