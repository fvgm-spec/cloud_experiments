#Importing required libraries
import os
import pandas as pd
from smart_open import smart_open

aws_key = os.environ['AWS_ACCESS_KEY_ID']
aws_secret = os.environ['AWS_SECRET_ACCESS_KEY']

def read_csv_from_bucket(bucket_name :str,
                         object_key: str) -> pd.DataFrame:

    path = 's3://{}:{}@{}/{}.csv'.format(aws_key, aws_secret, bucket_name, object_key)
    df = pd.read_csv(smart_open(path))

    return df

