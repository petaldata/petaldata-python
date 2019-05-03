import sys
sys.path.append("/Users/dlite/projects/petaldata-python")
import petaldata

from dotenv import load_dotenv
load_dotenv(override=True)
import os

import smart_open
import pandas as pd
import boto3
import pickle

bucket = 'petaldata-test2'
petaldata.storage.S3.aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
petaldata.storage.S3.aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
s3 = petaldata.storage.S3(bucket)
print(s3.list_permissions())

















