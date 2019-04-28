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

# https://pypi.org/project/smart-open/

session = boto3.Session(
    aws_access_key_id="AKIAWOLBJJZJJAIIO33L",
    aws_secret_access_key="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"
)

# pd.read_pickle(smart_open.open("s3://petaldata-test2/card.pkl", transport_params=dict(session=session)))

# can't load pickle ...
# for line in smart_open.open("s3://petaldata-test2/card.pkl", transport_params=dict(session=session)):
#   print(repr(line))
#   break

# f=smart_open.open("s3://petaldata-test2/card.pkl", 'rb', transport_params=dict(session=session))
# # pickle.loads(f)
# df=pd.read_pickle(f, compression=None)
# print(df.info)

# https://gist.github.com/uhho/a1490ae2abd112b556dcd539750aa151
# using csv ... says dtypes preserved? 

# persisting in a csv
# https://stackoverflow.com/questions/50047237/how-to-preserve-dtypes-of-dataframes-when-using-to-csv

# saving pickle to s3
# https://stackoverflow.com/questions/49120069/writing-a-pickle-file-to-an-s3-bucket-in-aws

bucket = 'petaldata-test2'
petaldata.storage.S3.aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
petaldata.storage.S3.aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
s3 = petaldata.storage.S3(bucket)
print(s3.list_permissions())

















