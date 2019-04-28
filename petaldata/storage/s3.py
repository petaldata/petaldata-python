import boto3
import io
import smart_open
import pickle
import pandas as pd
from petaldata.storage.abstract_storage import AbstractStorage

class S3(AbstractStorage):
  enabled = False
  aws_access_key_id = None
  aws_secret_access_key = None
  bucket_name = None

  def __init__(self,base_pickle_filename):
    self.__client = None
    self.__resource = None
    super().__init__(base_pickle_filename)

  def available(self):
    return self.list_permissions();

  def read_pickle_dataframe(self):
    if not self.pickle_file_exists():
      print("\tS3 Pickle file does not exist at",self.pickle_uri())
      return None
    else:
      print("\tS3 Pickle exists at",self.pickle_uri())
      print("\tS3 Loading {} MB Pickle file...".format(self.file_size_in_mb(self.base_pickle_filename)))
      return self.stream_and_read_pickle()

  def save(self,resource):
    print("\tSaving S3 file...")
    pickle_buffer = io.BytesIO()
    resource.df.to_pickle(pickle_buffer,compression=None)
    obj = self.resource().Object(self.bucket_name, self.base_pickle_filename).put(Body=pickle_buffer.getvalue())
    print("\t...Done. Size (MB)=",self.file_size_in_mb(self.base_pickle_filename))
    return obj

  def lookup(self,key):
    return self.bucket().Object(key)

  def delete(self):
    if self.pickle_file_exists():
      obj = self.lookup(self.base_pickle_filename)
      print("S3 Deleting pickle file.\n\tSize (MB)=",self.bytes_to_mb(obj.content_length))
      return obj.delete()

  def file_size_in_mb(self,key):
    return self.bytes_to_mb(self.lookup(key).content_length)

  def session(self):
    return boto3.Session(
      aws_access_key_id=self.aws_access_key_id,
      aws_secret_access_key=self.aws_secret_access_key
    )

  def pickle_uri(self):
    return "s3://{}/{}".format(self.bucket_name,self.base_pickle_filename)

  def stream_and_read_pickle(self):
    f=smart_open.open(self.pickle_uri(), 'rb', transport_params=dict(session=self.session()))
    pickle.loads(f.read())
    return pd.read_pickle(f, compression=None)

  # https://stackoverflow.com/questions/47056628/verifying-s3-credentials-w-o-get-or-put-using-boto3
  def list_permissions(self):
    try:
      response = self.client().head_bucket(Bucket=self.bucket_name)
      # see response['ResponseMetadata']['HTTPStatusCode']
      return True
    except Exception as e:
      print("Error trying to fetch bucket=",self.bucket_name,"Error:",e)
      return False

  def client(self):
    if self.__client is None:
      self.__client = boto3.client('s3', aws_access_key_id=self.aws_access_key_id, aws_secret_access_key=self.aws_secret_access_key)
    return self.__client

  def resource(self):
    if self.__resource is None:
      self.__resource = boto3.resource('s3', aws_access_key_id=self.aws_access_key_id, aws_secret_access_key=self.aws_secret_access_key)
    return self.__resource

  def bucket(self):
    return self.resource().Bucket(self.bucket_name)

  def pickle_file_exists(self):
    return self.file_exists(self.bucket(),self.base_pickle_filename)

  @staticmethod
  def file_exists(bucket,key):
    objs = list(bucket.objects.filter(Prefix=key))
    if len(objs) > 0 and objs[0].key == key:
        return True
    else:
        return False