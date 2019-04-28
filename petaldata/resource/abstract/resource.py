import pandas as pd
import requests
import datetime
import os
from datetime import datetime
from datetime import date

import petaldata
from petaldata import util
from petaldata.storage import *

class Resource(object):
  def __init__(self,base_pickle_filename="stripe_invoice.pkl"):
    self.csv_filename = None
    # self.base_pickle_filename = base_pickle_filename
    # self.pickle_filename = petaldata.cache_dir + base_pickle_filename
    self.df = None
    self.__metadata = None
    self.local = Local(base_pickle_filename)
    self.s3 = S3.if_enabled(base_pickle_filename)

  def load(self):
    print("Attempting to load saved pickle file...")
    if (self.df is None) & (self.local.enabled == True): self.df = self.local.read_pickle_dataframe() 
    if (self.df is None) & (self.s3 is not None): self.df = self.s3.read_pickle_dataframe() 

    if self.df is None:
      print("\tNo cached files exist.")
      self.download()
      self.load_from_download()

    if self.df is None:
      print("\tUnable to load dataframe.")
    else:
      return self.df

  def update(self):
    print("Updating...")
    print("\t Most recent row=",self.updated_at)
    new = type(self)()
    new.download(created_gt=self.updated_at)
    new.load_from_download() # don't want to save a pickle! would override existing.
    old_count = self.df.shape[0]
    if new.df.shape[0] > 0:
      # only contact if rows found, otherwise dtypes can change
      self.df = pd.concat([self.df,new.df]).drop_duplicates().reset_index(drop=True)

    new_count = self.df.shape[0] - old_count
    if new_count > 0:
      print("Added {} new rows. Now with {} total rows.".format(new_count, self.df.shape[0]))
      print("\tTime Range:",self.df[self.CREATED_AT_FIELD].min(), "-", self.df[self.CREATED_AT_FIELD].max())
      # TODO - I think hubspot saves after update ... make consistent
    else:
      print("No new rows.")
    return self

  @property
  def updated_at(self):
    return self.df[self.CREATED_AT_FIELD].max()

  def load_from_download(self):
    print("Loading {} MB CSV file...".format(Local.file_size_in_mb(self.csv_filename)))
    dataframe = pd.read_csv(self.csv_filename,parse_dates = self.metadata.get("convert_dates"))
    dataframe.set_index(self.metadata.get("index"),inplace=True)
    dataframe = self.set_date_tz(dataframe)
    self.df = dataframe
    print("\t...Done. Dataframe Shape:",self.df.shape)
    count = self.df.shape[0]
    if count > 0:
      print("\tTime Range:",self.df[self.CREATED_AT_FIELD].min(), "-", self.df[self.CREATED_AT_FIELD].max())
    return self.df

  def request_params(self,created_gt,_offset):
    pass

  def reset(self):
    print("Resetting...")
    if self.local.enabled == True: self.local.delete()
    if self.s3: self.s3.delete()

    self.df = None
    self.__metadata = None
    return self

  def save(self):
    print("Saving to Pickle file...")
    if self.local.enabled == True: self.local.save(self)
    if s3: self.s3.save(self)

    return True

  def download(self,created_gt=None,_offset=None):
    first_chunk = True
    start_time = datetime.now()
    filename = self.local.csv_download_filename(self.CSV_FILE_PREFIX,start_time)
    print("Starting download to",filename,"...")
    with requests.get(self.api_url+".csv", headers=self.request_headers, params=self.request_params(created_gt,_offset), stream=True) as r:
        r.raise_for_status()
        with open(filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=None):
                # TODO - how to handle 500 errors in chunks?
                if chunk: # filter out keep-alive new chunks
                    if (first_chunk):
                        first_chunk = False
                        print("\t...will update progress every 25 MB of data transfer.")
                        print("\tSaving to", filename)
                    f.write(chunk)
                    size_in_mb = Local.file_size_in_mb(filename)
                    if ((size_in_mb > 1) & (len(chunk) > 1000) & (size_in_mb % 25 == 0) ):
                        print("\tDownloaded", size_in_mb, "MB...")

    size_in_mb = Local.file_size_in_mb(filename)
    time_delta = datetime.now() - start_time
    print("\t...Done.\n\tFile Size=",size_in_mb,"MB." " Total Time=", 
          round(time_delta.seconds/60.0,2), "minutes", "\n\tLocation:", filename)
    self.csv_filename = filename
    return self.csv_filename

  @property
  def metadata(self):
    if self.__metadata is None:
      self.__metadata = self.get_metadata()

    return self.__metadata

  def get_metadata(self):
    r = requests.get(self.api_url+"/metadata.pandas",  headers=self.request_headers)
    r.raise_for_status()
    metadata = r.json()
    print("Loaded metadata w/keys=",list(metadata.keys()))
    return metadata

  def set_date_tz(self,dataframe):
    for col in dataframe.columns:
      if dataframe[col].dtype == 'datetime64[ns]':
        # Sends down tz already (ex: "2019-04-22T00:00:00Z")
        # Strips out tz info so don't have to worry about comparing tz-aware datetimes
        dataframe[col] = dataframe[col].dt.tz_convert(None)
    return dataframe

  @property
  def api_url(self):
    return petaldata.api_base + self.RESOURCE_URL

  # @classmethod
  # def from_pickle(cls, filename):
  #   cache_dir = os.path.dirname(os.path.realpath(filename))
  #   petaldata.cache_dir = cache_dir
  #   instance = cls(pickle_filename=filename)
  #   instance.load_dataframe_from_pickle()

  #   return instance