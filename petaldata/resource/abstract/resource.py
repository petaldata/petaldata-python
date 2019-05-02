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
      print("\t...Done. Dataframe Shape:",self.df.shape)
      return self.df

  # TODO - rename to merge?
  # https://en.wikipedia.org/wiki/Merge_(SQL)
  def update(self, created_gt=None):
    print("Updating...")
    if created_gt is None:
      print("\tSetting created_gt=",self.updated_at)
      created_gt = self.updated_at
    else:
      print("\tcreated >",created_gt)
    new = type(self)()
    new.download(created_gt=created_gt)
    new.load_from_download() # don't want to save a pickle! would override existing.
    self.upsert(new)
    return self

  def upsert(self,other_resource):
    old_count = self.df.shape[0]
    # only concat if rows found, otherwise dtypes can change
    # https://stackoverflow.com/questions/33001585/pandas-dataframe-concat-update-upsert
    if other_resource.df.shape[0] > 0:
      print("\tInserting new rows")
      print("created before concat:",self.df.created.dtype,self.df.created.head(5))
      print("other created before concat:",other_resource.df.created.dtype,other_resource.df.created.head(5))
      df = pd.concat([self.df, other_resource.df[~other_resource.df.index.isin(self.df.index)]])
      print("created after concat:",df.created.dtype,df.created.head(5))
      print("\tUpdating existing rows")
      df.update(other_resource.df)
      # timezone is stripped after update. add back.
      df = self.set_date_tz(df)
      print("created after set_date_z:",df.created.dtype,df.created.head(5))
      self.df = df

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

  def load_from_download(self,filename=None):
    if filename is None:
      filename = self.csv_filename
    else:
      filename = self.local.dir + filename
    print("Loading {} MB CSV file...".format(Local.file_size_in_mb(filename)))
    dataframe = pd.read_csv(filename,parse_dates = self.metadata.get("convert_dates"))
    dataframe.set_index(self.metadata.get("index"),inplace=True)
    print("created from download:",dataframe.created.dtype,dataframe.created.head(3))
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
    if self.s3: self.s3.save(self)

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

  @staticmethod
  def set_date_tz(dataframe):
    for col in dataframe.columns:
      # this is a tz-naive timestamp. w/a tz would look like "datetime64[ns, UTC]"
      if dataframe[col].dtype == 'datetime64[ns]':
        dataframe[col] = dataframe[col].dt.tz_localize('UTC')
        # Strips out tz info so don't have to worry about comparing tz-aware datetimes
        # dataframe[col] = dataframe[col].dt.tz_convert(None)
    return dataframe

  @property
  def api_url(self):
    return petaldata.api_base + self.RESOURCE_URL