import pandas as pd
import requests
import calendar
import datetime
import os
from datetime import datetime
from datetime import date

import petaldata
from petaldata import util

class Resource(object):
  def __init__(self,pickle_filename="stripe_invoice.pkl"):
    self.csv_filename = None
    self.pickle_filename = petaldata.cache_dir + pickle_filename
    self.df = None
    self.__metadata = None
    pass

  def load(self):
    # TODO - attempt to load from CSV file, which could happen with an incomplete download
    self.load_dataframe_from_pickle()
    if self.df is None:
      self.download()
      self.load_from_download()

  def load_dataframe_from_pickle(self):
    if not self.pickle_file_exists():
      print("Pickle file does not exist at",self.pickle_filename)
      return None
    print("Loading {} MB Pickle file...".format(util.file_size_in_mb(self.pickle_filename)))
    print("\t...",self.pickle_filename)
    self.df = pd.read_pickle(self.pickle_filename)
    print("\t...Done. Dataframe Shape:",self.df.shape)
    print("\tTime Range:",self.df[self.CREATED_AT_FIELD].min(), "-", self.df[self.CREATED_AT_FIELD].max())
    return self.df

  def load_from_download(self):
    print("Loading {} MB CSV file...".format(util.file_size_in_mb(self.csv_filename)))
    dataframe = pd.read_csv(self.csv_filename,parse_dates = self.metadata.get("convert_dates"))
    dataframe.set_index(self.metadata.get("index"),inplace=True)
    # How??? TypeError: Already tz-aware, use tz_convert to convert.
    # dataframe = self.__set_date_tz(dataframe)
    self.df = dataframe
    print("\t...Done. Dataframe Shape:",self.df.shape)
    count = self.df.shape[0]
    if count > 0:
      print("\tTime Range:",self.df[self.CREATED_AT_FIELD].min(), "-", self.df[self.CREATED_AT_FIELD].max())
    return self.df

  def __csv_download_filename(self,start_time):
    return petaldata.cache_dir + self.CSV_FILE_PREFIX + start_time.strftime("%Y%m%d-%H%M%S") + ".csv"

  def __request_params(self,created_gt,_offset):
    pass

  def delete(self):
    if os.path.exists(self.pickle_filename):
      print("Deleting pickle file.\n\tSize (MB)=",util.file_size_in_mb(self.pickle_filename),"\n\t",self.pickle_filename)
      os.remove(self.pickle_filename)
    else:
      print("No pickle file exists at",self.pickle_filename)

  def reset(self):
    self.delete()
    self.df = None
    self.__metadata = None
    return self


  def save(self):
    self.df.to_pickle(self.pickle_filename)
    print("Saved Dataframe to pickle. Size (MB)=",util.file_size_in_mb(self.pickle_filename))
    return self.pickle_filename

  def download(self,created_gt=None,_offset=None):
    first_chunk = True
    start_time = datetime.now()
    filename = self.__csv_download_filename(start_time)
    print("Starting download to",filename,"...")
    with requests.get(self.api_url+".csv", headers=self.request_headers, params=self.__request_params(created_gt,_offset), stream=True) as r:
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
                    size_in_mb = util.file_size_in_mb(filename)
                    if ((size_in_mb > 1) & (len(chunk) > 1000) & (size_in_mb % 25 == 0) ):
                        print("\tDownloaded", size_in_mb, "MB...")

    size_in_mb = util.file_size_in_mb(filename)
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

  def pickle_file_exists(self):
    return os.path.isfile(self.pickle_filename)

  @property
  def api_url(self):
    return petaldata.api_base + self.RESOURCE_URL

  @classmethod
  def from_pickle(cls, filename):
    cache_dir = os.path.dirname(os.path.realpath(filename))
    petaldata.cache_dir = cache_dir
    instance = cls(pickle_filename=filename)
    instance.load_dataframe_from_pickle()

    return instance