import pandas as pd
import requests
import calendar
import datetime
import os
from datetime import datetime
from datetime import date

import petaldata
from petaldata import util

class Contact(object):
  RESOURCE_URL = '/hubspot/contacts'

  def __init__(self,pickle_filename="hubspot_contact.pkl"):
    self.csv_filename = None
    self.pickle_filename = petaldata.cache_dir + pickle_filename
    self.df = None
    self.__metadata = None
    pass

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

  def pickle_file_exists(self):
    return os.path.isfile(self.pickle_filename)

  @property
  def metadata(self):
    if self.__metadata is None:
      self.__metadata = self.get_metadata()

    return self.__metadata

  def get_metadata(self):
    r = requests.get(self.api_url+"/metadata.pandas",  headers=self.request_headers)
    r.raise_for_status()
    metadata = r.json()
    print("Loaded Hubspot metadata w/keys=",list(metadata.keys()))
    return metadata

  @property
  def request_headers(self):
    return {
      "Authorization": "Bearer %s" % (petaldata.api_key,),
      'HUBSPOT_API_KEY': petaldata.datasets.hubspot.api_key
    }

  def load(self):
    # TODO - attempt to load from CSV file, which could happen with an incomplete download
    self.load_dataframe_from_pickle()
    if self.df is None:
      self.download()
      self.load_from_download()

  def update(self):
    contact_new = Contact()
    contact_new.download(created_gt=self.updated_at)
    contact_new.load_from_download() # don't want to save a pickle! would override existing.
    self.df = pd.concat([self.df,contact_new.df], verify_integrity=True)
    new_contacts_count = contact_new.df.shape[0]
    if new_contacts_count > 0:
      print("Added {} new contacts. Now with {} total contacts.".format(new_contacts_count, self.df.shape[0]))
      print("Time Range:",self.df.createdate.min(), "-", self.df.createdate.max())
      self.save()
    else:
      print("No new contacts.")
    return self

  def save(self):
    self.df.to_pickle(self.pickle_filename)
    print("Saved Dataframe to pickle. Size (MB)=",util.file_size_in_mb(self.pickle_filename))
    return self.pickle_filename

  def load_dataframe_from_pickle(self):
    if not self.pickle_file_exists():
      print("Pickle file does not exist at",self.pickle_filename)
      return None
    print("Loading {} MB Pickle file...".format(util.file_size_in_mb(self.pickle_filename)))
    print("\t...",self.pickle_filename)
    self.df = pd.read_pickle(self.pickle_filename)
    print("\t...Done. Dataframe Shape:",self.df.shape)
    print("\tTime Range:",self.df.createdate.min(), "-", self.df.createdate.max())
    return self.df

  @property
  def updated_at(self):
    return self.df.createdate.max()

  def load_from_download(self):
    print("Loading {} MB CSV file...".format(util.file_size_in_mb(self.csv_filename)))
    dataframe = pd.read_csv(self.csv_filename,parse_dates = self.metadata.get("convert_dates"))
    dataframe.set_index(self.metadata.get("index"),inplace=True)
    dataframe = self.__set_date_tz(dataframe)
    self.df = dataframe
    print("\t...Done. Dataframe Shape:",self.df.shape)
    contacts_count = self.df.shape[0]
    if contacts_count > 0:
      print("\tTime Range:",self.df.createdate.min(), "-", self.df.createdate.max())
    return self.df

  def __csv_download_filename(self,start_time):
    return petaldata.cache_dir + "hubspot_contact_" + start_time.strftime("%Y%m%d-%H%M%S") + ".csv"

  def download(self,created_gt=None,_offset=None):
    first_chunk = True
    start_time = datetime.now()
    filename = self.__csv_download_filename(start_time)
    with requests.get(self.api_url+".csv", headers=self.request_headers, params=self.__request_params(created_gt,_offset), stream=True) as r:
        r.raise_for_status()
        with open(filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=None):
                # TODO - how to handle 500 errors in chunks?
                if chunk: # filter out keep-alive new chunks
                    if (first_chunk):
                        first_chunk = False
                        print("Starting download...will update progress every 25 MB of data transfer.")
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

  def __set_date_tz(self,dataframe):
    for col in self.metadata.get("convert_dates"):
        if dataframe[col].count() > 0:
            # Hubspot sends down tz already (ex: "2019-04-22T00:00:00Z")
            # Strips out tz info so don't have to worry about comparing tz-aware datetimes
            # df[col] = pd.to_datetime(df_payments[col], utc=True).dt.tz_convert(None)
            dataframe[col] = dataframe[col].dt.tz_convert(None)
    return dataframe

  def __request_params(self,created_gt,_offset):
    params = {}
    if created_gt is not None:
       params['created_gt'] = calendar.timegm(created_gt.timetuple())
    if _offset is not None:
        params['_offset'] = _offset
    return params
