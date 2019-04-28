import os
from petaldata.storage.abstract_storage import AbstractStorage
from petaldata import util
import pandas as pd

class Local(AbstractStorage):
  enabled = True
  dir = None

  def available(self):
    return self.dir_exists()

  def dir_exists(self):
    return os.path.isdir(self.dir)

  def read_pickle_dataframe(self):
    if not self.pickle_file_exists():
      print("\tLocal Pickle file does not exist at",self.pickle_filename)
      return None
    print("\tLoading {} MB Pickle file...".format(self.file_size_in_mb(self.pickle_filename)))
    print("\t...",self.pickle_filename)
    self.df = pd.read_pickle(self.pickle_filename)
    return self.df

  def save(self,resource):
    print("\tSaving local file...")
    resource.df.to_pickle(self.pickle_filename)
    print("\t...Done. Size (MB)=",self.file_size_in_mb(self.pickle_filename))
    return self.pickle_filename

  def delete(self):
    if self.pickle_file_exists():
      print("\tDeleting local pickle file.\n\tSize (MB)=",self.file_size_in_mb(self.pickle_filename),"\n\t",self.pickle_filename)
      os.remove(self.pickle_filename)
    else:
      print("\tNo pickle file exists at",self.pickle_filename)

  @property
  def pickle_filename(self):
    return self.dir + self.base_pickle_filename

  def pickle_file_exists(self):
    return os.path.isfile(self.pickle_filename)

  @staticmethod
  def file_size_in_mb(filename):
    return AbstractStorage.bytes_to_mb(os.path.getsize(filename))

  def csv_download_filename(self,csv_file_prefix,start_time):
    return self.dir + csv_file_prefix + start_time.strftime("%Y%m%d-%H%M%S") + ".csv"
