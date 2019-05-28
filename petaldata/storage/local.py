import os
from petaldata.storage.abstract_storage import AbstractStorage
from petaldata import util
import pandas as pd

class Local(AbstractStorage):
  """
  Local Storage abstracts local file access patterns.
  """
  enabled = True
  dir = None

  DEFAUL_DIRECTORY_NAME = "petaldata_cache"

  def __init__(self,base_pickle_filename):
    """
    Initializes local storage, creating `petaldata.Local.dir` if it doesn't exist. If `petaldata.Local.dir` isn't specified,
    it is set to `petaldata.Local.default_directory_path`.
    """
    if (self.dir == None): self.dir = self.default_directory_path
    self._create_dir()
    super().__init__(base_pickle_filename)

  @property
  def default_directory_path(self):
    return os.getcwd() + "/{}/".format(self.DEFAUL_DIRECTORY_NAME)

  def _create_dir(self):
    """
    Creates `petaldata.Local.dir` if it doesn't exist.
    """
    if not self.dir_exists():
      print("Creating local storage directory={}".format(self.dir))
      os.makedirs(self.dir)

  def available(self):
    res = self.dir_exists()
    if res == False:
      print("The local storage directory does not exist at", self.dir)
    return res

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
