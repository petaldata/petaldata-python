import pandas as pd

class AbstractStorage(object):

  @classmethod
  def if_enabled(cls,base_pickle_filename):
    if (cls.enabled == True):
      return cls(base_pickle_filename)
    else:
      return None

  def __init__(self,base_pickle_filename):
    """
    Initializes storage, raising an exception if the storage isn't available.
    """
    self.base_pickle_filename = base_pickle_filename
    assert(self.available()), "{} storage isn't available.".format(self.__class__)

  def available(self):
    pass

  def pickle_file_exists(self):
    pass

  @staticmethod
  def bytes_to_mb(bytes):
    return round(bytes/(1024*1024.0),2)
