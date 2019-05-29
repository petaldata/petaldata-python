import petaldata
from petaldata.datasets.abstract import Dataset
import calendar

class Contacts(Dataset):
  RESOURCE_URL = '/hubspot/contacts'
  CSV_FILE_PREFIX = "hubspot_contacts"

  @property
  def request_headers(self):
    return {
      "Authorization": "Bearer %s" % (petaldata.api_key,),
      'HUBSPOT_API_KEY': petaldata.datasets.hubspot.api_key
    }

  def default_base_pickle_filename(self):
    return self.CSV_FILE_PREFIX+".pkl"

  def request_params(self,created_gt,limit,_offset):
    params = {}
    if created_gt is not None:
       params['created_gt'] = calendar.timegm(created_gt.timetuple())
    if limit is not None:
       params['limit'] = limit
    if _offset is not None:
        params['_offset'] = _offset
    return params
