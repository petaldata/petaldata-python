import petaldata
from petaldata.datasets.abstract import Dataset

class Card(Dataset):
  RESOURCE_URL = "/metabase/card/{id}"
  CSV_FILE_PREFIX = "metabase_card"

  def __init__(self,id):
    self.id = id
    super().__init__(base_pickle_filename=self.CSV_FILE_PREFIX+"_"+str(self.id)+".pkl")

  @property
  def api_url(self):
    return petaldata.api_base + self.RESOURCE_URL.format(id=self.id)

  @property
  def request_headers(self):
    return {
      "Authorization": "Bearer %s" % (petaldata.api_key,),
      "METABASE_URL": petaldata.datasets.metabase.url,
      "METABASE_USERNAME": petaldata.datasets.metabase.username,
      "METABASE_PASSWORD": petaldata.datasets.metabase.password,
      "METABASE_TOKEN": petaldata.datasets.metabase.token
    }

  def request_params(self,created_gt,limit,_offset):
    params = {}
    if created_gt is not None:
       params['created_gt'] = calendar.timegm(created_gt.timetuple())
    if _offset is not None:
        params['_offset'] = _offset
    return params
