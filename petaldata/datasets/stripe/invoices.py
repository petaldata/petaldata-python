import petaldata
from petaldata.datasets.abstract import Dataset
import calendar

class Invoices(Dataset):
  RESOURCE_URL = '/stripe/invoices'
  CSV_FILE_PREFIX = "stripe_invoices"

  @property
  def request_headers(self):
    return {
      "Authorization": "Bearer %s" % (petaldata.api_key,),
      'STRIPE_API_KEY': petaldata.datasets.stripe.api_key
    }

  def request_params(self,created_gt,limit,_offset):
    params = {}
    if created_gt is not None:
       params['created_gt'] = calendar.timegm(created_gt.timetuple())
    if limit is not None:
       params['limit'] = limit
    if _offset is not None:
        params['_offset'] = _offset
    return params
