import petaldata
from petaldata.resource.abstract import Resource

class Invoice(Resource):
  RESOURCE_URL = '/stripe/invoices'
  CREATED_AT_FIELD = 'created'
  CSV_FILE_PREFIX = "stripe_invoices_"

  @property
  def request_headers(self):
    return {
      "Authorization": "Bearer %s" % (petaldata.api_key,),
      'STRIPE-API-KEY': petaldata.resource.stripe.api_key
    }

  # def update(self):
  #   contact_new = Contact(self.cache_dir)
  #   contact_new.download(created_gt=self.updated_at)
  #   contact_new.load_from_download() # don't want to save a pickle! would override existing.
  #   self.df = pd.concat([self.df,contact_new.df], verify_integrity=True)
  #   new_contacts_count = contact_new.df.shape[0]
  #   if new_contacts_count > 0:
  #     print("Added {} new contacts. Now with {} total contacts.".format(new_contacts_count, self.df.shape[0]))
  #     print("Time Range:",self.df.createdate.min(), "-", self.df.createdate.max())
  #     self.save()
  #   else:
  #     print("No new contacts.")
  #   return self

  # @property
  # def updated_at(self):
  #   return self.df.createdate.max()

  # def load_from_download(self):
  #   print("Loading {} MB CSV file...".format(util.file_size_in_mb(self.csv_filename)))
  #   dataframe = pd.read_csv(self.csv_filename,parse_dates = self.metadata.get("convert_dates"))
  #   dataframe.set_index(self.metadata.get("index"),inplace=True)
  #   # How??? TypeError: Already tz-aware, use tz_convert to convert.
  #   # dataframe = self.__set_date_tz(dataframe)
  #   self.df = dataframe
  #   print("\t...Done. Dataframe Shape:",self.df.shape)
  #   contacts_count = self.df.shape[0]
  #   if contacts_count > 0:
  #     print("\tTime Range:",self.df.createdate.min(), "-", self.df.createdate.max())
  #   return self.df

  # def __csv_download_filename(self,start_time):
  #   return self.cache_dir + "hubspot_contact_" + start_time.strftime("%Y%m%d-%H%M%S") + ".csv"

  # def download(self,created_gt=None,_offset=None):
  #   first_chunk = True
  #   start_time = datetime.now()
  #   filename = self.__csv_download_filename(start_time)
  #   with requests.get(self.api_url+".csv", headers=self.request_headers, params=self.__request_params(created_gt,_offset), stream=True) as r:
  #       r.raise_for_status()
  #       with open(filename, 'wb') as f:
  #           for chunk in r.iter_content(chunk_size=None):
  #               # TODO - how to handle 500 errors in chunks?
  #               if chunk: # filter out keep-alive new chunks
  #                   if (first_chunk):
  #                       first_chunk = False
  #                       print("Starting download...will update progress every 25 MB of data transfer.")
  #                       print("\tSaving to", filename)
  #                   f.write(chunk)
  #                   size_in_mb = util.file_size_in_mb(filename)
  #                   if ((size_in_mb > 1) & (len(chunk) > 1000) & (size_in_mb % 25 == 0) ):
  #                       print("\tDownloaded", size_in_mb, "MB...")

  #   size_in_mb = util.file_size_in_mb(filename)
  #   time_delta = datetime.now() - start_time
  #   print("\t...Done.\n\tFile Size=",size_in_mb,"MB." " Total Time=", 
  #         round(time_delta.seconds/60.0,2), "minutes", "\n\tLocation:", filename)
  #   self.csv_filename = filename
  #   return self.csv_filename

  def __set_date_tz(self,dataframe):
    print(dataframe.createdate.max())
    # set timezone? https://stackoverflow.com/questions/26089670/unable-to-apply-methods-on-timestamps-using-series-built-ins/38488959
    for col in self.metadata.get("convert_dates"):
        if dataframe[col].count() > 0:
            dataframe[col] = dataframe[col].dt.tz_localize('UTC')
    return dataframe

  def __request_params(self,created_gt,_offset):
    params = {}
    if created_gt is not None:
       params['created_gt'] = calendar.timegm(created_gt.timetuple())
    if _offset is not None:
        params['_offset'] = _offset
    return params
