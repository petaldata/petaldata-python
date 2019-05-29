import pandas as pd
import datetime
from datetime import datetime
from datetime import date
import pygsheets

import petaldata
from petaldata.datasets.stripe.reports import query_filters
from petaldata.datasets.stripe.reports.adjusted_invoices import AdjustedInvoices


class AbstractStripeReport(object):
  def __init__(self,invoices,tz='UTC',end_time=datetime.now().astimezone()):
    """
    Parameters
    ----------
    invoices : stripe.Invoice or stripe.reports.AdjustedInvoices
    tz : str
        The timezone all report data should be presented with.
    end_time: tz-aware datetime

    Returns
    -------
    AbstractStripeReport
    """
    self.df = invoices.df
    self.tz = tz
    if invoices.__class__.__name__ == 'Invoices':
      self.df = AdjustedInvoices(invoices,tz='UTC',end_time=datetime.now().astimezone()).df

    self._gsheet_client = None

    self.end_timestamp = self.setup_time(end_time,tz=tz)

  
  @staticmethod
  def strip_frame_tz(df):
    print("Stripping timezone from datetime columns.")
    df = df.copy()
    for col in df.columns:
          if 'datetime64' in str(df[col].dtype):
            df[col] = df[col].apply(lambda x:datetime.replace(x,tzinfo=None))
    return df

  @staticmethod
  def setup_time(dt,tz=None):
    t=pd.Timestamp(dt)
    t=t.tz_convert(tz)
    return t

  @staticmethod
  def cents_to_dollars(df,cols=None):
    print("Converting cents to dollars. Cols=",cols)
    df[cols]=df[cols]/100
    return df


  def gsheet_client(self,creds):
    if self._gsheet_client is None:
      self._gsheet_client = pygsheets.authorize(custom_credentials=creds.with_scopes(['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive.metadata.readonly']))
      
    return self._gsheet_client;

  def find_or_create_wks(self,sh,worksheet_title):
    try:
      wks = sh.worksheet_by_title(worksheet_title)
      print("\t...opening existing worksheet. title=",worksheet_title)
    except pygsheets.exceptions.WorksheetNotFound:
      print("\t...creating new worksheet. title=",worksheet_title)
      wks = sh.add_worksheet(worksheet_title)

    return wks  


  def to_gsheet(self,creds,spreadsheet_title=None,worksheet_title=None):
    """
    Parameters
    ----------
    creds : google.oauth2.service_account.Credentials
        Google Authentication Credentials.
    spreadsheet_title : str
        The title of the Google spreadsheet to update. The spreadsheet must exist and the user associated with the creds must have read/write access to the sheet.
    worksheet_title: str
        The title of the worksheet to update (a worksheet is within a spreadsheet). The worksheet will be created if it doesn't exist.

    Returns
    -------
    None
    """
    frame = self.to_frame()

    print("Opening Google Sheet...title=",spreadsheet_title)

    # Must share sheet with "client_email" from JSON creds
    sh = self.gsheet_client(creds).open(spreadsheet_title)
    wks = self.find_or_create_wks(sh,worksheet_title)
    print("\t...updating worksheet")
    wks.clear()
    wks.set_dataframe(self.strip_frame_tz(frame),(1,1), copy_index=True, nan="")
    wks.cell('A1').value = frame.index.name
    print("\t...Done.")


