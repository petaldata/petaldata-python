import pandas as pd
import datetime
from datetime import datetime
from datetime import date
import pygsheets

import petaldata
from petaldata.resource.stripe.reports import query_filters

class AbstractStripeReport(object):
  def __init__(self,invoices,tz='UTC',end_time=datetime.now()):
    self.df = invoices.df
    self.tz = tz
    self.df = self.set_frame_tz(self.df,tz=tz)
    self.convert_annual_subs_to_monthly(self.df)
    self.df = self.add_simulated_annual_invoices(self.df)
    self._gsheet_client = None

    self.end_timestamp = self.setup_time(end_time,tz=tz)

  @staticmethod
  def set_frame_tz(df,tz = 'UTC'):
    for col in df.columns:
      if 'datetime64[ns' in str(df[col].dtype):
        df[col] = df[col].dt.tz_convert(tz)
    return df

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
    t=t.tz_localize(tz)#.tz_convert(None)
    return t

  @staticmethod
  def convert_annual_subs_to_monthly(df):
    print("Converting annual subscriptions to monthly")
    per_month = lambda amount, interval : amount / 12.0 if (interval == 'year') else amount
    df['amount_due_per_month'] = df.apply(lambda row: per_month(row.amount_due,row["subscription.plan.interval"]), axis=1)
    df['amount_paid_per_month'] = df.apply(lambda row: per_month(row.amount_paid,row["subscription.plan.interval"]), axis=1)

  @staticmethod
  def add_simulated_annual_invoices(df):
    print("Adding simulated monthly invoices from annual invoices...")
    frames = []
    for index, row in df[query_filters.annual_query(df) & (df.amount_due > 0)].iterrows():
        for datetime in pd.date_range(start=row["subscription.current_period_start"], 
                                  end=row["subscription.current_period_end"]-pd.offsets.MonthBegin(2), 
                                  freq='MS'):
            date = datetime.date()
            simulated_row = row.copy()
            simulated_row.name = row.name+"_"+str(date)
            t=pd.Timestamp(datetime)
            simulated_row.date = t
            simulated_row.created = t
            simulated_row["simulated"] = 1
            frame = simulated_row.to_frame().transpose()
            frame.index.name = "id"
            frames.append(frame)

    print("\t...concating")
    df_combined = pd.concat([df]+frames,verify_integrity=True,sort=False)
    print("\t...assigning dtypes")
    return AbstractStripeReport.assign_dtypes(df,df_combined)

  @staticmethod
  def assign_dtypes(original_df,other_df):
    return other_df.astype(original_df.dtypes.to_dict())


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


