import pandas as pd
import datetime
from datetime import datetime
from datetime import date

import petaldata
from petaldata.resource.stripe.reports import query_filters

class AbstractStripeReport(object):
  def __init__(self,invoices):
    self.df = invoices.df
    self.convert_annual_subs_to_monthly(self.df)
    self.df = self.add_simulated_annual_invoices(self.df)

  # def to_frame(self,tz='UTC'):
  #   self.df = self.set_frame_tz(self.df,tz=tz)

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
    t=t.tz_localize(tz).tz_convert(None)
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

  def to_gsheet(df,spreadsheet_name=None,worksheet_name=None,service_file=None):
    gc=pygsheets.authorize(service_file=service_file)

    print("Opening Google Sheet...")

    # Must share sheet with "client_email" from JSON creds file
    sh = gc.open('Copy of Revenue Reporting')

    wks = sh.worksheet_by_title("Monthly MRR via Invoices")
    print("\t...updating MRR worksheet")
    wks.clear(fields="*")



    wks.set_dataframe(strip_tz(grouped),(1,1), copy_index=True, nan="")
    print("\t...Done.")


