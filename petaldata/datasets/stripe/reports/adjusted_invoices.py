import pandas as pd
import datetime
from datetime import datetime
from datetime import date
import pygsheets

import petaldata
from petaldata.datasets.stripe.reports import query_filters

class AdjustedInvoices(object):
    def __init__(self,invoices,tz='UTC',end_time=datetime.now().astimezone()):
        self.df = invoices.df
        self.df = self.set_frame_tz(self.df,tz=tz)
        self.convert_annual_subs_to_monthly(self.df)
        self.df = self.add_simulated_annual_invoices(self.df)

    @staticmethod
    def set_frame_tz(df,tz = 'UTC'):
        for col in df.columns:
          if 'datetime64[ns' in str(df[col].dtype):
            df[col] = df[col].dt.tz_convert(tz)
        return df

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
        return AdjustedInvoices.assign_dtypes(df,df_combined)

    @staticmethod
    def assign_dtypes(original_df,other_df):
        return other_df.astype(original_df.dtypes.to_dict())