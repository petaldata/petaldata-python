import pandas as pd
import datetime
from datetime import datetime
from datetime import date

import petaldata
from petaldata.datasets.stripe.reports.abstract_stripe_report import AbstractStripeReport
from petaldata.datasets.stripe.reports import query_filters

class AbstractMoMDiff(AbstractStripeReport):
  def __init__(self,invoices,tz='UTC',end_time=datetime.now().astimezone()):
    super().__init__(invoices,tz=tz,end_time=end_time)

    self.prev_start,self.prev_end,self.cur_start,self.cur_end = self.create_time_ranges(self.end_timestamp)

  @property
  def df_prev(self):
    return self.df_for_time_range(self.prev_start,self.prev_end)

  @property
  def df_cur(self):
    return self.df_for_time_range(self.cur_start,self.cur_end)

  @staticmethod
  def currency_columns(df):
    return [i for i in df.columns if 'amount' in i]

  @property
  def df_prev_cur(self):
    return pd.merge(self.df_prev, self.df_cur, on='customer', how='inner', suffixes=('_prev','_cur'))

  @property
  def df_changes(self):
    return  self.df_prev_cur[self.df_prev_cur.amount_due_per_month_prev != self.df_prev_cur.amount_due_per_month_cur]
  
  def df_for_time_range(self,start_time,end_time):
    return self.df[(self.df.billing_reason != "manual") & query_filters.time_range_query(self.df,start_time,end_time) & query_filters.billing_status_query(self.df)]

  @staticmethod
  def create_time_ranges(cur_end):
      # cur_start ends up being in utc, not the tz 00:00 start time for the month.
      cur_start = cur_end.ceil(freq='D') - pd.offsets.MonthBegin(1)
      prev_end = cur_end - pd.DateOffset(months=1)
      prev_start = prev_end.ceil(freq='D') - pd.offsets.MonthBegin()

      print("Comparing ({} - {}) to previous month ({} - {})".format(cur_start,cur_end,prev_start,prev_end))

      return prev_start,prev_end,cur_start,cur_end