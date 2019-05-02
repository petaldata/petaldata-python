import pandas as pd
import datetime
from datetime import datetime
from datetime import date

import petaldata
from petaldata.resource.stripe.reports.abstract_stripe_report import AbstractStripeReport
from petaldata.resource.stripe.reports import query_filters

class MRRByMonth(AbstractStripeReport):
  def __init__(self,invoices):
    super().__init__(invoices)

  def to_frame(self,tz='UTC',end_time=datetime.now()):
    df = self.set_frame_tz(self.df,tz=tz)
    df = self.strip_frame_tz(self.df)
    end_timestamp = self.setup_time(end_time,tz=tz)

    df_filtered = df[(df.billing_reason != "manual") & query_filters.time_range_query(df,df.created.min(),end_timestamp) & query_filters.billing_status_query(df)]
    grouped = df_filtered.groupby(pd.Grouper(key="created",freq="M")).agg(
      {
        "amount_due": "sum", 
        "amount_paid": "sum",
        "amount_due_per_month": "sum", 
        "amount_paid_per_month": "sum",
        "simulated": "sum",
        "paid": 'sum',
        "created": 'count',
        "customer_email": pd.Series.nunique
      }
    )
    grouped.rename(columns={"customer_email": 'customers', "simulated": 'ongoing_annual_subscriptions'},inplace=True)
    grouped.sort_index(ascending=False)
    grouped = self.cents_to_dollars(grouped, cols=[i for i in grouped.columns if 'amount' in i])
    return grouped