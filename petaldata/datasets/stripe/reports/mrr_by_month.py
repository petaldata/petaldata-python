import pandas as pd
import datetime
from datetime import datetime
from datetime import date

import petaldata
from petaldata.datasets.stripe.reports.abstract_stripe_report import AbstractStripeReport
from petaldata.datasets.stripe.reports import query_filters

class MRRByMonth(AbstractStripeReport):

  def __init__(self,invoices,tz='UTC',end_time=datetime.now().astimezone()):
    super().__init__(invoices,tz=tz,end_time=end_time)

  def to_frame(self):
    df_filtered = self.df[(self.df.billing_reason != "manual") & query_filters.time_range_query(self.df,self.df.created.min(),self.end_timestamp) & query_filters.billing_status_query(self.df)]
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

  def to_gsheet(self,creds,spreadsheet_title=None,worksheet_title="Monthly MRR via Invoices"):
    super().to_gsheet(creds,spreadsheet_title,worksheet_title)
