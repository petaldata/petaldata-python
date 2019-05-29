import pandas as pd
import datetime
from datetime import datetime
from datetime import date

import petaldata
from petaldata.datasets.stripe.reports.abstract_stripe_report import AbstractStripeReport
from petaldata.datasets.stripe.reports import query_filters

class RevenueByPlanByMonth(AbstractStripeReport):

  def to_frame(self):
    by_plan = self.df[(self.df.billing_reason != "manual") & query_filters.time_range_query(self.df,self.df.created.min(),self.end_timestamp) & query_filters.billing_status_query(self.df)].groupby([pd.Grouper(key="created",freq="M"),"subscription.plan.id"]).agg(
        {
            "amount_due_per_month": "sum", 
            "amount_paid_per_month": "sum",
            "paid": 'sum',
            "created": 'count',
            "customer_email": pd.Series.nunique
        }
    )
    by_plan.rename(columns={"customer_email": 'customers'},inplace=True)
    by_plan = self.cents_to_dollars(by_plan, cols=[i for i in by_plan.columns if 'amount' in i])  

    return by_plan  

  def to_gsheet(self,creds,spreadsheet_title=None,worksheet_title="Monthly Revenue by Plan ID"):
    super().to_gsheet(creds,spreadsheet_title,worksheet_title)
