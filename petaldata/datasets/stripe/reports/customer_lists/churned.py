import pandas as pd
import datetime
from datetime import datetime
from datetime import date

import petaldata
from petaldata.datasets.stripe.reports.customer_lists.abstract_mom_diff import AbstractMoMDiff
from petaldata.datasets.stripe.reports import query_filters

class Churned(AbstractMoMDiff):
  """
  Generates a list of churned customers in the current month.
  """

  def to_frame(self):
    df = pd.merge(self.df_prev, self.df_cur, on='customer', how='left', suffixes=('','_cur'))
    df.set_index("customer",inplace=True)
    df = self.cents_to_dollars(df,cols=self.currency_columns(df))
    return df[df.amount_due_per_month_cur.isna()][['customer_email','amount_due_per_month',
    'amount_paid_per_month',
    'created','subscription.plan.id']]

  def to_gsheet(self,creds,spreadsheet_title=None,worksheet_title="Customers - Churned"):
    super().to_gsheet(creds,spreadsheet_title,worksheet_title)