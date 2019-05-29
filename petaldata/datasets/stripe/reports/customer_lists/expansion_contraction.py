import pandas as pd
import datetime
from datetime import datetime
from datetime import date

import petaldata
from petaldata.datasets.stripe.reports.customer_lists.abstract_mom_diff import AbstractMoMDiff
from petaldata.datasets.stripe.reports import query_filters

class ExpansionContraction(AbstractMoMDiff):
  """
  Generates a list of customers who were invoiced for different amounts in the current versus previous month.
  """

  def to_frame(self):
    """Returns a Dataframe of customers with revenue changes."""
    df = self.df_changes
    df.set_index("customer",inplace=True)
    df = self.cents_to_dollars(df,cols=self.currency_columns(df))
    df['change'] = df.amount_due_per_month_cur - df.amount_due_per_month_prev
    return df[['customer_email_prev','amount_due_per_month_prev',
    'amount_due_per_month_cur','change','created_prev','created_cur','subscription.plan.id_cur']]

  def to_gsheet(self,creds,spreadsheet_title=None,worksheet_title="Customers - Expansion+Contraction"):
    super().to_gsheet(creds,spreadsheet_title,worksheet_title)