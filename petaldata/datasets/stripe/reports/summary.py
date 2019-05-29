import pandas as pd
import datetime
from datetime import datetime
from datetime import date

import petaldata
from petaldata.datasets.stripe.reports.abstract_stripe_report import AbstractStripeReport
from petaldata.datasets.stripe.reports.mtd_revenue import MTDRevenue
from petaldata.datasets.stripe.reports import query_filters

class Summary(AbstractStripeReport):

  def __init__(self,invoices,tz='UTC',end_time=datetime.now().astimezone()):
    super().__init__(invoices,tz=tz,end_time=end_time)
    self.mtd_report = MTDRevenue(invoices,tz=tz,end_time=end_time,fullRange=False)

  def to_frame(self):
    pass
    

  def to_gsheet(self,creds,spreadsheet_title=None,worksheet_title="Summary"):
    print("Opening Google Sheet...title=",spreadsheet_title)

    # Must share sheet with "client_email" from JSON creds
    sh = self.gsheet_client(creds).open(spreadsheet_title)

    wks = self.find_or_create_wks(sh,worksheet_title) 

    df_mtd = self.mtd_report.to_frame()   

    print("\t...updating worksheet")
    # rev
    wks.cell('I6').value = df_mtd.amount_due_per_month.max()
    wks.cell('I7').value = df_mtd["amount_due_per_month (Previous Month)"].max()
    wks.cell('J6').value = df_mtd.amount_paid_per_month.max()
    wks.cell('J7').value = df_mtd["amount_paid_per_month (Previous Month)"].max()
    # customers
    wks.cell('I11').value = df_mtd.customers.max()
    wks.cell('I12').value = df_mtd["customers (Previous Month)"].max()
    # logging updated at
    wks.cell('I3').value = str(self.setup_time(datetime.now().astimezone(),tz=self.tz))
    print("\t...Done.")
