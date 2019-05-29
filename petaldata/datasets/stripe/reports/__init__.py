import datetime
from datetime import datetime

from petaldata.datasets.stripe.reports.abstract_stripe_report import AbstractStripeReport
from petaldata.datasets.stripe.reports.adjusted_invoices import AdjustedInvoices

# Aggregrate Reports
from petaldata.datasets.stripe.reports.mrr_by_month import MRRByMonth
from petaldata.datasets.stripe.reports.revenue_by_plan_by_month import RevenueByPlanByMonth
from petaldata.datasets.stripe.reports.mtd_revenue import MTDRevenue
from petaldata.datasets.stripe.reports.summary import Summary

# Customer Lists
from petaldata.datasets.stripe.reports.customer_lists import *

def all(invoices,tz='UTC',end_time=datetime.now().astimezone()):
  reports = []
  res=map(lambda cls: cls(invoices,tz=tz,end_time=end_time), [MRRByMonth,RevenueByPlanByMonth,MTDRevenue,New,ExpansionContraction,Churned,Summary])
  return list(res)