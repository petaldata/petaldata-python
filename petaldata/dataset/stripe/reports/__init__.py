import datetime
from datetime import datetime

from petaldata.dataset.stripe.reports.abstract_stripe_report import AbstractStripeReport
from petaldata.dataset.stripe.reports.adjusted_invoices import AdjustedInvoices

# Aggregrate Reports
from petaldata.dataset.stripe.reports.mrr_by_month import MRRByMonth
from petaldata.dataset.stripe.reports.revenue_by_plan_by_month import RevenueByPlanByMonth
from petaldata.dataset.stripe.reports.mtd_revenue import MTDRevenue
from petaldata.dataset.stripe.reports.summary import Summary

# Customer Lists
from petaldata.dataset.stripe.reports.customer_lists import *

def all(invoices,tz='UTC',end_time=datetime.now().astimezone()):
  reports = []
  res=map(lambda cls: cls(invoices,tz=tz,end_time=end_time), [MRRByMonth,RevenueByPlanByMonth,MTDRevenue,New,ExpansionContraction,Churned,Summary])
  return list(res)