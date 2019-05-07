import datetime
from datetime import datetime

from petaldata.resource.stripe.reports.abstract_stripe_report import AbstractStripeReport
from petaldata.resource.stripe.reports.adjusted_invoices import AdjustedInvoices

# Aggregrate Reports
from petaldata.resource.stripe.reports.mrr_by_month import MRRByMonth
from petaldata.resource.stripe.reports.revenue_by_plan_by_month import RevenueByPlanByMonth
from petaldata.resource.stripe.reports.mtd_revenue import MTDRevenue
from petaldata.resource.stripe.reports.summary import Summary

# Customer Lists
from petaldata.resource.stripe.reports.customer_lists import *

def all(invoices,tz='UTC',end_time=datetime.now()):
  reports = []
  res=map(lambda cls: cls(invoices,tz=tz,end_time=end_time), [MRRByMonth,RevenueByPlanByMonth,MTDRevenue,New,ExpansionContraction,Churned,Summary])
  return list(res)