# General Setup

import sys
sys.path.append("/Users/dlite/projects/petaldata-python")
import petaldata

from dotenv import load_dotenv
load_dotenv(override=True)
import os

# Configuration

petaldata.api_base = 'http://localhost:3001'
petaldata.resource.stripe.api_key = os.getenv("STRIPE_API_KEY")
petaldata.storage.Local.dir = os.getenv("CACHE_DIR")

# Loads Stripe Invoices. 

invoices = petaldata.resource.stripe.Invoice()
invoices.load()

# Generate dataframe with MRR by month

report = petaldata.resource.stripe.reports.MRRByMonth(invoices)
df = report.to_frame(tz='America/Denver')
print(df)