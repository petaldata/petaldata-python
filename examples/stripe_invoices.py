# General Setup

import sys
sys.path.append("/Users/dlite/projects/petaldata-python")
import petaldata

from dotenv import load_dotenv
load_dotenv(override=True)
import os

# Configuration

petaldata.api_base = 'http://localhost:3001'
petaldata.dataset.stripe.api_key = os.getenv("STRIPE_API_KEY")
petaldata.storage.Local.dir = os.getenv("CACHE_DIR")

# Loads Stripe Invoices. 

invoices = petaldata.dataset.stripe.Invoices()
invoices.load()
invoices.update(created_gt=petaldata.util.days_ago(30))
invoices.save()