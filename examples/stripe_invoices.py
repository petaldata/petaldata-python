# General Setup

import sys
sys.path.append("/Users/dlite/projects/petaldata-python")
import petaldata

from dotenv import load_dotenv
load_dotenv(override=True)
import os

# Configuration

petaldata.api_base = 'http://localhost:3001'
petaldata.resource.hubspot.api_key = os.getenv("STRIPE_API_KEY")
petaldata.cache_dir = os.getenv("CACHE_DIR")

# Loads Stripe Invoices. 

invoices = petaldata.resource.stripe.Invoice()
invoices.load()
invoices.update()
invoices.save()
# invoices.reset()