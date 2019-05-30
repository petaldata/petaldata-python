# Downloads Stripe Invoices.
# Usage:
# * STRIPE_API_KEY=[INSERT] python -i examples/stripe_invoices.py
# * Access the dataframe via `invoices.df`

# General Setup
import os

# Loads dev-specific configuration if env var. DEV=true.
if (os.getenv("DEV") == 'true'):
  import dev_config

import petaldata

# Configuration

petaldata.datasets.stripe.api_key = os.getenv("STRIPE_API_KEY")

# Download Stripe Invoices. 

invoices = petaldata.datasets.stripe.Invoices()
invoices.download(limit=10)