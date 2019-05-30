# Downloads Stripe Invoices, storing results in an Amazon S3 bucket.
# Usage:
# * STRIPE_API_KEY=[INSERT] python -i examples/stripe_invoices.py
# * Access the dataframe via `invoices.df`

# General Setup
import os

# Loads dev-specific configuration if env var. DEV=true.
if (os.getenv("DEV") == 'true'):
  import dev_config

import petaldata
import datetime

# Configuration

petaldata.datasets.stripe.api_key = os.getenv("STRIPE_API_KEY")

petaldata.storage.S3.aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
petaldata.storage.S3.aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
petaldata.storage.S3.bucket_name = os.getenv("AWS_BUCKET")

petaldata.storage.S3.enabled = True
petaldata.storage.Local.enabled = False

# Downloads Stripe Invoices, saving the invoices to S3 storage. 

invoices = petaldata.datasets.stripe.Invoices()
invoices.download(limit=10)
invoices.save()