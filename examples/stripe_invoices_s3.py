# General Setup

import sys
sys.path.append("/Users/dlite/projects/petaldata-python")
import petaldata

from dotenv import load_dotenv
load_dotenv(override=True)
import os
import datetime

# Configuration

petaldata.api_base = 'http://localhost:3001'
petaldata.resource.stripe.api_key = os.getenv("STRIPE_API_KEY")
petaldata.storage.Local.dir = os.getenv("CACHE_DIR")

petaldata.storage.S3.aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
petaldata.storage.S3.aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
petaldata.storage.S3.bucket_name = 'petaldata-test2'

petaldata.storage.S3.enabled = True
petaldata.storage.Local.enabled = False

# Loads Stripe Invoices, using S3 storage. 

invoices = petaldata.resource.stripe.Invoice()
invoices.load()
invoices.update(created_gt=petaldata.util.days_ago(30))
invoices.save()