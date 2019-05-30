# Downloads Hubspot contacts.
# Usage:
# * HUBSPOT_API_KEY=[INSERT] python -i examples/hubspot_contacts.py
# * Access the dataframe via `contacts.df`

import os

# Loads dev-specific configuration if env var. DEV=true.
if (os.getenv("DEV") == 'true'):
  import dev_config

import petaldata

# Configuration
petaldata.datasets.hubspot.api_key = os.getenv("HUBSPOT_API_KEY")

# Downloads Hubspot Contacts.
contacts = petaldata.datasets.hubspot.Contacts()
contacts.download(limit=10)