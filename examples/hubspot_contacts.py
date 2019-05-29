# General Setup

import sys
sys.path.append("/Users/dlite/projects/petaldata-python")
import petaldata

from dotenv import load_dotenv
load_dotenv()
import os

# Configuration

petaldata.api_base = 'http://localhost:3001'
petaldata.datasets.hubspot.api_key = os.getenv("HUBSPOT_API_KEY")

# Loads Hubspot Contacts. 

contacts = petaldata.datasets.hubspot.Contacts()
# contacts.download(limit=10)
# TODO - NEXT
contacts.upsert()