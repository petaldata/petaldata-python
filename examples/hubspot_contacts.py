# General Setup

import sys
sys.path.append("/Users/dlite/projects/petaldata-python")
import petaldata

from dotenv import load_dotenv
load_dotenv()
import os

# Configuration

petaldata.api_base = 'http://localhost:3001'
petaldata.resource.hubspot.api_key = os.getenv("HUBSPOT_API_KEY")

# Loads Hubspot Contacts. 

# First attempts to load from a saved Pickle file in the +cache_dir+, then fetches new contacts and
# saves a new Pickle file.
contact = petaldata.resource.hubspot.Contact(cache_dir = os.getenv("CACHE_DIR"))
contact.load()
contact.update()