# General Setup

import sys
sys.path.append("/Users/dlite/projects/petaldata-python")
import petaldata

from dotenv import load_dotenv
load_dotenv()
import os

# Configuration

petaldata.api_base = 'http://localhost:3001'
petaldata.dataset.hubspot.api_key = os.getenv("HUBSPOT_API_KEY")
petaldata.cache_dir = os.getenv("CACHE_DIR")

# Loads Hubspot Contacts. 

# First attempts to load from a saved Pickle file in the +cache_dir+, then fetches new contacts and
# saves a new Pickle file.
contact = petaldata.dataset.hubspot.Contact()
contact.metadata
contact.load()
contact.update()