# General Setup

import sys
sys.path.append("/Users/dlite/projects/petaldata-python")
import petaldata

from dotenv import load_dotenv
load_dotenv(override=True)
import os

# Configuration

petaldata.api_base = 'http://localhost:3001'
petaldata.storage.Local.dir = os.getenv("CACHE_DIR")

petaldata.dataset.metabase.url = os.getenv("METABASE_URL")
petaldata.dataset.metabase.username = os.getenv("METABASE_USERNAME")
petaldata.dataset.metabase.password = os.getenv("METABASE_PASSWORD")
petaldata.dataset.metabase.token = os.getenv("METABASE_TOKEN")

# Loads Card

card = petaldata.dataset.metabase.Card(266)
card.load()
card.save()