# Downloads the output of a Metabase card.
# Usage:
# * METABASE_URL=[INSERT] METABASE_USERNAME=[INSERT] METABASE_PASSWORD=[INSERT] CARD=[ID OF CARD] python -i examples/metabase_card.py
# * Access the dataframe via `card.df`

# General Setup
import os

# Loads dev-specific configuration if env var. DEV=true.
if (os.getenv("DEV") == 'true'):
  import dev_config

import petaldata

# Configuration
petaldata.datasets.metabase.url = os.getenv("METABASE_URL")
petaldata.datasets.metabase.username = os.getenv("METABASE_USERNAME")
petaldata.datasets.metabase.password = os.getenv("METABASE_PASSWORD")

# Download Card

card = petaldata.datasets.metabase.Card(os.getenv("CARD"))
card.download()