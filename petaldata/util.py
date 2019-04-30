import datetime
from datetime import datetime, timedelta

def days_ago(days,time=None):
  if time is None:
    time = datetime.now()
  return time + timedelta(-days)