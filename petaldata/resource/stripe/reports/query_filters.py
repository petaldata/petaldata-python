def annual_query(df):
  return df["subscription.plan.interval"] == 'year'

def time_range_query(df,start_time,end_time):
    return ((df.created >= start_time) & (df.created < end_time))  & (df.amount_due > 0) # TODO - move amount due to separate filter

def billing_status_query(df):
    return (df.status != 'draft') & (df.status != 'void')

# query filters
def by_day_query(df,start_time,end_time,manual=False):
    return time_range_query(df,start_time,end_time) & (df.billing_reason != "manual" if not manual else df.billing_reason == "manual") & billing_status_query(df)
