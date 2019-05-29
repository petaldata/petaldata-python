import pandas as pd
import datetime
from datetime import datetime
from datetime import date

import petaldata
from petaldata.datasets.stripe.reports.abstract_stripe_report import AbstractStripeReport
from petaldata.datasets.stripe.reports import query_filters

class MTDRevenue(AbstractStripeReport):

    def __init__(self,invoices,tz='UTC',end_time=datetime.now().astimezone(), fullRange=True):
        super().__init__(invoices,tz=tz,end_time=end_time)
        self.fullRange = fullRange
        print("MTD - comparing ({} - {}) to previous month ({} - {})".format(self.cur_start,self.cur_end,self.prev_start,self.prev_end))

    @property
    def df_prev(self):
        return self.df[query_filters.by_day_query(self.df,self.prev_start,self.prev_end)]

    @property
    def df_cur(self):
        return self.df[query_filters.by_day_query(self.df,self.cur_start,self.cur_end)]

    def how_join(self,df1,df2):
        if len(df1.index) > len(df2.index):
            how = "left"
        else:
            how = "outer"

        return how

    def to_frame(self):
        prev_month_by_day = self.group_and_agg(self.df_prev)
        cur_month_by_day = self.group_and_agg(self.df_cur)

        df_mtd=cur_month_by_day.cumsum().reset_index().join(prev_month_by_day.cumsum().reset_index(), rsuffix=" (Previous Month)",
            how=self.how_join(cur_month_by_day,prev_month_by_day))
        df_mtd.index += 1 
        df_mtd.index.name = 'Day'

        # Hack to prevent Segfault stripping timezones from NaT values ... this changes the dtype to object
        df_mtd["created (Previous Month)"] = df_mtd["created (Previous Month)"].dt.date
        df_mtd.created = df_mtd.created.dt.date

        return df_mtd

    @property
    def cur_start(self):
        return self.end_timestamp.ceil(freq='D') - pd.offsets.MonthBegin(1)

    @property
    def cur_end(self):
        return self.end_timestamp

    @property
    def prev_start(self):
        return self.prev_end.ceil(freq='D') - pd.offsets.MonthBegin()

    @property
    def prev_end(self):
        if self.fullRange:
            e = self.cur_start
        else:
            e = self.cur_end - pd.DateOffset(months=1)

        return e

    @staticmethod
    def group_and_agg(df):
        grouped = df.groupby(pd.Grouper(key="created",freq="D")).agg(
            {'amount_paid_per_month': 'sum',
             'amount_due_per_month': 'sum', 
             'customer_email': pd.Series.nunique,
             'simulated': 'sum'}
        )
        grouped = MTDRevenue.cents_to_dollars(grouped, cols=[i for i in grouped.columns if 'amount' in i])    
        return grouped.rename(columns={"customer_email": "customers", "simulated": "ongoing_annual_subscriptions"})
    
    def to_gsheet(self,creds,spreadsheet_title=None,worksheet_title="MTD"):
        super().to_gsheet(creds,spreadsheet_title,worksheet_title)
