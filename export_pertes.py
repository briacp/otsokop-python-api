from datetime import datetime
from dateutil.relativedelta import relativedelta
from otsokop.odoo import Odoo
import logging
import os
import sys
import pandas as pd
import json

client = Odoo()

def main():
    dfs = []
    for year in range(2023, 2026):
        for month in range(1, 13):
            date_start = datetime.strptime(f"{year}-{month}", "%Y-%m")
            pertes_df = export_pertes(date_start)
            print(pertes_df)
            dfs.append(pertes_df)

    df = pd.concat(dfs, ignore_index=True)
    df.to_csv(f"output/pertes_otsokop.csv", index=False)
    print(df)


def export_pertes(current_date):
    end_date = current_date + relativedelta(months=1, days=-1)
    print(f"export pertes {current_date}, {end_date}")
    return client.export_pertes(current_date, end_date)


if __name__ == "__main__":
    sys.exit(main())
