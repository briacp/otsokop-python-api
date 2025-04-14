from datetime import datetime
from dateutil.relativedelta import relativedelta
from otsokop.odoo import Odoo
import logging
import os
import sys
import pandas as pd
import json

try:
    with open("app_settings.json") as f:
        config = json.load(f)
except Exception:
    config = {}

ODOO_SERVER = config.get("odoo.server") or os.getenv("ODOO_SERVER")
ODOO_DB = config.get("odoo.database") or os.getenv("ODOO_DB")
ODOO_USERNAME = config.get("odoo.username") or os.getenv("ODOO_USERNAME")
ODOO_SECRET = config.get("odoo.password") or os.getenv("ODOO_SECRET")

def main():
    dfs = []
    for year in range(2023,2026):
        for month in range(1,13):
            date_start = datetime.strptime(f"{year}-{month}", "%Y-%m")
            pertes_df = export_pertes(date_start)
            dfs.append(pertes_df)

    df = pd.concat(dfs, ignore_index=True)
    df.to_csv(f"output/pertes_otsokop.csv", index=False)

def export_pertes(current_date):
    end_date = current_date + relativedelta(months=1, days=-1)

    client = Odoo(
        server=ODOO_SERVER,
        database=ODOO_DB,
        username=ODOO_USERNAME,
        password=ODOO_SECRET,
        logging_level=logging.INFO,
    )

    return client.export_pertes(
        current_date, end_date
    )

if __name__ == "__main__":
    sys.exit(main())
