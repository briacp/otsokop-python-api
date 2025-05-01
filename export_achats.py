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
client = Odoo(
    server=ODOO_SERVER,
    database=ODOO_DB,
    username=ODOO_USERNAME,
    password=ODOO_SECRET,
    logging_level=logging.INFO,
)


def main():
    achats_df = client.get_purchase_orders(
        "2025-03-01", "2025-03-31", include_order_lines=True
    )
    achats_df[0].to_csv(f"output/achats_otsokop.csv", index=False)
    achats_df[1].to_csv(f"output/achats_details_otsokop.csv", index=False)


def export_achats(current_date):
    end_date = current_date + relativedelta(months=1, days=-1)


if __name__ == "__main__":
    sys.exit(main())
