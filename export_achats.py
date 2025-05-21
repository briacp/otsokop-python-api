from datetime import datetime
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from otsokop.odoo import Odoo
import json
import logging
import os
import pandas as pd
import sys

client = Odoo()


def main():
    achats_df = client.get_purchase_orders(
        "2025-03-01", "2025-03-31", include_order_lines=True
    )
    achats_df[0].to_csv(f"output/achats_otsokop.csv", index=False)
    achats_df[1].to_csv(f"output/achats_details_otsokop.csv", index=False)
    print(achats_df[0])
    print(achats_df[1])


def export_achats(current_date):
    end_date = current_date + relativedelta(months=1, days=-1)


if __name__ == "__main__":
    sys.exit(main())
