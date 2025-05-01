from babel.dates import format_date
from babel.numbers import format_decimal
from datetime import date
from datetime import datetime
from dateutil.relativedelta import relativedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from otsokop.odoo import Odoo
import holidays
import logging
import os
import smtplib
import sys
import json

MONTH_FORMAT = "MMMM' 'yyyy"
LOCALE = "fr_FR"
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
    for year in (2023, 2024, 2025):
        for month in (1, 2, 3):
            date_start = datetime.strptime(f"{year}-{month}", "%Y-%m")
            monthly_stats_content = monthly_stats(date_start)


def monthly_stats(current_date):
    content = []
    end_date = current_date + relativedelta(months=1, days=-1)

    client = Odoo(
        server=ODOO_SERVER,
        database=ODOO_DB,
        username=ODOO_USERNAME,
        password=ODOO_SECRET,
        logging_level=logging.INFO,
    )

    order_dataframes = client.get_pos_orders(
        current_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d"),
        include_order_lines=False,
    )
    orders = order_dataframes[0]

    current = format_date(current_date, MONTH_FORMAT, locale="fr_FR")

    if orders.empty:
        print("(aucune donnée disponible)")

    order_summary(current, orders)
    return content


def order_summary(current, orders):
    print(current)
    print(f"CA Total      : {currency(orders['amount_total'].sum())}")
    print(f"Panier Moyen  : {currency(orders['amount_total'].mean())}")
    print(f"Coop acheteurs: {orders['partner_id'].nunique()}")
    print(f"Nb commandes  : {orders['id'].count()}")
    print("---")


def currency(n):
    return format_decimal(n, "#,##0.##;-# ¤", locale=LOCALE)


if __name__ == "__main__":
    sys.exit(main())
