from otsokop.odoo import Odoo
from otsokop.odoo import banner as otsokop_banner
import calendar
import pandas as pd
from sqlalchemy import create_engine
import os
import sys

SKIP_ORDERS = False
DELETE_CACHE = False

print(otsokop_banner)
client = Odoo("app_settings.json")

db_name = f"output/odoo_2023-2024.db"
xlsx_name = f"output/odoo_2023-2024.xlsx"
writer = pd.ExcelWriter(xlsx_name, datetime_format="YYYY-MM-DD hh:mm")

if os.path.isfile(db_name):
    os.remove(db_name)

if os.path.isfile("output/members_address.csv"):
    members_address = pd.read_csv("output/members_address.csv")
else:
    members_address = pd.DataFrame()

print(f"Creating SQLite DB {db_name}")
engine = create_engine(f"sqlite:///{db_name}", echo=False)

print("Exporting all members...")
# if DELETE_CACHE:
#     del client._cache["get_all_members"]
members = client.get_all_members()

members = members.merge(
    members_address[["id", "distance_in_m", "latitude", "longitude"]],
    on="id",
    how="left",
)

members.to_sql(name="partner", con=engine, index=False)

members[members["customer"] == True].to_excel(
    writer,
    sheet_name="partner",
    index=False,
    columns=[
        "id",
        "create_date",
        # "name",
        # "street",
        # "street2",
        "city",
        "gender",
        "age",
        "distance_in_m",
        # "latitude",
        # "longitude",
        # ---
        # "shift_type",
        # "working_state",
        # "is_unsubscribed",
        # "is_worker_member",
        # "is_member",
        # "customer",
        # "supplier",
        # "is_squadleader",
        # "is_exempted",
        # "cooperative_state",
    ],
)

print("Exporting all products...")
if DELETE_CACHE:
    del client._cache["get_all_products"]
products = client.get_all_products()
products.to_sql(name="product", con=engine, index=False)
products.to_excel(
    writer,
    sheet_name="product",
    columns=[
        "id",
        "name",
        "create_date",
        "active",
        "theoritical_price",
        "sale_ok",
        "rack_location",
        "categ_id",
        "categ_name",
    ],
    index=False,
)

if SKIP_ORDERS:
    writer.close()
    sys.exit(1)

print("Exporting all POS orders...")

# Fetching all the year's sales takes a long time
orders = pd.DataFrame()
order_lines = pd.DataFrame()

for year in range(2023, 2025):
    for month in range(1, 13):
        last_day = calendar.monthrange(year, month)[1]
        if DELETE_CACHE:
            print(f"Delete cache for {year}-{month}...")
            del client._cache[
                f"get_pos_orders-{year}-{month:02d}-01 00:00:00-{year}-{month:02d}-{last_day} 23:59:59"
            ]
        print(
            f"Exporting all POS orders for {year}-{month:02d}-01 / {year}-{month:02d}-{last_day}"
        )
        (month_orders, month_order_lines) = client.get_pos_orders(
            f"{year}-{month:02d}-01", f"{year}-{month:02d}-{last_day}"
        )

        orders = pd.concat([orders, month_orders], ignore_index=True)
        order_lines = pd.concat([order_lines, month_order_lines], ignore_index=True)

del orders["lines"]
del orders["partner_name"]

print(orders)

order_lines["partner_id"] = order_lines["order_id"].map(
    orders.set_index("id")["partner_id"]
)

orders.to_excel(
    writer,
    sheet_name="order",
    index=False,
    columns=["id", "date_order", "partner_id", "amount_total"],
)
order_lines.to_excel(
    writer,
    sheet_name="order_line",
    index=False,
    columns=[
        # "id",
        "order_id",
        "date_order",
        "partner_id",
        "product_id",
        "product_name",
        "price_subtotal_incl",
        "qty",
        "discount",
    ],
)

del order_lines["product_name"]
del order_lines["date_order"]
del order_lines["partner_id"]
orders.to_sql(name="order", if_exists="append", con=engine, index=False)
order_lines.to_sql(name="order_line", if_exists="append", con=engine, index=False)

writer.close()
