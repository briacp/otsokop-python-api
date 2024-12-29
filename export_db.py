from otsokop.odoo import Odoo
from otsokop.odoo import banner as otsokop_banner
import calendar
from sqlalchemy import create_engine
import os

print(otsokop_banner)
client = Odoo("../../assets/cfg/app_settings.json")

db_name = f"odoo_2023-2024.db"
os.remove(db_name)
print(f"Creating SQLite DB {db_name}")
engine = create_engine(f"sqlite:///{db_name}", echo=False)

# TODO - transform dates from TEXT to DATETIME in SQLite

# SQL
print("Exporting all products...")
products = client.get_all_products()
products.to_sql(name="product", con=engine, index=False)

print("Exporting all members...")
client.get_all_members().to_sql(name="partner", con=engine, index=False)

print("Exporting all POS orders...")
# Fetching all the year's sales takes a long time
for year in range(2023, 2025):
    for month in range(1, 13):
        last_day = calendar.monthrange(year, month)[1]
        print(
            f"Exporting all POS orders for {year}-{month:02d}-01 / {year}-{month:02d}-{last_day}"
        )
        order_dataframes = client.get_pos_orders(
            f"{year}-{month:02d}-01", f"{year}-{month:02d}-{last_day}"
        )
        del order_dataframes[0]["lines"]
        del order_dataframes[0]["partner_name"]
        del order_dataframes[1]["product_name"]

        order_dataframes[0].to_sql(
            name="order", if_exists="append", con=engine, index=False
        )
        order_dataframes[1].to_sql(
            name="order_line", if_exists="append", con=engine, index=False
        )
