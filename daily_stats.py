import sys
from datetime import date
from datetime import datetime
from dateutil.relativedelta import relativedelta
from otsokop.odoo import Odoo
from otsokop.odoo import banner as otsokop_banner

offline = False
date_start = sys.argv[1] if len(sys.argv) >= 2 else (date.today() + relativedelta(days=-2)).strftime("%Y-%m-%d")
date_end = sys.argv[2] if len(sys.argv) >= 3 else date_start

client = Odoo("../../assets/cfg/app_settings.json")

prod_racks = client.products_by_racks()
print(prod_racks)
sys.exit(12)

order_dataframes = client.get_pos_orders(date_start, date_end)
orders = order_dataframes[0]
lines = order_dataframes[1]

date_start = datetime.strptime(date_start, "%Y-%m-%d")
print(otsokop_banner)
print("")
print("Résumé -", date_start.strftime("%A %Y-%m-%d"))

if orders.empty:
    print("(aucune donnée disponible)")
else:
    print("  Passages    :", orders["id"].count())
    print("  Total       :", round(orders["amount_total"].sum(), 2), "€")
    print("  Panier Moy. :", round(orders["amount_total"].mean(), 2), "€")
    print("  Panier Med. :", round(orders["amount_total"].median(), 2), "€")
print("")

print("---")
print(orders.sort_values(by="create_date"))
print("---")

top_sales = (
    lines.groupby(["product_name", "product_id", "discount"])
    .agg({"price_subtotal_incl": "sum", "qty": "sum"})
    .sort_values(by="price_subtotal_incl", ascending=False)
    .head(20)
)
print(top_sales)
