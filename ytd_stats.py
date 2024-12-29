import sys
from datetime import date
from datetime import datetime
from dateutil.relativedelta import relativedelta
from otsokop.odoo import Odoo
from otsokop.odoo import banner as otsokop_banner
import logging
import holidays

fr_holidays = holidays.FR()
date_start = sys.argv[1] if len(sys.argv) >= 2 else (date.today() + relativedelta(days=-2)).strftime("%Y-%m-%d")

date_start = datetime.strptime(date_start, "%Y-%m-%d")

if date_start in fr_holidays:
    print(f"Le {date_start.strftime('%Y-%m-%d')} est un jour ferié ({fr_holidays.get(date_start)}), le magasin était fermé")
    sys.exit(1)
if date_start.weekday() == 6:
    print(f"Le {date_start.strftime('%Y-%m-%d')} est un dimanche, le magasin était fermé")
    sys.exit(2)
 
client = Odoo("../../assets/cfg/app_settings.json", logging.INFO)
order_dataframes = client.get_pos_orders(date_start.strftime("%Y-%m-%d"))
orders = order_dataframes[0]

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

last_year_date = date_start + relativedelta(years=-1, weekday=date_start.weekday())

is_holiday = last_year_date in fr_holidays

order_dataframes = client.get_pos_orders(last_year_date.strftime("%Y-%m-%d"))
orders = order_dataframes[0]

print("Résumé année préc. -", last_year_date.strftime("%A %Y-%m-%d"))
if is_holiday:
    print(f"** Jour ferié ({fr_holidays.get(last_year_date)})")

if orders.empty:
    print("(aucune donnée disponible)")
else:
    print("  Passages    :", orders["id"].count())
    print("  Total       :", round(orders["amount_total"].sum(), 2), "€")
    print("  Panier Moy. :", round(orders["amount_total"].mean(), 2), "€")
    print("  Panier Med. :", round(orders["amount_total"].median(), 2), "€")
print("")
