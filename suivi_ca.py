from otsokop.odoo import Odoo
from datetime import date
from datetime import datetime
from dateutil.relativedelta import relativedelta
import sys
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

opt_date_start = (
    sys.argv[1]
    if len(sys.argv) >= 2
    else (date.today() + relativedelta(months=-1)).strftime("%Y-%m")
)

base_date = datetime.strptime(opt_date_start, "%Y-%m")
start_date = base_date + relativedelta(months=-1, years=-1)
end_date = base_date + relativedelta(months=1)

logging.info(f"Suivi CA {start_date} - {end_date}")

FILE_PREFIX = "Suivi CA"
MONEY_FORMAT = "#,##0.00 €"
PERCENT_FORMAT = "0.00%"

client = Odoo("app_settings.json")

all_products = client.get_all_products()
print(all_products)

# value.forEach(entry => {
#     otso.products[entry.id] = {
#         id: entry.id,
#         category_id: entry.categ_id[0],
#         category: entry.categ_id[1],
#         name: entry.name,
#         rack_location: entry.rack_location,
#         //farming_method: entry.farming_method,
#         create_date: entry.create_date,
#         theoritical_price: entry.theoritical_price,
#         is_active: entry.active,
#         sale_ok: entry.sale_ok,
#         months: {
#         },
#         total: {
#             ca: [],
#             remise: [],
#             pertes: [],
#         }
#     };

# });


export_ventes = client.export_ventes(start_date, end_date)
print(export_ventes)
export_pertes = client.export_pertes(start_date, end_date)
print(export_pertes)
