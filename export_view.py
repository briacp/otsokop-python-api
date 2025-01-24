from otsokop.odoo import Odoo
from otsokop.odoo import banner as otsokop_banner
import pandas as pd

client = Odoo("../../assets/cfg/app_settings.json")

# addons/products/models/product.py
# def get_history_price(self, company_id, date=None):
#     history = self.env['product.price.history'].search([
#         ('company_id', '=', company_id),
#         ('product_id', 'in', self.ids),
#         ('datetime', '<=', date or fields.Datetime.now())], order='datetime desc,id desc', limit=1)
#     return history.cost or 0.0

error_products = [
    29234,
    28651,
    29197,
    28655,
    28654,
    29161,
    29232,
    28956,
    26928,
    29123,
    28666,
    29014,
    28653,
    30502,
    29015,
    28982,
    28981,
    28984,
    28983,
    28617,
    28390,
    29077,
    28391,
    29054,
    28616,
    29157,
    30592,
]

# error_products = [28651]

price_history = client.execute_kw(
    "product.price.history",
    "search_read",
    [
        [("product_id", "in", error_products), ("cost", "=", 0)],
        [
            "id",
            # "company_id",
            "cost",
            "create_date",
            # "create_uid",
            "datetime",
            "display_name",
            "__last_update",
            "product_id",
            # "write_date",
            "write_uid",
        ],
    ],
)

for p in price_history:
    print(f"Delete price history {p['id']}")

    deleted_price_history = client.execute_kw(
        "product.price.history",
        "unlink",
        [p["id"]],
    )

    print(deleted_price_history)

#     p["product_name"] = p["product_id"][1]
#     p["product_id"] = p["product_id"][0]
#     p["write_name"] = p["write_uid"][1]
#     p["write_uid"] = p["write_uid"][0]

# price_history = pd.DataFrame(price_history)
# print(price_history)
# price_history.to_excel("output/price_history.xlsx", index=False)
