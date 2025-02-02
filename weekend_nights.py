from otsokop.odoo import Odoo
import pandas as pd

client = Odoo("app_settings.json")

cache_key = f"weekend_nights"
if (cached_result := client._check_cache(cache_key)) is not None:
    pos_orders = cached_result
else:
    pos_orders = client.execute_kw(
        "pos.order",
        "search_read",
        [
            [
                "&",
                ["state", "=", "done"],
                "|",
                "|",
                "|",
                "|",
                "|",
                "|",
                "&",
                ["create_date", ">=", "2023-11-04 16:00:00"],
                ["create_date", "<", "2023-11-04 22:00:00"],
                "&",
                ["create_date", ">=", "2023-11-25 16:00:00"],
                ["create_date", "<=", "2023-11-25 22:00:00"],
                "&",
                ["create_date", ">=", "2023-12-02 16:00:00"],
                ["create_date", "<=", "2023-12-02 22:00:00"],
                "&",
                ["create_date", ">=", "2023-12-09 16:00:00"],
                ["create_date", "<=", "2023-12-09 22:00:00"],
                "&",
                ["create_date", ">=", "2023-12-16 16:00:00"],
                ["create_date", "<=", "2023-12-16 22:00:00"],
                "&",
                ["create_date", ">=", "2023-12-23 16:00:00"],
                ["create_date", "<=", "2023-12-23 22:00:00"],
                "&",
                ["create_date", ">=", "2023-12-30 16:00:00"],
                ["create_date", "<=", "2023-12-30 22:00:00"],
            ],
            ["create_date", "partner_id", "amount_total"],
        ],
    )

    for pos_order in pos_orders:
        partner = pos_order["partner_id"] or [0, None]
        pos_order["partner_id"] = partner[0]
        pos_order["partner_name"] = partner[1]

    pos_orders = pd.DataFrame(pos_orders)
    client._set_cache(cache_key, pos_orders)

print("Summary - Christmas Weekends")
print("  Count  :", pos_orders["id"].count())
print("  Sum    :", round(pos_orders["amount_total"].sum(), 2), "€")
print("  Median :", round(pos_orders["amount_total"].median(), 2), "€")
print("  Mean   :", round(pos_orders["amount_total"].mean(), 2), "€")
print("")
print(pos_orders)
