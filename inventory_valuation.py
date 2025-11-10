from datetime import datetime
from dateutil.relativedelta import relativedelta
from otsokop.odoo import Odoo
import logging
import os
import sys
import pandas as pd
import json

client = Odoo()


def main():    
    # Get the stock valuation account for the product
    product = client.execute_kw(
        "product.product",
        "read",
        [[product_id]],
        {"fields": ["categ_id"]}
    )
    
    category = client.execute_kw(
        "product.category",
        "read",
        [product["categ_id"]],
        {"fields": ["property_stock_valuation_account_id"]}
    )[0]
    
    valuation_account_id = category["property_stock_valuation_account_id"][0]
    
    # Get account move lines for this product up to the date
    move_lines = client.execute_kw(
        db, uid, password,
        "account.move.line",
        "search_read",
        [[
            ("product_id", "=", product_id),
            ("account_id", "=", valuation_account_id),
            ("date", "<=", date_str),
            ("parent_state", "=", "posted")
        ]],
        {
            "fields": ["debit", "credit", "quantity", "date"],
            "order": "date asc"
        }
    )
    
    total_value = sum(line["debit"] - line["credit"] for line in move_lines)
    total_quantity = sum(line.get("quantity", 0) for line in move_lines)
    
    if total_quantity > 0:
        return total_value / total_quantity
    
    return 0.0


if __name__ == "__main__":
    sys.exit(main())
