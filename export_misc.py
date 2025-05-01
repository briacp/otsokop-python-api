from otsokop.odoo import Odoo
from otsokop.odoo import banner as otsokop_banner
import pandas as pd
import sys

client = Odoo("app_settings.json")


def main():
    print("misc export...")
    # stock_picking()
    # stock_quant()
    # product_categories()
    # portal_users_stats()
    # product_labels()
    product_list()


def product_list():
    products = client.get_all_products()
    products.to_csv("output/products.csv", index=False)


def product_labels():
    labels = client.execute_kw(
        "product.label",
        "search_read",
        [
            [],
            ["code", "name"],
        ],
    )
    # for r in products:
    #     r["parent_id"] = r["parent_id"][0] if r["parent_id"] else None
    labels = pd.DataFrame(labels)
    print(labels)
    labels.to_csv("output/labels.csv", index=False)


def portal_users_stats():
    users = client.execute_kw(
        "res.users",
        "search_read",
        [
            [],
            ["id", "display_name", "activity_state", "log_ids", "login_date"],
        ],
    )
    # for r in products:
    #     r["parent_id"] = r["parent_id"][0] if r["parent_id"] else None
    users = pd.DataFrame(users)
    print(users)
    users.to_excel("output/users.xlsx", index=False)


def product_categories():
    products = client.execute_kw(
        "product.category",
        "search_read",
        [
            [],
            [
                "id",
                "display_name",
                "parent_id",
                "product_count",
            ],
        ],
    )
    for r in products:
        r["parent_id"] = r["parent_id"][0] if r["parent_id"] else None
    products = pd.DataFrame(products)
    print(products)
    products.to_excel("output/product_categories.xlsx", index=False)


def stock_picking():
    picking = client.execute_kw(
        "stock.picking",
        "search_read",
        [
            [["id", "=", 34769]],
            [
                "create_date",
                "product_id",
                "quantity",
                "reserved_quantity",
                "move_ids_without_package",
            ],
        ],
    )
    picking = pd.DataFrame(picking)
    print(picking)

    move_ids = client.execute_kw(
        "stock.move",
        "search_read",
        [
            [
                # ["id", "in", [ 1167800, 1167801 ]]
                ["reserved_availability", ">", 0],
                ["create_date", ">=", "2024-12-01"],
            ],
            [
                "create_date",
                "product_id",
                "reserved_availability",
            ],
        ],
    )
    move_ids = pd.DataFrame(move_ids)
    move_ids = move_ids[move_ids["reserved_availability"] > 0]
    print(move_ids)
    move_ids.to_csv("output/move_ids.csv")


def stock_quant():

    reserved = client.execute_kw(
        "stock.quant",
        "search_read",
        [
            [["reserved_quantity", ">", 0]],
            [
                "create_date",
                "product_id",
                "quantity",
                "reserved_quantity",
            ],
        ],
    )

    for r in reserved:
        r["product_name"] = r["product_id"][1]
        r["product_id"] = r["product_id"][0]
    #     if product["barcode"] is False:
    #         product["barcode"] = ""
    #     if product["default_code"] is False:
    #         product["default_code"] = ""
    reserved = pd.DataFrame(reserved)
    print(reserved)
    reserved.to_excel("output/articles_reserves.xlsx", index=False)


def product_product():
    if (cached_result := client._check_cache("product.product")) is not None:
        products = cached_result
    else:
        products = client.execute_kw(
            "product.product",
            "search_read",
            [
                [],
                [
                    "default_code",
                    "name",
                    # "lst_price",
                    # "theoritical_price",
                    "standard_price",
                    "qty_available",
                    # "virtual_available",
                    "uom_id",
                    "barcode",
                ],
            ],
        )
        client._set_cache("product.product", products, Odoo.SECONDS_IN_DAY)

    for product in products:
        product["uom_id"] = product["uom_id"][1]
        if product["barcode"] is False:
            product["barcode"] = ""
        if product["default_code"] is False:
            product["default_code"] = ""

    products = pd.DataFrame(products)
    products.to_excel("output/product.product.xlsx", index=False)
    print(products)


if __name__ == "__main__":
    sys.exit(main())
