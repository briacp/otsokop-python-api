from otsokop.odoo import Odoo
from otsokop.odoo import banner as otsokop_banner
import pandas as pd
import sys

client = Odoo()


def main():
    # stock_picking()
    action_balance_qty()


# https://raw.githubusercontent.com/hydrosIII/odoo12-stock-correction/refs/heads/master/models/models.py
def action_balance_qty():
    datetime_start = "2024-12-01 00:00:00"
    datetime_end = "2024-12-31 23:59:59"

    # sqlscrpt=(
    #         "select product_id,location_id,location_dest_id,qty_done,lot_id "
    #         "from stock_move_line "
    #         "where state='done';"
    #         )
    res = client.execute_kw(
        "stock.move.line",
        "search_read",
        [
            [
                ["create_date", ">=", client._to_utc(datetime_start)],
                ["create_date", "<=", client._to_utc(datetime_end)],
                ["state", "=", "done"],
            ],
            ["product_id", "location_id", "location_dest_id", "qty_done", "lot_id"],
        ],
    )

    # Reset all stock.quant quantity
    # for i in self.search([]):
    #     i.write({
    #         'quantity': 0
    #         })

    for i in res:
        product_id = i["product_id"][0]
        location_id = i["location_id"][0]
        location_dest_id = i["location_dest_id"][0]
        lot_id = i["lot_id"][0] if i["lot_id"] else False

        # sqrcrd = self.search([
        #         ('product_id','=',i['product_id']),
        #         ('location_id','=',i['location_id']),
        #         ('lot_id','=', i.get('lot_id'))
        #         ], limit=1)
        # if len(sqrcrd) == 0:
        #     self.create({
        #         'product_id': i['product_id'],
        #         'location_id': i['location_id'],
        #         'lot_id' : i.get('lot_id'),
        #         'quantity': -i['qty_done']
        #         })
        # else:
        #     sqrcrd.write({
        #         'quantity': sqrcrd.quantity - i['qty_done']
        #         })
        sqrcrd = client.execute_kw(
            "stock.quant",
            "search_read",
            [
                [
                    ["product_id", "=", product_id],
                    ["location_id", "=", location_id],
                    ["lot_id", "=", lot_id],
                ],
                ["quantity"],
                0,  # offset
                1,  # limit
            ],
        )

        if len(sqrcrd) == 0:
            new_quantity = -i["qty_done"]
            print(
                f"TODO: create stock.quant entry for ORIG {product_id}/{location_id}/{lot_id} -> {new_quantity}"
            )
        else:
            new_quantity = sqrcrd[0]["quantity"] - i["qty_done"]
            print(
                f"TODO: update stock.quant entry for ORIG {product_id}/{location_id}/{lot_id} -> {sqrcrd[0]['quantity']} => {new_quantity}"
            )

        # sqrcrd = self.search([
        #         ('product_id','=',i['product_id']),
        #         ('location_id','=',i['location_dest_id']),
        #         ('lot_id','=', i.get('lot_id'))
        #         ], limit=1)
        # if len(sqrcrd) == 0:
        #     self.create({
        #         'product_id': i['product_id'],
        #         'location_id': i['location_dest_id'],
        #         'lot_id' : i.get('lot_id'),
        #         'quantity': i['qty_done']
        #         })
        # else:
        #     sqrcrd.write({
        #         'quantity': sqrcrd.quantity + i['qty_done']
        #         })
        sqrcrd = client.execute_kw(
            "stock.quant",
            "search_read",
            [
                [
                    ["product_id", "=", product_id],
                    ["location_id", "=", location_dest_id],
                    ["lot_id", "=", lot_id],
                ],
                ["quantity"],
                0,  # offset
                1,  # limit
            ],
        )

        if len(sqrcrd) == 0:
            new_quantity = i["qty_done"]
            print(
                f"TODO: create stock.quant entry for DEST {product_id}/{location_dest_id}/{lot_id} -> {new_quantity}"
            )
        else:
            new_quantity = sqrcrd[0]["quantity"] + i["qty_done"]
            print(
                f"TODO: update stock.quant entry for DEST {product_id}/{location_dest_id}/{lot_id} -> {sqrcrd[0]['quantity']} => {new_quantity}"
            )


if __name__ == "__main__":
    sys.exit(main())
