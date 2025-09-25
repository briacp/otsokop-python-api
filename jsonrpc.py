import requests, os, json
from dotenv import load_dotenv

load_dotenv()

url = os.getenv("ODOO_SERVER")
db = os.getenv("ODOO_DATABASE")
username = os.getenv("ODOO_USERNAME")
_password = os.getenv("ODOO_SECRET")
uid = None


def do_request(payload):
    response = requests.post(f"{url}/jsonrpc", json=payload)
    # print(response.text)
    return response.json()


do_request(
    {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {"service": "common", "method": "version", "args": []},
    }
)


res = do_request(
    {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "service": "common",
            "method": "authenticate",
            "args": [db, username, _password, {}],
        },
    }
)

uid = res["result"]

res = do_request(
    {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "service": "object",
            "method": "execute",
            "args": [
                db,
                str(uid),
                _password,
                # "product.product",
                # "search_read",
                # [["name", "like", "BAGUETTE"]],
                # ["code", "name", "active"],
                "product.history",
                "search_read",
                [
                    ["product_id", "=", 28168],
                    # ["date", ">=", "2025-01-01 00:00:00"],
                    # ["date", "<", "2025-05-01 00:00:00"],
                ],
                [
                    "history_range",
                    "product_id",
                    "from_date",
                    "to_date",
                    "start_qty",
                    "purchase_qty",
                    "sale_qty",
                    "loss_qty",
                    "end_qty",
                    "incoming_qty",
                    "outgoing_qty",
                    "virtual_qty",
                    "ignored",
                ],
            ],
            "kwargs": {"lang": "fr_FR"},
        },
        "id": 123,
    },
)

print(json.dumps(res))
