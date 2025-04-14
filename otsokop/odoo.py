from datetime import datetime
from re import search
import diskcache
import json
import logging
import pandas as pd
import pytz
import sys
import xmlrpc.client

banner = """
 ____ _____ ____  ____  _  __ ____  ____ 
/  _ Y__ __Y ___\/  _ \/ |/ //  _ \/  __\\
| / \| / \ |    \| / \||   / | / \||  \/|
| \_/| | | \___ || \_/||   \ | \_/||  __/
\____/ \_/ \____/\____/\_|\_\\\____/\_/   
"""


class Odoo:
    XMLRPC_DEBUG = False
    DISABLE_CACHE = False
    SECONDS_IN_DAY = 60 * 60 * 24

    def __init__(
        self,
        config_file=None,
        *,
        server=None,
        database=None,
        username=None,
        password=None,
        debug=None,
        timezone=None,
        logging_level=logging.DEBUG,
    ):
        self.config_file = config_file
        self._uid = None
        self._cache = diskcache.Cache("cache")
        logging.basicConfig(
            level=logging_level,  # Set minimum log level
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )

        # Load config from a JSON file
        # Load configuration
        config = {}
        if config_file:
            with open(config_file) as f:
                config = json.load(f)
            self.config_file = config_file

        # Set attributes with priority to directly passed parameters
        self.url = server or config.get("odoo.server")
        self.db = database or config.get("odoo.database")
        self.username = username or config.get("odoo.username")
        self._password = password or config.get("odoo.password")
        self.debug = (
            debug if debug is not None else config.get("odoo.debug", Odoo.XMLRPC_DEBUG)
        )

        timezone_value = timezone or config.get("user.timezone")
        if timezone_value:
            self._local_tz = pytz.timezone(timezone_value)
        else:
            self._local_tz = pytz.UTC

        # Validate required parameters
        required_params = ["url", "db", "username", "_password"]
        missing_params = [
            param for param in required_params if not getattr(self, param)
        ]
        if missing_params:
            raise ValueError(
                f"Missing required parameters: {', '.join(missing_params)}. "
                "Please provide them either through config file or as parameters."
            )

    def _connect(self):
        # Connect to Odoo
        logging.info(f"Connecting to Odoo server {self.url} {self.db}...")
        uid = xmlrpc.client.ServerProxy(
            f"{self.url}/xmlrpc/2/common", verbose=self.debug
        ).authenticate(self.db, self.username, self._password, {})

        if uid:
            self._uid = uid
        else:
            raise Exception(f"Failed to authenticate to {self.url}.")

        self.odoo = xmlrpc.client.ServerProxy(
            f"{self.url}/xmlrpc/2/object", verbose=self.debug
        )

    def execute_kw(self, model, method, params, *mapping: None):
        if not (self._uid):
            self._connect()

        # logging.debug(f"execute_kw {model} {method}")
        try:
            return self.odoo.execute_kw(
                self.db, self._uid, self._password, model, method, params, mapping
            )
        except xmlrpc.client.Fault as err:
            print("Odoo error occured - code: %d" % err.faultCode, file=sys.stderr)
            print("Fault string: %s" % err.faultString, file=sys.stderr)
            # raise Exception("Odoo XMLRPC Exception")

    def get_pos_orders(
        self, date_start, date_end: str = None, include_order_lines=True
    ):
        if not (date_end):
            date_end = date_start

        if not (search(" \\d{2}:\\d{2}:\\d{2}$", date_start)):
            datetime_start = f"{date_start} 00:00:00"
        else:
            datetime_start = date_start

        if not (search(" \\d{2}:\\d{2}:\\d{2}$", date_end)):
            datetime_end = f"{date_end} 23:59:59"
        else:
            datetime_end = date_end

        logging.debug(
            f"get_pos_orders {self._to_utc(datetime_start)} - {self._to_utc(datetime_end)}"
        )

        cache_key = f"get_pos_orders-{datetime_start}-{datetime_end}-{include_order_lines}"
        if (cached_result := self._check_cache(cache_key)) is not None:
            return cached_result

        data_orders = []
        data_order_lines = []

        pos_orders = self.execute_kw(
            "pos.order",
            "search_read",
            [
                [
                    ["date_order", ">=", self._to_utc(datetime_start)],
                    ["date_order", "<=", self._to_utc(datetime_end)],
                    "|",
                    "|",
                    ["state", "=", "done"],
                    ["state", "=", "paid"],
                    ["state", "=", "invoiced"],
                ],
                ["date_order", "partner_id", "amount_total", "state", "lines"],
            ],
        )

        for pos_order in pos_orders:
            partner = pos_order["partner_id"] or [0, None]
            pos_order["partner_id"] = partner[0]
            pos_order["partner_name"] = partner[1]
            data_orders.append(pos_order)

            if include_order_lines:
                pos_order_lines = self.execute_kw(
                    "pos.order.line",
                    "search_read",
                    [
                        [["id", "in", pos_order["lines"]]],
                        ["product_id", "price_subtotal_incl", "qty", "discount"],
                    ],
                )

                for pos_order_line in pos_order_lines:
                    pos_order_line["date_order"] = pos_order["date_order"]
                    pos_order_line["order_id"] = pos_order["id"]
                    pos_order_line["product_name"] = pos_order_line["product_id"][1]
                    pos_order_line["product_id"] = pos_order_line["product_id"][0]
                    data_order_lines.append(pos_order_line)
            else:
                pos_order_lines = []

        orders = pd.DataFrame(data_orders)
        orders["date_order"] = pd.to_datetime(orders["date_order"])

        result = [orders, pd.DataFrame(data_order_lines)]

        if not (result[0].empty):
            self._set_cache(cache_key, result)
        # else:
        #     del self._cache[cache_key]

        return result

    def get_purchase_orders(
        self, date_start, date_end: str = None, include_order_lines=True
    ):
        if not (date_end):
            date_end = date_start

        if not (search(" \\d{2}:\\d{2}:\\d{2}$", date_start)):
            datetime_start = f"{date_start} 00:00:00"
        else:
            datetime_start = date_start

        if not (search(" \\d{2}:\\d{2}:\\d{2}$", date_end)):
            datetime_end = f"{date_end} 23:59:59"
        else:
            datetime_end = date_end

        logging.debug(
            f"get_purchase_orders {self._to_utc(datetime_start)} - {self._to_utc(datetime_end)}"
        )

        cache_key = f"get_purchase_orders-{datetime_start}-{datetime_end}-{include_order_lines}"
        if (cached_result := self._check_cache(cache_key)) is not None:
            return cached_result

        data_orders = []
        data_order_lines = []

        purchase_orders = self.execute_kw(
            "purchase.order",
            "search_read",
            [
                [
                    ["date_order", ">=", self._to_utc(datetime_start)],
                    ["date_order", "<=", self._to_utc(datetime_end)],
                ],
                ["date_order", "display_name", "partner_id", "amount_total", "amount_untaxed", "invoice_status", "state", "order_line"],
            ],
        )

        for purchase_order in purchase_orders:
            partner = purchase_order["partner_id"] or [0, None]
            purchase_order["partner_id"] = partner[0]
            purchase_order["partner_name"] = partner[1]
            data_orders.append(purchase_order)

            if include_order_lines:
                purchase_order_lines = self.execute_kw(
                    "purchase.order.line",
                    "search_read",
                    [
                        [["id", "in", purchase_order["order_line"]]],
                        ["product_id", "price_subtotal", "price_tax", "price_total", "price_unit", "product_qty", "product_uom_qty", "qty_invoiced", "qty_received"],
                    ],
                )

                for purchase_order_line in purchase_order_lines:
                    purchase_order_line["date_order"] = purchase_order["date_order"]
                    purchase_order_line["order_id"] = purchase_order["id"]
                    purchase_order_line["product_name"] = purchase_order_line["product_id"][1]
                    purchase_order_line["product_id"] = purchase_order_line["product_id"][0]
                    data_order_lines.append(purchase_order_line)
            else:
                purchase_order_lines = []

        orders = pd.DataFrame(data_orders)
        orders["date_order"] = pd.to_datetime(orders["date_order"])

        result = [orders, pd.DataFrame(data_order_lines)]

        if not (result[0].empty):
            self._set_cache(cache_key, result)
        # else:
        #     del self._cache[cache_key]

        return result

    # @cached_results
    def get_all_products(self):
        logging.debug("Getting the list of all products...")
        if (cached_result := self._check_cache("get_all_products")) is not None:
            return cached_result

        all_products = self.execute_kw(
            "product.product",
            "search_read",
            [
                ["|", ["active", "=", "true"], ["active", "=", "false"]],
                [
                    "name",
                    "rack_location",
                    "create_date",
                    "theoritical_price",
                    "active",
                    "sale_ok",
                    "categ_id",
                ],
            ],
        )

        for product in all_products:
            product["categ_name"] = product["categ_id"][1]
            product["categ_id"] = product["categ_id"][0]

        all_products = pd.DataFrame(all_products)
        all_products["create_date"] = pd.to_datetime(all_products["create_date"])

        self._set_cache("get_all_products", all_products, Odoo.SECONDS_IN_DAY)

        return all_products

    def get_all_members(self):
        logging.debug("Getting the list of all coop members...")
        if (cached_result := self._check_cache("get_all_members")) is not None:
            return cached_result

        all_members = self.execute_kw(
            "res.partner",
            "search_read",
            [
                [["is_member", "=", "true"]],
                [
                    "name",
                    "city",
                    "street",
                    "street2",
                    "gender",
                    "age",
                    "is_squadleader",
                    "shift_type",
                    "is_exempted",
                    "working_state",
                    "is_unsubscribed",
                    "is_worker_member",
                    # "is_member",
                    "customer",
                    "supplier",
                    "cooperative_state",
                    "create_date",
                ],
            ],
        )
        all_members = pd.DataFrame(all_members)

        self._set_cache("get_all_members", all_members, Odoo.SECONDS_IN_DAY)

        return all_members

    def export_ventes(self, date_start, date_end):
        cache_key = f"export_ventes-{date_start}-{date_end}"
        if (cached_result := self._check_cache(cache_key)) is not None:
            return cached_result

        pos_order_lines = self.execute_kw(
            "pos.order.line",
            "search_read",
            [
                [
                    ["create_date", ">=", date_start.isoformat()],
                    ["create_date", "<", date_end.isoformat()],
                ],
                [
                    "order_id",
                    "create_date",
                    "product_id",
                    "qty",
                    "price_subtotal_incl",
                    "discount",
                ],
                0,  # offset
                0,  # limit
                "product_id,id",
            ],
        )

        result = pd.DataFrame(pos_order_lines)

        result["create_date"] = pd.to_datetime(result["create_date"])

        self._set_cache(cache_key, result)
        return result

    def export_pertes(self, date_start, date_end):
        cache_key = f"export_pertes-{date_start}-{date_end}"
        if (cached_result := self._check_cache(cache_key)) is not None:
            return cached_result

        stock_moves = self.execute_kw(
            "stock.move",
            "search_read",
            [
                [
                    # Otsolab: Pertes
                    ["picking_type_id", "=", 14],
                    # Emplacements Virtuels/Pertes d'inventaire
                    [
                        "location_dest_id",
                        "=",
                        5,
                    ],
                    ["date_expected", ">=", date_start.isoformat()],
                    ["date_expected", "<", date_end.isoformat()],
                ],
                [
                    "date_expected",
                    "name",
                    "location_id",
                    "product_id",
                    "product_qty",
                    "price_unit",
                ],
                0,  # offset
                0,  # limit
                "product_id,id",
            ],
        )

        result = pd.DataFrame(stock_moves)
        self._set_cache(cache_key, result)
        return result

    # Liste des produits vendable d'un rayon. Cette liste peut servir de base à l'inventaire.
    def products_by_racks(self):

        result = self.execute_kw(
            "product.product",
            "search_read",
            [
                # query
                [["sale_ok", "=", True]],  # , ["rack_location", "in", racks]],
                # fields
                [
                    "rack_location",
                    # "product_variant_ids",
                    "categ_id",
                    # "name",
                    # "barcode",
                    # "uom_id",
                    # "qty_available",
                ],
                0,
                0,
                "rack_location,name",
            ],
        )

        for product in result:
            product["categ"] = product["categ_id"][1]

        result = pd.DataFrame(result, columns=["rack_location", "categ"])
        result.drop_duplicates()
        return result.shape

    def _check_cache(self, cache_key):
        if Odoo.DISABLE_CACHE:
            return None

        if cache_key in self._cache:
            logging.debug(f"using cached {cache_key}...")
            return self._cache[cache_key]
        else:
            return None

    def _set_cache(self, cache_key, data, expire=None):
        if Odoo.DISABLE_CACHE:
            return
        logging.debug(f"setting cached {cache_key}...")
        # expire time in seconds
        self._cache.set(cache_key, data, expire=expire)

    def _to_utc(self, local_datetime_str: str):
        return self._local_tz.localize(
            datetime.strptime(local_datetime_str, "%Y-%m-%d %H:%M:%S")
        ).astimezone(pytz.utc)

    def _to_local_tz(self, utc_datetime_str: str):
        return pytz.utc.localize(
            datetime.strptime(utc_datetime_str, "%Y-%m-%d %H:%M:%S")
        ).astimezone(self._local_tz)

    def dump_model(self):
        if not (self._uid):
            self._connect()

        all_models = self.execute_kw(
            "ir.model",
            "search_read",
            [
                # query
                [], 
                # fields
                [
                    "name",
                    "model",
                    "state",
                    "field_id",
                    "view_ids",
                    "transient",
                    "access_ids",
                ],
                0,
                0,
            ],

        )

        # Print the list of models with their names
        for model in all_models:
            print(f"- {model['model']}:")
            print(f"  name: {model['name']}")
            print(f"  fields:")
            model_fields = model["field_id"]
            model_fields = self.execute_kw(
                "ir.model.fields",
                "search_read",
                [
                    [["id", "in", model_fields]],
                    [
                        # "model_id",
                        "name",
                        "field_description",
                        "ttype",
                        # "state",
                        "required",
                        "readonly",
                        # "translate",
                        # "groups",
                        # "selection",
                        "size",
                        # "on_delete",
                        "relation",
                        "relation_field",
                        # "domain",
                        # "index",
                    ],
                ],
            )

            for field in model_fields:
                print(f"    - {field['name']}:")
                for attr in field.keys():
                    print(f"      {attr}: {field[attr]}")
                print("\n")
