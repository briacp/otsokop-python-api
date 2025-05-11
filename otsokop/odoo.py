import diskcache, json, logging, pandas as pd, pytz, sys, xmlrpc.client, yaml


from datetime import datetime
from re import search

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
    DISABLE_CACHE_GET = False
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
        self._cache.expire()
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
        (datetime_start, datetime_end) = self._interval_dates(date_start, date_end)

        logging.debug(f"get_pos_orders {datetime_start} - {datetime_end}")

        cache_key = (
            f"get_pos_orders-{datetime_start}-{datetime_end}-{include_order_lines}"
        )
        if (cached_result := self._check_cache(cache_key)) is not None:
            return cached_result

        data_orders = []
        data_order_lines = []

        pos_orders = self.execute_kw(
            "pos.order",
            "search_read",
            [
                [
                    ["date_order", ">=", datetime_start],
                    ["date_order", "<=", datetime_end],
                    ["state", "in", ["done", "paid", "invoiced"]],
                ],
                [
                    "date_order",
                    "partner_id",
                    "amount_total",
                    "amount_tax",
                    "amount_return",
                    "amount_paid",
                    "state",
                    "lines",
                ],
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
                        [
                            "product_id",
                            "price_subtotal",
                            "price_subtotal_incl",
                            "price_unit",
                            "qty",
                            "discount",
                        ],
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

        return result

    def get_report_pos_orders(self, date_start, date_end: str = None):
        (datetime_start, datetime_end) = self._interval_dates(date_start, date_end)

        logging.debug(f"get_report_pos_orders {datetime_start} - {datetime_end}")

        cache_key = f"get_report_pos_orders-{datetime_start}-{datetime_end}"
        if (cached_result := self._check_cache(cache_key)) is not None:
            return cached_result

        report_pos_orders = self.execute_kw(
            "report.pos.order",
            "search_read",
            [
                [
                    ["date", ">=", datetime_start],
                    ["date", "<=", datetime_end],
                ],
                [
                    "date",
                    "average_price",
                    "invoiced",
                    "nbr_lines",
                    "price_sub_total",
                    "price_total",
                    "product_qty",
                    "state",
                    "total_discount",
                    "order_id",
                    "partner_id",
                    # "product_categ_id",
                    "product_id",
                ],
            ],
        )

        for pos_order in report_pos_orders:
            if pos_order["partner_id"]:
                pos_order["partner_id"] = pos_order["partner_id"][0]
            else:
                pos_order["partner_id"] = 0
            pos_order["order_id"] = pos_order["order_id"][0]
            pos_order["product_categ_id"] = pos_order["product_categ_id"][0]
            pos_order["product_id"] = pos_order["product_id"][0]

        report_pos_orders = pd.DataFrame(report_pos_orders)
        report_pos_orders["date"] = pd.to_datetime(report_pos_orders["date"])

        if not (report_pos_orders.empty):
            self._set_cache(cache_key, report_pos_orders)

        return report_pos_orders

    def get_purchase_orders(
        self, date_start, date_end: str = None, include_order_lines=True
    ):
        (datetime_start, datetime_end) = self._interval_dates(date_start, date_end)

        logging.debug(f"get_purchase_orders {datetime_start} - {datetime_end}")

        cache_key = (
            f"get_purchase_orders-{datetime_start}-{datetime_end}-{include_order_lines}"
        )
        if (cached_result := self._check_cache(cache_key)) is not None:
            return cached_result

        data_orders = []
        data_order_lines = []

        purchase_orders = self.execute_kw(
            "purchase.order",
            "search_read",
            [
                [
                    ["date_order", ">=", datetime_start],
                    ["date_order", "<=", datetime_end],
                    # ["state", "in", ["purchase", "done"]],
                ],
                [
                    "date_order",
                    "display_name",
                    "partner_id",
                    "amount_total",
                    "amount_tax",
                    "amount_untaxed",
                    "invoice_status",
                    "state",
                    "order_line",
                ],
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
                        [
                            "product_id",
                            "price_subtotal",
                            "price_tax",
                            "price_total",
                            "price_unit",
                            "product_qty",
                            "qty_invoiced",
                            "qty_received",
                        ],
                    ],
                )

                for purchase_order_line in purchase_order_lines:
                    purchase_order_line["date_order"] = purchase_order["date_order"]
                    purchase_order_line["order_id"] = purchase_order["id"]
                    purchase_order_line["product_name"] = purchase_order_line[
                        "product_id"
                    ][1]
                    purchase_order_line["product_id"] = purchase_order_line[
                        "product_id"
                    ][0]
                    data_order_lines.append(purchase_order_line)
            else:
                purchase_order_lines = []

        orders = pd.DataFrame(data_orders)
        orders = orders.drop("order_line", axis=1)
        orders["date_order"] = pd.to_datetime(orders["date_order"])

        result = [orders, pd.DataFrame(data_order_lines)]

        if not (result[0].empty):
            self._set_cache(cache_key, result)

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
                ["|", ["active", "=", True], ["active", "=", False]],
                [
                    "name",
                    "rack_location",
                    "create_date",
                    "theoritical_price",
                    "active",
                    "label_ids",
                    "sale_ok",
                    "categ_id",
                    "product_tmpl_id",
                    "barcode",
                ],
            ],
        )

        for product in all_products:
            product["categ_name"] = product["categ_id"][1]
            product["categ_id"] = product["categ_id"][0]
            product["product_tmpl_id"] = product["product_tmpl_id"][0]

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
                [["is_member", "=", True]],
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
        (datetime_start, datetime_end) = self._interval_dates(date_start, date_end)

        cache_key = f"export_ventes-{datetime_start}-{datetime_end}"
        if (cached_result := self._check_cache(cache_key)) is not None:
            return cached_result

        pos_order_lines = self.execute_kw(
            "pos.order.line",
            "search_read",
            [
                [
                    ["create_date", ">=", datetime_start],
                    ["create_date", "<", datetime_end],
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
        (datetime_start, datetime_end) = self._interval_dates(date_start, date_end)

        cache_key = f"export_pertes-{datetime_start}-{datetime_end}"
        if (cached_result := self._check_cache(cache_key)) is not None:
            return cached_result

        stock_moves = self.execute_kw(
            "stock.move",
            "search_read",
            [
                [
                    ["state", "=", "done"],
                    # Otsolab: Pertes
                    ["picking_type_id", "=", 14],
                    # Emplacements Virtuels/Pertes d'inventaire
                    [
                        "location_dest_id",
                        "=",
                        5,
                    ],
                    ["date_expected", ">=", date_start],
                    ["date_expected", "<", date_end],
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

        for sm in stock_moves:
            self._remove_odoo_id(sm, ["product_id", "location_id"])

        result = pd.DataFrame(stock_moves)
        result["date_expected"] = pd.to_datetime(result["date_expected"])
        self._set_cache(cache_key, result)
        return result

    def get_account_invoices(self, date_start, date_end, include_invoice_lines=True):
        (datetime_start, datetime_end) = self._interval_dates(date_start, date_end)

        cache_key = f"get_invoices-{datetime_start}-{datetime_end}"
        if (cached_result := self._check_cache(cache_key)) is not None:
            return cached_result

        invoices = self.execute_kw(
            "account.invoice",
            "search_read",
            [
                [
                    ["date", ">=", datetime_start],
                    ["date", "<", datetime_end],
                    ["state", "not in", ["draft"]],
                ],
                [
                    "amount_tax",
                    "amount_total",
                    "amount_untaxed",
                    "date",
                    "date_invoice",
                    "invoice_line_ids",
                    # "invoice_line_tax_ids",
                    "number",
                    "partner_id",
                    "purchase_id",
                    "state",
                    "type",
                ],
            ],
        )

        invoice_line_ids = []
        for invoice in invoices:
            invoice_line_ids.extend(invoice["invoice_line_ids"])
            self._remove_odoo_id(invoice, ["partner_id", "purchase_id"])

        invoices = pd.DataFrame(invoices)

        try:
            invoices["date"] = pd.to_datetime(invoices["date"])
            invoices["date_invoice"] = pd.to_datetime(invoices["date_invoice"])
            # invoices["date_due"] = pd.to_datetime(invoices["date_due"])
        except:
            pass

        if include_invoice_lines:
            invoice_lines = self.execute_kw(
                "account.invoice.line",
                "search_read",
                [
                    [["id", "in", invoice_line_ids]],
                    [
                        "account_id",
                        "invoice_id",
                        "discount",
                        "price_subtotal",
                        "price_tax",
                        "price_total",
                        "price_unit",
                        "quantity",
                        "product_id",
                    ],
                ],
            )

            for invoice_line in invoice_lines:
                self._remove_odoo_id(
                    invoice_line, ["account_id", "invoice_id", "product_id"]
                )

            invoice_lines = pd.DataFrame(invoice_lines)
        else:
            invoice_lines = pd.DataFrame()

        try:
            invoices = invoices.drop("invoice_line_ids", axis=1)
        except:
            pass

        result = [invoices, invoice_lines]

        if not (result[0].empty):
            self._set_cache(cache_key, result)

        return result

    def get_account_move_line(self, date_start, date_end):
        (datetime_start, datetime_end) = self._interval_dates(date_start, date_end)

        cache_key = f"get_account_move_line-{datetime_start}-{datetime_end}"
        if (cached_result := self._check_cache(cache_key)) is not None:
            return cached_result

        move_lines = self.execute_kw(
            "account.move.line",
            "search_read",
            [
                [
                    ["date", ">=", datetime_start],
                    ["date", "<=", datetime_end],
                    ["parent_state", "=", "posted"],
                ],
                [
                    "journal_id",
                    "date",
                    "move_id",
                    "account_id",
                    "name",
                    "debit",
                    "credit",
                ],
            ],
        )

        for line in move_lines:
            line["move_ref"] = line["move_id"][1]
            self._remove_odoo_id(line, ["journal_id", "account_id", "move_id"])
            if line["debit"] > 0:
                line["dc_flag"] = "D"
            else:
                line["dc_flag"] = "C"

        move_lines = pd.DataFrame(move_lines)
        move_lines["date"] = pd.to_datetime(move_lines["date"])

        if not (move_lines.empty):
            self._set_cache(cache_key, move_lines)

        return move_lines

    def _remove_odoo_id(self, odoo_object, oddo_id_fields):
        for odoo_field in oddo_id_fields:
            if odoo_object[odoo_field]:
                odoo_object[odoo_field] = odoo_object[odoo_field][0]

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
        if Odoo.DISABLE_CACHE or Odoo.DISABLE_CACHE_GET:
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

    def delete_cache_by_prefix(self, prefix):
        count = 0
        all_keys = list(self._cache)
        for key in all_keys:
            if isinstance(key, str) and key.startswith(prefix):
                self._cache.delete(key)
                count += 1
        return count

    def _interval_dates(self, date_start, date_end=None):
        if date_end is None:
            date_end = date_start

        if isinstance(date_start, datetime):
            return date_start, date_end

        if not (search(" \\d{2}:\\d{2}:\\d{2}$", date_start)):
            date_start = f"{date_start} 00:00:00"

        if not (search(" \\d{2}:\\d{2}:\\d{2}$", date_end)):
            date_end = f"{date_end} 23:59:59"

        return self._to_utc(date_start), self._to_utc(date_end)

    def _to_utc(self, local_datetime_str: str):
        return self._local_tz.localize(
            datetime.strptime(local_datetime_str, "%Y-%m-%d %H:%M:%S")
        ).astimezone(pytz.utc)

    def _to_local_tz(self, utc_datetime_str: str):
        return pytz.utc.localize(
            datetime.strptime(utc_datetime_str, "%Y-%m-%d %H:%M:%S")
        ).astimezone(self._local_tz)

    # deprecated
    def dump_model(self):
        logging.warning("deprecated, use `dump_model_yaml` instead")
        self.dump_model_yaml("output/odoo_model.yml")

    def dump_model_yaml(self, output_file_path):
        if not (self._uid):
            self._connect()

        yml_models = {}

        all_models = self.execute_kw(
            "ir.model",
            "search_read",
            [
                [],
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

        ignored_models_category = (
            "base_import.",
            "change.password.",
            "cleanup.",
            "computed.",
            "confirm.",
            "date.range.",
            "db.backup.",
            "digest.",
            "event.",
            "im_livechat.",
            "ir.actions.",
            "link.",
            "mail.",
            "mass.editing.",
            "report",
            "replace",
            "sms.",
            "theme.",
            "web_editor.",
            "website.",
            "wizard.",
            "wiz.",
        )

        # Print the list of models with their names
        for model in all_models:
            skip = False
            for skipped in ignored_models_category:
                if model["model"].startswith(skipped):
                    skip = True
                    break

            if skip:
                continue

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

            for f in model_fields:
                del f["id"]
                if f["size"] == 0:
                    del f["size"]
                if f["relation_field"] == False:
                    del f["relation_field"]
                if f["relation"] == False:
                    del f["relation"]
                if f["required"] == False:
                    del f["required"]
                if f["readonly"] == False:
                    del f["readonly"]
                f["label"] = f["field_description"]
                del f["field_description"]

            yml_models[model["model"]] = {
                "description": model["name"],
                "fields": model_fields,
            }

        with open(output_file_path, "w") as yaml_file:
            yaml.dump(yml_models, yaml_file)
