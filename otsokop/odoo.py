import diskcache, logging, os, pandas as pd, pytz, re, sys, xmlrpc.client, yaml

from dotenv import load_dotenv
from datetime import datetime
from re import search, sub
from otsokop.odoo_cache import odoo_cache

banner = """
 ____ _____ ____  ____  _  __ ____  ____ 
/  _ Y__ __Y ___\/  _ \/ |/ //  _ \/  __\\
| / \| / \ |    \| / \||   / | / \||  \/|
| \_/| | | \___ || \_/||   \ | \_/||  __/
\____/ \_/ \____/\____/\_|\_\\\____/\_/   
"""


class Odoo:
    DISABLE_CACHE = False
    ONE_DAY = 60 * 60 * 24

    def __init__(
        self,
        *,
        server=None,
        database=None,
        username=None,
        password=None,
        debug=None,
        timezone=None,
        logging_level=None,
    ):
        load_dotenv()

        self._cache = diskcache.Cache("cache")
        self._cache.expire()
        logging.basicConfig(
            level=Odoo._set_log_level(
                logging_level or os.getenv("LOGGING_LEVEL") or "INFO"
            ),
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )

        # Set attributes with priority to directly passed parameters
        self._uid = None
        self.url = server or os.getenv("ODOO_SERVER")
        self.db = database or os.getenv("ODOO_DATABASE")
        self.username = username or os.getenv("ODOO_USERNAME")
        self._password = password or os.getenv("ODOO_SECRET")
        self.debug = (
            debug if debug is not None else Odoo._str_to_bool(os.getenv("ODOO_DEBUG"))
        )

        timezone_value = timezone or os.getenv("ODOO_TIMEZONE")
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
                "Please provide them either through .env file or as parameters."
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

    @odoo_cache()
    def get_pos_orders(
        self, date_start, date_end: str = None, include_order_lines=True
    ):
        (datetime_start, datetime_end) = self._interval_dates(date_start, date_end)

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
        self._set_zeros_to_none(orders, ["partner_id"])

        result = [orders, pd.DataFrame(data_order_lines)]

        return result

    @odoo_cache()
    def get_report_pos_orders(self, date_start, date_end: str = None):
        (datetime_start, datetime_end) = self._interval_dates(date_start, date_end)

        logging.debug(f"get_report_pos_orders {datetime_start} - {datetime_end}")

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

        return report_pos_orders

    @odoo_cache()
    def get_purchase_orders(
        self, date_start, date_end: str = None, include_order_lines=True
    ):
        (datetime_start, datetime_end) = self._interval_dates(date_start, date_end)

        logging.debug(f"get_purchase_orders {datetime_start} - {datetime_end}")

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
            supplier = purchase_order["partner_id"] or [0, None]
            purchase_order["partner_id"] = supplier[0]
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

        return result

    @odoo_cache(ttl=ONE_DAY, force_fetch=False)
    def get_products(self):
        logging.debug("Getting the list of all products...")

        results = self.execute_kw(
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
                    "base_price",
                    "taxes_id",
                    "fiscal_classification_id",
                    "coeff1_id",
                    # "coeff1_inter",
                    # "coeff1_inter_sp",
                    "coeff2_id",
                    # "coeff2_inter",
                    # "coeff2_inter_sp",
                    "coeff3_id",
                    # "coeff3_inter",
                    # "coeff3_inter_sp",
                    "coeff4_id",
                    # "coeff4_inter",
                    # "coeff4_inter_sp",
                    "coeff5_id",
                    # "coeff5_inter",
                    # "coeff5_inter_sp",
                    # "coeff6_id",
                    # "coeff6_inter",
                    # "coeff6_inter_sp",
                    # "coeff7_id",
                    # "coeff7_inter",
                    # "coeff7_inter_sp",
                    # "coeff8_id",
                    # "coeff8_inter",
                    # "coeff8_inter_sp",
                    # "coeff9_id",
                    # "coeff9_inter",
                    # "coeff9_inter_sp",
                ],
            ],
        )

        for line in results:
            line["deref"] = False
            line["tax_id"] = line["taxes_id"][0] if line["taxes_id"] else None
            self._remove_odoo_id(
                line,
                [
                    "product_tmpl_id",
                    "categ_id",
                    "coeff1_id",
                    "coeff2_id",
                    "coeff3_id",
                    "coeff4_id",
                    "coeff5_id",
                    # "coeff6_id",
                    # "coeff7_id",
                    # "coeff8_id",
                    # "coeff9_id",
                    "taxes_id",
                    "fiscal_classification_id",
                ],
            )
            if line["rack_location"]:
                rack = line["rack_location"]
                if (
                    search("[bD][eé]ref", rack, flags=re.IGNORECASE)
                    or rack == "Pas de rotation"
                ):
                    line["deref"] = True

                rack = sub("[bD][eé]ref", "", rack, flags=re.IGNORECASE)
                rack = sub("\s*-\s*", "", rack)
                rack = sub("Pas de rotation", "", rack)
                rack = rack.strip()
                if rack == "":
                    rack = None
                line["rack_location"] = rack

        results = pd.DataFrame(results)
        self._set_zeros_to_none(
            results,
            [
                "barcode",
                "rack_location",
                "theoritical_price",
                "base_price",
                "coeff1_id",
                "coeff2_id",
                "coeff3_id",
                "coeff4_id",
                "coeff5_id",
                # "coeff6_id",
                # "coeff7_id",
                # "coeff8_id",
                # "coeff9_id",
            ],
        )

        results = results.rename(
            columns={
                "categ_id": "product_category_id",
                "product_tmpl_id": "product_template_id",
                "rack_location": "product_rack_code",
            }
        )

        results["create_date"] = pd.to_datetime(results["create_date"])

        return results

    @odoo_cache(ttl=ONE_DAY)
    def get_partners(self):
        results = self.execute_kw(
            "res.partner",
            "search_read",
            [
                ["|", ["active", "=", True], ["active", "=", False]],
                [
                    "name",
                    "city",
                    "street",
                    "street2",
                    "zip",
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
                    "function",
                    "mobile",
                    "email",
                    "purchase_target",
                    "default_supplierinfo_discount",
                    "create_date",
                ],
            ],
        )
        results = pd.DataFrame(results)
        self._set_zeros_to_none(
            results,
            [
                "street",
                "street2",
                "city",
                "zip",
                "mobile",
                "email",
                "gender",
                "age",
                "function",
                "purchase_target",
                "default_supplierinfo_discount",
            ],
        )
        return results

    @odoo_cache(force_fetch=False)
    def get_product_losses(self, date_start, date_end):
        (datetime_start, datetime_end) = self._interval_dates(date_start, date_end)

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
                    ["date_expected", ">=", datetime_start],
                    ["date_expected", "<=", datetime_end],
                ],
                [
                    "date_expected",
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

        if not stock_moves:
            return None

        result = pd.DataFrame(stock_moves)
        result["date_expected"] = pd.to_datetime(result["date_expected"])
        result = result.rename(columns={"location_id": "stock_location_id"})

        return result

    @odoo_cache()
    def get_stock_moves(self, date_start, date_end):
        (datetime_start, datetime_end) = self._interval_dates(date_start, date_end)

        stock_moves = self.execute_kw(
            "stock.move",
            "search_read",
            [
                [
                    ["date_expected", ">=", datetime_start],
                    ["date_expected", "<=", datetime_end],
                ],
                [
                    "date_expected",
                    "location_id",
                    "location_dest_id",
                    "product_id",
                    "product_qty",
                    "price_unit",
                    "picking_type_id",
                    "state",
                ],
            ],
        )

        for sm in stock_moves:
            self._remove_odoo_id(
                sm, ["product_id", "location_id", "picking_type_id", "location_dest_id"]
            )
        result = pd.DataFrame(stock_moves)
        result["date_expected"] = pd.to_datetime(result["date_expected"])
        self._set_zeros_to_none(result, ["picking_type_id"])
        result = result.rename(
            columns={
                "location_id": "stock_location_id",
                "location_dest_id": "dest_stock_location_id",
                "picking_type_id": "stock_picking_type_id",
            }
        )

        return result

    @odoo_cache(force_fetch=False)
    def get_stock_move_lines(self, date_start, date_end):
        (datetime_start, datetime_end) = self._interval_dates(date_start, date_end)

        result = self.execute_kw(
            "stock.move.line",
            "search_read",
            [
                [
                    ["date", ">=", datetime_start],
                    ["date", "<", datetime_end],
                ],
                [
                    "date",
                    "location_id",
                    "location_dest_id",
                    "move_id",
                    "product_qty",
                    "product_id",
                    "product_uom_id",
                    "product_uom_qty",
                    "qty_done",
                    "state",
                ],
            ],
        )

        for row in result:
            self._remove_odoo_id(
                row,
                [
                    "move_id",
                    "location_dest_id",
                    "location_id",
                    "product_uom_id",
                    "product_id",
                ],
            )

        result = pd.DataFrame(result)
        result = result.rename(
            columns={
                "location_id": "stock_location_id",
                "location_dest_id": "dest_stock_location_id",
                "move_id": "stock_move_id",
                "product_uom_id": "uom_id",
            }
        )
        self._set_zeros_to_none(result, ["stock_move_id"])
        result["date"] = pd.to_datetime(result["date"])
        return result

    @odoo_cache()
    def get_account_invoices(self, date_start, date_end, include_invoice_lines=True):
        (datetime_start, datetime_end) = self._interval_dates(date_start, date_end)

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
        self._set_zeros_to_none(invoices, ["purchase_id"])

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
            self._set_zeros_to_none(invoice_lines, ["product_id"])
        else:
            invoice_lines = pd.DataFrame()

        try:
            invoices = invoices.drop("invoice_line_ids", axis=1)
        except:
            pass

        result = [invoices, invoice_lines]

        return result

    @odoo_cache()
    def get_account_move_lines(self, date_start, date_end):
        (datetime_start, datetime_end) = self._interval_dates(date_start, date_end)

        result = self.execute_kw(
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

        for line in result:
            line["move_ref"] = line["move_id"][1]
            self._remove_odoo_id(line, ["journal_id", "account_id", "move_id"])
            if line["debit"] > 0:
                line["dc_flag"] = "D"
            else:
                line["dc_flag"] = "C"

        result = pd.DataFrame(result)
        result["date"] = pd.to_datetime(result["date"])
        result = result.rename(
            columns={
                "journal_id": "account_journal_id",
                "move_id": "account_move_id",
            }
        )

        return result

    @odoo_cache(ttl=ONE_DAY)
    def get_product_templates(self):
        results = self.execute_kw(
            "product.template",
            "search_read",
            [
                ["|", ["active", "=", True], ["active", "=", False]],
                [
                    "name",
                    "active",
                    "available_in_pos",
                    "storage",
                    "sale_ok",
                    "default_code",
                    "create_date",
                    "type",
                    "label_ids",
                ],
            ],
        )

        results = pd.DataFrame(results)
        results["create_date"] = pd.to_datetime(results["create_date"])
        results["storage"].replace(to_replace=0, value=pd.NA, inplace=True)
        results["default_code"].replace(to_replace=0, value=pd.NA, inplace=True)

        return results

    @odoo_cache(ttl=ONE_DAY)
    def get_product_coefficients(self):
        result = self.execute_kw(
            "product.coefficient",
            "search_read",
            [
                ["|", ["active", "=", True], ["active", "=", False]],
                ["name", "active", "note", "operation_type", "value"],
            ],
        )
        result = pd.DataFrame(result)
        self._set_zeros_to_none(result, ["note"])
        return result

    @odoo_cache(ttl=ONE_DAY)
    def get_account_journals(self):
        result = self.execute_kw(
            "account.journal",
            "search_read",
            [
                ["|", ["active", "=", True], ["active", "=", False]],
                ["code", "name", "active"],
            ],
        )
        result = pd.DataFrame(result)
        return result

    @odoo_cache(ttl=ONE_DAY)
    def get_stock_picking_types(self):
        result = self.execute_kw(
            "stock.picking.type",
            "search_read",
            [
                ["|", ["active", "=", True], ["active", "=", False]],
                ["name", "code", "active"],
            ],
        )
        result = pd.DataFrame(result)
        return result

    @odoo_cache(ttl=ONE_DAY)
    def get_uoms(self):
        result = self.execute_kw(
            "uom.uom",
            "search_read",
            [
                ["|", ["active", "=", True], ["active", "=", False]],
                ["name", "measure_type", "rounding", "uom_type", "active"],
            ],
        )
        result = pd.DataFrame(result)
        return result

    @odoo_cache(ttl=ONE_DAY)
    def get_account_taxes(self):
        result = self.execute_kw(
            "account.tax",
            "search_read",
            [
                [],
                [
                    "id",
                    "account_id",
                    "name",
                    "active",
                    "amount",
                    "amount_type",
                ],
            ],
        )
        for r in result:
            self._remove_odoo_id(r, ["account_id"])

        result = pd.DataFrame(result)
        return result

    @odoo_cache(ttl=ONE_DAY)
    def get_account_fiscal_classification(self):
        result = self.execute_kw(
            "account.product.fiscal.classification",
            "search_read",
            [
                [],
                [
                    "id",
                    "name",
                    "active",
                    "description",
                ],
            ],
        )
        result = pd.DataFrame(result)
        self._set_zeros_to_none(result, ["description"])
        return result

    @odoo_cache(ttl=ONE_DAY)
    def get_accounts(self):
        result = self.execute_kw(
            "account.account",
            "search_read",
            [
                [],
                [
                    "id",
                    "code",
                    "name",
                    "user_type_id",
                ],
            ],
        )
        for r in result:
            r["user_type"] = r["user_type_id"][1] if r["user_type_id"] else None
            r["user_type_id"] = r["user_type_id"][0] if r["user_type_id"] else None

        result = pd.DataFrame(result)
        return result

    @odoo_cache(ttl=ONE_DAY)
    def get_stock_locations(self):
        results = pd.DataFrame(
            self.execute_kw(
                "stock.location",
                "search_read",
                [
                    [],
                    ["name", "comment"],
                ],
            )
        )
        results["comment"].replace(to_replace=0, value=pd.NA, inplace=True)

        return results

    @odoo_cache()
    def get_product_history(self, date_start, date_end):
        (datetime_start, datetime_end) = self._interval_dates(date_start, date_end)

        result = self.execute_kw(
            "product.history",
            "search_read",
            [
                [
                    ["from_date", ">=", datetime_start],
                    ["to_date", "<=", datetime_end],
                ],
                [
                    "from_date",
                    "to_date",
                    "product_id",
                    "location_id",
                    "loss_qty",
                    "end_qty",
                    "virtual_qty",
                    "sales_qty",
                    "incoming_qty",
                    "purchase_qty",
                    "production_qty",
                    "outgoing_qty",
                    "ignored",
                ],
            ],
        )

        if result is None:
            return None

        for line in result:
            self._remove_odoo_id(line, ["product_id", "location_id"])

        result = pd.DataFrame(result)
        result["from_date"] = pd.to_datetime(result["from_date"])
        result["to_date"] = pd.to_datetime(result["to_date"])
        result = result.rename(columns={"location_id": "stock_location_id"})
        return result

    @odoo_cache(ttl=ONE_DAY)
    def get_product_labels(self):
        result = self.execute_kw(
            "product.label",
            "search_read",
            [
                [],
                ["code", "name"],
            ],
        )
        result = pd.DataFrame(result)
        return result

    @odoo_cache(ttl=ONE_DAY)
    def get_product_categories(self):
        results = self.execute_kw(
            "product.category",
            "search_read",
            [
                [],
                [
                    "id",
                    "display_name",
                    "parent_id",
                    # "product_count",
                ],
            ],
        )

        for r in results:
            r["parent_id"] = r["parent_id"][0] if r["parent_id"] else None

        results = pd.DataFrame(results)
        results = results.rename(columns={"display_name": "name"})
        return results

    """
    Liste des produits vendable d'un rayon. Cette liste peut servir de base à l'inventaire.
    """

    @odoo_cache(ttl=ONE_DAY)
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
                    "name",
                    "barcode",
                    "uom_id",
                    "qty_available",
                ],
                0,
                0,
                "rack_location,name",
            ],
        )

        for product in result:
            product["categ"] = product["categ_id"][1]

        result = pd.DataFrame(result, columns=["rack_location", "categ"])
        result.drop_duplicates(inplace=True)
        return result.shape

    def _remove_odoo_id(self, odoo_object, oddo_id_fields):
        for odoo_field in oddo_id_fields:
            if odoo_object[odoo_field]:
                odoo_object[odoo_field] = odoo_object[odoo_field][0]

    def _set_zeros_to_none(self, df, oddo_fields):
        for odoo_field in oddo_fields:
            df[odoo_field].replace(to_replace=0, value=pd.NA, inplace=True)

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

        if data is None:
            return

        logging.debug(f"setting cached {cache_key}...")
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
            yaml.dump([yml_models], yaml_file)

    def _str_to_bool(value):
        return value and value.lower() == "true"

    def _set_log_level(level_name):
        level_name = level_name.upper()
        level = getattr(logging, level_name, None)

        if level is None:
            raise ValueError(f"Invalid log level: {level_name}")

        logging.getLogger().setLevel(level)
