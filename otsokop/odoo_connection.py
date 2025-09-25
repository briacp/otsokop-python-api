import requests
import logging
import os
import xmlrpc.client
import pytz
import sys


class OdooConnection:
    def __init__(self, *, server, database, username, password, timezone, debug=False):
        self.url = server.rstrip("/")
        self.db = database
        self.username = username
        self.password = password
        self.session = requests.Session()
        self.uid = None
        self.debug = debug

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

        timezone_value = timezone or os.getenv("ODOO_TIMEZONE")
        if timezone_value:
            self._local_tz = pytz.timezone(timezone_value)
        else:
            self._local_tz = pytz.UTC

    def authenticate_xmlrpc(self):
        # Connect to Odoo
        logging.info(f"Connecting to Odoo server {self.url} {self.database}...")
        uid = xmlrpc.client.ServerProxy(
            f"{self.url}/xmlrpc/2/common", verbose=self.debug
        ).authenticate(self.database, self.username, self._password, {})

        if uid:
            self._uid = uid
        else:
            raise Exception(f"Failed to authenticate to {self.url}.")

        self.odoo = xmlrpc.client.ServerProxy(
            f"{self.url}/xmlrpc/2/object", verbose=self.debug
        )

    def execute_kw_xmlrpc(self, model, method, params, *mapping: None):
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

    def _str_to_bool(self, value):
        return value and value.lower() == "true"

    def authenticate(self):
        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "db": self.db,
                "login": self.username,
                "password": self.password,
            },
            "id": 1,
        }
        resp = self.session.post(f"{self.url}/web/session/authenticate", json=payload)
        result = resp.json()
        if self.debug:
            logging.info(f"Odoo auth response: {result}")
        if "result" in result and result["result"].get("uid"):
            self.uid = result["result"]["uid"]
        else:
            raise Exception(f"Authentication failed: {result}")

    def call(self, model, method, args=None, kwargs=None):
        if self.uid is None:
            self.authenticate()

        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "model": model,
                "method": method,
                "args": args or [],
                "kwargs": kwargs or {},
                "context": {"uid": self.uid},
            },
            "id": 1,
        }
        resp = self.session.post(f"{self.url}/web/dataset/call_kw", json=payload)
        response = resp.json()
        if self.debug:
            logging.info(f"Odoo call {model}.{method} response: {response}")
        if "result" in response:
            return response["result"]
        elif "error" in response:
            raise Exception(f"Odoo JSON-RPC Error: {response['error']}")
        else:
            raise Exception(f"Unknown Odoo JSON-RPC response: {response}")
