import logging, os, pandas as pd, sqlalchemy as sa, sys

from datetime import datetime
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from otsokop.odoo import Odoo
from sqlalchemy import inspect, VARCHAR
from sqlalchemy.sql import text

start_date = "2021-01-01"
end_date = "2025-05-31"

INCLUDE_PRODUCT_TEMPLATE = True
INCLUDE_PRODUCT_PRICE_HISTORY = False

client = Odoo()
engine = sa.create_engine(os.getenv("MYSQL_ENGINE"))


def iterate_months(start_date, end_date):
    current_date = start_date.replace(day=1)
    while current_date <= end_date:
        yield current_date
        current_date += relativedelta(months=1)


def dump_mysql(df, table_name, dtype=None):
    df.to_sql(name=table_name, con=engine, if_exists="append", index=False, dtype=dtype)


def truncate_tables():
    with engine.connect() as connection:
        with connection.begin():
            inspector = inspect(engine)
            table_names = inspector.get_table_names()

            connection.execute(text("SET FOREIGN_KEY_CHECKS=0"))
            for table in table_names:
                connection.execute(text(f"DROP TABLE {table}"))
            connection.execute(text("SET FOREIGN_KEY_CHECKS=1"))


def add_constraints():
    inspector = inspect(engine)

    tables_to_alter = {}
    all_tables = inspector.get_table_names()

    for table_name in all_tables:
        columns = inspector.get_columns(table_name)
        pk_exists = inspector.get_pk_constraint(table_name).get("constrained_columns")

        needs_pk = not pk_exists and any(col["name"] == "id" for col in columns)

        potential_fks = []
        for col in columns:
            col_name = col["name"]
            if col_name.endswith("_id") and col_name != "id":
                referenced_table = col_name[:-3]  # Remove '_id' suffix
                if referenced_table in all_tables:
                    potential_fks.append((col_name, referenced_table))
                else:
                    logging.info(f"No FK for `{col_name}` in `{table_name}`")

        if needs_pk or potential_fks:
            tables_to_alter[table_name] = {
                "needs_pk": needs_pk,
                "potential_fks": potential_fks,
            }

    with engine.begin() as conn:
        # First loop to add PK
        for table_name, info in tables_to_alter.items():
            if info["needs_pk"]:
                try:
                    conn.execute(
                        text(f"ALTER TABLE `{table_name}` ADD PRIMARY KEY (`id`);")
                    )
                    logging.info(f"Added PK to `{table_name}`")
                except Exception as e:
                    logging.error(f"Could not add PK to `{table_name}`: {e}")

        # Second loop to add FK
        for table_name, info in tables_to_alter.items():
            for fk_col, ref_table in info["potential_fks"]:
                try:
                    constraint_name = f"fk_{table_name}_{ref_table}"
                    conn.execute(
                        text(
                            f"""
                        ALTER TABLE `{table_name}` 
                        ADD CONSTRAINT `{constraint_name}` 
                        FOREIGN KEY (`{fk_col}`) 
                        REFERENCES `{ref_table}`(`id`);
                    """
                        )
                    )
                    logging.info(
                        f"Added FK `{fk_col}` to `{table_name}` referencing `{ref_table}`"
                    )
                except Exception as e:
                    logging.error(
                        f"Could not add foreign key `{fk_col}` to `{table_name}`: {e}"
                    )


def execute_sql(conn, sql):
    conn.execute(text(sql))


def main(start_date, end_date):
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    logging.info("Dropping existing tables...")
    truncate_tables()

    # -------------------------------------------------------------------------
    # Table `product_rack`

    logging.info("Export `product_rack` table...")
    df = pd.read_csv("resources/racks.tsv", sep="\t")
    dump_mysql(df, "product_rack", {"code": VARCHAR(25)})

    # -------------------------------------------------------------------------
    # Tables `product` & `product_label`

    logging.info("Export `product` & `product_label` tables...")

    df = client.get_products()

    template_labels = pd.DataFrame(columns=["product_id", "product_label_id"])
    for _, row in df.iterrows():
        for l in row["label_ids"]:
            template_labels = pd.concat(
                [
                    pd.DataFrame([[row["id"], l]], columns=template_labels.columns),
                    template_labels,
                ],
                ignore_index=True,
            )
    df = df.drop("label_ids", axis=1)

    if not INCLUDE_PRODUCT_TEMPLATE:
        df = df.drop("product_template_id", axis=1)

    dump_mysql(df, "product", {"product_rack_code": VARCHAR(25)})
    dump_mysql(template_labels, "map_product_label_product")

    # -------------------------------------------------------------------------
    # Table `account_journal`

    logging.info("Export `account_journal` table...")
    dump_mysql(client.get_account_journals(), "account_journal")

    # -------------------------------------------------------------------------
    # Table `account`

    logging.info("Export `account` table...")
    dump_mysql(client.get_accounts(), "account")

    # -------------------------------------------------------------------------
    # Table `product_template` & `product_template_label`
    if INCLUDE_PRODUCT_TEMPLATE:

        logging.info("Export `product_template` & `product_template_label` tables...")

        templates = client.get_product_templates()

        template_labels = pd.DataFrame(
            columns=["product_template_id", "product_label_id"]
        )
        for t in templates.itertuples(index=False):
            for l in t.label_ids:
                template_labels = pd.concat(
                    [
                        pd.DataFrame([[t.id, l]], columns=template_labels.columns),
                        template_labels,
                    ],
                    ignore_index=True,
                )
        dump_mysql(template_labels, "product_template_label")

        templates = templates.drop("label_ids", axis=1)
        dump_mysql(templates, "product_template")

    # -------------------------------------------------------------------------
    # Table `product_label`

    logging.info("Export `product_label` table...")
    dump_mysql(client.get_product_labels(), "product_label")

    # -------------------------------------------------------------------------
    # Table `stock_location`

    logging.info("Export `stock_location` table...")
    dump_mysql(client.get_stock_locations(), "stock_location")

    # -------------------------------------------------------------------------
    # Table `product_category`

    logging.info("Export `product_category` table...")
    dump_mysql(client.get_product_categories(), "product_category")

    logging.info("Export `stock_picking_type` table...")
    dump_mysql(client.get_stock_picking_types(), "stock_picking_type")

    logging.info("Export `uom` table...")
    dump_mysql(client.get_uoms(), "uom")

    # -------------------------------------------------------------------------
    # Table `partner`

    logging.info("Export `partner` table...")
    dump_mysql(client.get_partners(), "partner")

    # -------------------------------------------------------------------------
    # Table `product_loss`

    logging.info("Export `product_loss` table...")
    dump_mysql(client.get_product_losses(start_date, end_date), "product_loss")

    # loop for each month to avoid requesting huge amount of data in a single
    # call.

    for month_date in iterate_months(start_date, end_date):
        logging.info(f"Fetching data for {month_date.strftime('%Y-%m-%d')}...")

        end = month_date + relativedelta(months=1, days=-1)

        # ---------------------------------------------------------------------
        # Table `pos_order_detail`

        logging.info("    * pos_order_detail")

        df = client.get_pos_orders(
            month_date.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")
        )
        details = df[1]
        details = details.drop("product_name", axis=1)
        details = details.drop("date_order", axis=1)
        details = details.rename(columns={"order_id": "pos_order_id"})
        dump_mysql(details, "pos_order_detail")

        # ---------------------------------------------------------------------
        # Table `pos_order`

        logging.info("    * pos_order")

        df = df[0].drop("lines", axis=1)
        df = df.drop("partner_name", axis=1)
        dump_mysql(df, "pos_order")

        # ---------------------------------------------------------------------
        # Table `purchase_detail`

        logging.info("    * purchase_order_detail")

        df = client.get_purchase_orders(
            month_date.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")
        )
        details = df[1]
        details = details.drop("product_name", axis=1)
        details = details.drop("date_order", axis=1)
        details = details.rename(
            columns={"order_id": "purchase_order_id", "display_name": "name"}
        )
        dump_mysql(details, "purchase_order_detail")

        purchase_order = df[0]
        # XXX temporary, done in odoo.py
        purchase_order = purchase_order.rename(columns={"supplier_id": "partner_id"})
        dump_mysql(purchase_order, "purchase_order")

        # ---------------------------------------------------------------------
        # Table `account_invoice`

        logging.info("    * account_invoice & account_invoice_line")

        invoices = client.get_account_invoices(
            month_date.strftime("%Y-%m-%d"),
            end.strftime("%Y-%m-%d"),
            include_invoice_lines=True,
        )
        dump_mysql(invoices[0], "account_invoice")
        invoices[1] = invoices[1].rename(columns={"invoice_id": "account_invoice_id"})
        dump_mysql(invoices[1], "account_invoice_line")

        # ---------------------------------------------------------------------
        # Table `account_move_line`

        logging.info("    * account_move_line")

        account_move_line = client.get_account_move_lines(
            month_date.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")
        )
        dump_mysql(account_move_line, "account_move_line")

        # ---------------------------------------------------------------------
        # Table `stock_move`

        logging.info("    * stock_move")
        stock_move = client.get_stock_moves(
            month_date.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")
        )
        # stock_move = stock_move.rename(columns={"picking_type_id", "stock_picking_type_id"})
        dump_mysql(stock_move, "stock_move")

        # ---------------------------------------------------------------------
        # Table `stock_move_line`

        logging.info("    * stock_move_line")
        stock_move_line = client.get_stock_move_lines(
            month_date.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")
        )
        # XXX temporary, done in odoo.py
        stock_move_line = stock_move_line.rename(columns={"product_uom_id": "uom_id"})
        dump_mysql(stock_move_line, "stock_move_line")

        # ---------------------------------------------------------------------
        # Table `product_history`

        logging.info("    * product_history")

        result = client.get_product_history(
            month_date.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")
        )
        # XXX temporary, done in odoo.py
        result = result.rename(columns={"location_id": "stock_location_id"})
        dump_mysql(result, "product_history")

    add_constraints()

    # manual FK
    with engine.begin() as conn:
        execute_sql(
            conn,
            "ALTER TABLE stock_move_line ADD CONSTRAINT fk_stock_move_line_location_dest_id FOREIGN KEY (dest_stock_location_id)  REFERENCES stock_location(id)",
        )
        execute_sql(
            conn,
            "ALTER TABLE stock_move ADD CONSTRAINT fk_stock_move_location_dest_id FOREIGN KEY (dest_stock_location_id)  REFERENCES stock_location(id)",
        )

        execute_sql(conn, "ALTER TABLE product_rack ADD PRIMARY KEY (code)")
        execute_sql(
            conn,
            "ALTER TABLE product ADD CONSTRAINT fk_product_rack FOREIGN KEY (product_rack_code) REFERENCES product_rack(code)",
        )

        # conn.execute(
        #     text(
        #         "ALTER TABLE category ADD CONSTRAINT fk_category_parent_id FOREIGN KEY (parent_id)  REFERENCES category(id)"
        #     )
        # )

    logging.info("Dump complete")
    logging.info(
        f"""Create a SQL dump with the following command:
    mysqldump --skip-lock-tables --routines --add-drop-table --disable-keys --extended-insert -u {os.getenv('MYSQL_USERNAME')}  -p{os.getenv('MYSQL_PASSWORD')} --host={os.getenv('MYSQL_HOST')} --port={os.getenv('MYSQL_PORT')} --protocol tcp {os.getenv('MYSQL_DATABASE')} | gzip -c > /tmp/{os.getenv('MYSQL_DATABASE')}.sql.gz

    """
    )


if __name__ == "__main__":
    sys.exit(main(start_date, end_date))
