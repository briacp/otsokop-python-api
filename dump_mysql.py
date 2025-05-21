import logging, os, pandas as pd, sqlalchemy as sa, sys

from datetime import datetime
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from otsokop.odoo import Odoo
from sqlalchemy import inspect
from sqlalchemy.sql import text

start_date = "2022-01-01"
end_date = "2025-05-01"

INCLUDE_PRODUCT_TEMPLATE = False
INCLUDE_PRODUCT_PRICE_HISTORY = False

client = Odoo()
engine = sa.create_engine(os.getenv("MYSQL_ENGINE"))


def iterate_months(start_date, end_date):
    current_date = start_date.replace(day=1)
    while current_date <= end_date:
        yield current_date
        current_date += relativedelta(months=1)


def dump_mysql(df, table_name):
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists="append",
        index=False,
    )


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


def main(start_date, end_date):
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    logging.info("Dropping existing tables...")
    truncate_tables()

    # -------------------------------------------------------------------------
    # Table `rack`

    logging.info("Export `rack` table...")
    df = pd.read_csv("resources/racks.tsv", sep="\t")
    dump_mysql(df, "rack")

    # -------------------------------------------------------------------------
    # Tables `product` & `product_label`

    logging.info("Export `product` & `product_label` tables...")

    df = client.get_all_products()
    df = df.drop("categ_name", axis=1)
    template_labels = pd.DataFrame(columns=["product_id", "label_id"])
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
    df = df.rename(
        columns={
            "categ_id": "category_id",
            "product_tmpl_id": "product_template_id",
        }
    )
    dump_mysql(df, "product")
    dump_mysql(template_labels, "product_label")

    # -------------------------------------------------------------------------
    # Table `account_journal`

    logging.info("Export `account_journal` table...")
    journals = pd.DataFrame(
        client.execute_kw(
            "account.journal",
            "search_read",
            [
                [],
                ["code", "name"],
            ],
        )
    )
    dump_mysql(journals, "account_journal")

    # -------------------------------------------------------------------------
    # Table `account`

    logging.info("Export `account` table...")

    accounts = client.execute_kw(
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
    for r in accounts:
        r["user_type"] = r["user_type_id"][1] if r["user_type_id"] else None
        r["user_type_id"] = r["user_type_id"][0] if r["user_type_id"] else None

    dump_mysql(pd.DataFrame(accounts), "account")

    # -------------------------------------------------------------------------
    # Table `product_template` & `product_template_label`
    if INCLUDE_PRODUCT_TEMPLATE:

        logging.info("Export `product_template` & `product_template_label` tables...")

        templates = client.execute_kw(
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
                    "label_ids",
                ],
            ],
        )

        template_labels = pd.DataFrame(columns=["product_template_id", "label_id"])
        for t in templates:
            for l in t["label_ids"]:
                template_labels = pd.concat(
                    [
                        pd.DataFrame([[t["id"], l]], columns=template_labels.columns),
                        template_labels,
                    ],
                    ignore_index=True,
                )
        dump_mysql(template_labels, "product_template_label")

        df = pd.DataFrame(templates)
        df = df.drop("label_ids", axis=1)
        dump_mysql(df, "product_template")

    # -------------------------------------------------------------------------
    # Table `label`

    logging.info("Export `label` table...")

    df = pd.DataFrame(
        client.execute_kw(
            "product.label",
            "search_read",
            [
                [],
                ["code", "name"],
            ],
        )
    )
    dump_mysql(df, "label")

    # -------------------------------------------------------------------------
    # Table `location`

    logging.info("Export `stock_location` table...")

    df = pd.DataFrame(
        client.execute_kw(
            "stock.location",
            "search_read",
            [
                [],
                ["name", "comment"],
            ],
        )
    )
    df["comment"].replace(to_replace=0, value=pd.NA, inplace=True)
    dump_mysql(df, "stock_location")

    # -------------------------------------------------------------------------
    # Table `product_price_history`

    if INCLUDE_PRODUCT_PRICE_HISTORY:

        logging.info("Export `product_price_history` table...")

        price_history = client.execute_kw(
            "product.price.history",
            "search_read",
            [
                [],
                ["create_date", "cost", "product_id"],
            ],
        )
        for r in price_history:
            r["product_id"] = r["product_id"][0] if r["product_id"] else None
        df = pd.DataFrame(price_history)
        dump_mysql(df, "product_price_history")

    # -------------------------------------------------------------------------
    # Table `category`

    logging.info("Export `category` table...")

    categories = client.execute_kw(
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
    for r in categories:
        r["parent_id"] = r["parent_id"][0] if r["parent_id"] else None

    categories = pd.DataFrame(categories)
    categories = categories.rename(columns={"display_name": "name"})
    dump_mysql(categories, "category")

    # -------------------------------------------------------------------------
    # Table `member`

    logging.info("Export `member` table...")

    df = client.get_all_members()
    dump_mysql(df, "member")

    # -------------------------------------------------------------------------
    # Table `supplier`

    logging.info("Export `supplier` table...")

    df = client.get_suppliers()
    dump_mysql(df, "supplier")

    # -------------------------------------------------------------------------
    # Table `product_loss`

    logging.info("Export `loss` table...")

    df = client.get_product_losses(start_date, end_date)
    df = df.drop("name", axis=1)
    df = df.rename(columns={"location_id": "stock_location_id"})
    dump_mysql(df, "product_loss")

    # loop for each month to avoid requesting huge amount of data in a single
    # call.

    for month_date in iterate_months(start_date, end_date):
        logging.info(f"Fetching data for {month_date.strftime('%Y-%m-%d')}...")

        end = month_date + relativedelta(months=1, days=-1)

        # ---------------------------------------------------------------------
        # Table `pos_order_detail`

        logging.info("    - pos_order_detail")

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

        logging.info("    - pos_order")

        df = df[0].drop("lines", axis=1)
        df = df.drop("partner_name", axis=1)
        dump_mysql(df, "pos_order")

        # ---------------------------------------------------------------------
        # Table `purchase_detail`

        logging.info("    - purchase_order_detail")

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
        dump_mysql(purchase_order, "purchase_order")

        # ---------------------------------------------------------------------
        # Table `account_invoice`

        logging.info("    - acount_invoice & account_invoice_line")

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

        logging.info("    - account_move_line")

        account_move_line = client.get_account_move_lines(
            month_date.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")
        )
        dump_mysql(account_move_line, "account_move_line")

    add_constraints()
    logging.info("Dump complete")
    logging.info(
        f"""Create a SQL dump with the following command:
    mysqldump --skip-lock-tables --routines --add-drop-table --disable-keys --extended-insert -u {os.getenv('MYSQL_USERNAME')}  -p{os.getenv('MYSQL_PASSWORD')} --host={os.getenv('MYSQL_HOST')} --port={os.getenv('MYSQL_PORT')} --protocol tcp {os.getenv('MYSQL_DATABASE')} | gzip -c > /tmp/{os.getenv('MYSQL_DATABASE')}.sql.gz

    """
    )


if __name__ == "__main__":
    sys.exit(main(start_date, end_date))
