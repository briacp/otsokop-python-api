import pandas as pd
import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.sql import text
from datetime import datetime, timedelta
from otsokop.odoo import Odoo
from otsokop.odoo import banner as otsokop_banner
from dateutil.relativedelta import relativedelta

start_date = "2023-01-01"
end_date = "2025-04-01"

client = Odoo("app_settings.json")
engine = sa.create_engine("mysql+pymysql://root:admin@localhost:3306/otsokop_odoo")


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
        # dtype=dtype,
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


truncate_tables()

df = pd.read_csv("racks.tsv", sep="\t")
dump_mysql(df, "rack")

df = client.get_all_products()
df = df.drop("categ_name", axis=1)
df = df.rename(columns={"categ_id": "category_id"})
dump_mysql(df, "product")

templates = client.execute_kw(
    "product.template",
    "search_read",
    [
        ["|", ["active", "=", "true"], ["active", "=", "false"]],
        ["name", "active", "available_in_pos", "storage", "sale_ok", "label_ids"],
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

categories = client.execute_kw(
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
for r in categories:
    r["parent_id"] = r["parent_id"][0] if r["parent_id"] else None
dump_mysql(pd.DataFrame(categories), "category")

df = client.get_all_members()
dump_mysql(df, "member")

start_date = datetime.strptime(start_date, "%Y-%m-%d")
end_date = datetime.strptime(end_date, "%Y-%m-%d")

df = client.export_pertes(start_date, end_date)
df = df.drop("name", axis=1)
dump_mysql(df, "loss")

for month_date in iterate_months(start_date, end_date):
    print(month_date.strftime("%Y-%m-%d"))

    end = month_date + relativedelta(months=1, days=-1)

    df = client.get_pos_orders(
        month_date.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")
    )
    details = df[1]
    details = details.drop("product_name", axis=1)
    details = details.drop("date_order", axis=1)
    dump_mysql(details, "pos_order_detail")

    df = df[0].drop("lines", axis=1)
    df = df.drop("partner_name", axis=1)
    dump_mysql(df, "pos_order")

    df = client.get_purchase_orders(
        month_date.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")
    )
    details = df[1]
    details = details.drop("product_name", axis=1)
    details = details.drop("date_order", axis=1)
    dump_mysql(details, "purchase_detail")

    df = df[0].drop("order_line", axis=1)
    dump_mysql(df, "purchase")
