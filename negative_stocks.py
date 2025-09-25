from babel.dates import format_date
from babel.numbers import format_decimal
from datetime import date
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from otsokop.odoo import Odoo
import pandas as pd
import logging
import os
import smtplib
import sys

LOCALE = "fr_FR"
DAY_FORMAT = "EEEE'<br/>'dd MMMM'<br/>'yyyy"
MONTH_FORMAT = "MMMM'<br/>'yyyy"

load_dotenv()

SENDER_EMAIL = os.getenv("SENDER_EMAIL")
# comma separated list of recipients
RECEIVER_EMAIL = os.getenv("RECEIVER_EMAIL")
# generated with https://myaccount.google.com/apppasswords
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
SEND_EMAIL = True

def main():
    client = Odoo()
    stocks = client.execute_kw(
        "product.template",
        "search_read",
        [
            [
                ["qty_available", "<", 0],
            ],
            [
                "name",
                "rack_location",  # emplacement
                "standard_price",  # cout
                "qty_available",  # stock
                # "product_variant_ids/stock_value", # valeur du stock
            ],
            0,  # offset
            0,  # limit
            "rack_location,qty_available",  # sort
        ],
    )
    stocks = pd.DataFrame(stocks)
    stocks["total"] = stocks["standard_price"] * stocks["qty_available"]

    stocks.to_excel(
        "stocks_negatifs.xlsx",
        index=False,
        sheet_name="Stocks négatifs",
        float_format="%.2f",
        freeze_panes=(1, 2),
        columns=[
            "id",
            "name",
            "rack_location",
            "standard_price",
            "qty_available",
            "total",
        ],
        header=["Id", "Produit", "Rayon", "Coût", "Stock", "Valeur"],
    )

    content = start_html()
    content.extend(
        [
            """
        <p>Bonjour,</p>
        <p>Voici en pièce-jointe la liste des produits dont le stock est négatif.</p>
    """
        ])

    content.extend(
        [
            """
        <p>Bonne journée,</p>
        """
            "</body></html>"
        ]
    )

    send_email(content)


def start_html():
    content = []
    content.append("<html><head>")
    content.append("<style>")
    content.append(
        """
    body { font-family: 'Gill Sans', 'Gill Sans MT', Calibri, 'Trebuchet MS', sans-serif; }
    h2 { color: #9c5c34; }
    table {  border-collapse: collapse; border: solid 1px #2b392b; }
    th, td { padding: 0.2em; }
    /*
    tr th:first-child { width: 220px; }
    .up:after { content: "▲"; }
    .down:after { content: "▼"; }
    table tr:first-child th { padding: 1em; color: #fff; background-color: #405140; }
    tr:nth-child(odd) { background-color:#eeedbb; }
    */
    th.stat {width: 220px; }
    .up { color: #2e822b; }
    .down { color: #b72525; }
    th { text-align: right; }
    td { text-align: right; }
    td.gap { width: 125px; }
    th.values { width: 150px; }
    td.current { font-weight: bold; }
    """
    )
    content.append("</style>")
    content.append("</head><body>")
    return content


def send_email(body, attachments=None):
    if not SEND_EMAIL:
        print("\n".join(body))
        return

    recipients = RECEIVER_EMAIL.split(",")

    msg = MIMEMultipart()
    msg["From"] = SENDER_EMAIL
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = "[Otsokop] Suivi desstocks négatifs"

    msg.attach(MIMEText("\n".join(body), "html"))

    msg.attach(MIMEApplication(open("stocks_negatifs.xlsx", "rb").read(), Name="stocks_negatifs.xlsx"))

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(SENDER_EMAIL, EMAIL_PASSWORD)
            server.sendmail(SENDER_EMAIL, recipients, msg.as_string())
        logging.info("Email envoyé avec succès.")
    except Exception as e:
        logging.error(f"Erreur lors de l'envoi de l'email : {e}")


if __name__ == "__main__":
    sys.exit(main())
