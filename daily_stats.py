from babel.dates import format_date
from babel.numbers import format_decimal
from datetime import date
from datetime import datetime
from dateutil.relativedelta import relativedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from otsokop.odoo import Odoo
import holidays
import logging
import os
import smtplib
import sys
import json

FR_HOLIDAYS = holidays.FR()
LOCALE = "fr_FR"
DAY_FORMAT = "EEEE'<br/>'dd MMMM'<br/>'yyyy"
MONTH_FORMAT = "MMMM'<br/>'yyyy"

SEND_EMAIL = True

try:
    with open("app_settings.json") as f:
        config = json.load(f)
except Exception:
    config = {}

ODOO_SERVER = config.get("odoo.server") or os.getenv("ODOO_SERVER")
ODOO_DB = config.get("odoo.database") or os.getenv("ODOO_DB")
ODOO_USERNAME = config.get("odoo.username") or os.getenv("ODOO_USERNAME")
ODOO_SECRET = config.get("odoo.password") or os.getenv("ODOO_SECRET")

SENDER_EMAIL = config.get("email.sender") or os.getenv("SENDER_EMAIL")
RECEIVER_EMAIL = config.get("email.recipient") or os.getenv("RECEIVER_EMAIL")
# generated with https://myaccount.google.com/apppasswords
EMAIL_PASSWORD = config.get("email.password") or os.getenv("EMAIL_PASSWORD")


def main():
    date_start = (
        sys.argv[1]
        if len(sys.argv) >= 2
        else (date.today() + relativedelta(days=-1)).strftime("%Y-%m-%d")
    )
    date_start = datetime.strptime(date_start, "%Y-%m-%d")

    content = start_html()
    content.extend(
        [
            """
        <p>Bonjour,</p>
        <p>
            Voici les derniers indicateurs de ventes à Otsokop :
        </p>
    """
        ]
    )

    daily_stats_content = daily_stats(date_start)
    if daily_stats_content is not None:
        content.extend(daily_stats_content)

    monthly_stats_content = None
    if date_start.day == 1:
        monthly_stats_content = monthly_stats(datetime.strptime("2025-01", "%Y-%m"))
        content.extend(monthly_stats_content)

    content.extend(
        [
            """
        <p>Bonne journée,</p>
        """
            "</body></html>"
        ]
    )

    # Only send email if we have something to send...
    if (daily_stats_content is not None) or (monthly_stats_content is not None):
        send_email(content)


def daily_stats(current_date):
    content = []
    if current_date in FR_HOLIDAYS:
        print(
            f"Le {format_date(current_date, locale=LOCALE)} est un jour ferié ({FR_HOLIDAYS.get(current_date)}), le magasin était fermé"
        )
        return None
    if current_date.weekday() == 6:
        print(
            f"Le {format_date(current_date, locale=LOCALE)} est un dimanche, le magasin était fermé"
        )
        return None

    client = Odoo(
        server=ODOO_SERVER,
        database=ODOO_DB,
        username=ODOO_USERNAME,
        password=ODOO_SECRET,
        logging_level=logging.INFO,
    )
    order_dataframes = client.get_pos_orders(current_date.strftime("%Y-%m-%d"))
    orders = order_dataframes[0]

    current = format_date(current_date, DAY_FORMAT, locale="fr_FR")

    if orders.empty:
        content.append("<p><i>(aucune donnée disponible)</i></p>")
        return content

    last_year_date = current_date + relativedelta(
        years=-1, weekday=current_date.weekday()
    )

    is_holiday = last_year_date in FR_HOLIDAYS

    order_dataframes = client.get_pos_orders(last_year_date.strftime("%Y-%m-%d"))
    orders_previous = order_dataframes[0]

    previous = format_date(last_year_date, DAY_FORMAT, locale="fr_FR")

    content.append("<h2>Rapport quotidien de vente</h2>")
    if is_holiday:
        content.append(f"<p><i>Jour ferié ({FR_HOLIDAYS.get(last_year_date)})</i></p>")

    if orders_previous.empty:
        orders_previous = None

    order_summary(content, current, previous, orders, orders_previous)

    content.append("")
    return content


def monthly_stats(current_date):
    content = []
    end_date = current_date + relativedelta(months=1)

    client = Odoo(
        server=ODOO_SERVER,
        database=ODOO_DB,
        username=ODOO_USERNAME,
        password=ODOO_SECRET,
        logging_level=logging.INFO,
    )
    order_dataframes = client.get_pos_orders(
        current_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
    )
    orders = order_dataframes[0]

    # content.append("<hr/>")

    current = format_date(current_date, MONTH_FORMAT, locale="fr_FR")

    if orders.empty:
        content.append("<p><i>(aucune donnée disponible)</i></p>")
        return content

    last_year_date = current_date + relativedelta(years=-1)
    end_date = last_year_date + relativedelta(months=1)

    previous = format_date(last_year_date, MONTH_FORMAT, locale="fr_FR")

    order_dataframes = client.get_pos_orders(
        last_year_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
    )
    orders_previous = order_dataframes[0]

    if orders_previous.empty:
        orders_previous = None

    content.append("<h2>Rapport mensuel de vente</h2>")
    order_summary(content, current, previous, orders, orders_previous)
    return content


def order_summary(content, current, previous, orders, orders_previous):
    content.append("<table>")

    # ------------------------------
    content.append(
        f"  <tr style='padding: 1em; color: #fff; background-color: #405140;'><th></th>"
    )
    content.append(f"<th class='values'>{current}</th>")
    content.append(f"<th class='values'>{previous}</th>")
    content.append(f"<th class='values_gap'></th></tr>")

    # ------------------------------
    # Total Sales
    content.append(f"  <tr style='background-color: #eeedbb'>")
    content.append("<th class='stat'>Chiffre d'affaire</th>")
    content.append(
        f"   <td class='current'>{currency(orders['amount_total'].sum())} €</td>"
    )
    if orders_previous is not None:
        content.append(
            f"   <td> {currency(orders_previous['amount_total'].sum())} €</td>"
        )
        add_gap(
            content, orders["amount_total"].sum(), orders_previous["amount_total"].sum()
        )
    else:
        content.append("<td><i>--</i></td>")
    content.append("</tr>")

    # ------------------------------
    # Panier moyen
    content.append(f"<tr>")
    content.append("<th class='stat'>Panier moyen</th>")
    content.append(
        f"<td class='current'>{currency(orders['amount_total'].mean())} €</td>"
    )
    if orders_previous is not None:
        content.append(
            f"<td> {currency(orders_previous['amount_total'].mean())} €</td>"
        )
        add_gap(
            content,
            orders["amount_total"].mean(),
            orders_previous["amount_total"].mean(),
        )
    else:
        content.append("<td><i>--</i></td>")
    content.append("</tr>")

    # ------------------------------
    # Panier médian
    content.append(f"<tr style='background-color: #eeedbb'>")
    content.append("<th class='stat'>Panier médian</th>")
    content.append(
        f"<td class='current'>{currency(orders['amount_total'].median())} €</td>"
    )
    if orders_previous is not None:
        content.append(
            f"<td>{currency(orders_previous['amount_total'].median())} €</td>"
        )
        add_gap(
            content,
            orders["amount_total"].median(),
            orders_previous["amount_total"].median(),
        )
    else:
        content.append("<td><i>--</i></td>")
    content.append("</tr>")

    # ------------------------------
    # Nombre de commandes
    content.append(f"<tr>")
    content.append("<th class='stat'>Nombre de commandes  </th>")
    content.append(f"<td class='current'> {orders['id'].count()}</td>")
    if orders_previous is not None:
        content.append(f"<td> {orders_previous['id'].count()}</td>")
        add_gap(content, orders["id"].count(), orders_previous["id"].count())
    else:
        content.append("<td><i>--</i></td>")
    content.append("</tr>")

    # ------------------------------
    # Coops/Acheteurs
    content.append(f"<tr style='background-color: #eeedbb'>")
    content.append("<th class='stat'>Coops/Acheteurs</th>")
    content.append(f"<td class='current'> {orders['partner_id'].nunique()}</td>")
    if orders_previous is not None:
        content.append(f"<td>{orders_previous['partner_id'].nunique()}</td>")
        add_gap(
            content,
            orders["partner_id"].nunique(),
            orders_previous["partner_id"].nunique(),
        )
    else:
        content.append("<td><i>--</i></td>")
    content.append("</tr>")

    content.append("</table>")

    content.append("")

    # current_year_sales = orders["amount_total"].sum()
    # last_year_sales = orders_previous["amount_total"].sum()
    # content.append(
    #     f"<p>Indice de réalisation (CA actuel / CA année dernière) : <strong>{current_year_sales / last_year_sales * 100:.02f}%</strong></p>"
    # )

    return content


def currency(n):
    return format_decimal(n, "#,##0.##;-# ¤", locale=LOCALE)


def percent(n):
    return format_decimal(n, "0.## %", locale=LOCALE)


def add_gap(content, current_value, previous_value):
    if current_value > previous_value:
        tag = "up"
        icon = " ▲"
    elif current_value < previous_value:
        tag = "down"
        icon = " ▼"
    else:
        tag = "stable"
        icon = ""

    gap = current_value - previous_value
    gap_pc = current_value / previous_value
    content.append(f"<td class='gap {tag}'>")
    # content.append(f"<span class='gap_v'>{gap:.2f}</span>&nbsp;|&nbsp;")
    content.append(f"<span class='gap_pc'>{percent(gap_pc)}</span>")
    content.append(icon)
    content.append("</td>")


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


def send_email(body):
    if not SEND_EMAIL:
        print("\n".join(body))
        return

    msg = MIMEMultipart()
    msg["From"] = SENDER_EMAIL
    msg["To"] = RECEIVER_EMAIL
    msg["Subject"] = "[Otsokop] Suivi des indicateurs de ventes"

    msg.attach(MIMEText("\n".join(body), "html"))

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(SENDER_EMAIL, EMAIL_PASSWORD)
            server.sendmail(SENDER_EMAIL, RECEIVER_EMAIL, msg.as_string())
        logging.info("Email envoyé avec succès.")
    except Exception as e:
        logging.error(f"Erreur lors de l'envoi de l'email : {e}")


if __name__ == "__main__":
    sys.exit(main())
