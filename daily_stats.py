import sys
from datetime import date
from datetime import datetime
from dateutil.relativedelta import relativedelta
from otsokop.odoo import Odoo
from otsokop.odoo import banner as otsokop_banner
import logging
import holidays
from babel.dates import format_date


with_banner = True
fr_holidays = holidays.FR()


def main():
    date_start = (
        sys.argv[1]
        if len(sys.argv) >= 2
        else (date.today() + relativedelta(days=-1)).strftime("%Y-%m-%d")
    )
    date_start = datetime.strptime(date_start, "%Y-%m-%d")

    daily_stats(date_start)

    if date_start.day == 1:
        monthly_stats(datetime.strptime("2025-01", "%Y-%m"))


def daily_stats(current_date):

    if current_date in fr_holidays:
        print(
            f"Le {format_date(current_date, locale='fr_FR')} est un jour ferié ({fr_holidays.get(current_date)}), le magasin était fermé"
        )
        return 1
    if current_date.weekday() == 6:
        print(
            f"Le {format_date(current_date, locale='fr_FR')} est un dimanche, le magasin était fermé"
        )
        return 2

    client = Odoo("../../assets/cfg/app_settings.json", logging.INFO)
    order_dataframes = client.get_pos_orders(current_date.strftime("%Y-%m-%d"))
    orders = order_dataframes[0]

    if with_banner:
        print(otsokop_banner)
        print("")
    print(
        "Ventes pour le", format_date(current_date, "EEEE dd MMM yyyy", locale="fr_FR")
    )

    current_year_sales = orders["amount_total"].sum()

    if orders.empty:
        print("(aucune donnée disponible)")
    else:
        print(order_summary(orders))
    print("")

    last_year_date = current_date + relativedelta(
        years=-1, weekday=current_date.weekday()
    )

    is_holiday = last_year_date in fr_holidays

    order_dataframes = client.get_pos_orders(last_year_date.strftime("%Y-%m-%d"))
    orders = order_dataframes[0]

    print(
        "Ventes pour le",
        format_date(last_year_date, "EEEE dd MMM yyyy", locale="fr_FR"),
    )
    if is_holiday:
        print(f"** Jour ferié ({fr_holidays.get(last_year_date)})")

    if orders.empty:
        print("(aucune donnée disponible)")
    else:
        print(order_summary(orders))
        print("")
        last_year_sales = orders["amount_total"].sum()
        print(
            f"Indice de réalisation (CA actuel / CA année dernière) : {current_year_sales / last_year_sales * 100:.02f}%"
        )
    print("")


def monthly_stats(current_date):

    end_date = current_date + relativedelta(months=1)

    client = Odoo("../../assets/cfg/app_settings.json", logging.INFO)
    order_dataframes = client.get_pos_orders(
        current_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
    )
    orders = order_dataframes[0]

    if with_banner:
        print("")

    print(
        "Ventes pour le mois de", format_date(current_date, "MMMM yyyy", locale="fr_FR")
    )

    current_year_sales = orders["amount_total"].sum()

    if orders.empty:
        print("(aucune donnée disponible)")
    else:
        print(order_summary(orders))
    print("")

    last_year_date = current_date + relativedelta(years=-1)
    end_date = last_year_date + relativedelta(months=1)

    order_dataframes = client.get_pos_orders(
        last_year_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
    )
    orders = order_dataframes[0]

    print(
        "Ventes pour le mois de",
        format_date(last_year_date, "MMMM yyyy", locale="fr_FR"),
    )

    if orders.empty:
        print("(aucune donnée disponible)")
    else:
        last_year_sales = orders["amount_total"].sum()
        print(order_summary(orders))
        print("")
        print(
            f"Indice de réalisation (CA actuel / CA année dernière) : {current_year_sales / last_year_sales * 100:.02f}%"
        )
    print("")


def order_summary(orders):
    print(f"  * Chiffre d'affaire    : {orders['amount_total'].sum():.02f} €")
    print(f"  * Panier moyen         : {orders['amount_total'].mean():.02f} €")
    print(f"  * Panier median        : {orders['amount_total'].median():.02f} €")
    print(f"  * Nombre de commandes  : {orders['id'].count()}")
    print(f"  * Coops/Acheteurs      : {orders['partner_id'].nunique()}")
    return ""


if __name__ == "__main__":
    sys.exit(main())
