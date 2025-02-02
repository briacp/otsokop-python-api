from otsokop.odoo import Odoo
from otsokop.odoo import banner as otsokop_banner
import pandas as pd
from geopy.geocoders import BANFrance
from geopy import distance
import time
import simplekml as sk


geolocator = BANFrance(user_agent="otsokop_geocode")
print(otsokop_banner)
client = Odoo("app_settings.json")

FETCH_ODOO = False
OTSOKOP_COORDS = (43.502103, -1.468321)

if FETCH_ODOO:
    if (cached_result := client._check_cache("members_address")) is not None:
        members_address = cached_result
    else:
        members_address = client.execute_kw(
            "res.partner",
            "search_read",
            [
                [["is_member", "=", "true"]],
                [
                    # "name",
                    "street",
                    "street2",
                    "zip",
                    "city",
                ],
            ],
        )
        client._set_cache("members_address", members_address, Odoo.SECONDS_IN_DAY)

    members_address = pd.DataFrame(members_address)
else:
    members_address = pd.read_csv("output/members_address.csv")


def fetch_location(members_address):
    for index, m in members_address.iterrows():
        if not pd.isna(m["latitude"]):
            continue
        address = f"{m['street']} {m['zip']} {m['city']}"
        print(f"Geocoding {address}")
        location = geolocator.geocode(address)
        if location is not None:
            members_address.at[index, "latitude"] = location.latitude
            members_address.at[index, "longitude"] = location.longitude
        else:
            members_address.at[index, "latitude"] = None
            members_address.at[index, "longitude"] = None
        members_address.to_csv("output/members_address.csv", index=False)
        time.sleep(1)


def distance_to_otsokop(members_address):
    for index, m in members_address.iterrows():
        if pd.isna(m["latitude"]):
            continue
        members_address.at[index, "distance_in_m"] = int(
            distance.distance((m["latitude"], m["longitude"]), OTSOKOP_COORDS).m
        )
    members_address.to_csv("output/members_address.csv", index=False)


def members_kml(members_address):
    kml = sk.Kml(name="Otsokop members")
    style = sk.Style()

    blank_icons = sk.Style()
    blank_icons.iconstyle.scale = 0
    blank_icons.iconstyle.icon = None

    kml.newpoint(
        name="Otsokop",
        coords=[(-1.468321, 43.502103)],
    )

    for index, m in members_address.iterrows():
        if pd.isna(m["latitude"]):
            continue
        kml.newpoint(
            # name=m['id'],
            coords=[(m["longitude"], m["latitude"])],
        )

    kml.save(f"output/otsokop_members.kml")


fetch_location(members_address)
distance_to_otsokop(members_address)
members_kml(members_address)
