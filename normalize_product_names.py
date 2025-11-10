import re
import unicodedata
from otsokop.odoo import Odoo
import pandas as pd

client = Odoo()


def remove_diacritics(text):
    """
    Remove diacritics (accents) from text
    """
    if not text:
        return text

    # Normalize to NFD (decomposed form) and filter out combining characters
    normalized = unicodedata.normalize("NFD", text)
    without_diacritics = "".join(
        char for char in normalized if unicodedata.category(char) != "Mn"
    )
    return without_diacritics


def process_name(name, category):
    """
    Process the name according to the rules:
    1. Remove diacritics
    2. Convert to uppercase
    """
    if not name:
        return name

    # Trim whitespace, remove diacritics and convert to uppercase
    processed_name = re.sub(r"\s+", " ", remove_diacritics(name.strip()))
    processed_name = processed_name.upper()
    processed_name = re.sub(r"(\d)\sG\b", "$1G", processed_name)

    # Add "V - " prefix for "Vrac" category if not already present
    # if category == "Vrac" and not processed_name.startswith("V - "):
    #    processed_name = re.sub(r'\s*\(?(EN )?VRAC\)?\s*', ' ', processed_name).strip()
    #    processed_name = re.sub(r"\s+", " ", processed_name)
    #    processed_name = "V - " + processed_name

    return processed_name


def process_odoo_model(model_name, dry_run=False):
    print("Processing Odoo model:", model_name)
    rows = client.execute_kw(
        model_name,
        "search_read",
        [
            [],
            ["name"],
        ],
        {"context": {"lang": "fr_FR"}},
    )

    print(f"Found {len(rows)} records in {model_name}")

    for row in rows:
        row_id = row["id"][0] if isinstance(row["id"], list) else row["id"]
        original_name = row["name"]
        normalized_name = process_name(original_name, "")
        if original_name != normalized_name:
            print(f"Updating ID {row_id}: '{row['name']}' -> '{normalized_name}'")
            if not dry_run:
                result = client.execute_kw(
                    model_name,
                    "write",
                    [
                        [row["id"]],
                        {"name": normalized_name},
                    ],
                    {"context": {"lang": "fr_FR"}},
                )
                print(f"  -> {result}")


def main():
    #process_odoo_model("res.partner")
    process_odoo_model("product.product")
    #process_odoo_model("product.template")
    #process_odoo_model("product.category")


if __name__ == "__main__":
    main()
