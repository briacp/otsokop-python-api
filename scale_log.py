#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Based on: product_to_scale_bizerba/models/product_scale_log.py
"""

from otsokop.odoo import Odoo
import os
import sys
import logging
import base64
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger(__name__)


class OdooScaleLogClient:
    """Client to generate scale log files from Odoo via XML-RPC"""

    # Constants from original class
    _EXTERNAL_SIZE_ID_RIGHT = 4
    _DELIMITER = ","

    _ACTION_MAPPING = {
        "create": "A",
        "write": "M",
        "unlink": "B",
    }

    _ENCODING_MAPPING = {
        "iso-8859-1": "\r\n",
        "cp1252": "\n",
        "utf-8": "\n",
    }

    _TRANSLATED_TERM = {
        0x2018: 0x27,  # ' to '
        0x2019: 0x27,  # ' to '
        0x201C: 0x22,  # " to "
        0x201D: 0x22,  # " to "
    }

    _EXTERNAL_TEXT_ACTION_CODE = "C"
    _EXTERNAL_TEXT_DELIMITER = "#"

    def __init__(self):
        self.client = Odoo()

    def _clean_value(self, value, product_line):
        """Clean value according to product line configuration"""
        if not value:
            return ""

        value = str(value)

        # Handle multiline
        if product_line.get("multiline_length"):
            res = ""
            current_val = value
            while current_val:
                res += current_val[: product_line["multiline_length"]]
                current_val = current_val[product_line["multiline_length"] :]
                if current_val:
                    res += product_line.get("multiline_separator", "")
        else:
            res = value

        # Remove delimiter if present
        if product_line.get("delimiter"):
            return res.replace(product_line["delimiter"], "")
        else:
            return res

    def _generate_external_text(self, value, product_line, external_id, log):
        """Generate external text line"""
        external_text_list = [
            self._EXTERNAL_TEXT_ACTION_CODE,
            log["scale_group_external_identity"],
            external_id,
            self._clean_value(value, product_line),
        ]
        return self._EXTERNAL_TEXT_DELIMITER.join(external_text_list)

    def _generate_image_file_name(self, product_id, field_value, extension):
        """Generate image file name"""
        if field_value:
            return f"{product_id}.PNG"  # {extension}"
        else:
            return ""

    def _float_round(self, value, precision):
        """Round float to precision"""
        if not precision or precision == 0:
            return value
        return round(value / precision) * precision

    def _compute_text(self, log, product, scale_group, scale_system, product_lines):
        """
        Compute product text and external text for a log entry

        Returns:
            dict: {'product_text': str, 'external_text': str}
        """
        action = log["action"]
        product_text = self._ACTION_MAPPING[action] + self._DELIMITER
        external_texts = []

        # Process each product line
        for product_line in product_lines:
            field_name = product_line.get("field_name")
            value = product.get(field_name) if field_name else None

            line_type = product_line["type"]

            if line_type == "id":
                product_text += str(product["id"])

            elif line_type == "numeric":
                numeric_value = float(value) if value else 0.0
                coefficient = product_line.get("numeric_coefficient", 1.0)
                rounding = product_line.get("numeric_round", 1.0)
                value = self._float_round(numeric_value * coefficient, rounding)
                product_text += str(value).replace(".0", "")

            elif line_type == "text":
                product_text += self._clean_value(value, product_line)

            elif line_type == "external_text":
                external_id = str(product["id"]) + str(product_line["id"]).rjust(
                    self._EXTERNAL_SIZE_ID_RIGHT, "0"
                )
                external_texts.append(
                    self._generate_external_text(value, product_line, external_id, log)
                )
                product_text += external_id

            elif line_type == "constant":
                product_text += self._clean_value(
                    product_line.get("constant_value", ""), product_line
                )

            elif line_type == "external_constant":
                external_id = str(product_line["id"])
                external_texts.append(
                    self._generate_external_text(
                        product_line.get("constant_value", ""),
                        product_line,
                        external_id,
                        log,
                    )
                )
                product_text += external_id

            elif line_type == "many2one":
                if value:
                    if isinstance(value, list):
                        # XML-RPC returns many2one as [id, name]
                        related_field = product_line.get("related_field_name")
                        if not related_field:
                            product_text += str(value[0])
                        else:
                            # Need to fetch related field
                            related_obj = self.client.execute_kw(
                                product_line["related_model"],
                                "read",
                                [value[0]],
                                [related_field],
                            )
                            if related_obj:
                                product_text += str(
                                    related_obj[0].get(related_field, "")
                                )

            elif line_type == "many2many":
                if value and isinstance(value, list):
                    x2many_range = product_line.get("x2many_range", 1)
                    if x2many_range <= len(value):
                        item_id = value[x2many_range - 1]
                        related_field = product_line.get("related_field_name")
                        if related_field:
                            item_obj = self.client.execute_kw(
                                product_line["related_model"],
                                "read",
                                [item_id],
                                [related_field],
                            )
                            if item_obj:
                                item_value = item_obj[0].get(related_field, "")
                                product_text += self._clean_value(
                                    item_value, product_line
                                )
                        else:
                            product_text += str(item_id)

            elif line_type == "product_image":
                extension = product_line.get("suffix", ".PNG")
                product_text += self._generate_image_file_name(
                    product["id"], value, extension
                )

            # Add delimiter if specified
            if product_line.get("delimiter"):
                product_text += product_line["delimiter"]

        # Get line break for encoding
        encoding = scale_system.get("encoding", "utf-8")
        break_line = self._ENCODING_MAPPING.get(encoding, "\n")

        return {
            "product_text": product_text + break_line,
            "external_text": (
                break_line.join(external_texts) + break_line if external_texts else ""
            ),
        }

    def get_unsent_logs(self):
        """Get all unsent scale logs"""
        log_ids = self.client.execute_kw(
            "product.scale.log",
            "search",
            [
                [["sent", "=", False]],
                0,
                0,
                "log_date",
            ],
        )
        if not log_ids:
            _logger.info("No unsent logs found")
            return []

        # Read logs with related fields
        logs = self.client.execute_kw(
            "product.scale.log",
            "read",
            [
                log_ids,
                ["id", "log_date", "action", "product_id", "scale_system_id", "sent"],
            ],
        )

        return logs

    def generate_files(self, output_folder, log_ids=None):
        """
        Generate scale files locally for unsent logs

        Args:
            output_folder: Local folder path to save generated files
            log_ids: List of log IDs to process (if None, processes all unsent)

        Returns:
            bool: True if successful
        """
        # Create output folder if it doesn't exist
        os.makedirs(output_folder, exist_ok=True)

        # Get logs to process
        if log_ids is None:
            logs = self.get_unsent_logs()
        else:
            logs = self.client.execute_kw(
                "product.scale.log",
                "search_read",
                [
                    log_ids,
                    [
                        "id",
                        "log_date",
                        "sent",
                        "scale_system_id",
                        "product_id",
                        "action",
                    ],
                ],
            )

        if not logs:
            _logger.info("No logs to process")
            return False

        # Group logs by scale system
        system_map = {}
        for log in logs:
            system_id = (
                log["scale_system_id"][0]
                if isinstance(log["scale_system_id"], list)
                else log["scale_system_id"]
            )

            if system_id not in system_map:
                system_map[system_id] = []
            system_map[system_id].append(log)

        # Process each scale system
        for scale_system_id, system_logs in system_map.items():
            _logger.info(
                f"Processing {len(system_logs)} logs for scale system {scale_system_id}"
            )

            # Get scale system configuration
            scale_system = self.client.execute_kw(
                "product.scale.system",
                "read",
                [
                    [scale_system_id],
                    [
                        "encoding",
                        "product_text_file_pattern",
                        "external_text_file_pattern",
                        "send_images",
                    ],
                ],
            )[0]

            # Get product lines configuration
            product_line_ids = self.client.execute_kw(
                "product.scale.system.product.line",
                "search",
                [[["scale_system_id", "=", scale_system_id]]],
            )

            product_lines = self.client.execute_kw(
                "product.scale.system.product.line",
                "read",
                [
                    product_line_ids,
                    [
                        "id",
                        "type",
                        "field_id",
                        "delimiter",
                        "numeric_coefficient",
                        "numeric_round",
                        "constant_value",
                        "multiline_length",
                        "multiline_separator",
                        "related_field_id",
                        "x2many_range",
                        "suffix",
                    ],
                ],
            )

            # Convert field_id references to field names
            for pl in product_lines:
                if pl.get("field_id"):
                    field_id = (
                        pl["field_id"][0]
                        if isinstance(pl["field_id"], list)
                        else pl["field_id"]
                    )
                    field_info = self.client.execute_kw(
                        "ir.model.fields", "read", [[field_id], ["name", "relation"]]
                    )
                    if field_info:
                        pl["field_name"] = field_info[0]["name"]
                        pl["related_model"] = field_info[0].get("relation")

            # Generate text lines
            product_text_lst = []
            external_text_lst = []
            saved_images = set()

            for log in system_logs:
                # Get product data
                product_id = (
                    log["product_id"][0]
                    if isinstance(log["product_id"], list)
                    else log["product_id"]
                )

                # Get all fields needed
                field_names = [
                    pl.get("field_name") for pl in product_lines if pl.get("field_name")
                ]
                field_names = list(set(["id", "scale_group_id"] + field_names))

                product = self.client.execute_kw(
                    "product.product", "read", [[product_id], field_names]
                )[0]

                # Get scale group
                scale_group_id = (
                    product["scale_group_id"][0]
                    if isinstance(product["scale_group_id"], list)
                    else product["scale_group_id"]
                )

                scale_group = self.client.execute_kw(
                    "product.scale.group",
                    "read",
                    [[scale_group_id], ["external_identity"]],
                )[0]

                log["scale_group_external_identity"] = scale_group["external_identity"]

                # Compute texts
                texts = self._compute_text(
                    log, product, scale_group, scale_system, product_lines
                )

                _logger.info(texts["product_text"].strip())

                if texts["product_text"]:
                    product_text_lst.append(texts["product_text"])
                if texts["external_text"]:
                    external_text_lst.append(texts["external_text"])

                # Save product images if needed
                if False and scale_system.get("send_images"):
                    for product_line in product_lines:
                        if product_line["type"] == "product_image":
                            field_name = product_line.get("field_name")
                            if field_name and product.get(field_name):
                                extension = product_line.get("suffix") or ".PNG"
                                image_filename = self._generate_image_file_name(
                                    product["id"], product[field_name], extension
                                )

                                if (
                                    image_filename
                                    and image_filename not in saved_images
                                ):
                                    image_path = os.path.join(
                                        output_folder, image_filename
                                    )
                                    image_data = base64.b64decode(product[field_name])
                                    with open(image_path, "wb") as f:
                                        f.write(image_data)
                                    saved_images.add(image_filename)
                                    _logger.info(f"Saved image: {image_filename}")
                                elif image_filename in saved_images:
                                    _logger.debug(
                                        f"Skipped duplicate image: {image_filename}"
                                    )

            # Write text files
            encoding = scale_system.get("encoding", "utf-8")
            now = datetime.now()

            if product_text_lst:
                product_filename = now.strftime(
                    scale_system["product_text_file_pattern"]
                )
                product_path = os.path.join(output_folder, product_filename)
                with open(product_path, "w", encoding=encoding, errors="ignore") as f:
                    for line in product_text_lst:
                        raw_text = line
                        if encoding != "utf-8":
                            raw_text = raw_text.translate(self._TRANSLATED_TERM)
                        f.write(raw_text)
                _logger.info(f"Generated product file: {product_filename}")

            if external_text_lst:
                external_filename = now.strftime(
                    scale_system["external_text_file_pattern"]
                )
                external_path = os.path.join(output_folder, external_filename)
                with open(external_path, "w", encoding=encoding, errors="ignore") as f:
                    for line in external_text_lst:
                        raw_text = line
                        if encoding != "utf-8":
                            raw_text = raw_text.translate(self._TRANSLATED_TERM)
                        f.write(raw_text)
                _logger.info(f"Generated external text file: {external_filename}")

        _logger.info(f"Successfully generated files in {output_folder}")
        return True

    def mark_logs_as_sent(self, log_ids):
        """Mark logs as sent"""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.client.execute_kw(
            "product.scale.log",
            "write",
            [
                log_ids,
                {"sent": True, "last_send_date": now},
            ],
        )
        _logger.info(f"Marked {len(log_ids)} logs as sent")


def main():
    OUTPUT_FOLDER = "/tmp/scale_files"

    try:
        # Initialize client
        client = OdooScaleLogClient()

        # Generate files for all unsent logs
        success = client.generate_files(OUTPUT_FOLDER)

        if success:
            # Optionally mark logs as sent
            unsent_logs = client.get_unsent_logs()
            if unsent_logs:
                log_ids = [log["id"] for log in unsent_logs]
                _logger.info(f"Marking {len(log_ids)} logs as sent")
                client.mark_logs_as_sent(log_ids)

    except Exception as e:
        _logger.error(f"Error: {e}", exc_info=True)


if __name__ == "__main__":
    main()
