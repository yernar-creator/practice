#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import sys
import json
import fitz
import time
import logging
import traceback
import psycopg2
import pytesseract

from pathlib import Path
from datetime import datetime
from PIL import Image, ImageEnhance
from psycopg2.extras import execute_batch


CONFIG = {
    "PDF_PATH": r"C:\Users\Lenovo\Desktop\atkdata\2atk.pdf",

    "DB": {
        "dbname": "atkparsing",
        "user": "postgres",
        "password": "1234",
        "host": "localhost",
        "port": "5432"
    },

    "DEBUG_DIR": "enterprise_debug",
    "LOG_FILE": "enterprise_parser.log"
}

pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(CONFIG["LOG_FILE"], encoding="utf-8")
    ]
)

log = logging.getLogger(__name__)


DDL = """
DROP TABLE IF EXISTS appendix3_trade_transactions;

CREATE TABLE appendix3_trade_transactions(
    id SERIAL PRIMARY KEY,
    trade_type TEXT,
    trade_id TEXT,
    pair TEXT,
    base_currency TEXT,
    quote_currency TEXT,
    side TEXT,
    quantity NUMERIC,
    price NUMERIC,
    fee NUMERIC,
    total NUMERIC,
    trade_date TIMESTAMP
);
"""


INSERT_TRADE = """
INSERT INTO appendix3_trade_transactions(
trade_type, trade_id, pair,
base_currency, quote_currency,
side, quantity, price,
fee, total, trade_date
)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""


def ensure_debug():
    Path(CONFIG["DEBUG_DIR"]).mkdir(exist_ok=True)


def preprocess(img):
    img = img.convert("L")
    img = ImageEnhance.Contrast(img).enhance(2.5)
    img = ImageEnhance.Sharpness(img).enhance(2.5)
    return img


def safe_decimal(v):
    try:
        v = str(v)
        v = re.sub(r"[^\d.]", "", v)
        return float(v)
    except:
        return None


def safe_datetime(v):
    try:
        return datetime.strptime(v, "%d.%m.%Y %H:%M:%S")
    except:
        return None


def load_pdf():

    doc = fitz.open(CONFIG["PDF_PATH"])

    full_text = ""

    for i, page in enumerate(doc):

        pix = page.get_pixmap(matrix=fitz.Matrix(4, 4))

        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)

        img = preprocess(img)

        img.save(f"{CONFIG['DEBUG_DIR']}/page_{i+1}.png")

        text = pytesseract.image_to_string(
            img,
            config="--oem 3 --psm 4"
        )

        with open(
            f"{CONFIG['DEBUG_DIR']}/page_{i+1}.txt",
            "w",
            encoding="utf-8"
        ) as f:
            f.write(text)

        full_text += text + "\n"

    return full_text


def parse_appendix3(text):

    rows = []

    trade_pattern = re.compile(
        r"(USDT-KZT-\d+-\d{10,})",
        re.IGNORECASE
    )

    all_ids = list(trade_pattern.finditer(text))

    log.info(f"FOUND TRADE IDS: {len(all_ids)}")

    for i, match in enumerate(all_ids):

        start = match.start()

        end = all_ids[i+1].start() if i+1 < len(all_ids) else len(text)

        block = text[start:end]

        trade_id = match.group(1)

        date_match = re.search(
            r"(\d{2}\.\d{2}\.\d{4}\s+\d{2}:\d{2}:\d{2})",
            block
        )

        trade_date = safe_datetime(
            date_match.group(1)
        ) if date_match else None

        nums = re.findall(
            r"\d+\.\d+|\d+",
            block
        )

        nums = [
            safe_decimal(n)
            for n in nums
            if safe_decimal(n)
        ]

        nums = [
            n for n in nums
            if n not in [
                24.05,
                6.10,
                2025
            ]
        ]

        log.info(f"{trade_id}: {nums}")

        quantity = None
        price = None
        fee = None
        total = None

        if len(nums) >= 4:

            possible = nums[-4:]

            quantity = possible[0]
            price = possible[1]
            fee = possible[2]
            total = possible[3]

        elif len(nums) >= 3:

            possible = nums[-3:]

            price = possible[0]
            fee = possible[1]
            total = possible[2]

            quantity = round(total / price, 2)

        rows.append((
            "taker",
            trade_id,
            "USDT/KZT",
            "USDT",
            "KZT",
            "buy",
            quantity,
            price,
            fee,
            total,
            trade_date
        ))

    return rows


def main():

    ensure_debug()

    text = load_pdf()

    with open(
        f"{CONFIG['DEBUG_DIR']}/FULL_OCR.txt",
        "w",
        encoding="utf-8"
    ) as f:
        f.write(text)

    trades = parse_appendix3(text)

    log.info(f"TOTAL PARSED: {len(trades)}")

    conn = psycopg2.connect(**CONFIG["DB"])

    cur = conn.cursor()

    cur.execute(DDL)

    execute_batch(cur, INSERT_TRADE, trades)

    conn.commit()

    cur.close()
    conn.close()

    log.info("DONE")


if __name__ == "__main__":
    main()