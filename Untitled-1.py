#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, sys, fitz, pytesseract, psycopg2, logging, traceback
from pathlib import Path
from PIL import Image
from datetime import datetime
from psycopg2.extras import execute_batch

CONFIG = {
    "PDF_PATH": r"C:\Users\Lenovo\Desktop\atkdata\2atk.pdf",
    "DB": {"dbname": "atkparsing", "user": "postgres", "password": "1234", "host": "localhost", "port": "5432"},
    "DEBUG_DIR": "enterprise_debug",
    "TESSERACT_PATH": r"C:\Program Files\Tesseract-OCR\tesseract.exe"
}

pytesseract.pytesseract.tesseract_cmd = CONFIG["TESSERACT_PATH"]
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# --- SQL ---
DDL = """
DROP TABLE IF EXISTS appendix1_account_statement CASCADE;
DROP TABLE IF EXISTS appendix2_transactions CASCADE;
DROP TABLE IF EXISTS appendix3_trade_transactions CASCADE;

CREATE TABLE appendix1_account_statement(id SERIAL PRIMARY KEY, currency VARCHAR(30), total NUMERIC(30,8), available NUMERIC(30,8), estimated_btc NUMERIC(30,8), trx_identifier TEXT, eth_identifier TEXT, ton_identifier TEXT, bnb_identifier TEXT, other_identifier TEXT, statement_date DATE);
CREATE TABLE appendix2_transactions(id SERIAL PRIMARY KEY, tx_type VARCHAR(30), amount NUMERIC(30,8), currency VARCHAR(30), wallet_address TEXT, transaction_hash TEXT, status VARCHAR(30), tx_date TIMESTAMP, period_start DATE, period_end DATE);
CREATE TABLE appendix3_trade_transactions(id SERIAL PRIMARY KEY, trade_type VARCHAR(30), trade_id TEXT, pair VARCHAR(30), base_currency VARCHAR(30), quote_currency VARCHAR(30), side VARCHAR(20), quantity NUMERIC(30,8), price NUMERIC(30,8), fee NUMERIC(30,8), total NUMERIC(30,8), trade_date TIMESTAMP, period_start DATE, period_end DATE);
"""

# --- UTILS ---
def clean_num(val):
    if not val: return 0
    try:
        s = str(val).replace(",", ".").replace(" ", "")
        s = re.sub(r"[^\d.\-]", "", s)
        return float(s) if s else 0
    except: return 0

def clean_dt(val):
    if not val: return None
    # Ищем формат ДД.ММ.ГГГГ ЧЧ:ММ:СС или просто ДД.ММ.ГГГГ
    m = re.search(r"(\d{2})\.(\d{2})\.(\d{4})(?:\s+(\d{2}:\d{2}:\d{2}))?", val)
    if m:
        d, mon, y, t = m.groups()
        try: return datetime.strptime(f"{d}.{mon}.{y} {t or '00:00:00'}", "%d.%m.%Y %H:%M:%S")
        except: return None
    return None

# --- OCR ENGINE ---
def get_pdf_content_via_ocr():
    Path(CONFIG["DEBUG_DIR"]).mkdir(exist_ok=True)
    doc = fitz.open(CONFIG["PDF_PATH"])
    full_text = ""
    
    log.info(f"Начинаю OCR обработку {len(doc)} страниц...")
    for i, page in enumerate(doc):
        # Рендерим страницу для OCR
        pix = page.get_pixmap(matrix=fitz.Matrix(3, 3))
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        # Распознаем текст (английский + русский)
        page_text = pytesseract.image_to_string(img, lang='eng+rus')
        full_text += f"\n--- PAGE {i+1} ---\n" + page_text
        log.info(f"Страница {i+1} распознана.")
    
    # Исправленная строка (одинарные кавычки внутри f-строки)
    debug_path = os.path.join(CONFIG['DEBUG_DIR'], 'debug_ocr.txt')
    with open(debug_path, "w", encoding="utf-8") as f:
        f.write(full_text)
    return full_text

# --- PARSING ---
def parse_all(text):
    # Даты периода
    period_match = re.search(r"(\d{2}\.\d{2}\.\d{4})\s*-\s*(\d{2}\.\d{2}\.\d{4})", text)
    p_start = datetime.strptime(period_match.group(1), "%d.%m.%Y").date() if period_match else None
    p_end = datetime.strptime(period_match.group(2), "%d.%m.%Y").date() if period_match else None

    # 1. Appendix 1 (Balances)
    a1_data = []
    balances = re.findall(r"(USDT|BTC|ETH|BNB|KZT)\s+([\d\s\.,]{3,})\s+([\d\s\.,]{3,})\s+([\d\s\.,]{3,})", text, re.I)
    for b in balances:
        a1_data.append((b[0].upper(), clean_num(b[1]), clean_num(b[2]), clean_num(b[3]), None, None, None, None, None, p_start))

    # 2. Appendix 2 (Transactions)
    a2_data = []
    txs = re.findall(r"(Withdraw|Deposit)\s+([\d\s\.,]+)\s+(USDT|BTC|ETH|BNB|TON|TRX)\s+.*?\s+(\d{2}\.\d{2}\.\d{4}\s+\d{2}:\d{2}:\d{2})", text, re.I)
    for t in txs:
        a2_data.append((t[0].lower(), clean_num(t[1]), t[2].upper(), None, None, "completed", clean_dt(t[3]), p_start, p_end))

    # 3. Appendix 3 (Trade Transactions)
    a3_data = []
    # Паттерн: Ищем строки, где есть пара (напр. BTC/USDT), затем Buy/Sell, затем пачка цифр и дата
    trades = re.findall(r"(\S+/\S+)\s+(Buy|Sell|Maker|Taker)\s+([\d\s\.,]+)\s+([\d\s\.,]+)\s+([\d\s\.,]+)\s+([\d\s\.,]+)\s+(\d{2}\.\d{2}\.\d{4}\s+\d{2}:\d{2}:\d{2})", text, re.I)

    for t in trades:
        pair, side, qty, price, fee, total, dt = t
        a3_data.append((
            "unknown", "ID-FIXED", pair, 
            pair.split('/')[0] if '/' in pair else "", 
            pair.split('/')[1] if '/' in pair else "",
            side.lower(), clean_num(qty), clean_num(price), clean_num(fee), clean_num(total),
            clean_dt(dt), p_start, p_end
        ))

    return a1_data, a2_data, a3_data

def main():
    try:
        log.info("Запуск PARSER v5.4.1 (Fixed Syntax)")
        text = get_pdf_content_via_ocr()
        a1, a2, a3 = parse_all(text)
        
        log.info(f"Итог OCR: App1={len(a1)}, App2={len(a2)}, App3={len(a3)}")
        
        conn = psycopg2.connect(**CONFIG["DB"])
        with conn.cursor() as cur:
            cur.execute(DDL)
            if a1: execute_batch(cur, "INSERT INTO appendix1_account_statement(currency, total, available, estimated_btc, trx_identifier, eth_identifier, ton_identifier, bnb_identifier, other_identifier, statement_date) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", a1)
            if a2: execute_batch(cur, "INSERT INTO appendix2_transactions(tx_type, amount, currency, wallet_address, transaction_hash, status, tx_date, period_start, period_end) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)", a2)
            if a3: execute_batch(cur, "INSERT INTO appendix3_trade_transactions(trade_type, trade_id, pair, base_currency, quote_currency, side, quantity, price, fee, total, trade_date, period_start, period_end) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", a3)
        conn.commit()
        conn.close()
        log.info("Готово. Данные в базе.")
        
    except Exception as e:
        log.error(f"Ошибка: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()