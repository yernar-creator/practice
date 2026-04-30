import psycopg2
from datetime import datetime

CONFIG = {
    "DB": {
        "dbname":   "atkparsing",
        "user":     "postgres",
        "password": "1234",
        "host":     "localhost",
        "port":     "5432",
    }
}

INSERT_SUMMARY = """
INSERT INTO summary_turnover (
    metric_name, currency, total_amount, period_start, period_end
) VALUES (%s, %s, %s, %s, %s)
"""

def connect():
    return psycopg2.connect(**CONFIG["DB"])

def main():
    conn = connect()
    cur = conn.cursor()

    # ─────────────────────────────
    # ПЕРИОД
    # ─────────────────────────────
    cur.execute("""
        SELECT MIN(period_start), MAX(period_end)
        FROM appendix2_transactions
    """)
    period_start, period_end = cur.fetchone()

    # ─────────────────────────────
    # DEPOSITS
    # ─────────────────────────────
    cur.execute("""
        SELECT currency, SUM(amount)
        FROM appendix2_transactions
        WHERE tx_type = 'deposit'
        GROUP BY currency
    """)
    deposits = cur.fetchall()

    # ─────────────────────────────
    # WITHDRAWS
    # ─────────────────────────────
    cur.execute("""
        SELECT currency, SUM(amount)
        FROM appendix2_transactions
        WHERE tx_type = 'withdraw'
        GROUP BY currency
    """)
    withdraws = cur.fetchall()

    # ─────────────────────────────
    # TRADES (KZT оборот)
    # ─────────────────────────────
    cur.execute("""
        SELECT quote_currency, SUM(total)
        FROM appendix3_trade_transactions
        GROUP BY quote_currency
    """)
    trades_kzt = cur.fetchall()

    # ─────────────────────────────
    # TRADES (USDT оборот)
    # ─────────────────────────────
    cur.execute("""
        SELECT base_currency, SUM(quantity)
        FROM appendix3_trade_transactions
        GROUP BY base_currency
    """)
    trades_usdt = cur.fetchall()

    # ─────────────────────────────
    # ВСТАВКА
    # ─────────────────────────────
    rows = []

    for cur_name, amount in deposits:
        rows.append(("deposit_turnover", cur_name, amount, period_start, period_end))

    for cur_name, amount in withdraws:
        rows.append(("withdraw_turnover", cur_name, amount, period_start, period_end))

    for cur_name, amount in trades_kzt:
        rows.append(("trade_turnover_kzt", cur_name, amount, period_start, period_end))

    for cur_name, amount in trades_usdt:
        rows.append(("trade_turnover_base", cur_name, amount, period_start, period_end))

    for r in rows:
        cur.execute(INSERT_SUMMARY, r)

    conn.commit()

    print("✅ SUMMARY ГОТОВ:")
    for r in rows:
        print(r)

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
    #Adapted classification logic:
is_deposit_block    = "deposit"    in text_blob and "txid"        in text_blob
is_withdrawal_block = "withdrawal" in text_blob or  "withdraw"    in text_blob
is_trade_block      = "filled"     in text_blob or  "avg price"   in text_blob \
                   or "trading fee" in text_blob
#Adapted date parser for ISO format:
date_match = re.search(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", text)
date = datetime.strptime(date_match.group(0), "%Y-%m-%d %H:%M:%S")
#Asset and amount extraction:
asset_match = re.search(r"(USDT|BTC|ETH|OKB|KZT)", text)
nums   = re.findall(r"\d+\.?\d*", text)
amount = parse_float(nums[0])  if nums           else None
fee    = parse_float(nums[-1]) if len(nums) >= 2 else None
