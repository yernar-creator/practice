import psycopg2

CONFIG = {
    "DB": {
        "dbname":   "atkparsing",
        "user":     "postgres",
        "password": "1234",
        "host":     "localhost",
        "port":     "5432",
    }
}

INSERT_SQL = """
INSERT INTO wallet_turnover (
    wallet_address, currency,
    total_deposit, total_withdraw, net_flow,
    tx_count, period_start, period_end
) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
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
    # АГРЕГАЦИЯ ПО КОШЕЛЬКАМ
    # ─────────────────────────────
    cur.execute("""
        SELECT
            wallet_address,
            currency,
            SUM(CASE WHEN tx_type='deposit'  THEN amount ELSE 0 END),
            SUM(CASE WHEN tx_type='withdraw' THEN amount ELSE 0 END),
            COUNT(*)
        FROM appendix2_transactions
        GROUP BY wallet_address, currency
    """)

    rows = []

    for wallet, currency, dep, wd, cnt in cur.fetchall():
        dep = dep or 0
        wd  = wd or 0
        net = dep - wd

        rows.append((
            wallet,
            currency,
            dep,
            wd,
            net,
            cnt,
            period_start,
            period_end
        ))

    # ─────────────────────────────
    # ВСТАВКА
    # ─────────────────────────────
    for r in rows:
        cur.execute(INSERT_SQL, r)

    conn.commit()

    print("✅ WALLET TURNOVER ГОТОВ:")
    for r in rows:
        print(r)

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()