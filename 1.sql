import pandas as pd
from sqlalchemy import create_engine, text

# ========================= НАСТРОЙКИ =========================
DB_CONFIG = {
    "user": "postgres",
    "password": "1234",
    "host": "localhost",
    "port": "5432",
    "dbname": "ALL EYEZONME"
}

file_path = r"C:\Users\Lenovo\Desktop\выгрузки для парсинга\OKX\UUID-512124366557994035.xlsx"

engine = create_engine(
    f'postgresql://{DB_CONFIG["user"]}:{DB_CONFIG["password"]}@{DB_CONFIG["host"]}:{DB_CONFIG["port"]}/{DB_CONFIG["dbname"]}'
)

# Маппинг: лист в Excel → нужные колонки
MAPPINGS = {
    "login_info": ["uuid", "login_time", "login_ip", "device_id", "fpid", "ua", "update_time"],
    "account_balance": ["currency_symbol", "total_equity_account", "date"],
    "deposit_history": ["uuid", "currency", "address", "amount", "txid", "creation_time", "update_time"],
    "funding_account_history": ["currency_id", "symbol", "type_en_name", "size", "before_balance", "after_balance", "refer_id", "create_time"],
    "withdrawal_history": ["uuid", "currency", "address", "amount", "txid", "creation_time", "update_time", "chain_name"],
    "fiat_history": ["creation_time", "order_id", "base_currency", "quote_currency", "price", "token_amount", "fiat_amount", "payment_time", "seller_bank_account", "seller_bank_branch", "seller_bank", "payment_type", "extra_type"],
    "user_info": ["account_cr", "user_name", "country_region", "id_number", "mobile_number", "email", "uuid", "nationality_en"]
}


def run_full_pipeline():
    try:
        # === ШАГ 1: Создание таблицы account_balance ===
        print(">>> Шаг 1: Проверка структуры таблиц...")
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS account_balance (
                    currency_symbol TEXT,
                    total_equity_account NUMERIC,
                    date TIMESTAMP,
                    "SOURCE FILE" TEXT
                );
            """))

        # === ШАГ 2: Загрузка данных из Excel ===
        print("\n>>> Шаг 2: Загрузка данных из Excel...")
        xls = pd.ExcelFile(file_path)
        
        for sheet in xls.sheet_names:
            clean_name = sheet.lower().replace(' ', '*')
            
            if clean_name in MAPPINGS:
                print(f"📄 Загружаю лист: '{sheet}'")
                df = pd.read_excel(xls, sheet_name=sheet)
                
                # Приводим колонки к нижнему регистру
                df.columns = [c.lower().strip() for c in df.columns]
                
                # Оставляем только нужные колонки
                target_cols = MAPPINGS[clean_name]
                df = df[[c for c in target_cols if c in df.columns]]
                
                # Чистка текста (верхний регистр)
                for col in df.select_dtypes(include=['object']).columns:
                    df[col] = df[col].astype(str).str.upper().str.replace('*', ' ')
                
                # Добавляем источник
                df['SOURCE FILE'] = sheet.upper().replace('*', ' ')
                
                # Записываем в базу (append)
                df.to_sql(clean_name, engine, if_exists='append', index=False)
                print(f"    Успешно! +{len(df)} строк.")

        # === ШАГ 3: Расчёт отчёта по оборотам ===
        print("\n>>> Шаг 3: Генерация отчета по оборотам...")
        
        u_info = pd.read_sql('SELECT uuid, user_name, email FROM user_info', engine)
        dep = pd.read_sql('SELECT uuid, amount FROM deposit_history', engine)
        wdl = pd.read_sql('SELECT uuid, amount FROM withdrawal_history', engine)
        
        dep_sum = dep.groupby('uuid')['amount'].sum().reset_index().rename(columns={'amount': 'TOTAL_IN'})
        wdl_sum = wdl.groupby('uuid')['amount'].sum().reset_index().rename(columns={'amount': 'TOTAL_OUT'})
        
        report = pd.merge(u_info, dep_sum, on='uuid', how='left')
        report = pd.merge(report, wdl_sum, on='uuid', how='left').fillna(0)
        report['NET_TURNOVER'] = report['TOTAL_IN'] - report['TOTAL_OUT']
        
        # Приводим названия колонок к верхнему регистру
        report.columns = [c.upper().replace('*', ' ') for c in report.columns]
        
        report.to_sql('user_turnover_report', engine, if_exists='replace', index=False)
        
        print("\n🚀 ВСЁ ВЫПОЛНЕНО УСПЕШНО!")
        print("Таблица 'user_turnover_report' создана в базе.")

    except Exception as e:
        print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")


if __name__ == "__main__":
    run_full_pipeline()