import os
import pandas as pd
import shutil
import glob
from sqlalchemy import text
from .config import get_engine, get_connection, SCHEMA

archive_dir = "archive"

def create_archive_dir():
    os.makedirs(archive_dir, exist_ok=True)

def archive_file(file_path):
    try:
        if os.path.exists(file_path):
            file_name = os.path.basename(file_path)
            archive_path = os.path.join(archive_dir, f"{file_name}.backup")
            shutil.move(file_path, archive_path)
            return True
    except Exception as e:
        print(f"Ошибка: {e}")
        return False


#=============================================================================
# Загрузка всех файлов в STG-таблицы

def csv2sql(path, table_name):
    try:
        engine = get_engine()
        df = pd.read_csv(path, sep=';')
        df['transaction_date'] = pd.to_datetime(df['transaction_date'])
        df.to_sql(name=table_name, con=engine, schema=SCHEMA, if_exists="append", index=False)
        archive_file(path)
        return True
    except Exception as e:
        print(f"Ошибка загрузки {path}: {e}")
        return False

def excel2sql(path, table_name, truncate=False):
    try:
        engine = get_engine()
        df = pd.read_excel(path)

        if_exists_mode = "replace" if truncate else "append"
        
        df.to_sql(name=table_name, con=engine, schema=SCHEMA, if_exists=if_exists_mode, index=False)
        archive_file(path)
        return True
    except Exception as e:
        print(f"Ошибка загрузки {path}: {e}")
        return False




def sql2stg():
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                tables = ['stg_clients', 'stg_cards', 'stg_accounts']
                for table in tables:
                    cursor.execute(f"TRUNCATE TABLE {SCHEMA}.{table}")

 
                # Загрузка clients
                cursor.execute(f"""
                    insert into {SCHEMA}.stg_clients (client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, create_dt, update_dt)
                    select client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, create_dt, update_dt
                    from clients
                """)
                
                # Загрузка cards
                cursor.execute(f"""
                    insert into {SCHEMA}.stg_cards (card_num, account, create_dt, update_dt)
                    select card_num, account, create_dt, update_dt
                    from cards
                """)

                # Загрузка accounts
                cursor.execute(f"""
                    insert into {SCHEMA}.stg_accounts (account, valid_to, client, create_dt, update_dt)
                    select account, valid_to, client, create_dt, update_dt
                    from accounts
                """)
                
                conn.commit()
                return True
    except Exception as e:
        print(f"Ошибка загрузки из SQL: {e}")
        return False

def sql2sql(sql_file_path):
    try:
        with open(sql_file_path, 'r', encoding='utf-8') as file:
            sql_data = file.read()
        
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_data)
                conn.commit()
        return True
    except Exception as e:
        print(f"Ошибка при выполнении SQL: {e}")
        return False


def process_all_files():
    """Обработка всех файлов за день"""
    create_archive_dir()
    success = True
    
    print("создание таблиц-источников")
    sql2sql("ddl_dml.sql")

    print("Загрузка файлов транзакций")
    for f in sorted(glob.glob("data/transactions_*.txt")):
        if not csv2sql(f, "stg_transactions"):
            success = False
    
    print("Загрузка файлов терминалов")
    for f in sorted(glob.glob("data/terminals_*.xlsx")):
        if not excel2sql(f, "stg_terminals", truncate=True):
            success = False
    
    print("Загрузка файлов черного списка...")
    for f in sorted(glob.glob("data/passport_blacklist_*.xlsx")):
        if not excel2sql(f, "stg_passport_blacklist"):
            success = False
    
    print("Загрузка данных из SQL")
    if not sql2stg():
        success = False
    


    return success