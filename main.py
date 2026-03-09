import psycopg2
from dotenv import load_dotenv
import os
import pandas as pd
import glob
from sqlalchemy import create_engine, text
from datetime import datetime
import shutil

load_dotenv()

connection = psycopg2.connect(
    host = "localhost",
    database = "postgres",
    user = os.getenv("DATABASE_USER"),
    password = os.getenv("DATABASE_PASSWORD"),
    port = 5432
)

cursor = connection.cursor()

cursor.execute("set search_path to final_project")

dsn = create_engine("postgresql://{user}:{password}@localhost:5432/postgres".format(
    user=os.getenv("DATABASE_USER"),
    password=os.getenv("DATABASE_PASSWORD")
))


#=============================================================================
# Создание архива

archive_dir = "archive"
os.makedirs(archive_dir, exist_ok=True)

def archive_file(file_path):
    try:
        if os.path.exists(file_path):
            file_name = os.path.basename(file_path)
            archive_path = os.path.join(archive_dir, f"{file_name}.backup")
            shutil.move(file_path, archive_path)
            return True
    except Exception as e:
        print(f"Ошибка при архивации: {e}")
        return False

#=============================================================================
# Загрузка всех файлов в STG-таблицы

def csv2sql(path, table_name):
    df = pd.read_csv(path, sep=';')
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])
    df.to_sql(name=table_name, con=dsn, schema="final_project", if_exists="append", index=False)

    archive_file(path)

def excel2sql(path, table_name):
    df = pd.read_excel(path)
    df.to_sql(name=table_name, con=dsn, schema="final_project", if_exists="replace", index=False)

    archive_file(path)

# for f in sorted(glob.glob("data/transactions_*.txt")):
#     csv2sql(f, "stg_transactions")

# for f in sorted(glob.glob("data/terminals_*.xlsx")):
#     excel2sql(f, "stg_terminals")

# for f in sorted(glob.glob("data/passport_blacklist_*.xlsx")):
#     excel2sql(f, "stg_passport_blacklist")

def sql2stg():
    tables = ['stg_clients', 'stg_cards', 'stg_accounts']
    for table in tables:
        cursor.execute(f"TRUNCATE TABLE {table}")
    connection.commit()

    try:
        cursor.execute("""
            insert into stg_clients (client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, create_dt, update_dt)
            select client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, create_dt, update_dt
            from clients
        """)
    except Exception as e:
        print(f"Ошибка при загрузке clients: {e}")
    
    try:
        cursor.execute("""
            insert into stg_cards (card_num, account, create_dt, update_dt)
            select card_num, account, create_dt, update_dt
            from cards
        """)
    except Exception as e:
        print(f"  Ошибка при загрузке cards: {e}")

    try:
        cursor.execute("""
            insert into stg_accounts (account, valid_to, client, create_dt, update_dt)
            select account, valid_to, client, create_dt, update_dt
            from accounts
        """)
    except Exception as e:
        print(f"Ошибка при загрузке accounts: {e}")
    
    connection.commit()

def sql2sql(sql_file_path):

    with open(sql_file_path, 'r', encoding='utf-8') as file:
        sql_data = file.read()

    try:
        cursor.execute(sql_data)
        connection.commit()
    except Exception as e:
        print(f"Ошибка при выполнении SQL: {e}")
        connection.rollback()
    
    sql2stg()
    
# sql2sql("ddl_dml.sql")


# ====================================================================
# Таблицы измерений 

def dim_terminals_hist():
    today = datetime.now().date()
    
    try:
        cursor.execute("""
            update dwh_dim_terminals_hist 
            set effective_to = %s
            where effective_to = '5999-12-31'
            and terminal_id in (
                select terminal_id from stg_terminals
                except
                select terminal_id from dwh_dim_terminals_hist 
                where effective_to = '5999-12-31'
            )
        """, (today,))

        cursor.execute("""
            insert into dwh_dim_terminals_hist (terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg)
            select 
                terminal_id, 
                terminal_type, 
                terminal_city, 
                terminal_address, 
                %s, 
                '5999-12-31', 
                false
            from stg_terminals
            where terminal_id not in (
                select terminal_id from dwh_dim_terminals_hist 
                where effective_to = '5999-12-31'
            )
        """, (today,))
        connection.commit()
    except Exception as e:
        print(f"Ошибка в dim_terminals_hist: {e}")
        connection.rollback()

def to_dwh_dim():
    try:
        cursor.execute("truncate table dwh_dim_clients")
        
        cursor.execute("""
            insert into dwh_dim_clients (client_id, last_name, first_name, patronymic, 
                                        date_of_birth, passport_num, passport_valid_to, phone)
            select client_id, last_name, first_name, patronymic, 
                   date_of_birth, passport_num, passport_valid_to, phone
            from stg_clients
        """)
        connection.commit()
    except Exception as e:
        print(f"  ошибка при загрузке dwh_dim_clients: {e}")
        connection.rollback()
        return

    try:
        cursor.execute("truncate table dwh_dim_cards")
        
        cursor.execute("""
            insert into dwh_dim_cards (card_num, account_num)
            select card_num, account
            from stg_cards
        """)
        connection.commit()
    except Exception as e:
        print(f"Ошибка при загрузке dwh_dim_cards: {e}")
        connection.rollback()
        return
    
    try:
        cursor.execute("truncate table dwh_dim_accounts")
        
        cursor.execute("""
            insert into dwh_dim_accounts (account_num, client, valid_to)
            select account, client, valid_to
            from stg_accounts
        """)
        connection.commit()
    except Exception as e:
        print(f"Ошибка при загрузке dwh_dim_accounts: {e}")
        connection.rollback()
        return


    dim_terminals_hist()
    
# to_dwh_dim()

# ========================================================================
# Таблицы фактов

def to_dwh_fact():
    try:
        cursor.execute("""
            insert into dwh_fact_transactions (transaction_id, transaction_date, amount, card_num, oper_type, oper_result, terminal, create_dt)
            select transaction_id, transaction_date, amount, card_num, oper_type, oper_result, terminal, current_timestamp
            from stg_transactions t1
            where not exists (
                select 1 from dwh_fact_transactions t2 
                where t2.transaction_id = t1.transaction_id
            )
        """)
        connection.commit()
    except Exception as e:
        print(f"Ошибка при загрузке dwh_fact_transactions: {e}")
        connection.rollback()
    
    try:
        cursor.execute("""
            insert into dwh_fact_passport_blacklist (date, passport, effective_from, effective_to)
            select distinct 
                t1.date, 
                t1.passport, 
                current_date, 
                '5999-12-31'::date
            from stg_passport_blacklist t1
            where not exists (
                select 1 from dwh_fact_passport_blacklist t2 
                where t2.passport = t1.passport 
                and t2.date = t1.date
            )
        """)
        connection.commit()
    except Exception as e:
        print(f"Ошибка при загрузке dwh_fact_passport_blacklist: {e}")
        connection.rollback()


# to_dwh_fact()


# ========================================================================
# Определение мошеннических операций

def rep_fraud():
    report_dt = datetime.now()
    total_fraud = 0
    
    # просроченный паспорт
    try:
        cursor.execute("""
            insert into rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
            select distinct 
                t1.transaction_date,
                t4.passport_num,
                t4.last_name || ' ' || t4.first_name || ' ' || coalesce(t4.patronymic, '') as fio,
                t4.phone,
                'операция с просроченным паспортом',
                %s
            from dwh_fact_transactions t1
            join dwh_dim_cards t2 on t1.card_num = t2.card_num
            join dwh_dim_clients t4 on t4.client_id = t2.account_num
            join dwh_fact_passport_blacklist t3 on t4.passport_num = t3.passport
            where t3.date <= t1.transaction_date::date
        """, (report_dt,))
        count = cursor.rowcount
        total_fraud += count
        print(f"просроченный паспорт: {count}")
        connection.commit()
    except Exception as e:
        print(f"Ошибка в паспорте: {e}")
        connection.rollback()
    
    # недействующий договор
    try:
        cursor.execute("""
            insert into rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
            select distinct
                t1.transaction_date,
                t4.passport_num,
                t4.last_name || ' ' || t4.first_name || ' ' || coalesce(t4.patronymic, '') as fio,
                t4.phone,
                'операция по недействующему договору',
                %s
            from dwh_fact_transactions t1
            join dwh_dim_cards t2 on t1.card_num = t2.card_num
            join dwh_dim_clients t4 on t4.client_id = t2.account_num
            join dwh_dim_accounts t3 on t3.client = t4.client_id
            where t3.valid_to < t1.transaction_date::date or t3.valid_to is null
        """, (report_dt,))
        count = cursor.rowcount
        total_fraud += count
        print(f"недействующий договор: {count}")
        connection.commit()
    except Exception as e:
        print(f"Ошибка в договоре: {e}")
        connection.rollback()
    
    #разные города за 1 час
    try:
        cursor.execute("""
            insert into rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
            select 
                t2.transaction_date,
                t7.passport_num,
                t7.last_name || ' ' || t7.first_name || ' ' || coalesce(t7.patronymic, '') as fio,
                t7.phone,
                'операции в разных городах в течение часа',
                %s
            from dwh_fact_transactions t1
            join dwh_fact_transactions t2 on t1.card_num = t2.card_num
            join dwh_dim_terminals_hist t3 on t1.terminal = t3.terminal_id
            join dwh_dim_terminals_hist t4 on t2.terminal = t4.terminal_id
            join dwh_dim_cards t5 on t1.card_num = t5.card_num
            join dwh_dim_clients t7 on t7.client_id = t5.account_num
            where t2.transaction_date > t1.transaction_date
              and t3.effective_from <= t1.transaction_date::date 
              and t3.effective_to >= t1.transaction_date::date
              and t4.effective_from <= t2.transaction_date::date 
              and t4.effective_to >= t2.transaction_date::date
              and extract(epoch from (t2.transaction_date - t1.transaction_date)) / 3600 <= 1
              and t3.terminal_city != t4.terminal_city
        """, (report_dt,))
        count = cursor.rowcount
        total_fraud += count
        print(f"разные города за час: {count}")
        connection.commit()
    except Exception as e:
        print(f"Ошибка города в час: {e}")
        connection.rollback()
    
    #подбор суммы
    try:
        cursor.execute("""
            with ops_20min as (
                select 
                    transaction_id,
                    transaction_date,
                    card_num,
                    replace(amount, ',', '.')::numeric as amount_numeric,
                    oper_result,
                    lag(replace(amount, ',', '.')::numeric) over (partition by card_num order by transaction_date) as prev_amount,
                    lag(oper_result) over (partition by card_num order by transaction_date) as prev_result,
                    lag(transaction_date) over (partition by card_num order by transaction_date) as prev_date
                from dwh_fact_transactions
            )
            insert into rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
            select distinct
                t1.transaction_date,
                t3.passport_num,
                t3.last_name || ' ' || t3.first_name || ' ' || coalesce(t3.patronymic, '') as fio,
                t3.phone,
                'подбор суммы (последняя успешная после отказов)',
                %s
            from ops_20min t1
            join dwh_dim_cards t2 on t1.card_num = t2.card_num
            join dwh_dim_clients t3 on t3.client_id = t2.account_num
            where t1.oper_result = 'SUCCESS'
              and t1.prev_result = 'FAILURE'
              and t1.amount_numeric < t1.prev_amount
              and extract(epoch from (t1.transaction_date - t1.prev_date)) / 60 <= 20
        """, (report_dt,))
        count = cursor.rowcount
        total_fraud += count
        print(f"подбор суммы: {count}")
        connection.commit()
    except Exception as e:
        print(f"Ошибка в подборе суммы: {e}")
        connection.rollback()
    
    print(f"Кол-во мошеннических операций: {total_fraud}")
    
# rep_fraud()




