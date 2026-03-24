from .config import get_connection, SCHEMA
from datetime import datetime

# Таблицы измерений 
# ====================================================================

def dim_terminals_hist():
    today = datetime.now().date()
    
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    update {SCHEMA}.dwh_dim_terminals_hist 
                    set effective_to = %s
                    where effective_to = '5999-12-31'
                    and terminal_id in (
                        select terminal_id from {SCHEMA}.stg_terminals
                        except
                        select terminal_id from {SCHEMA}.dwh_dim_terminals_hist 
                        where effective_to = '5999-12-31'
                    )
                """, (today,))


                cursor.execute(f"""
                    insert into {SCHEMA}.dwh_dim_terminals_hist (terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg)
                    select 
                        terminal_id, 
                        terminal_type, 
                        terminal_city, 
                        terminal_address, 
                        %s, 
                        '5999-12-31', 
                        false
                    from {SCHEMA}.stg_terminals
                    where terminal_id not in (
                        select terminal_id from {SCHEMA}.dwh_dim_terminals_hist 
                        where effective_to = '5999-12-31'
                    )
                """, (today,))
                conn.commit()
                return True
    except Exception as e:
        print(f"Ошибка в dim_terminals_hist: {e}")
        return False


def to_dwh_dim():
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                # Загрузка клиентов
                cursor.execute(f"truncate table {SCHEMA}.dwh_dim_clients")
                cursor.execute(f"""
                    insert into {SCHEMA}.dwh_dim_clients 
                    (client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone)
                    select client_id, last_name, first_name, patronymic, 
                           date_of_birth, passport_num, passport_valid_to, phone
                    from {SCHEMA}.stg_clients
                """)
                # Загрузка карт
                cursor.execute(f"truncate table {SCHEMA}.dwh_dim_cards")
                cursor.execute(f"""
                    insert into {SCHEMA}.dwh_dim_cards (card_num, account_num)
                    select card_num, account
                    from {SCHEMA}.stg_cards
                """)
                # Загрузка счетов
                cursor.execute(f"Truncate table {SCHEMA}.dwh_dim_accounts")
                cursor.execute(f"""
                    insert into {SCHEMA}.dwh_dim_accounts (account_num, client, valid_to)
                    select account, client, valid_to
                    from {SCHEMA}.stg_accounts
                """)

                conn.commit()
                

        dim_terminals_hist()
        
        print("Измерения загружены")
        return True
        
    except Exception as e:
        print(f"Ошибка при загрузке измерений: {e}")
        return False


# Таблицы фактов
# ========================================================================

def to_dwh_fact():
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                # Загрузка транзакций
                cursor.execute(f"""
                    insert into {SCHEMA}.dwh_fact_transactions (transaction_id, transaction_date, amount, card_num, oper_type, oper_result, terminal, create_dt)
                    select transaction_id, transaction_date, amount, card_num, oper_type, oper_result, terminal, current_timestamp
                    from {SCHEMA}.stg_transactions t1
                    where not exists (
                        select 1 from {SCHEMA}.dwh_fact_transactions t2 
                        where t2.transaction_id = t1.transaction_id
                    )
                """)
                trans_count = cursor.rowcount
                print(f"Добавлено {trans_count} транзакций")
                
                # Загрузка черного списка паспортов
                cursor.execute(f"""
                    insert into {SCHEMA}.dwh_fact_passport_blacklist (date, passport, effective_from, effective_to)
                    select distinct 
                        t1.date, 
                        t1.passport, 
                        current_date, 
                        '5999-12-31'::date
                    from {SCHEMA}.stg_passport_blacklist t1
                    where not exists (
                        select 1 from {SCHEMA}.dwh_fact_passport_blacklist t2 
                        where t2.passport = t1.passport 
                        and t2.date = t1.date
                    )
                """)
                blacklist_count = cursor.rowcount
                print(f"Добавлено {blacklist_count} записей")
                
                conn.commit()
                return True
                
    except Exception as e:
        print(f"Ошибка при загрузке фактов: {e}")
        return False


# Основная функция для загрузки всего DWH
# ========================================================================

def load_dwh():
    """Загрузка всех данных в DWH"""
    
    if not to_dwh_dim():
        return False
    
    if not to_dwh_fact():
        return False
    
    return True