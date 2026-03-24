from .config import get_connection, SCHEMA
from datetime import datetime

# Определение мошеннических операций
# ========================================================================

def rep_fraud():
    report_dt = datetime.now()
    total_fraud = 0
    
    # 1. Просроченный/заблокированный паспорт
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    insert into {SCHEMA}.rep_fraud 
                    (event_dt, passport, fio, phone, event_type, report_dt)
                    select distinct 
                        t1.transaction_date,
                        t4.passport_num,
                        t4.last_name || ' ' || t4.first_name || ' ' || coalesce(t4.patronymic, '') as fio,
                        t4.phone,
                        'операция с просроченным паспортом',
                        %s
                    from {SCHEMA}.dwh_fact_transactions t1
                    join {SCHEMA}.dwh_dim_cards t2 on t1.card_num = t2.card_num
                    join {SCHEMA}.dwh_dim_clients t4 on t4.client_id = t2.account_num
                    join {SCHEMA}.dwh_fact_passport_blacklist t3 on t4.passport_num = t3.passport
                    where t3.date <= t1.transaction_date::date
                """, (report_dt,))
                count = cursor.rowcount
                total_fraud += count
                conn.commit()
                print(f"Просроченный паспорт: {count}")
    except Exception as e:
        print(f"Ошибка в паспорте: {e}")
    
    # 2. Недействующий договор
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    insert into {SCHEMA}.rep_fraud 
                    (event_dt, passport, fio, phone, event_type, report_dt)
                    select distinct
                        t1.transaction_date,
                        t4.passport_num,
                        t4.last_name || ' ' || t4.first_name || ' ' || coalesce(t4.patronymic, '') as fio,
                        t4.phone,
                        'операция по недействующему договору',
                        %s
                    from {SCHEMA}.dwh_fact_transactions t1
                    join {SCHEMA}.dwh_dim_cards t2 on t1.card_num = t2.card_num
                    join {SCHEMA}.dwh_dim_clients t4 on t4.client_id = t2.account_num
                    join {SCHEMA}.dwh_dim_accounts t3 on t3.client = t4.client_id
                    where t3.valid_to < t1.transaction_date::date or t3.valid_to is null
                """, (report_dt,))
                count = cursor.rowcount
                total_fraud += count
                conn.commit()
                print(f"Недействующий договор: {count}")
    except Exception as e:
        print(f"Ошибка в договоре: {e}")
    
    # 3. Операции в разных городах в течение часа
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    insert into {SCHEMA}.rep_fraud 
                    (event_dt, passport, fio, phone, event_type, report_dt)
                    select 
                        t2.transaction_date,
                        t7.passport_num,
                        t7.last_name || ' ' || t7.first_name || ' ' || coalesce(t7.patronymic, '') as fio,
                        t7.phone,
                        'операции в разных городах в течение часа',
                        %s
                    from {SCHEMA}.dwh_fact_transactions t1
                    join {SCHEMA}.dwh_fact_transactions t2 on t1.card_num = t2.card_num
                    join {SCHEMA}.dwh_dim_terminals_hist t3 on t1.terminal = t3.terminal_id
                    join {SCHEMA}.dwh_dim_terminals_hist t4 on t2.terminal = t4.terminal_id
                    join {SCHEMA}.dwh_dim_cards t5 on t1.card_num = t5.card_num
                    join {SCHEMA}.dwh_dim_clients t7 on t7.client_id = t5.account_num
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
                conn.commit()
                print(f"Разные города за час: {count}")
    except Exception as e:
        print(f"Ошибка города в час: {e}")
    
    # 4. Подбор суммы (успешная операция после отказов)
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    with ops_20min as (
                        select 
                            transaction_id,
                            transaction_date,
                            card_num,
                            replace(amount, ',', '.')::numeric as amount_numeric,
                            oper_result,
                            lag(replace(amount, ',', '.')::numeric) over 
                                (partition by card_num order by transaction_date) as prev_amount,
                            lag(oper_result) over 
                                (partition by card_num order by transaction_date) as prev_result,
                            lag(transaction_date) over 
                                (partition by card_num order by transaction_date) as prev_date
                        from {SCHEMA}.dwh_fact_transactions
                    )
                    insert into {SCHEMA}.rep_fraud 
                    (event_dt, passport, fio, phone, event_type, report_dt)
                    select distinct
                        t1.transaction_date,
                        t3.passport_num,
                        t3.last_name || ' ' || t3.first_name || ' ' || coalesce(t3.patronymic, '') as fio,
                        t3.phone,
                        'подбор суммы (последняя успешная после отказов)',
                        %s
                    from ops_20min t1
                    join {SCHEMA}.dwh_dim_cards t2 on t1.card_num = t2.card_num
                    join {SCHEMA}.dwh_dim_clients t3 on t3.client_id = t2.account_num
                    where t1.oper_result = 'SUCCESS'
                      and t1.prev_result = 'FAILURE'
                      and t1.amount_numeric < t1.prev_amount
                      and extract(epoch from (t1.transaction_date - t1.prev_date)) / 60 <= 20
                """, (report_dt,))
                count = cursor.rowcount
                total_fraud += count
                conn.commit()
                print(f"Подбор суммы: {count}")
    except Exception as e:
        print(f"Ошибка в подборе суммы: {e}")
    
    print(f"Кол-во мошеннических операций: {total_fraud}")
    return total_fraud


# Основная функция
# ========================================================================

def detect_fraud():
    return rep_fraud()