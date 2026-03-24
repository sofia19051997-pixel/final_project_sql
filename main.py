
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from py_scripts.config import get_connection, SCHEMA
from py_scripts.file_loader import process_all_files, sql2sql
from py_scripts.dwh_loader import load_dwh
from py_scripts.fraud_detection import detect_fraud



def main():
    
    # создание таблиц (если не созданы)
    if not sql2sql("sql_scripts/create_tables.sql"):
        print("Таблицы уже созданы")
    # загрузка файлов в stg
    if not process_all_files():
        print("Ошибка при загрузке файлов в stg")
        return
    # загрузка данных в dwh
    if not load_dwh():
        print("Ошибка при загрузке dwh")
        return 
    # поиск мошеннических операций
    fraud_count = detect_fraud()
    
    print(f"найдено мошеннических операций: {fraud_count}")

if __name__ == "__main__":
    main()