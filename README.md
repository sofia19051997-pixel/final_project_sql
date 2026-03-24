# SQL проект ¬- Выявление мошеннических операций

## Описание
ETL процесс для загрузки ежедневных выгрузок данных в хранилище и построения отчета по мошенническим операциям в банковской системе.

##  Возможности
- Автоматическая загрузка данных из CSV и Excel файлов
- Загрузка данных в PostgreSQL
- Построение DWH (SCD1 и SCD2)
- Выявление мошеннических операций по 4 признакам:
  - Операция с просроченным/заблокированным паспортом
  - Операция по недействующему договору
  - Операции в разных городах в течение одного часа
  - Подбор суммы (более 3 операций за 20 минут)
- Архивирование обработанных файлов с расширением `.backup`
- Инкрементальная загрузка данных


## Структура проекта
```
 final_project_sql/
│
├── main.py              # Запуск процесса
├── ddl_dml.sql          # Скрипты создания таблиц BANK
├── requirementxt        # Зависимости Python
├── .gitignore           # Игнорируемые файлы
├── README.md            # Документацияция
│
├──py_scripts/
│   ├── __init__.py             
│   ├── config.py          # Конфигурация и подключение
│   ├── file_loader.py     # Загрузка файлов в STG
│   ├── dwh_loader.py      # Загрузка DWH
│   └── fraud_detection.py # Поиск мошеннических операций
│
├──sql_scripts/
│   └── create_tables.sql
│
├── data/                          # Исходные файлы
│ ├── transactions_DDMMYYYY.txt
│ ├── terminals_DDMMYYYY.xlsx
│ └── passport_blacklist_DDMMYYYY.xlsx
│
└── archive/                  # Обработанные файлы (.backup)
````

## Установка и запуск

1. **Клонируйте репозиторий**
```bash
git clone https://github.com/sofia19051997-pixel/final_project_sql.git
cd final_project_sql
````

2. **Создайте виртуальное окружение и установите зависимости**

```bash
python -m venv venv
source venv/bin/activate   # Linux / macOS
venv\Scripts\activate      # Windows

pip install -r requirements.txt
```

3. **Создайте файл `.env`**

```env
DB_HOST=localhost
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=database_password
DB_PORT=5432
```


4. **Запустите проект**

```bash
python main.py
```

## Пример подключения

```python
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
host = os.getenv("DATABASE_HOST"),
database = os.getenv("DATABASE_NAME"),
port = os.getenv("DATABASE_PORT"),
user= os.getenv("DATABASE_USER"),
password = os.getenv("DATABASE_PASSWORD")
)

dsn = create_engine("postgresql://{user}:{password}@l{host}:{port}/{database}".format(
	host = os.getenv("DATABASE_HOST"),
	database = os.getenv("DATABASE_NAME"),
	port = os.getenv("DATABASE_PORT"),
	user = os.getenv("DATABASE_USER"),
	password = os.getenv("DATABASE_PASSWORD")
))
	

def test_connection():
    try:
        with dsn.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            print("Подключение успешно:", result.scalar())
    except Exception as e:
        print("❌ Ошибка подключения:", e)
```

