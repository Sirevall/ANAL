import psycopg2
import requests
import os
import psycopg2.extras
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

def get_query(query_name):
    queries_path=os.getenv('QUERIES_FILE_PATH')
    with open(queries_path, "r", encoding="utf-8") as f:
        content = f.read().strip().split("\n\n")  # Разделяем запросы по пустым строкам
        for block in content:
            lines = block.strip().split("\n")
            key = lines[0].split("=")[0].strip()  # Берем название запроса (ключ)
            value = "\n".join(lines[1:]).strip()  # Берем сам SQL-запрос
            if key == query_name:
                return value
    raise ValueError(f"SQL-запрос '{query_name}' не найден в файле queries.sql")

def execute_query(query_name):
    try:
        conn, cursor = connect_db()
        sql_query = get_query(query_name)
        cursor.execute(sql_query)
        print(f"Запрос выполнен: {query_name}")
    except Exception as e:
        print(f"Ошибка выполнения SQL-запроса '{query_name}': {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

def create_table():
    try:
        create_main_table_query = get_query("CREATE_MAIN_TABLE")
        conn, cursor = connect_db()
        cursor.execute(create_main_table_query)
        print("Таблица успешно создана или существует.")
    
    except Exception as e:
        print(f"Ошибка при подключении к базе данных: {e}")

    finally:
        if conn:
            cursor.close()
            conn.close()

def connect_db():
    conn = psycopg2.connect(
        host=os.getenv('HOST'),
        user=os.getenv('USER'),
        password=os.getenv('PASSWORD'),
        dbname=os.getenv('DB_NAME')
    )
    conn.autocommit = True
    cursor = conn.cursor()

    cursor.execute("SET search_path TO sirevall;")
    return conn, cursor
            
def clear_table():
    try:
        table_for_cleaning = get_query("TRUNCATE_TABLE")
        conn, cursor = connect_db()
        cursor.execute(table_for_cleaning)
        print("Все подчищено")
    
    except Exception as e:
        print(f"Ошибка при подключении к базе данных: {e}")

    finally:
        if conn:
            cursor.close()
            conn.close()
            
def get_data_capacity():
    base_url = "https://api.coincap.io/v2/assets/"
    response = requests.get(base_url)
    response.raise_for_status()
    assets = response.json()["data"]
    print(f"Найдено {len(assets)} активов")
    return assets

def fetch_crypto_data(start_date, data, days, asset_limit=None):
    
    base_url = "https://api.coincap.io/v2/assets/"
    start_timestamp = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp() * 1000)
    end_timestamp = int((datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days)).timestamp() * 1000)
    
    all_data = []
    asset_limit = min(asset_limit, len(data)) if asset_limit else len(data)
    assets_to_process = data[:asset_limit]

    for asset in assets_to_process:
        asset_id = asset["id"]
        try:
            history_url = f"{base_url}{asset_id}/history?interval=h1&start={start_timestamp}&end={end_timestamp}"
            history_response = requests.get(history_url)
            history_response.raise_for_status()
            history_data = history_response.json()["data"]

            for entry in history_data:
                all_data.append((
                    asset_id,
                    datetime.fromtimestamp(entry["time"] / 1000),
                    float(entry["priceUsd"])
                ))

        except requests.exceptions.RequestException as e:
            print(f"Ошибка при запросе к API ({asset_id}): {e}")
            continue
    print("Данные удачно получены")
    return all_data

def insert_crypto_data(all_data):
    if not all_data:
        print("Нет данных для записи в БД")
        return

    try:
        conn, cursor = connect_db()
        insert_query = get_query("INSERT_DATA")
        psycopg2.extras.execute_values(cursor, insert_query, all_data)
        print(f"Добавлено {len(all_data)} записей в базу данных.")
    except Exception as e:
        print(f"Ошибка при записи в базу данных: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()
            print("Закрыто соединение с базой данных.")

def sql_from_dbeaver():
    try:
        create_agg_table_query = get_query("CREATE_AGG_TABLE")
        conn, cursor = connect_db()
        cursor.execute(create_agg_table_query)
        print("Таблица успешно создана или существует.")
    
    except Exception as e:
        print(f"Ошибка при подключении к базе данных: {e}")

    finally:
        if conn:
            cursor.close()
            conn.close()
            print("Закрыто соединение")
                       
if __name__ == "__main__":
    create_table()
    clear_table()
    data = get_data_capacity()
    days = 30
    asset_count = 10
    start_date = "2024-11-01"
    data_for_insert = fetch_crypto_data(start_date, data, days, asset_count)
    insert_crypto_data(data_for_insert)
    sql_from_dbeaver()
    print("канэц")