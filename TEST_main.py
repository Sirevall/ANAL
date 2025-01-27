import psycopg2
import requests
import time
from config import host, user, password, db_name
from datetime import datetime, timedelta

def create_table():
    try:
        conn, cursor = connect_db()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS crypto_rates_raw_h1 (
            asset VARCHAR (100),
            datetime TIMESTAMP,
            rate FLOAT (8)
        );
        """)
        print("Таблица успешно создана или существует.")
    
    except Exception as e:
        print(f"Ошибка при подключении к базе данных: {e}")

    finally:
        if conn:
            cursor.close()
            conn.close()

def connect_db():
    conn = psycopg2.connect(
        host=host,
        user=user,
        password=password,
        dbname=db_name
        )
    conn.autocommit = True
    cursor = conn.cursor()

    cursor.execute("SET search_path TO sirevall;")
    return conn,cursor
            
def clear_table():
    try:
        conn, cursor = connect_db()

        cursor.execute("""
        TRUNCATE TABLE crypto_rates_raw_h1;
        """)
        print("Насрано... Убираем")
    
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

def write_data(start_date, data, days):
    try:
        base_url = "https://api.coincap.io/v2/assets/"
    
        start_timestamp = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp() * 1000)
        end_timestamp = int((datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days)).timestamp() * 1000)
        
        conn, cursor = connect_db()
        current_asset = 1
    
        for asset in data:
            asset_id = asset["id"]
            try:                
                history_url = f"{base_url}{asset_id}/history?interval=h1&start={start_timestamp}&end={end_timestamp}"
                history_response = requests.get(history_url)
                history_response.raise_for_status()
                history_data = history_response.json()["data"]
                
                for entry in history_data:
                    insert_query = '''
                    INSERT INTO crypto_rates_raw_h1 (asset, datetime, rate)
                    VALUES (%s, %s, %s)
                    '''
                    cursor.execute(insert_query, (
                        asset_id,
                        datetime.fromtimestamp(entry["time"] / 1000),
                        float(entry["priceUsd"])
                    ))
                                
            except Exception as e:
                print(f"Ошибка при подключении к базе данных: {e}")
                continue
            except requests.exceptions.RequestException as e:
                print(f"Ошибка при запросе к API: {e}")
            finally:
                print(f"Добавлены данные:{asset_id}-{current_asset}")
                current_asset += 1
    finally:
        if conn:
            cursor.close()
            conn.close()
            print("Закрыто соединение")

def sql_from_dbeaver():
    try:
        conn, cursor = connect_db()

        cursor.execute("""
        CREATE TABLE sirevall.crypto_rates_agg AS
        SELECT
            asset,
            datetime AS hour,
            AVG(rate) AS avg_price,
            MAX(rate) AS max_price,
            MIN(rate) AS min_price,
            AVG(rate) OVER (PARTITION BY asset, DATE(datetime)) AS daily_avg_price
        FROM
            sirevall.crypto_rates_raw_h1
        GROUP BY
            asset,
            datetime,
            rate
        ORDER BY
            asset,
            hour;
        """)
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
    start_date = "2024-11-01"
    write_data(start_date, data, days)
    sql_from_dbeaver()
    print("канэц")