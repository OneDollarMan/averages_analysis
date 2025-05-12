import logging
from enum import Enum
from pathlib import Path
from typing import List
import duckdb
from datetime import datetime


# Названия таблиц в базе
INPUT_TABLE_NAME = 'parquet_data'
TRADE_POINTS_TABLE_NAME = 'trade_points'
INDICATORS_TABLE_NAME = 'indicators'
class DataTypesEnum(str, Enum):
    parquet = 'parquet'
    csv = 'csv'


def load_file_to_duckdb(
        paths: List[str],
        db_connection: duckdb.DuckDBPyConnection,
        table_name: str,
        data_type: DataTypesEnum = DataTypesEnum.parquet,
        overwrite: bool = False,
):
    """
    Загружает файлы в DuckDB (в файл базы данных).
    """
    # Проверка входных данных
    for path in paths:
        if not Path(path).exists():
            raise FileNotFoundError(f"File not found: {path}")

    # Формируем SQL для загрузки
    if overwrite:
        db_connection.execute(f"DROP TABLE IF EXISTS {table_name}")

    # Загружаем все файлы через UNION ALL
    if data_type == DataTypesEnum.parquet:
        sql = f"""
            CREATE TABLE {table_name} AS 
            SELECT * FROM read_parquet({paths})
        """
    else:
        sql = f"""
              CREATE TABLE {table_name} AS
              SELECT *
              FROM read_csv_auto({paths}, header=True) \
              """
    db_connection.execute(sql)

    print(f"Загружено {len(paths)} файлов в таблицу '{table_name}'")


def main(load_files: bool = False):
    try:
        # Подключение с файлом
        conn = duckdb.connect('analysis.db')

        # Загружаем файлы в базу
        print("1/4: Загрузка данных остатков...")
        if load_files:
            load_file_to_duckdb(
                paths=['static/data_part_1.parquet', 'static/data_part_2.parquet', 'static/data_part_3.parquet',
                       'static/data_part_4.parquet', 'static/data_part_5.parquet'],
                db_connection=conn, table_name=INPUT_TABLE_NAME, overwrite=True
            )
            load_file_to_duckdb(
                paths=['static/Справочник ТТ.csv'], db_connection=conn, table_name=TRADE_POINTS_TABLE_NAME,
                data_type=DataTypesEnum.csv, overwrite=True
            )
            load_file_to_duckdb(
                paths=['static/Данные по показателям_1.parquet', 'static/Данные по показателям_2.parquet',
                       'static/Данные по показателям_3.parquet', 'static/Данные по показателям_4.parquet',
                       'static/Данные по показателям_5.parquet'],
                db_connection=conn, table_name=INDICATORS_TABLE_NAME, overwrite=True
            )
        else:
            print('Загрузка файлов пропущена')

        conn.execute(f"""
            CREATE OR REPLACE TABLE Stocks AS 
            SELECT 
                final_store_id AS Store, 
                final_item_id AS Item, 
                dt AS Dt, 
                stock AS Stock_pc
            FROM {INPUT_TABLE_NAME}
            WHERE final_store_id IN (
                SELECT "Код склада"
                FROM {TRADE_POINTS_TABLE_NAME}
                WHERE "Тип склада" = 'Маленький склад'
            )
            AND dt BETWEEN '2025-01-14' AND '2025-03-31'
        """)

        print("2/4: Загрузка данных продаж...")
        conn.execute(f"""
            CREATE OR REPLACE TABLE Sales AS 
            SELECT 
                "Код Склада" AS Store, 
                "Код товара" AS Item, 
                "Дата"::DATE AS Dt, 
                SUM("Отгрузки, шт.") AS Sales_pc,
                SUM("Отгрузки, руб.") AS Sales_rub
            FROM {INDICATORS_TABLE_NAME}
            WHERE "Код Склада" IN (
                SELECT "Код склада"
                FROM {TRADE_POINTS_TABLE_NAME}
                WHERE "Тип склада" = 'Маленький склад'
            )
            AND "Дата"::DATE BETWEEN '2025-01-14' AND '2025-03-31'
            GROUP BY 1, 2, 3
        """)

        print("3/4: Подготовка объединенных данных...")
        conn.execute("""
            CREATE OR REPLACE TEMP VIEW combined_view AS
            SELECT 
                COALESCE(s.Dt, st.Dt) AS Dt,
                COALESCE(s.Store, st.Store) AS Store,
                COALESCE(s.Item, st.Item) AS Item,
                COALESCE(s.Sales_pc, 0) AS Sales_pc,
                COALESCE(s.Sales_rub, 0) AS Sales_rub,
                COALESCE(st.Stock_pc, 0) AS Stock_pc
            FROM Sales s
            FULL OUTER JOIN Stocks st
                ON s.Dt = st.Dt AND s.Store = st.Store AND s.Item = st.Item
            WHERE s.Sales_pc > 0 OR st.Stock_pc > 0
        """)

        print("4/4: Выполнение финального агрегирования...")
        conn.execute("""
            CREATE OR REPLACE TABLE Averages AS
            SELECT 
                Store,
                Item,
                AVG(Sales_pc)  AS Avg_sales_pc,
                AVG(Sales_rub) AS Avg_sales_rub,
                COUNT(*)       AS Days_with_data,
                SUM(Sales_rub) AS Total_sales_rub,
                SUM(Sales_pc)  AS Total_sales_pc,
                MAX(Stock_pc)  AS Max_stock_pc
            FROM combined_view
            GROUP BY Store, Item
            --ORDER BY Store, Item \
        """)

        print("Сохранение результатов...")
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'avg_sales_{timestamp}.csv'
        conn.execute(f"""
            COPY Averages TO '{filename}' (
                FORMAT CSV,
                HEADER TRUE,
                DELIMITER ';',
                QUOTE '"'
            )
        """)
        print(f"Результат сохранен в файл: {filename}")

    except Exception as e:
        logging.exception(repr(e))

    finally:
        if 'conn' in locals():
            conn.close()
            print("Соединение закрыто.")


if __name__ == "__main__":
    main(load_files=True)
