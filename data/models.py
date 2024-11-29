import sqlite3
from sqlite3 import Error
import time
from datetime import datetime
from okx import MarketData  # Импортируем класс для получения рыночных данных
from config import config  # Импортируем конфигурацию

class Database:
    def __init__(self, db_file):
        """Инициализация базы данных."""
        self.connection = self.create_connection(db_file)
        self.create_table()
        self.create_strategy_signals_table()  # Добавьте этот вызов

    def create_connection(self, db_file):
        """Создание соединения с SQLite базой данных."""
        try:
            conn = sqlite3.connect(db_file)
            print(f"Соединение с {db_file} успешно установлено.")
            return conn
        except Error as e:
            print(f"Ошибка '{e}' при создании соединения с {db_file}.")
            return None

    def create_table(self):
        """Создание таблицы для хранения исторических данных."""
        try:
            sql_create_table = """
            CREATE TABLE IF NOT EXISTS historical_data (
                symbol TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                datetime TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                PRIMARY KEY (symbol, timestamp)  -- Устанавливаем уникальный ключ
            );
            """
            cursor = self.connection.cursor()
            cursor.execute(sql_create_table)
            print("Таблица 'historical_data' успешно создана.")
        except Error as e:
            print(f"Ошибка '{e}' при создании таблицы.")

    def create_strategy_signals_table(self):
        """Создание таблицы для хранения сигналов стратегии."""
        try:
            sql_create_table = """
            CREATE TABLE IF NOT EXISTS strategy_signals (
                symbol TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                signal TEXT NOT NULL,
                PRIMARY KEY (symbol, timestamp)  -- Устанавливаем уникальный ключ
            );
            """
            cursor = self.connection.cursor()
            cursor.execute(sql_create_table)
            print("Таблица 'strategy_signals' успешно создана.")
        except Error as e:
            print(f"Ошибка '{e}' при создании таблицы.")

    def insert_signal(self, timestamp, signal):
        """Вставка сигнала в таблицу strategy_signals."""
        sql_insert_signal = """
        INSERT OR REPLACE INTO strategy_signals (timestamp, signal)
        VALUES (?, ?);
        """
        cursor = self.connection.cursor()
        cursor.execute(sql_insert_signal, (timestamp, signal))
        self.connection.commit()


    def insert_or_update_data(self, symbol, timestamp, open_price, high_price, low_price, close_price, volume):
        """Вставка или обновление данных в таблице."""
        # Преобразование timestamp в читаемый формат datetime
        datetime_str = self.convert_timestamp_to_datetime(timestamp)
        
        sql_insert_or_update = """
        INSERT OR REPLACE INTO historical_data 
        (symbol, timestamp, datetime, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """
        cursor = self.connection.cursor()
        cursor.execute(sql_insert_or_update, (
            symbol, 
            timestamp, 
            datetime_str, 
            open_price, 
            high_price, 
            low_price, 
            close_price, 
            volume
        ))
        self.connection.commit()

    def convert_timestamp_to_datetime(self, timestamp):
        """
        Преобразование timestamp в читаемый формат datetime.
        
        :param timestamp: Timestamp в миллисекундах
        :return: Строка с датой и временем
        """
        dt = datetime.fromtimestamp(int(timestamp) / 1000)
        return dt.strftime('%Y-%m-%d %H:%M:%S')

    def bulk_insert_historical_data(self, symbol, historical_candles):
        """
        Массовая вставка исторических данных с сортировкой по timestamp.
        
        :param symbol: Символ торговой пары
        :param historical_candles: Список исторических свечей
        """
        try:
            # Сортируем свечи по timestamp в порядке возрастания
            sorted_candles = sorted(historical_candles['data'], key=lambda x: int(x[0]))
            
            data_to_insert = []
            for candle in sorted_candles:
                timestamp = candle[0]
                datetime_str = self.convert_timestamp_to_datetime(timestamp)
                data_to_insert.append((
                    symbol, 
                    timestamp, 
                    datetime_str, 
                    candle[1],  # open
                    candle[2],  # high
                    candle[3],  # low
                    candle[4],  # close
                    candle[5]   # volume
                ))

            sql_bulk_insert = """
            INSERT OR REPLACE INTO historical_data 
            (symbol, timestamp, datetime, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """
            
            cursor = self.connection.cursor()
            cursor.executemany(sql_bulk_insert, data_to_insert)
            self.connection.commit()
            
            print(f"Загружено {len(data_to_insert)} исторических свечей для {symbol}")
        
        except Exception as e:
            print(f"Ошибка при массовой вставке данных: {e}")

    def get_sorted_historical_data(self, symbol):
        """
        Получение отсортированных исторических данных для символа.
        
        :param symbol: Символ торговой пары
        :return: Список отсортированных записей
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT symbol, timestamp, datetime, open, high, low, close, volume 
                FROM historical_data 
                WHERE symbol = ? 
                ORDER BY timestamp ASC
            """, (symbol,))
            
            return cursor.fetchall()
        
        except Exception as e:
            print(f"Ошибка при получении исторических данных: {e}")
            return []

def fetch_and_store_data(symbol, timeframe='5m', limit=5000):
    """
    Получ и сохранение исторических данных для указанного символа.
    
    :param symbol: Символ торговой пары
    :param timeframe: Временной интервал свечей
    :param limit: Количество загружаемых свечей
    """
    db = Database("historical_data.db")
    market_api = MarketData.MarketAPI(
        api_key=config.api_key,
        api_secret_key=config.api_secret_key,
        passphrase=config.passphrase,
        flag=config.flag
    )
    
    try:
        # Загрузка исторических данных
        historical_candles = market_api.get_candlesticks(symbol, bar=timeframe, limit=str(limit))
        
        if historical_candles and 'data' in historical_candles:
            # Массовая вставка исторических данных
            db.bulk_insert_historical_data(symbol, historical_candles)
        
            # Получение и вывод отсортированных исторических данных для проверки
            # sorted_data = db.get_sorted_historical_data(symbol)
            # print(f"Исторические данные для {symbol}:")
            # for row in sorted_data:
            #     print(f"{row[1]}\t{row[2]}\t{row[3]}\t{row[4]}\t{row[5]}\t{row[6]}\t{row[7]}")
        
        # Бесконечный цикл обновления данных каждую секунду
        while True:
            # Получаем последнюю свечу
            latest_candles = market_api.get_candlesticks(symbol, bar=timeframe, limit='1')
            
            if latest_candles and 'data' in latest_candles:
                latest_data = latest_candles['data'][0]
                
                # Вставляем или обновляем последнюю свечу
                db.insert_or_update_data(
                    symbol, 
                    latest_data[0],  # timestamp
                    latest_data[1],  # open
                    latest_data[2],  # high
                    latest_data[3],  # low
                    latest_data[4],  # close
                    latest_data[5]   # volume
                )
                # Пример вставки сигнала

            # Ждем 1 секунду перед следующим обновлением
            time.sleep(2)

    except Exception as e:
        print(f"Ошибка при получении данных: {e}")

if __name__ == "__main__":
    fetch_and_store_data('XRP-USDT')
    db = Database("trading_data.db")
    db.insert_signal('XRP-USDT', int(time.time()), 'buy')  # Вставка сигнала покупки
    db.insert_signal('XRP-USDT', int(time.time()), 'sell')  # Вставка сигнала продажи