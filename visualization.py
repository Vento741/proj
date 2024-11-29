import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import numpy as np
import sqlite3
from datetime import datetime
import threading
import time
from data.models import fetch_and_store_data  # Импортируем функцию загрузки данных
from config import config  # Импортируем конфигурацию
import talib

class HistoricalDataVisualizer:
    def __init__(self, symbol='XRP-USDT', db_path='historical_data.db'):
        """
        Инициализация визуализатора исторических данных.
        
        :param symbol: Торговый символ
        :param db_path: Путь к базе данных SQLite
        """
        self.symbol = symbol
        self.db_path = db_path
        
        # Настройки графика
        plt.style.use('default')
        self.fig, (self.ax1, self.ax2, self.ax3) = plt.subplots(3, 1, figsize=(15, 10), 
                                                                  gridspec_kw={'height_ratios': [3, 1, 1]})
        self.fig.suptitle(f'Исторические данные {symbol}', fontsize=16)
        
        # Линии для графика цены
        self.line_close, = self.ax1.plot([], [], label='Close Price', color='white')
        
        # Линии для объема
        self.volume_bars = self.ax2.bar([], [], color='blue', alpha=0.5)
        
        # Линии для RSI
        self.line_rsi, = self.ax3.plot([], [], label='RSI', color='orange')
        
        # Настройка осей
        self.ax1.set_xlabel('Время')
        self.ax1.set_ylabel('Цена')
        self.ax1.grid(True)
        
        self.ax2.set_xlabel('Время')
        self.ax2.set_ylabel('Объем')
        self.ax2.grid(True)
        
        self.ax3.set_xlabel('Время')
        self.ax3.set_ylabel('RSI')
        self.ax3.axhline(70, color='red', linestyle='--', label='Overbought')
        self.ax3.axhline(30, color='green', linestyle='--', label='Oversold')
        self.ax3.grid(True)
        
        # Добавление легенды
        self.ax1.legend()
        self.ax3.legend()

    def get_historical_data(self):
        """
        Получение исторических данных из базы данных.
        
        :return: Кортеж с массивами timestamps, close price, volume и rsi
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute(f"""
                SELECT timestamp, close, volume 
                FROM historical_data 
                WHERE symbol = '{self.symbol}' 
                ORDER BY timestamp ASC
            """)
            
            data = cursor.fetchall()
            conn.close()
            
            timestamps = [datetime.fromtimestamp(int(row[0])/1000) for row in data]
            close_prices = [row[1] for row in data]
            volumes = [row[2] for row in data]
            
            # Расчет RSI
            rsi = talib.RSI(np.array(close_prices), timeperiod=14)
            
            return timestamps, close_prices, volumes, rsi
        
        except Exception as e:
            print(f"Ошибка при получении данных: {e}")
            return [], [], [], []

    def get_signals(self):
        """
        Получение сигналов стратегии из базы данных.
        
        :return: Список сигналов
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute(f"""
                SELECT timestamp, signal 
                FROM strategy_signals 
                WHERE symbol = '{self.symbol}' 
                ORDER BY timestamp ASC
            """)
            
            signals = cursor.fetchall()
            conn.close()
            
            return [(datetime.fromtimestamp(int(row[0])/1000), row[1]) for row in signals]
        
        except Exception as e:
            print(f"Ошибка при получении сигналов: {e}")
            return []

    def update_plot(self, frame):
        """
        Обновление графика в реальном времени.
        
        :param frame: Текущий кадр анимации
        """
        timestamps, close_prices, volumes, rsi = self.get_historical_data()
        signals = self.get_signals()

        if timestamps:
            # Обновление линии закрытия
            self.line_close.set_data(timestamps, close_prices)
            self.ax1.set_xlim(timestamps[0], timestamps[-1])
            self.ax1.set_ylim(min(close_prices) * 0.95, max(close_prices) * 1.05)

            # Обновление объемов
            self.volume_bars.remove()
            self.volume_bars = self.ax2.bar(timestamps, volumes, color='blue', alpha=0.5)

            # Обновление RSI
            self.line_rsi.set_data(timestamps, rsi)
            self.ax3.set_xlim(timestamps[0], timestamps[-1])
            self.ax3.set_ylim(0, 100)

            # Отображение сигналов стратегии
            for signal_time, signal in signals:
                if signal == 'buy':
                    self.ax1.plot(signal_time, close_prices[timestamps.index(signal_time)], 'g^', markersize=10, label='Buy Signal')
                elif signal == 'sell':
                    self.ax1.plot(signal_time, close_prices[timestamps.index(signal_time)], 'rv', markersize=10, label='Sell Signal')

            self.ax1.legend(loc='upper left')

        return self.line_close, self.volume_bars, self.line_rsi

    def run(self):
        """
        Запуск визуализатора.
        """
        self.animation = FuncAnimation(self.fig, self.update_plot, interval=1000, blit=False, save_count=100)
        plt.show()

if __name__ == "__main__":
    visualizer = HistoricalDataVisualizer()
    visualizer.run()