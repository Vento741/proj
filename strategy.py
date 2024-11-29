import time
import numpy as np
import talib
import logging
from okx import Trade, MarketData, Account
from config import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TradingStrategy:
    """
    Класс торговой стратегии, реализующий алгоритм торговли криптовалютой 
    с использованием индекса относительной силы (RSI) для сигналов входа и выхода.

    Стратегия использует API OKX для получения рыночных данных, 
    осуществления торговли и управления счетом. Реализует динамический 
    подход к торговле с двухэтапной покупкой и предопределенными 
    механизмами стоп-лосса и тейк-профита.

    Атрибуты:
        config (object): Настройки конфигурации торговой стратегии
        buy1_size (int): Размер первого ордера на покупку
        buy2_size (int): Размер второго ордера на покупку
        stop_loss_percent (float): Процент стоп-лосса
        take_profit_percent (float): Процент тейк-профита
        rsi_length (int): Период расчета RSI
        rsi_overbought (int): Порог перекупленности RSI
        rsi_oversold (int): Порог перепроданности RSI
    """

    def __init__(self, config_instance=config):
        """
        Инициализация торговой стратегии с настройками конфигурации и подключениями API.

        Args:
            config_instance (object, optional): Настройки конфигурации. 
            По умолчанию используется импортированный config.
        """
        self.config = config_instance
        
        # Параметры торговли
        self.buy1_size = 40       
        self.buy2_size = 7        
        self.stop_loss_percent = 3  
        self.take_profit_percent = 5  
        self.buy2_offset = 4       
        self.immediate_exit_threshold = 2  
        
        # Параметры RSI
        self.rsi_length = 14
        self.rsi_overbought = 67
        self.rsi_oversold = 33
        
        # Инициализация подключений к API
        self.trade_api = Trade.TradeAPI(
            api_key=self.config.api_key,
            api_secret_key=self.config.api_secret_key,
            passphrase=self.config.passphrase,
            flag=self.config.flag
        )
        
        self.market_api = MarketData.MarketAPI(
            api_key=self.config.api_key,
            api_secret_key=self.config.api_secret_key,
            passphrase=self.config.passphrase,
            flag=self.config.flag
        )
        
        self.account_api = Account.AccountAPI(
            api_key=self.config.api_key,
            api_secret_key=self.config.api_secret_key,
            passphrase=self.config.passphrase,
            flag=self.config.flag
        )
        
        # Отслеживание позиции
        self.position_size = 0
        self.position_avg_price = 0

    def get_historical_prices(self, symbol, limit=100):
        """
        Получение исторических данных о ценах для указанного торгового символа.

        Args:
            symbol (str): Торговый символ (например, 'XRP-USDT')
            limit (int, optional): Количество исторических свечей для получения. 
            По умолчанию 100.

        Returns:
            list: Список цен закрытия для указанного символа
        """
        try:
            candles = self.market_api.get_candlesticks(symbol, bar='5m', limit=str(limit))
            return [float(candle[5]) for candle in candles['data']]
        except Exception as e:
            logger.error(f"Ошибка получения исторических цен: {e}")
            return []

    def calculate_rsi(self, prices):
        """
        Расчет индекса относительной силы (RSI) для заданных ценовых данных.

        Args:
            prices (list): Список исторических цен

        Returns:
            numpy.ndarray: Массив значений RSI
        """
        return talib.RSI(np.array(prices), timeperiod=self.rsi_length)

    def get_current_price(self, symbol):
        """
        Получение текущей цены для указанного торгового символа.

        Args:
            symbol (str): Торговый символ (например, 'XRP-USDT')

        Returns:
            float: Текущая цена символа
        """
        ticker = self.market_api.get_ticker(symbol)
        return float(ticker['data'][0]['last'])

    def check_entry_conditions(self, prices, current_price):
        """
        Проверка условий для входа в позицию.

        Args:
            prices (list): Список исторических цен
            current_price (float): Текущая цена

        Returns:
            str или None: Сигнал для входа ('buy1' или 'buy2') или None, 
            если условия входа не выполнены
        """
        rsi_values = self.calculate_rsi(prices)
        current_rsi = rsi_values[-1]
        
        # Условия для первой покупки
        if self.position_size == 0 and current_rsi < self.rsi_oversold:
            return 'buy1'
        
        # Условия для второй покупки
        if (self.position_size == self.buy1_size and 
            current_price > self.position_avg_price * (1 + self.buy2_offset / 100)):
            return 'buy2'
        
        return None

    def check_exit_conditions(self, prices, current_price):
        """
        Проверка условий для выхода из позиции.

        Args:
            prices (list): Список исторических цен
            current_price (float): Текущая цена

        Returns:
            str или None: Сигнал для выхода ('immediate_exit' или 'tp_sl') 
            или None, если условия выхода не выполнены
        """
        if self.position_size == 0:
            return None
        
        # Немедленный выход
        if current_price < self.position_avg_price * (1 - self.immediate_exit_threshold / 100):
            return 'immediate_exit'
        
        # Тейк-профит и стоп-лосс
        stop_loss_price = self.position_avg_price * (1 - self.stop_loss_percent / 100)
        take_profit_price = self.position_avg_price * (1 + self.take_profit_percent / 100)
        
        if current_price <= stop_loss_price or current_price >= take_profit_price:
            return 'tp_sl'
        
        return None

    def enter_position(self, symbol, side, size):
        """
        Вход в торговую позицию.

        Args:
            symbol (str): Торговый символ
            side (str): Сторона сделки ('buy' или 'sell')
            size (float): Размер позиции

        Returns:
            dict или None: Результат ордера или None в случае ошибки
        """
        try:
            current_price = self.get_current_price(symbol)
            
            order = self.trade_api.place_order(
                instId=symbol,
                tdMode='cross',
                side=side,
                ordType='market',
                sz=str(size)
            )
            
            # Обновление состояния позиции
            self.position_size += size
            self.position_avg_price = current_price
            
            logger.info(f"Вход в позицию {symbol}: {side}, размер {size}, цена {current_price}")
            return order
        except Exception as e:
            logger.error(f"Ошибка входа в позицию: {e}")
            return None

    def exit_position(self, symbol):
        """
        Выход из торговой позиции.

        Args:
            symbol (str): Торговый символ

        Returns:
            dict или None: Результат ордера или None в случае ошибки
        """
        try:
            side = 'sell' if self.position_size > 0 else 'buy'
            
            order = self.trade_api.place_order(
                instId=symbol,
                tdMode='cross',
                side=side,
                ordType='market',
                sz=str(self.position_size)
            )
            
            # Сброс состояния позиции
            self.position_size = 0
            self.position_avg_price = 0
            
            logger.info(f"Выход из позиции {symbol}")
            return order
        except Exception as e:
            logger.error(f"Ошибка выхода из позиции: {e}")
            return None

    def run_strategy(self, symbol):
        """
        Основной метод выполнения торговой стратегии.

        Непрерывный цикл мониторинга рынка, проверки условий входа и выхода, 
        и выполнения торговых операций.

        Args:
            symbol (str): Торговый символ для выполнения стратегии
        """
        while True:
            try:
                # Получение исторических цен и текущей цены
                historical_prices = self.get_historical_prices(symbol)
                current_price = self.get_current_price(symbol)
                
                # Проверка условий входа
                entry_signal = self.check_entry_conditions(historical_prices, current_price)
                
                if entry_signal == 'buy1':
                    self.enter_position(symbol, 'buy', self.buy1_size)
                elif entry_signal == 'buy2':
                    self.enter_position(symbol, 'buy', self.buy2_size)
                
                # Проверка условий выхода
                exit_signal = self.check_exit_conditions(historical_prices, current_price)
                
                if exit_signal in ['immediate_exit', 'tp_sl']:
                    self.exit_position(symbol)
                
            except Exception as e:
                logger.error(f"Ошибка в основном цикле стратегии: {e}")
            
            # Пауза перед следующей итерацией
            time.sleep(10)  # 1 минута

if __name__ == "__main__":
    strategy = TradingStrategy()
    strategy.run_strategy('XRP-USDT')