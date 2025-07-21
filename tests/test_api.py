import unittest
import json
from fastapi.testclient import TestClient
from app.api import app
import pandas as pd
import numpy as np

class TestFastAPI(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)
    
    def test_get_data_empty(self):
        response = self.client.get("/data?instrument=DOESNOTEXIST")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIsInstance(data, list)
        self.assertEqual(len(data), 0)
    
    def test_post_data_valid(self):
        test_data = {
            "date": "2024-01-15T09:30:00",
            "open": 150.50,
            "high": 155.75,
            "low": 149.25,
            "close": 153.00,
            "volume": 1000000,
            "instrument": "HINDALCO"
        }
        response = self.client.post("/data", json=test_data)
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["open"], 150.50)
        self.assertEqual(data["close"], 153.00)
        self.assertEqual(data["instrument"], "HINDALCO")
    
    def test_post_data_invalid_types(self):
        test_data = {
            "date": "2024-01-15T09:30:00",
            "open": "invalid",
            "high": 155.75,
            "low": 149.25,
            "close": 153.00,
            "volume": 1000000,
            "instrument": "HINDALCO"
        }
        response = self.client.post("/data", json=test_data)
        self.assertEqual(response.status_code, 422)
    
    def test_post_data_missing_fields(self):
        test_data = {
            "date": "2024-01-15T09:30:00",
            "open": 150.50
        }
        response = self.client.post("/data", json=test_data)
        self.assertEqual(response.status_code, 422)
    
    def test_strategy_performance_no_data(self):
        response = self.client.get("/strategy/performance?instrument=DOESNOTEXIST")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn("message", data)
    
    def test_strategy_performance_with_params(self):
        response = self.client.get("/strategy/performance?short_window=10&long_window=30")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        if "short_window" in data:
            self.assertEqual(data["short_window"], 10)
            self.assertEqual(data["long_window"], 30)

class TestMovingAverageCalculations(unittest.TestCase):
    def test_moving_average_calculation(self):
        dates = pd.date_range('2024-01-01', periods=30, freq='D')
        prices = [100 + i + np.random.normal(0, 2) for i in range(30)]
        df = pd.DataFrame({
            'datetime': dates,
            'close': prices
        })
        df.set_index('datetime', inplace=True)
        short_window = 5
        long_window = 20
        df['short_ma'] = df['close'].rolling(window=short_window, min_periods=1).mean()
        df['long_ma'] = df['close'].rolling(window=long_window, min_periods=1).mean()
        self.assertIn('short_ma', df.columns)
        self.assertIn('long_ma', df.columns)
        short_ma_std = df['short_ma'].std()
        long_ma_std = df['long_ma'].std()
        self.assertGreater(short_ma_std, long_ma_std)
    
    def test_signal_generation(self):
        dates = pd.date_range('2024-01-01', periods=25, freq='D')
        prices = [100] * 10 + [110] * 15
        df = pd.DataFrame({
            'datetime': dates,
            'close': prices
        })
        df.set_index('datetime', inplace=True)
        df['short_ma'] = df['close'].rolling(window=5, min_periods=1).mean()
        df['long_ma'] = df['close'].rolling(window=20, min_periods=1).mean()
        df['signal'] = 0
        df.loc[df['short_ma'] > df['long_ma'], 'signal'] = 1
        df.loc[df['short_ma'] < df['long_ma'], 'signal'] = -1
        self.assertIn('signal', df.columns)
        self.assertTrue(all(signal in [-1, 0, 1] for signal in df['signal']))
    
    def test_crossover_detection(self):
        # Ensure a crossover occurs
        dates = pd.date_range('2024-01-01', periods=25, freq='D')
        prices = [100] * 10 + [200] * 15  # This will guarantee a crossover
        df = pd.DataFrame({
            'datetime': dates,
            'close': prices
        })
        df.set_index('datetime', inplace=True)
        df['short_ma'] = df['close'].rolling(window=5, min_periods=1).mean()
        df['long_ma'] = df['close'].rolling(window=20, min_periods=1).mean()
        df['signal'] = 0
        df.loc[df['short_ma'] > df['long_ma'], 'signal'] = 1
        df.loc[df['short_ma'] < df['long_ma'], 'signal'] = -1
        df['position'] = df['signal'].diff().fillna(0)
        buy_signals = df[df['position'] == 2]
        self.assertGreaterEqual(len(buy_signals), 1)

class TestDataValidation(unittest.TestCase):
    def test_stock_data_model_validation(self):
        from app.api import StockData
        valid_data = {
            "datetime": "2024-01-15T09:30:00",
            "open": 150.50,
            "high": 155.75,
            "low": 149.25,
            "close": 153.00,
            "volume": 1000000,
            "instrument": "HINDALCO"
        }
        try:
            stock_data = StockData(**valid_data)
            self.assertEqual(stock_data.open, 150.50)
            self.assertEqual(stock_data.volume, 1000000)
            self.assertEqual(stock_data.instrument, "HINDALCO")
        except Exception as e:
            self.fail(f"Valid data should not raise exception: {e}")
    
    def test_stock_data_model_invalid(self):
        from app.api import StockData
        invalid_data = {
            "datetime": "2024-01-15T09:30:00",
            "open": 150.50,
            "high": 155.75,
            "low": 149.25,
            "close": 153.00,
            "volume": -1000,
            "instrument": "HINDALCO"
        }
        with self.assertRaises(Exception):
            StockData(**invalid_data)

if __name__ == '__main__':
    unittest.main() 