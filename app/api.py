from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel, Field, validator
from typing import List
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import pandas as pd

app = FastAPI()

# Pydantic models
class StockData(BaseModel):
    datetime: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    instrument: str

    @validator('volume')
    def volume_must_be_non_negative(cls, v):
        if v < 0:
            raise ValueError('volume must be non-negative')
        return v

class StockDataCreate(BaseModel):
    datetime: str = Field(..., alias="date")
    open: float
    high: float
    low: float
    close: float
    volume: int
    instrument: str = "HINDALCO"

    @validator('volume')
    def volume_must_be_non_negative(cls, v):
        if v < 0:
            raise ValueError('volume must be non-negative')
        return v

# Remove in-memory storage

def get_db_connection():
    try:
        connection = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            database=os.getenv("POSTGRES_DB", "invsto_db"),
            user=os.getenv("POSTGRES_USER", "invsto_user"),
            password=os.getenv("POSTGRES_PASSWORD", "invsto_password"),
            port=os.getenv("POSTGRES_PORT", "5432")
        )
        return connection
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

@app.get("/data", response_model=List[StockData])
def get_data(instrument: str = "HINDALCO", limit: int = 100, offset: int = 0, conn = Depends(get_db_connection)):
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        query = "SELECT datetime, close, high, low, open, volume, instrument FROM stock_data"
        params = []
        if instrument:
            query += " WHERE instrument = %s"
            params.append(instrument)
        query += " ORDER BY datetime DESC LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        cursor.execute(query, params)
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        data = []
        for row in results:
            row = dict(row)
            if isinstance(row["datetime"], (str, type(None))):
                pass
            else:
                row["datetime"] = row["datetime"].isoformat()
            data.append(row)
        return data
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.post("/data", response_model=StockData)
def add_data(stock: StockDataCreate, conn = Depends(get_db_connection)):
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(
            """
            INSERT INTO stock_data (datetime, close, high, low, open, volume, instrument)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING datetime, close, high, low, open, volume, instrument
            """,
            (
                stock.datetime,
                stock.close,
                stock.high,
                stock.low,
                stock.open,
                stock.volume,
                stock.instrument
            )
        )
        result = cursor.fetchone()
        conn.commit()
        cursor.close()
        conn.close()
        row = dict(result)
        if isinstance(row["datetime"], (str, type(None))):
            pass
        else:
            row["datetime"] = row["datetime"].isoformat()
        return row
    except Exception as e:
        conn.rollback()
        conn.close()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/strategy/performance")
def get_strategy_performance(instrument: str = "HINDALCO", short_window: int = 5, long_window: int = 20, conn = Depends(get_db_connection)):
    """Calculate moving averages, generate buy/sell signals, and return strategy performance."""
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        query = """
            SELECT datetime, close FROM stock_data
        """
        params = []
        if instrument:
            query += " WHERE instrument = %s"
            params.append(instrument)
        query += " ORDER BY datetime ASC"
        cursor.execute(query, params)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        if not rows:
            return {"message": "No data found for the given instrument."}
        df = pd.DataFrame(rows)
        df['datetime'] = pd.to_datetime(df['datetime'])
        df['close'] = df['close'].astype(float)
        df.set_index('datetime', inplace=True)
        df['short_ma'] = df['close'].rolling(window=short_window, min_periods=1).mean()
        df['long_ma'] = df['close'].rolling(window=long_window, min_periods=1).mean()
        df['signal'] = 0
        df.loc[df['short_ma'] > df['long_ma'], 'signal'] = 1
        df.loc[df['short_ma'] < df['long_ma'], 'signal'] = -1
        df['position'] = df['signal'].diff().fillna(0)
        # Buy/sell signals
        buy_signals = df[df['position'] == 2].index.strftime('%Y-%m-%d').tolist()
        sell_signals = df[df['position'] == -2].index.strftime('%Y-%m-%d').tolist()
        num_buys = len(buy_signals)
        num_sells = len(sell_signals)
        total_trades = min(num_buys, num_sells)
        # Simple performance: cumulative returns
        df['returns'] = df['close'].pct_change().fillna(0)
        df['strategy_returns'] = df['returns'] * df['signal'].shift(1).fillna(0)
        cumulative_return = (df['strategy_returns'] + 1).prod() - 1
        return {
            "buy_signals": buy_signals,
            "sell_signals": sell_signals,
            "num_buys": num_buys,
            "num_sells": num_sells,
            "total_trades": total_trades,
            "cumulative_return": cumulative_return,
            "short_window": short_window,
            "long_window": long_window,
            "instrument": instrument
        }
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Strategy calculation error: {str(e)}") 