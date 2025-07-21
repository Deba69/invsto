from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from typing import List, Dict, Any
from pydantic import BaseModel
from datetime import datetime

app = FastAPI(
    title="Invsto API",
    description="Stock market data API",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database connection
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

# Pydantic models
class StockData(BaseModel):
    datetime: datetime
    close: float
    high: float
    low: float
    open: float
    volume: int
    instrument: str

class StockDataResponse(BaseModel):
    id: int
    datetime: datetime
    close: float
    high: float
    low: float
    open: float
    volume: int
    instrument: str
    created_at: datetime
    updated_at: datetime

# API endpoints
@app.get("/")
async def root():
    return {"message": "Welcome to Invsto API", "version": "1.0.0"}

@app.get("/health")
async def health_check():
    try:
        conn = get_db_connection()
        conn.close()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "database": "disconnected", "error": str(e)}

@app.get("/stock-data/", response_model=List[StockDataResponse])
async def get_stock_data(
    instrument: str = None,
    limit: int = 100,
    offset: int = 0,
    conn = Depends(get_db_connection)
):
    """Get stock data with optional filtering by instrument"""
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
            SELECT id, datetime, close, high, low, open, volume, instrument, 
                   created_at, updated_at
            FROM stock_data
        """
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
        
        return [dict(row) for row in results]
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/stock-data/{stock_id}", response_model=StockDataResponse)
async def get_stock_data_by_id(stock_id: int, conn = Depends(get_db_connection)):
    """Get specific stock data by ID"""
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(
            "SELECT id, datetime, close, high, low, open, volume, instrument, created_at, updated_at FROM stock_data WHERE id = %s",
            (stock_id,)
        )
        result = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if not result:
            raise HTTPException(status_code=404, detail="Stock data not found")
        
        return dict(result)
    except HTTPException:
        conn.close()
        raise
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.post("/stock-data/", response_model=StockDataResponse)
async def create_stock_data(stock_data: StockData, conn = Depends(get_db_connection)):
    """Create new stock data entry"""
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(
            """
            INSERT INTO stock_data (datetime, close, high, low, open, volume, instrument)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id, datetime, close, high, low, open, volume, instrument, created_at, updated_at
            """,
            (
                stock_data.datetime,
                stock_data.close,
                stock_data.high,
                stock_data.low,
                stock_data.open,
                stock_data.volume,
                stock_data.instrument
            )
        )
        result = cursor.fetchone()
        conn.commit()
        
        cursor.close()
        conn.close()
        
        return dict(result)
    except Exception as e:
        conn.rollback()
        conn.close()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/instruments/")
async def get_instruments(conn = Depends(get_db_connection)):
    """Get list of all available instruments"""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT instrument FROM stock_data ORDER BY instrument")
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return {"instruments": [row[0] for row in results]}
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 