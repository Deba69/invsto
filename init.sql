-- Initialize the database and create the stock_data table

-- Create the stock_data table
CREATE TABLE IF NOT EXISTS stock_data (
    id SERIAL PRIMARY KEY,
    datetime TIMESTAMP NOT NULL,
    close DECIMAL(10, 2) NOT NULL,
    high DECIMAL(10, 2) NOT NULL,
    low DECIMAL(10, 2) NOT NULL,
    open DECIMAL(10, 2) NOT NULL,
    volume BIGINT NOT NULL,
    instrument VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_stock_data_datetime ON stock_data(datetime);
CREATE INDEX IF NOT EXISTS idx_stock_data_instrument ON stock_data(instrument);
CREATE INDEX IF NOT EXISTS idx_stock_data_datetime_instrument ON stock_data(datetime, instrument);

-- Create a function to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create a trigger to automatically update the updated_at column
CREATE TRIGGER update_stock_data_updated_at 
    BEFORE UPDATE ON stock_data 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

COMMENT ON TABLE stock_data IS 'Stock market data for various instruments';
COMMENT ON COLUMN stock_data.datetime IS 'Date and time of the stock data point';
COMMENT ON COLUMN stock_data.close IS 'Closing price of the stock';
COMMENT ON COLUMN stock_data.high IS 'Highest price during the period';
COMMENT ON COLUMN stock_data.low IS 'Lowest price during the period';
COMMENT ON COLUMN stock_data.open IS 'Opening price of the stock';
COMMENT ON COLUMN stock_data.volume IS 'Trading volume for the period';
COMMENT ON COLUMN stock_data.instrument IS 'Stock symbol or instrument name'; 