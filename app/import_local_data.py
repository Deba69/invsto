import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import os

class LocalDataImporter:
    def __init__(self):
        self.db_params = {
            'host': os.getenv("POSTGRES_HOST", "localhost"),
            'database': os.getenv("POSTGRES_DB", "invsto_db"),
            'user': os.getenv("POSTGRES_USER", "invsto_user"),
            'password': os.getenv("POSTGRES_PASSWORD", "invsto_password"),
            'port': os.getenv("POSTGRES_PORT", "5432")
        }
    
    def load_csv_data(self, csv_file_path):
        """Load data from local CSV file"""
        try:
            print(f"Loading data from: {csv_file_path}")
            df = pd.read_csv(csv_file_path)
            print(f"Loaded {len(df)} rows of data")
            print(f"Columns: {list(df.columns)}")
            return df
            
        except Exception as e:
            print(f"Error loading data: {str(e)}")
            return None
    
    def clean_data(self, df):
        """Clean and prepare data for database insertion"""
        try:
            # Display first few rows to understand the data structure
            print("\nFirst 5 rows of data:")
            print(df.head())
            
            # Check column names and rename if needed
            print(f"\nOriginal columns: {list(df.columns)}")
            
            # Ensure required columns exist
            required_columns = ['datetime', 'open', 'high', 'low', 'close', 'volume']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                print(f"Warning: Missing required columns: {missing_columns}")
                print("Available columns:", list(df.columns))
                return None
            
            # Convert datetime column
            df['datetime'] = pd.to_datetime(df['datetime'])
            
            # Convert numeric columns
            numeric_columns = ['open', 'high', 'low', 'close', 'volume']
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Remove rows with missing data
            initial_count = len(df)
            df = df.dropna()
            final_count = len(df)
            
            if initial_count != final_count:
                print(f"Removed {initial_count - final_count} rows with missing data")
            
            # Ensure instrument column exists
            if 'instrument' not in df.columns:
                df['instrument'] = 'HINDALCO'
            
            print(f"Cleaned data: {len(df)} rows")
            print("\nSample of cleaned data:")
            print(df.head())
            
            return df
            
        except Exception as e:
            print(f"Error cleaning data: {str(e)}")
            return None
    
    def insert_data(self, df):
        """Insert data into PostgreSQL database"""
        try:
            connection = psycopg2.connect(**self.db_params)
            cursor = connection.cursor()
            
            # Check if data already exists
            cursor.execute("SELECT COUNT(*) FROM stock_data WHERE instrument = 'HINDALCO'")
            existing_count = cursor.fetchone()[0]
            print(f"Existing HINDALCO records in database: {existing_count}")
            
            # Clear existing HINDALCO data to avoid conflicts
            if existing_count > 0:
                print("Clearing existing HINDALCO data...")
                cursor.execute("DELETE FROM stock_data WHERE instrument = 'HINDALCO'")
                connection.commit()
                print("Existing data cleared.")
            
            # Insert new data with simple insert (no ON CONFLICT)
            insert_query = """
                INSERT INTO stock_data (datetime, open, high, low, close, volume, instrument)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            inserted_count = 0
            for index, row in df.iterrows():
                try:
                    cursor.execute(insert_query, (
                        row['datetime'],
                        float(row['open']),
                        float(row['high']),
                        float(row['low']),
                        float(row['close']),
                        int(row['volume']),
                        str(row['instrument'])
                    ))
                    inserted_count += 1
                    
                    # Commit every 100 rows to avoid large transactions
                    if inserted_count % 100 == 0:
                        connection.commit()
                        print(f"Inserted {inserted_count} rows...")
                        
                except Exception as e:
                    print(f"Error inserting row {index}: {str(e)}")
                    print(f"Row data: {row.to_dict()}")
                    connection.rollback()
                    continue
            
            # Final commit
            connection.commit()
            cursor.close()
            connection.close()
            
            print(f"Successfully inserted {inserted_count} new HINDALCO records")
            return inserted_count
            
        except Exception as e:
            print(f"Error inserting data: {str(e)}")
            try:
                connection.rollback()
                connection.close()
            except:
                pass
            return 0
    
    def run_import(self, csv_file_path):
        """Main method to run the complete import process"""
        print("Starting HINDALCO data import...")
        
        # Load data
        df = self.load_csv_data(csv_file_path)
        if df is None:
            return False
        
        # Clean data
        df = self.clean_data(df)
        if df is None:
            return False
        
        # Insert data
        inserted_count = self.insert_data(df)
        
        print(f"Import completed. Inserted {inserted_count} HINDALCO records.")
        return True

if __name__ == "__main__":
    # Path to your HINDALCO CSV file
    csv_file_path = "HINDALCO_1D.xlsx - HINDALCO.csv"
    
    importer = LocalDataImporter()
    success = importer.run_import(csv_file_path)
    
    if success:
        print("HINDALCO data import successful!")
    else:
        print("HINDALCO data import failed!") 