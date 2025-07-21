import csv
import psycopg2
import os
import sys
from datetime import datetime
from typing import Dict, List, Tuple
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('import.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class StockDataImporter:
    def __init__(self, csv_file_path: str):
        self.csv_file_path = csv_file_path
        self.db_params = {
            'host': os.getenv("POSTGRES_HOST", "localhost"),
            'database': os.getenv("POSTGRES_DB", "invsto_db"),
            'user': os.getenv("POSTGRES_USER", "invsto_user"),
            'password': os.getenv("POSTGRES_PASSWORD", "invsto_password"),
            'port': os.getenv("POSTGRES_PORT", "5432")
        }
        self.connection = None
        self.cursor = None
        
    def connect_to_database(self) -> bool:
        """Establish database connection"""
        try:
            self.connection = psycopg2.connect(**self.db_params)
            self.cursor = self.connection.cursor()
            logger.info("Successfully connected to PostgreSQL database")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            return False
    
    def close_connection(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")
    
    def validate_csv_structure(self, header: List[str]) -> bool:
        """Validate CSV header structure"""
        expected_columns = ['datetime', 'close', 'high', 'low', 'open', 'volume', 'instrument']
        if header != expected_columns:
            logger.error(f"Invalid CSV structure. Expected: {expected_columns}, Got: {header}")
            return False
        logger.info("CSV structure validation passed")
        return True
    
    def validate_row_data(self, row: Dict[str, str]) -> Tuple[bool, str]:
        """Validate individual row data"""
        try:
            # Validate datetime
            datetime.strptime(row['datetime'], '%Y-%m-%d %H:%M:%S')
            
            # Validate numeric fields
            float(row['close'])
            float(row['high'])
            float(row['low'])
            float(row['open'])
            int(row['volume'])
            
            # Validate instrument (non-empty string)
            if not row['instrument'].strip():
                return False, "Empty instrument name"
            
            return True, ""
        except ValueError as e:
            return False, f"Data validation error: {e}"
        except Exception as e:
            return False, f"Unexpected error: {e}"
    
    def check_existing_data(self) -> int:
        """Check if data already exists in the database"""
        try:
            self.cursor.execute("SELECT COUNT(*) FROM stock_data")
            count = self.cursor.fetchone()[0]
            logger.info(f"Database contains {count} existing records")
            return count
        except Exception as e:
            logger.error(f"Error checking existing data: {e}")
            return 0
    
    def parse_csv_data(self) -> List[Tuple]:
        """Parse CSV file and return validated data"""
        parsed_data = []
        invalid_rows = []
        
        try:
            with open(self.csv_file_path, 'r', encoding='utf-8') as file:
                csv_reader = csv.DictReader(file)
                
                # Validate header
                if not self.validate_csv_structure(csv_reader.fieldnames):
                    return []
                
                for row_num, row in enumerate(csv_reader, start=2):  # Start from 2 (header is row 1)
                    # Validate row data
                    is_valid, error_msg = self.validate_row_data(row)
                    
                    if is_valid:
                        try:
                            # Parse and prepare data for insertion
                            parsed_row = (
                                datetime.strptime(row['datetime'], '%Y-%m-%d %H:%M:%S'),
                                float(row['close']),
                                float(row['high']),
                                float(row['low']),
                                float(row['open']),
                                int(row['volume']),
                                row['instrument'].strip()
                            )
                            parsed_data.append(parsed_row)
                        except Exception as e:
                            invalid_rows.append((row_num, f"Data parsing error: {e}"))
                    else:
                        invalid_rows.append((row_num, error_msg))
                
                logger.info(f"Parsed {len(parsed_data)} valid rows from CSV")
                if invalid_rows:
                    logger.warning(f"Found {len(invalid_rows)} invalid rows")
                    for row_num, error in invalid_rows[:10]:  # Log first 10 errors
                        logger.warning(f"Row {row_num}: {error}")
                
                return parsed_data
                
        except FileNotFoundError:
            logger.error(f"CSV file not found: {self.csv_file_path}")
            return []
        except Exception as e:
            logger.error(f"Error parsing CSV file: {e}")
            return []
    
    def insert_data_batch(self, data: List[Tuple]) -> int:
        """Insert data in batches for better performance"""
        if not data:
            return 0
        
        insert_query = """
            INSERT INTO stock_data (datetime, close, high, low, open, volume, instrument)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        batch_size = 1000
        total_inserted = 0
        
        try:
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                self.cursor.executemany(insert_query, batch)
                self.connection.commit()
                total_inserted += len(batch)
                logger.info(f"Inserted batch {i//batch_size + 1}: {len(batch)} records (Total: {total_inserted})")
            
            logger.info(f"Successfully inserted {total_inserted} records")
            return total_inserted
            
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error inserting data: {e}")
            return 0
    
    def verify_data_integrity(self) -> Dict[str, any]:
        """Verify data integrity after import"""
        verification_results = {}
        
        try:
            # Check total count
            self.cursor.execute("SELECT COUNT(*) FROM stock_data")
            total_count = self.cursor.fetchone()[0]
            verification_results['total_records'] = total_count
            
            # Check for duplicates
            self.cursor.execute("""
                SELECT datetime, instrument, COUNT(*) 
                FROM stock_data 
                GROUP BY datetime, instrument 
                HAVING COUNT(*) > 1
            """)
            duplicates = self.cursor.fetchall()
            verification_results['duplicate_records'] = len(duplicates)
            
            # Check data ranges
            self.cursor.execute("""
                SELECT 
                    MIN(datetime) as earliest_date,
                    MAX(datetime) as latest_date,
                    COUNT(DISTINCT instrument) as unique_instruments,
                    MIN(close) as min_close,
                    MAX(close) as max_close,
                    MIN(volume) as min_volume,
                    MAX(volume) as max_volume
                FROM stock_data
            """)
            ranges = self.cursor.fetchone()
            verification_results['data_ranges'] = {
                'earliest_date': ranges[0],
                'latest_date': ranges[1],
                'unique_instruments': ranges[2],
                'price_range': (ranges[3], ranges[4]),
                'volume_range': (ranges[5], ranges[6])
            }
            
            # Check for null values
            self.cursor.execute("""
                SELECT 
                    COUNT(*) FILTER (WHERE datetime IS NULL) as null_datetime,
                    COUNT(*) FILTER (WHERE close IS NULL) as null_close,
                    COUNT(*) FILTER (WHERE high IS NULL) as null_high,
                    COUNT(*) FILTER (WHERE low IS NULL) as null_low,
                    COUNT(*) FILTER (WHERE open IS NULL) as null_open,
                    COUNT(*) FILTER (WHERE volume IS NULL) as null_volume,
                    COUNT(*) FILTER (WHERE instrument IS NULL) as null_instrument
                FROM stock_data
            """)
            null_counts = self.cursor.fetchone()
            verification_results['null_values'] = {
                'datetime': null_counts[0],
                'close': null_counts[1],
                'high': null_counts[2],
                'low': null_counts[3],
                'open': null_counts[4],
                'volume': null_counts[5],
                'instrument': null_counts[6]
            }
            
            logger.info("Data integrity verification completed")
            return verification_results
            
        except Exception as e:
            logger.error(f"Error during data integrity verification: {e}")
            return {}
    
    def run_import(self) -> bool:
        """Main import process"""
        logger.info(f"Starting import process for: {self.csv_file_path}")
        
        # Check if file exists
        if not os.path.exists(self.csv_file_path):
            logger.error(f"CSV file not found: {self.csv_file_path}")
            return False
        
        # Connect to database
        if not self.connect_to_database():
            return False
        
        try:
            # Check existing data
            existing_count = self.check_existing_data()
            if existing_count > 0:
                logger.warning(f"Database already contains {existing_count} records")
                response = input("Do you want to continue and add more data? (y/N): ")
                if response.lower() != 'y':
                    logger.info("Import cancelled by user")
                    return False
            
            # Parse CSV data
            parsed_data = self.parse_csv_data()
            if not parsed_data:
                logger.error("No valid data found in CSV file")
                return False
            
            # Insert data
            inserted_count = self.insert_data_batch(parsed_data)
            if inserted_count == 0:
                logger.error("Failed to insert any data")
                return False
            
            # Verify data integrity
            verification_results = self.verify_data_integrity()
            
            # Log verification results
            logger.info("=== DATA INTEGRITY VERIFICATION RESULTS ===")
            logger.info(f"Total records: {verification_results.get('total_records', 0)}")
            logger.info(f"Duplicate records: {verification_results.get('duplicate_records', 0)}")
            
            data_ranges = verification_results.get('data_ranges', {})
            if data_ranges:
                logger.info(f"Date range: {data_ranges.get('earliest_date')} to {data_ranges.get('latest_date')}")
                logger.info(f"Unique instruments: {data_ranges.get('unique_instruments')}")
                logger.info(f"Price range: {data_ranges.get('price_range')}")
                logger.info(f"Volume range: {data_ranges.get('volume_range')}")
            
            null_values = verification_results.get('null_values', {})
            if any(null_values.values()):
                logger.warning("Found null values:")
                for field, count in null_values.items():
                    if count > 0:
                        logger.warning(f"  {field}: {count}")
            
            logger.info("=== IMPORT PROCESS COMPLETED SUCCESSFULLY ===")
            return True
            
        except Exception as e:
            logger.error(f"Import process failed: {e}")
            return False
        finally:
            self.close_connection()

def main():
    """Main function"""
    csv_file = "HINDALCO_1D.xlsx - HINDALCO.csv"
    
    if not os.path.exists(csv_file):
        logger.error(f"CSV file not found: {csv_file}")
        sys.exit(1)
    
    importer = StockDataImporter(csv_file)
    success = importer.run_import()
    
    if success:
        logger.info("Data import completed successfully!")
        sys.exit(0)
    else:
        logger.error("Data import failed!")
        sys.exit(1)

if __name__ == "__main__":
    main() 