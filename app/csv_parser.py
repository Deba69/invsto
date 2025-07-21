import csv
import os
import sys
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CSVParser:
    def __init__(self, csv_file_path: str):
        self.csv_file_path = csv_file_path
        self.parsed_data = []
        self.invalid_rows = []
        self.statistics = {}
    
    def validate_file_exists(self) -> bool:
        """Check if CSV file exists"""
        if not os.path.exists(self.csv_file_path):
            logger.error(f"CSV file not found: {self.csv_file_path}")
            return False
        return True
    
    def validate_header(self, header: List[str]) -> bool:
        """Validate CSV header structure"""
        expected_columns = ['datetime', 'close', 'high', 'low', 'open', 'volume', 'instrument']
        
        if header != expected_columns:
            logger.error(f"Invalid CSV structure. Expected: {expected_columns}, Got: {header}")
            return False
        
        logger.info("CSV header validation passed")
        return True
    
    def validate_datetime(self, datetime_str: str) -> Optional[datetime]:
        """Validate and parse datetime string"""
        try:
            return datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            return None
    
    def validate_numeric(self, value: str, field_name: str) -> Optional[float]:
        """Validate numeric field"""
        try:
            num_value = float(value)
            if num_value < 0:
                logger.warning(f"Negative value found in {field_name}: {num_value}")
            return num_value
        except ValueError:
            return None
    
    def validate_integer(self, value: str, field_name: str) -> Optional[int]:
        """Validate integer field"""
        try:
            int_value = int(value)
            if int_value < 0:
                logger.warning(f"Negative value found in {field_name}: {int_value}")
            return int_value
        except ValueError:
            return None
    
    def validate_row(self, row: Dict[str, str], row_number: int) -> Tuple[bool, str, Optional[Tuple]]:
        """Validate individual row data"""
        try:
            # Validate datetime
            dt = self.validate_datetime(row['datetime'])
            if not dt:
                return False, f"Invalid datetime format: {row['datetime']}", None
            
            # Validate numeric fields
            close = self.validate_numeric(row['close'], 'close')
            if close is None:
                return False, f"Invalid close price: {row['close']}", None
            
            high = self.validate_numeric(row['high'], 'high')
            if high is None:
                return False, f"Invalid high price: {row['high']}", None
            
            low = self.validate_numeric(row['low'], 'low')
            if low is None:
                return False, f"Invalid low price: {row['low']}", None
            
            open_price = self.validate_numeric(row['open'], 'open')
            if open_price is None:
                return False, f"Invalid open price: {row['open']}", None
            
            volume = self.validate_integer(row['volume'], 'volume')
            if volume is None:
                return False, f"Invalid volume: {row['volume']}", None
            
            # Validate instrument
            instrument = row['instrument'].strip()
            if not instrument:
                return False, "Empty instrument name", None
            
            # Validate OHLC relationships
            if high < low:
                return False, f"High price ({high}) is less than low price ({low})", None
            
            if high < open_price or high < close:
                return False, f"High price ({high}) is less than open ({open_price}) or close ({close})", None
            
            if low > open_price or low > close:
                return False, f"Low price ({low}) is greater than open ({open_price}) or close ({close})", None
            
            # Create parsed row tuple
            parsed_row = (dt, close, high, low, open_price, volume, instrument)
            return True, "", parsed_row
            
        except Exception as e:
            return False, f"Unexpected error: {e}", None
    
    def parse_csv(self) -> bool:
        """Parse CSV file and validate all rows"""
        if not self.validate_file_exists():
            return False
        
        try:
            with open(self.csv_file_path, 'r', encoding='utf-8') as file:
                csv_reader = csv.DictReader(file)
                
                # Validate header
                if not self.validate_header(csv_reader.fieldnames):
                    return False
                
                # Parse rows
                for row_number, row in enumerate(csv_reader, start=2):  # Start from 2 (header is row 1)
                    is_valid, error_msg, parsed_row = self.validate_row(row, row_number)
                    
                    if is_valid and parsed_row:
                        self.parsed_data.append(parsed_row)
                    else:
                        self.invalid_rows.append((row_number, error_msg))
                
                # Generate statistics
                self.generate_statistics()
                
                logger.info(f"CSV parsing completed:")
                logger.info(f"  Valid rows: {len(self.parsed_data)}")
                logger.info(f"  Invalid rows: {len(self.invalid_rows)}")
                
                return True
                
        except Exception as e:
            logger.error(f"Error parsing CSV file: {e}")
            return False
    
    def generate_statistics(self):
        """Generate statistics about the parsed data"""
        if not self.parsed_data:
            return
        
        # Extract data for analysis
        dates = [row[0] for row in self.parsed_data]
        prices = [row[1] for row in self.parsed_data]  # close prices
        volumes = [row[5] for row in self.parsed_data]
        instruments = [row[6] for row in self.parsed_data]
        
        # Calculate statistics
        self.statistics = {
            'total_rows': len(self.parsed_data),
            'invalid_rows': len(self.invalid_rows),
            'date_range': (min(dates), max(dates)),
            'price_range': (min(prices), max(prices)),
            'volume_range': (min(volumes), max(volumes)),
            'unique_instruments': len(set(instruments)),
            'instruments': list(set(instruments)),
            'unique_dates': len(set(date.date() for date in dates))
        }
    
    def print_statistics(self):
        """Print parsed data statistics"""
        if not self.statistics:
            logger.warning("No statistics available. Run parse_csv() first.")
            return
        
        print("\n" + "="*50)
        print("CSV PARSING STATISTICS")
        print("="*50)
        
        print(f"📊 Total Rows: {self.statistics['total_rows']:,}")
        print(f"❌ Invalid Rows: {self.statistics['invalid_rows']:,}")
        print(f"✅ Success Rate: {((self.statistics['total_rows'] / (self.statistics['total_rows'] + self.statistics['invalid_rows'])) * 100):.2f}%")
        
        print(f"\n📅 Date Range:")
        print(f"   From: {self.statistics['date_range'][0]}")
        print(f"   To: {self.statistics['date_range'][1]}")
        print(f"   Unique Dates: {self.statistics['unique_dates']}")
        
        print(f"\n💰 Price Range:")
        print(f"   Min: {self.statistics['price_range'][0]:.2f}")
        print(f"   Max: {self.statistics['price_range'][1]:.2f}")
        
        print(f"\n📈 Volume Range:")
        print(f"   Min: {self.statistics['volume_range'][0]:,}")
        print(f"   Max: {self.statistics['volume_range'][1]:,}")
        
        print(f"\n🏢 Instruments:")
        print(f"   Count: {self.statistics['unique_instruments']}")
        print(f"   Names: {', '.join(self.statistics['instruments'])}")
        
        if self.invalid_rows:
            print(f"\n⚠️  First 5 Invalid Rows:")
            for row_num, error in self.invalid_rows[:5]:
                print(f"   Row {row_num}: {error}")
        
        print("="*50)
    
    def get_parsed_data(self) -> List[Tuple]:
        """Get parsed and validated data"""
        return self.parsed_data
    
    def get_invalid_rows(self) -> List[Tuple]:
        """Get list of invalid rows with error messages"""
        return self.invalid_rows
    
    def save_invalid_rows_report(self, filename: str = "invalid_rows_report.txt"):
        """Save invalid rows report to file"""
        try:
            with open(filename, 'w') as f:
                f.write("INVALID ROWS REPORT\n")
                f.write("=" * 50 + "\n\n")
                f.write(f"Total invalid rows: {len(self.invalid_rows)}\n\n")
                
                for row_num, error in self.invalid_rows:
                    f.write(f"Row {row_num}: {error}\n")
            
            logger.info(f"Invalid rows report saved to {filename}")
        except Exception as e:
            logger.error(f"Error saving invalid rows report: {e}")

def main():
    """Main function for CSV parsing"""
    if len(sys.argv) != 2:
        print("Usage: python csv_parser.py <csv_file_path>")
        sys.exit(1)
    
    csv_file = sys.argv[1]
    parser = CSVParser(csv_file)
    
    if parser.parse_csv():
        parser.print_statistics()
        parser.save_invalid_rows_report()
        
        if parser.invalid_rows:
            logger.warning(f"Found {len(parser.invalid_rows)} invalid rows")
            sys.exit(1)
        else:
            logger.info("CSV parsing completed successfully!")
            sys.exit(0)
    else:
        logger.error("CSV parsing failed!")
        sys.exit(1)

if __name__ == "__main__":
    main() 