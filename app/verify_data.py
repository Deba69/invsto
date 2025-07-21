import psycopg2
import os
import sys
from datetime import datetime
from typing import Dict, List, Tuple
import logging
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataVerifier:
    def __init__(self):
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
    
    def basic_statistics(self) -> Dict:
        """Get basic statistics about the data"""
        try:
            self.cursor.execute("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT instrument) as unique_instruments,
                    MIN(datetime) as earliest_date,
                    MAX(datetime) as latest_date,
                    COUNT(DISTINCT DATE(datetime)) as unique_dates
                FROM stock_data
            """)
            result = self.cursor.fetchone()
            
            return {
                'total_records': result[0],
                'unique_instruments': result[1],
                'earliest_date': result[2],
                'latest_date': result[3],
                'unique_dates': result[4]
            }
        except Exception as e:
            logger.error(f"Error getting basic statistics: {e}")
            return {}
    
    def check_duplicates(self) -> List[Tuple]:
        """Check for duplicate records"""
        try:
            self.cursor.execute("""
                SELECT datetime, instrument, COUNT(*) as count
                FROM stock_data 
                GROUP BY datetime, instrument 
                HAVING COUNT(*) > 1
                ORDER BY count DESC
            """)
            return self.cursor.fetchall()
        except Exception as e:
            logger.error(f"Error checking duplicates: {e}")
            return []
    
    def check_null_values(self) -> Dict:
        """Check for null values in each column"""
        try:
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
            result = self.cursor.fetchone()
            
            return {
                'datetime': result[0],
                'close': result[1],
                'high': result[2],
                'low': result[3],
                'open': result[4],
                'volume': result[5],
                'instrument': result[6]
            }
        except Exception as e:
            logger.error(f"Error checking null values: {e}")
            return {}
    
    def check_data_ranges(self) -> Dict:
        """Check data ranges and outliers"""
        try:
            self.cursor.execute("""
                SELECT 
                    instrument,
                    MIN(close) as min_close,
                    MAX(close) as max_close,
                    AVG(close) as avg_close,
                    MIN(volume) as min_volume,
                    MAX(volume) as max_volume,
                    AVG(volume) as avg_volume,
                    COUNT(*) as record_count
                FROM stock_data
                GROUP BY instrument
                ORDER BY instrument
            """)
            results = self.cursor.fetchall()
            
            ranges = {}
            for row in results:
                ranges[row[0]] = {
                    'price_range': (row[1], row[2]),
                    'avg_price': row[3],
                    'volume_range': (row[4], row[5]),
                    'avg_volume': row[6],
                    'record_count': row[7]
                }
            
            return ranges
        except Exception as e:
            logger.error(f"Error checking data ranges: {e}")
            return {}
    
    def check_data_consistency(self) -> List[Dict]:
        """Check for data consistency issues"""
        issues = []
        
        try:
            # Check for negative prices
            self.cursor.execute("""
                SELECT COUNT(*) FROM stock_data 
                WHERE close < 0 OR high < 0 OR low < 0 OR open < 0
            """)
            negative_prices = self.cursor.fetchone()[0]
            if negative_prices > 0:
                issues.append({
                    'type': 'negative_prices',
                    'count': negative_prices,
                    'description': 'Found records with negative prices'
                })
            
            # Check for negative volumes
            self.cursor.execute("""
                SELECT COUNT(*) FROM stock_data WHERE volume < 0
            """)
            negative_volumes = self.cursor.fetchone()[0]
            if negative_volumes > 0:
                issues.append({
                    'type': 'negative_volumes',
                    'count': negative_volumes,
                    'description': 'Found records with negative volumes'
                })
            
            # Check for invalid OHLC relationships
            self.cursor.execute("""
                SELECT COUNT(*) FROM stock_data 
                WHERE high < low OR high < open OR high < close OR
                      low > open OR low > close OR
                      open < low OR open > high OR
                      close < low OR close > high
            """)
            invalid_ohlc = self.cursor.fetchone()[0]
            if invalid_ohlc > 0:
                issues.append({
                    'type': 'invalid_ohlc',
                    'count': invalid_ohlc,
                    'description': 'Found records with invalid OHLC relationships'
                })
            
            # Check for future dates
            self.cursor.execute("""
                SELECT COUNT(*) FROM stock_data 
                WHERE datetime > CURRENT_TIMESTAMP
            """)
            future_dates = self.cursor.fetchone()[0]
            if future_dates > 0:
                issues.append({
                    'type': 'future_dates',
                    'count': future_dates,
                    'description': 'Found records with future dates'
                })
            
            return issues
            
        except Exception as e:
            logger.error(f"Error checking data consistency: {e}")
            return []
    
    def check_data_completeness(self) -> Dict:
        """Check data completeness by instrument and date"""
        try:
            self.cursor.execute("""
                SELECT 
                    instrument,
                    COUNT(*) as total_records,
                    COUNT(DISTINCT DATE(datetime)) as unique_dates,
                    MIN(datetime) as first_date,
                    MAX(datetime) as last_date
                FROM stock_data
                GROUP BY instrument
                ORDER BY instrument
            """)
            results = self.cursor.fetchall()
            
            completeness = {}
            for row in results:
                instrument, total_records, unique_dates, first_date, last_date = row
                
                # Calculate expected records (assuming daily data)
                if first_date and last_date:
                    date_diff = (last_date - first_date).days + 1
                    completeness[instrument] = {
                        'total_records': total_records,
                        'unique_dates': unique_dates,
                        'expected_records': date_diff,
                        'completeness_percentage': round((total_records / date_diff) * 100, 2) if date_diff > 0 else 0,
                        'first_date': first_date,
                        'last_date': last_date
                    }
            
            return completeness
            
        except Exception as e:
            logger.error(f"Error checking data completeness: {e}")
            return {}
    
    def generate_report(self) -> Dict:
        """Generate comprehensive data verification report"""
        logger.info("Starting data verification...")
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'basic_statistics': self.basic_statistics(),
            'null_values': self.check_null_values(),
            'duplicates': self.check_duplicates(),
            'data_ranges': self.check_data_ranges(),
            'consistency_issues': self.check_data_consistency(),
            'completeness': self.check_data_completeness()
        }
        
        return report
    
    def print_report(self, report: Dict):
        """Print formatted verification report"""
        print("\n" + "="*60)
        print("DATA VERIFICATION REPORT")
        print("="*60)
        
        # Basic Statistics
        stats = report.get('basic_statistics', {})
        if stats:
            print(f"\n📊 BASIC STATISTICS:")
            print(f"   Total Records: {stats.get('total_records', 0):,}")
            print(f"   Unique Instruments: {stats.get('unique_instruments', 0)}")
            print(f"   Date Range: {stats.get('earliest_date')} to {stats.get('latest_date')}")
            print(f"   Unique Dates: {stats.get('unique_dates', 0)}")
        
        # Null Values
        nulls = report.get('null_values', {})
        if any(nulls.values()):
            print(f"\n⚠️  NULL VALUES FOUND:")
            for field, count in nulls.items():
                if count > 0:
                    print(f"   {field}: {count}")
        else:
            print(f"\n✅ NO NULL VALUES FOUND")
        
        # Duplicates
        duplicates = report.get('duplicates', [])
        if duplicates:
            print(f"\n⚠️  DUPLICATE RECORDS FOUND: {len(duplicates)}")
            for dt, instrument, count in duplicates[:5]:  # Show first 5
                print(f"   {dt} - {instrument}: {count} records")
        else:
            print(f"\n✅ NO DUPLICATE RECORDS FOUND")
        
        # Consistency Issues
        issues = report.get('consistency_issues', [])
        if issues:
            print(f"\n⚠️  DATA CONSISTENCY ISSUES:")
            for issue in issues:
                print(f"   {issue['description']}: {issue['count']} records")
        else:
            print(f"\n✅ NO DATA CONSISTENCY ISSUES FOUND")
        
        # Data Ranges
        ranges = report.get('data_ranges', {})
        if ranges:
            print(f"\n📈 DATA RANGES BY INSTRUMENT:")
            for instrument, data in ranges.items():
                print(f"   {instrument}:")
                print(f"     Price Range: {data['price_range'][0]:.2f} - {data['price_range'][1]:.2f}")
                print(f"     Volume Range: {data['volume_range'][0]:,} - {data['volume_range'][1]:,}")
                print(f"     Records: {data['record_count']}")
        
        # Completeness
        completeness = report.get('completeness', {})
        if completeness:
            print(f"\n📅 DATA COMPLETENESS:")
            for instrument, data in completeness.items():
                print(f"   {instrument}: {data['completeness_percentage']}% complete "
                      f"({data['total_records']}/{data['expected_records']} records)")
        
        print("\n" + "="*60)
        print("VERIFICATION COMPLETED")
        print("="*60)
    
    def save_report(self, report: Dict, filename: str = "data_verification_report.json"):
        """Save verification report to JSON file"""
        try:
            with open(filename, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            logger.info(f"Report saved to {filename}")
        except Exception as e:
            logger.error(f"Error saving report: {e}")
    
    def run_verification(self) -> bool:
        """Run complete data verification process"""
        if not self.connect_to_database():
            return False
        
        try:
            report = self.generate_report()
            self.print_report(report)
            self.save_report(report)
            
            # Check if there are any critical issues
            critical_issues = (
                any(report.get('null_values', {}).values()) or
                report.get('duplicates', []) or
                report.get('consistency_issues', [])
            )
            
            if critical_issues:
                logger.warning("Data verification completed with issues found")
                return False
            else:
                logger.info("Data verification completed successfully - no issues found")
                return True
                
        except Exception as e:
            logger.error(f"Verification process failed: {e}")
            return False
        finally:
            self.close_connection()

def main():
    """Main function"""
    verifier = DataVerifier()
    success = verifier.run_verification()
    
    if success:
        logger.info("Data verification completed successfully!")
        sys.exit(0)
    else:
        logger.warning("Data verification completed with issues!")
        sys.exit(1)

if __name__ == "__main__":
    main() 