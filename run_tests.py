#!/usr/bin/env python3
"""
Test runner script for the Invsto FastAPI application.
Runs all unit tests and generates coverage reports.
"""

import unittest
import coverage
import sys
import os

def run_tests_with_coverage():
    """Run tests with coverage measurement"""
    
    # Start coverage measurement
    cov = coverage.Coverage()
    cov.start()
    
    # Discover and run tests
    loader = unittest.TestLoader()
    start_dir = 'tests'
    suite = loader.discover(start_dir, pattern='test_*.py')
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Stop coverage measurement
    cov.stop()
    cov.save()
    
    # Generate coverage report
    print("\n" + "="*50)
    print("COVERAGE REPORT")
    print("="*50)
    
    # Print coverage summary
    cov.report()
    
    # Generate HTML coverage report
    cov.html_report(directory='htmlcov')
    print(f"\nHTML coverage report generated in 'htmlcov' directory")
    
    # Check if coverage is at least 80%
    total_coverage = cov.report()
    if total_coverage < 80:
        print(f"\n⚠️  WARNING: Coverage is {total_coverage:.1f}%, which is below the required 80%")
    else:
        print(f"\n✅ Coverage is {total_coverage:.1f}%, which meets the 80% requirement")
    
    return result.wasSuccessful()

def run_tests_without_coverage():
    """Run tests without coverage measurement"""
    loader = unittest.TestLoader()
    start_dir = 'tests'
    suite = loader.discover(start_dir, pattern='test_*.py')
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result.wasSuccessful()

if __name__ == '__main__':
    print("Running Invsto FastAPI Tests")
    print("="*50)
    
    # Check if coverage is requested
    if '--no-coverage' in sys.argv:
        success = run_tests_without_coverage()
    else:
        success = run_tests_with_coverage()
    
    if success:
        print("\n✅ All tests passed!")
        sys.exit(0)
    else:
        print("\n❌ Some tests failed!")
        sys.exit(1) 