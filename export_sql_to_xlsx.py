#!/usr/bin/env python3
# pip install pandas pymysql sqlalchemy openpyxl python-dotenv
"""
MySQL to XLSX Export Script

This script reads a SQL query from a file and exports the results to an XLSX file.
Database configuration is handled via .env file.
"""

import pandas as pd
import pymysql
import sqlalchemy as sa
import os
import argparse
from datetime import datetime
import sys
from dotenv import load_dotenv

load_dotenv()

def read_sql_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            query = file.read().strip()
        
        if not query:
            raise ValueError("SQL file is empty")
        
        print(f"Successfully loaded SQL query from: {file_path}")
        return query
    
    except FileNotFoundError:
        print(f"Error: SQL file '{file_path}' not found")
        return None
    except Exception as e:
        print(f"Error reading SQL file: {str(e)}")
        return None

def mysql_to_xlsx(query, output_file=None):
    try:
        engine = sa.create_engine(os.getenv("MYSQL_ENGINE"))
        print("Connecting to MySQL database...")
        
        df = pd.read_sql(query, engine)
        
        print(f"Successfully fetched {len(df)} rows and {len(df.columns)} columns")
        
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"mysql_export_{timestamp}.xlsx"
        
        df.to_excel(output_file, index=False, engine='openpyxl', startrow=1, freeze_panes=(2,0))
        
        print(f"Data successfully exported to: {output_file}")
        engine.dispose()
        
        return df
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return None

def parse_arguments():
    """
    Parse command line arguments
    """
    parser = argparse.ArgumentParser(
        description='Export MySQL query results to XLSX file',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python mysql_to_xlsx.py query.sql
  python mysql_to_xlsx.py reports/sales_report.sql --output monthly_sales.xlsx
"""
    )
    
    parser.add_argument('sql_file', help='Path to SQL file containing the query')
    parser.add_argument('-o', '--output', 
                       help='Output XLSX filename (default: auto-generated with timestamp)')
    
    return parser.parse_args()

def main():
    args = parse_arguments()
    
    sql_query = read_sql_file(args.sql_file)
    if sql_query is None:
        sys.exit(1)
    
    output_filename = args.output
    if output_filename is None:
        base_name = os.path.splitext(os.path.basename(args.sql_file))[0]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"{base_name}_export_{timestamp}.xlsx"
    
    print(f"Output file: {output_filename}")
    print("-" * 50)
    
    df = mysql_to_xlsx(
        query=sql_query,
        output_file=output_filename
    )
    
    if df is not None:
        print("\nFirst 5 rows of the exported data:")
        print(df.head())
        print(f"\nExport completed successfully!")
    else:
        print("Export failed!")
        sys.exit(1)

if __name__ == "__main__":
    
    main()

