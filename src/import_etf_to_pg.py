import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import os

# Configuration
excel_path = r'd:\sourcecode\etf\resources\ETF分类汇总_202603267.xlsx'
sheet_name = 'ETF汇总表'
db_url = 'postgresql://postgres:Zmx1018$@67.216.207.73:5432/postgres'
table_name = 'etf_summary'

# Column Mapping
column_mapping = {
    '基金交易代码': 'fund_trade_code',
    '基金中文全称': 'fund_name_cn',
    'ETF扩位简称': 'etf_expanded_name',
    'cname': 'cname',
    'ETF基准指数代码': 'benchmark_index_code',
    'index_name': 'index_name',
    'setup_date': 'setup_date',
    'list_date': 'list_date',
    'list_status': 'list_status',
    'exchange': 'exchange',
    'mgr_name': 'mgr_name',
    'custod_name': 'custod_name',
    'mgt_fee': 'mgt_fee',
    '基金投资通道类型（境内、QDII）': 'investment_channel_type',
    'ETF基准指数中文全称': 'benchmark_index_name_cn',
    '存续状态（L上市 D退市 P待上市）': 'existence_status',
    '基金管理人简称': 'mgr_short_name',
    '基金管理人收取的费用': 'mgr_fee',
    '一级分类': 'primary_category',
    '二级分类': 'secondary_category'
}

def main():
    print("Loading Excel data...")
    # Read the Excel file
    try:
        df = pd.read_excel(excel_path, sheet_name=sheet_name)
    except FileNotFoundError:
        print(f"Error: Could not find Excel file at {excel_path}")
        return
    except ValueError as e:
        print(f"Error reading sheet '{sheet_name}': {e}")
        return

    # Keep only the columns we mapped
    columns_to_keep = [col for col in df.columns if col in column_mapping]
    df = df[columns_to_keep]

    # Rename the columns
    df.rename(columns=column_mapping, inplace=True)

    print("Cleaning data...")
    # Convert setup_date and list_date to standard datetime format if they are numeric
    for date_col in ['setup_date', 'list_date']:
        if date_col in df.columns:
            # Drop the decimal point e.g., 20130517.0 -> "20130517"
            # It might contain NaN, so replace NaN with None and handle the rest
            df[date_col] = df[date_col].fillna(0).astype('int64').astype(str)
            # Remove zeros
            df[date_col] = df[date_col].replace('0', np.nan)
            df[date_col] = pd.to_datetime(df[date_col], format='%Y%m%d', errors='coerce').dt.date

    # Converting numeric columns where appropriate
    for num_col in ['mgt_fee', 'mgr_fee']:
        if num_col in df.columns:
            df[num_col] = pd.to_numeric(df[num_col], errors='coerce')

    print("Connecting to PostgreSQL database...")
    # Use standard SQLAlchemy core types if needed but pandas `to_sql` derives them nicely
    try:
        engine = create_engine(db_url)
        print("Importing data to PostgreSQL table...")
        df.to_sql(table_name, engine, if_exists='replace', index=False, method='multi', chunksize=1000)
        print("Data imported successfully!")
    except Exception as e:
        print(f"Database error: {e}")

if __name__ == "__main__":
    main()
