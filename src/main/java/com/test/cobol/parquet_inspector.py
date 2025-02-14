import os
import pandas as pd
import pyarrow.parquet as pq
from fastparquet import ParquetFile

def inspect_parquet(file_path):
    try:
        # Load Parquet file
        parquet_file = pq.ParquetFile(file_path)
        
        print("\n📌 **Parquet File Info**")
        print(f"📂 File: {file_path}")
        print(f"📏 File Size: {os.path.getsize(file_path) / (1024*1024):.2f} MB")

        # Show schema
        print("\n📜 **Schema**")
        print(parquet_file.schema)

        # Show metadata
        print("\n📊 **Metadata**")
        print(parquet_file.metadata)

        # Read using fastparquet for quick insights
        pf = ParquetFile(file_path)
        row_count = pf.count
        col_count = len(pf.columns)

        print(f"\n🔢 **Dataset Stats:** {row_count} rows, {col_count} columns")

        # Show first few rows
        df = pf.to_pandas()
        print("\n👀 **First 5 Rows:**")
        print(df.head())

        # Show column-level statistics
        print("\n📈 **Column Statistics**")
        for col in df.columns:
            unique_vals = df[col].nunique()
            null_vals = df[col].isnull().sum()
            print(f"📌 {col}: Unique={unique_vals}, Nulls={null_vals}")

    except Exception as e:
        print(f"❌ Error reading file: {e}")

# Example usage
parquet_file_path = "your_file.parquet"  # Replace with your file path
inspect_parquet(parquet_file_path)
