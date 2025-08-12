#!/usr/bin/env python3
"""
Parse bulk_products_data.jsonl into a pandas DataFrame and display as a table.
"""

import json
import pandas as pd
from pathlib import Path

def parse_jsonl_to_dataframe(file_path):
    """Parse JSONL file into a pandas DataFrame."""
    data = []
    
    with open(file_path, 'r') as file:
        for line_num, line in enumerate(file, 1):
            try:
                # Parse each line as JSON
                record = json.loads(line.strip())
                data.append(record)
            except json.JSONDecodeError as e:
                print(f"Error parsing line {line_num}: {e}")
                continue
    
    return pd.DataFrame(data)

def main():
    # File path
    jsonl_file = "bulk_products_data.jsonl"
    
    # Check if file exists
    if not Path(jsonl_file).exists():
        print(f"Error: {jsonl_file} not found!")
        return
    
    print(f"Parsing {jsonl_file}...")
    
    # Parse JSONL to DataFrame
    df = parse_jsonl_to_dataframe(jsonl_file)
    
    print(f"Successfully parsed {len(df)} records")
    print(f"DataFrame shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    print("\n" + "="*80)
    
    # Derive entity type from the Shopify GID
    if "id" in df.columns:
        df["entityType"] = df["id"].astype(str).str.extract(r"gid:\/\/shopify\/([^\/]+)\/")
    
    # Flatten list-like columns for display (e.g., tags)
    if "tags" in df.columns:
        df["tags"] = df["tags"].apply(lambda x: ", ".join(x) if isinstance(x, list) else x)
    
    # Order a readable set of columns if present
    preferred_cols = [
        "entityType", "id", "__parentId", "title", "sku", "price", "compareAtPrice",
        "inventoryQuantity", "handle", "vendor", "productType", "tags", "status",
        "createdAt", "updatedAt", "url", "altText"
    ]
    existing_cols = [c for c in preferred_cols if c in df.columns]
    ordered_df = df[existing_cols + [c for c in df.columns if c not in existing_cols]]
    
    # Display first 20 rows
    print("First 20 rows of the parsed data:")
    print("="*80)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.max_colwidth", 100)
    pd.set_option("display.width", None)
    
    print(ordered_df.head(20))
    
    # Display summary statistics
    print("\n" + "="*80)
    print("Summary by entity type:")
    if "entityType" in ordered_df.columns:
        print(ordered_df["entityType"].value_counts())
    
    print("\n" + "="*80)
    print("Data types:")
    print(ordered_df.dtypes)
    
    return ordered_df

if __name__ == "__main__":
    df = main()
