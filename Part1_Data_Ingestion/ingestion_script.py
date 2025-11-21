#!/usr/bin/env python3
"""
Part 1: Data Ingestion Script for REWDP Dataset
Bronze Layer: Raw Data Ingestion

This script ingests the REWDP (Residential Energy and Weather Data Pakistan) dataset
and stores it in the Bronze layer with metadata.
"""

import json
import os
from datetime import datetime
from pathlib import Path

def ingest_rewdp_data(source_file, output_dir):
    """
    Ingest REWDP dataset and add metadata for Bronze layer.
    
    Args:
        source_file: Path to source data file (CSV or JSON)
        output_dir: Directory to save Bronze layer data
    """
    # Create output directory
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Metadata for ingestion
    ingestion_metadata = {
        'ingestion_timestamp': datetime.now().isoformat(),
        'source': 'LUMS Energy Institute - REWDP Dataset',
        'dataset': 'Residential Energy and Weather Data Pakistan',
        'description': 'Energy consumption and weather data from 60 households across 6 urban centers',
        'status': 'raw',
        'version': '1.0'
    }
    
    # Read source file (adjust based on actual format)
    bronze_records = []
    
    # If CSV format
    if source_file.endswith('.csv'):
        import csv
        with open(source_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Add ingestion metadata to each record
                bronze_record = {
                    **row,
                    '_ingestion_metadata': ingestion_metadata
                }
                bronze_records.append(bronze_record)
    
    # If JSON format
    elif source_file.endswith('.json') or source_file.endswith('.jsonl'):
        with open(source_file, 'r', encoding='utf-8') as f:
            if source_file.endswith('.jsonl'):
                # JSON Lines format
                for line in f:
                    record = json.loads(line.strip())
                    bronze_record = {
                        **record,
                        '_ingestion_metadata': ingestion_metadata
                    }
                    bronze_records.append(bronze_record)
            else:
                # Single JSON file
                data = json.load(f)
                if isinstance(data, list):
                    for record in data:
                        bronze_record = {
                            **record,
                            '_ingestion_metadata': ingestion_metadata
                        }
                        bronze_records.append(bronze_record)
                else:
                    bronze_record = {
                        **data,
                        '_ingestion_metadata': ingestion_metadata
                    }
                    bronze_records.append(bronze_record)
    
    # Save to Bronze layer as JSON Lines (one JSON object per line)
    output_file = os.path.join(output_dir, 'rewdp_bronze.jsonl')
    with open(output_file, 'w', encoding='utf-8') as f:
        for record in bronze_records:
            f.write(json.dumps(record) + '\n')
    
    print(f"✓ Ingested {len(bronze_records)} records to Bronze layer")
    print(f"✓ Output file: {output_file}")
    
    # Save metadata summary
    metadata_file = os.path.join(output_dir, 'ingestion_metadata.json')
    with open(metadata_file, 'w', encoding='utf-8') as f:
        json.dump({
            **ingestion_metadata,
            'total_records': len(bronze_records),
            'output_file': output_file
        }, f, indent=2)
    
    print(f"✓ Metadata saved to: {metadata_file}")
    return output_file

def ingest_all_csv_files(source_dir, output_dir):
    """
    Ingest all CSV files from rewdp_dataset directory structure.
    """
    import glob
    
    # Create output directory
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Metadata
    ingestion_metadata = {
        'ingestion_timestamp': datetime.now().isoformat(),
        'source': 'LUMS Energy Institute - REWDP Dataset',
        'dataset': 'Residential Energy and Weather Data Pakistan',
        'status': 'raw',
        'version': '1.0'
    }
    
    # Find all CSV files
    csv_files = glob.glob(os.path.join(source_dir, "**/*.csv"), recursive=True)
    
    if not csv_files:
        print(f"⚠️  No CSV files found in {source_dir}")
        return
    
    print(f"Found {len(csv_files)} CSV files")
    
    # Process all CSV files
    bronze_records = []
    for csv_file in csv_files:
        city = os.path.basename(os.path.dirname(csv_file))
        filename = os.path.basename(csv_file)
        
        print(f"Processing: {city}/{filename}")
        
        import csv
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Add city and metadata
                bronze_record = {
                    **row,
                    'city': city,
                    'source_file': filename,
                    '_ingestion_metadata': ingestion_metadata
                }
                bronze_records.append(bronze_record)
    
    # Save as JSON Lines
    output_file = os.path.join(output_dir, 'rewdp_bronze.jsonl')
    with open(output_file, 'w', encoding='utf-8') as f:
        for record in bronze_records:
            f.write(json.dumps(record) + '\n')
    
    print(f"\n✓ Ingested {len(bronze_records)} records")
    print(f"✓ Output: {output_file}")
    
    # Save metadata
    metadata_file = os.path.join(output_dir, 'ingestion_metadata.json')
    with open(metadata_file, 'w', encoding='utf-8') as f:
        json.dump({
            **ingestion_metadata,
            'total_records': len(bronze_records),
            'files_processed': len(csv_files),
            'output_file': output_file
        }, f, indent=2)
    
    return output_file

if __name__ == "__main__":
    # Configuration - Update path to your rewdp_dataset folder
    SOURCE_DIR = "../rewdp_dataset"  # Path to rewdp_dataset folder
    BRONZE_OUTPUT_DIR = "raw_data"
    
    print("=== REWDP Dataset Ingestion ===")
    print(f"Source directory: {SOURCE_DIR}")
    print(f"Output: {BRONZE_OUTPUT_DIR}/")
    
    if not os.path.exists(SOURCE_DIR):
        print(f"⚠️  Warning: Source directory '{SOURCE_DIR}' not found.")
        print("Update SOURCE_DIR path in this script to point to rewdp_dataset/")
    else:
        ingest_all_csv_files(SOURCE_DIR, BRONZE_OUTPUT_DIR)
        print("\n✓ Bronze layer ingestion complete!")
        print("Next step: Run Part 2 (Data Cleaning) notebook")

