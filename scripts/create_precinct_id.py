import os
import polars as pl
import re
import logging
from pathlib import Path

# Set up path handling
SCRIPT_DIR = Path(__file__).parent.absolute()
DATA_DIR = Path("output")  # Simplified - directly use the output directory
LOG_FILE = SCRIPT_DIR / 'csv_id_processing.log'

# Set up logging
logging.basicConfig(
    filename=str(LOG_FILE),
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# CONFIGURATION VARIABLES
WARD_PATTERN = re.compile(r'ward', re.IGNORECASE)
PRECINCT_PATTERN = re.compile(r'precinct', re.IGNORECASE)
OUTPUT_ID_COLUMN = 'precinct_id'
ID_COLUMNS_TO_REMOVE = ['id', 'ID']

def find_column_by_pattern(columns, pattern):
    """Find a column name that matches the given pattern."""
    for col in columns:
        if pattern.search(str(col)):
            return col
    return None

def process_csv(csv_path, dir_name, csv_name):
    """Process a single CSV file to create a precinct_id field."""
    try:
        # Read the CSV file
        df = pl.read_csv(csv_path)
        
        # Find ward and precinct columns
        columns = df.columns
        ward_col = find_column_by_pattern(columns, WARD_PATTERN)
        precinct_col = find_column_by_pattern(columns, PRECINCT_PATTERN)
        
        if not ward_col or not precinct_col:
            logging.error(f"Missing ward or precinct column in directory {dir_name} csv {csv_name}")
            return
        
        # Log the found columns
        logging.info(f"Found ward column: {ward_col} and precinct column: {precinct_col} in {csv_name}")
        
        # Create the precinct_id using a different approach
        # Instead of format(), we'll concatenate strings after formatting the numbers
        df = df.with_columns([
            pl.when(pl.col(ward_col).is_not_null() & pl.col(precinct_col).is_not_null())
            .then(
                # Convert ward to 2-digit string and precinct to 3-digit string
                pl.col(ward_col).cast(pl.Int32).cast(pl.Utf8).str.zfill(2) + 
                pl.col(precinct_col).cast(pl.Int32).cast(pl.Utf8).str.zfill(3)
            )
            .otherwise(None)
            .alias(OUTPUT_ID_COLUMN)
        ])
        
        # Remove unwanted ID columns
        columns_to_drop = []
        for col in columns:
            if col in ID_COLUMNS_TO_REMOVE or ('id' in col.lower() and col.lower() != OUTPUT_ID_COLUMN.lower()):
                columns_to_drop.append(col)
        
        if columns_to_drop:
            df = df.drop(columns_to_drop)
        
        # Log sample created IDs
        sample_ids = df.select(pl.col(OUTPUT_ID_COLUMN)).head(5).to_series().to_list()
        logging.info(f"Sample created IDs from {dir_name} csv {csv_name}: {sample_ids}")
        
        # Save the updated CSV
        df.write_csv(csv_path)
        logging.info(f"Successfully processed directory {dir_name} csv {csv_name}")
        
    except Exception as e:
        logging.error(f"Error processing directory {dir_name} csv {csv_name}: {str(e)}")
        logging.error(f"Error details: {type(e).__name__}")

def main():
    """Main function to traverse directories and process CSV files."""
    # Check if the data directory exists
    if not DATA_DIR.exists():
        print(f"Error: The directory {DATA_DIR} does not exist.")
        logging.error(f"The directory {DATA_DIR} does not exist.")
        return
    
    print(f"Processing directory: {DATA_DIR}")
    logging.info(f"Started processing directory: {DATA_DIR}")
    
    # Count for reporting
    total_files = 0
    processed_files = 0
    
    # Traverse the directory structure
    for dir_path in DATA_DIR.iterdir():
        if dir_path.is_dir():
            dir_name = dir_path.name
            
            # Process each CSV in the directory
            for csv_path in dir_path.glob('*.csv'):
                total_files += 1
                csv_name = csv_path.name
                print(f"Processing {csv_path}...")
                process_csv(csv_path, dir_name, csv_name)
                processed_files += 1
    
    print(f"Processing complete. Processed {processed_files} out of {total_files} files.")
    logging.info(f"Processing complete. Processed {processed_files} out of {total_files} files.")

if __name__ == "__main__":
    main()
    print("Check the log file for details.")
