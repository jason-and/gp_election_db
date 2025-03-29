import glob
import json
import logging
import multiprocessing
import os
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime

import duckdb
import polars as pl

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("election_etl.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
CONFIG = {
    "base_dir": "/home/andrew/projects/cgp_election_data/output/",  # Path to the numbered directories
    "metadata_path": "/home/andrew/projects/cgp_election_data/output/results-metadata.json",  # Path to metadata JSON file
    "db_path": "/home/andrew/projects/cgp_election_data/chicago_elections.db",  # Path for the output DuckDB database
    "batch_size": 100000,  # Number of rows to process in a batch
    "workers": max(1, multiprocessing.cpu_count() - 1)  # Leave one CPU free
}

def load_metadata():
    """Load the metadata JSON file and return it as a dictionary."""
    try:
        with open(CONFIG["metadata_path"], 'r') as f:
            metadata = json.load(f)
        
        # Count directories and years for logging
        dir_count = len([k for k in metadata.keys() if k.isdigit()])
        years = set()
        for dir_id, dir_data in metadata.items():
            if dir_id.isdigit() and 'year' in dir_data:
                try:
                    years.add(int(dir_data['year']))
                except (TypeError, ValueError):
                    pass
        
        logger.info(f"Loaded metadata with {dir_count} directories spanning years: {sorted(years)}")
        return metadata
    except Exception as e:
        logger.error(f"Failed to load metadata: {e}")
        raise

def init_database():
    """Initialize the database with a simplified schema."""
    try:
        # Connect to DuckDB
        con = duckdb.connect(CONFIG["db_path"])
        
        # Create a single denormalized table for results
        con.execute("""
            CREATE TABLE IF NOT EXISTS election_results (
                result_id INTEGER,
                year INTEGER,
                election_date VARCHAR,
                election_id INTEGER,
                contest_id INTEGER,
                contest_name VARCHAR,
                precinct_id VARCHAR,
                ward VARCHAR,
                precinct VARCHAR,
                total_votes INTEGER,
                option_name VARCHAR,
                option_votes INTEGER,
                option_percent DOUBLE
            )
        """)
        
        # Create a sequence table for tracking the next result_id
        con.execute("""
            CREATE TABLE IF NOT EXISTS sequence_values (
                name VARCHAR PRIMARY KEY,
                next_value INTEGER
            )
        """)
        
        # Initialize the result_id sequence if it doesn't exist
        con.execute("""
            INSERT INTO sequence_values (name, next_value)
            SELECT 'result_id', 1
            WHERE NOT EXISTS (SELECT 1 FROM sequence_values WHERE name = 'result_id')
        """)
        
        logger.info("Database initialized with tables")
        return con
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise

def process_csv_file(csv_path, election_id, year, election_date, contest_id, contest_name):
    """Process a single CSV file and return a Polars DataFrame of results."""
    try:
        # Read the CSV file with Polars
        df = pl.read_csv(csv_path, ignore_errors=True)
        
        # Check if dataframe is empty
        if df.is_empty():
            logger.warning(f"Empty dataframe from {csv_path}")
            return None
            
        # Check if required columns are present
        if 'precinct_id' not in df.columns:
            logger.warning(f"CSV file {csv_path} is missing precinct_id column")
            return None
        
        # Define standard output columns that every result will have
        standard_columns = [
            "result_id",        # Will be assigned later
            "year",             # From metadata 
            "election_date",    # From metadata
            "election_id",      # Directory ID
            "contest_id",       # File ID
            "contest_name",     # From metadata
            "precinct_id",      # From CSV
            "ward",             # From CSV if available
            "precinct",         # From CSV if available
            "total_votes",      # From CSV 'total' column if available
            "option_name",      # Will be candidate name or option (Yes/No)
            "option_votes",     # Will be votes for this option
            "option_percent"    # Will be percentage for this option if available
        ]
        
        # Handle different CSV types based on column structure
        
        # Special case: registration/turnout data
        if 'registered' in df.columns and 'turnout' in df.columns:
            # Create two options: registered and ballots
            results = []
            
            # Create 'registered' option
            registered_df = df.select(['precinct_id', 'ward', 'precinct', 'registered'])
            registered_df = registered_df.with_columns([
                pl.lit("registered").alias("option_name"),
                pl.col("registered").alias("option_votes"),
                pl.lit(None).alias("option_percent"),
                pl.lit(None).alias("total_votes")
            ])
            results.append(registered_df)
            
            # Create 'ballots' option
            ballots_df = df.select(['precinct_id', 'ward', 'precinct', 'ballots'])
            ballots_df = ballots_df.with_columns([
                pl.lit("ballots").alias("option_name"),
                pl.col("ballots").alias("option_votes"),
                pl.col("turnout").alias("option_percent"),
                pl.lit(None).alias("total_votes")
            ])
            results.append(ballots_df)
            
            # Combine the results
            results_df = pl.concat(results)
            
        else:
            # Normal case: election results
            
            # Determine columns
            ward_col = 'ward' if 'ward' in df.columns else None
            precinct_col = 'precinct' if 'precinct' in df.columns else None
            total_col = 'total' if 'total' in df.columns else None
            
            # Find voting option columns - they don't end with "Percent"
            option_cols = [col for col in df.columns 
                           if col not in ['precinct_id', 'ward', 'precinct', 'total', 'registered', 'ballots', 'turnout'] 
                           and not str(col).endswith('Percent')]
            
            if not option_cols:
                logger.warning(f"No voting option columns found in {csv_path}")
                return None
            
            # Create ID variables list for melting
            id_vars = ['precinct_id']
            if ward_col: id_vars.append(ward_col)
            if precinct_col: id_vars.append(precinct_col)
            if total_col: id_vars.append(total_col)
            
            # Melt the dataframe to get long format
            try:
                results_df = df.melt(
                    id_vars=id_vars,
                    value_vars=option_cols,
                    variable_name="option_name",
                    value_name="option_votes"
                )
            except Exception as e:
                logger.error(f"Melt operation failed on {csv_path}: {e}")
                return None
            
            # Add percentage values if available
            results_df = results_df.with_columns(pl.lit(None).alias("option_percent"))
            
            for option in option_cols:
                percent_col = f"{option} Percent"
                if percent_col in df.columns:
                    # Update percentage for matching rows
                    mask = results_df["option_name"] == option
                    if mask.any():
                        # Join percentage values
                        percent_df = df.select(['precinct_id', percent_col])
                        results_df = results_df.with_columns(
                            pl.when(mask)
                              .then(results_df.join(percent_df, on="precinct_id")[percent_col])
                              .otherwise(pl.col("option_percent"))
                              .alias("option_percent")
                        )
        
        # Rename 'total' to 'total_votes' if it exists
        if total_col:
            results_df = results_df.rename({total_col: "total_votes"})
        else:
            results_df = results_df.with_columns(pl.lit(None).alias("total_votes"))
        
        # Ensure all required columns exist with correct types
        if ward_col is None:
            results_df = results_df.with_columns(pl.lit(None).cast(pl.Utf8).alias("ward"))
        
        if precinct_col is None:
            results_df = results_df.with_columns(pl.lit(None).cast(pl.Utf8).alias("precinct"))
        
        # Handle data types and conversions explicitly
        # First, convert all values to strings to handle mixed types
        for col in ["option_votes", "total_votes"]:
            if col in results_df.columns:
                # Convert to string first, then extract numbers, then to integers
                results_df = results_df.with_columns(
                    pl.when(pl.col(col).is_null())
                      .then(pl.lit(0))
                      .otherwise(pl.col(col))
                      .alias(col)
                )
                
                # For numeric columns, ensure they're integers (handle string values)
                results_df = results_df.with_columns(
                    pl.col(col).cast(pl.Utf8)
                      .str.extract(r"(\d+)", 1)
                      .fill_null("0")
                      .cast(pl.Int32)
                      .alias(col)
                )
        
        # Convert option_percent to float
        if "option_percent" in results_df.columns:
            results_df = results_df.with_columns(
                pl.when(pl.col("option_percent").is_null())
                  .then(pl.lit(0.0))
                  .otherwise(pl.col("option_percent"))
                  .alias("option_percent")
            )
            
            results_df = results_df.with_columns(
                pl.col("option_percent").cast(pl.Utf8)
                  .str.extract(r"(\d+\.?\d*)", 1)
                  .fill_null("0.0")
                  .cast(pl.Float64)
                  .alias("option_percent")
            )
        
        # Add metadata columns that are missing
        metadata_columns = {
            "year": year,
            "election_date": election_date,
            "election_id": election_id,
            "contest_id": contest_id,
            "contest_name": contest_name,
            "result_id": -1  # Placeholder
        }
        
        for col_name, col_value in metadata_columns.items():
            if col_name not in results_df.columns:
                results_df = results_df.with_columns(pl.lit(col_value).alias(col_name))
        
        # Ensure we have all standard columns in the right order
        # First, make sure all standard columns exist
        for col in standard_columns:
            if col not in results_df.columns:
                results_df = results_df.with_columns(pl.lit(None).alias(col))
        
        # Then select only the standard columns in the right order
        results_df = results_df.select(standard_columns)
        
        logger.info(f"Processed {csv_path}: {len(results_df)} results")
        return results_df
        
    except Exception as e:
        logger.error(f"Failed to process CSV file {csv_path}: {e}")
        return None

def process_directory(args):
    """Process all CSV files in a directory."""
    directory_id, directory_path, metadata = args
    try:
        dir_id_str = str(directory_id)
        if dir_id_str not in metadata:
            logger.warning(f"Directory {directory_id} not found in metadata, skipping")
            return None
            
        dir_data = metadata[dir_id_str]
        year = dir_data.get('year')
        election_date = dir_data.get('date')
        
        # Get all CSV files in this directory
        csv_files = glob.glob(os.path.join(directory_path, "*.csv"))
        logger.info(f"Processing directory {directory_id}: found {len(csv_files)} CSV files")
        
        # Track successful and failed files
        success_count = 0
        error_count = 0
        all_results = []
        
        # Process each CSV file
        for csv_path in csv_files:
            # Extract file ID from filename
            file_name = os.path.basename(csv_path)
            file_id_str = file_name.split('.')[0]
            
            if not file_id_str.isdigit():
                logger.warning(f"Skipping non-numeric filename: {file_name}")
                continue
                
            file_id = int(file_id_str)
            
            # Get contest name from metadata
            contest_name = None
            if 'races' in dir_data and file_id_str in dir_data['races']:
                contest_name = dir_data['races'][file_id_str]
            
            # Process the CSV file
            results_df = process_csv_file(
                csv_path, directory_id, year, election_date, file_id, contest_name
            )
            
            if results_df is not None and len(results_df) > 0:
                all_results.append(results_df)
                success_count += 1
            else:
                error_count += 1
        
        logger.info(f"Directory {directory_id} processing complete: {success_count} succeeded, {error_count} failed")
        
        # Combine all results for this directory
        if all_results:
            try:
                # Since we've standardized the schema in process_csv_file,
                # concat should work without issues now
                combined_results = pl.concat(all_results)
                return combined_results
            except Exception as e:
                logger.error(f"Failed to combine results for directory {directory_id}: {e}")
                # Log the column names to help diagnose
                column_sets = []
                for i, df in enumerate(all_results):
                    column_sets.append(f"DataFrame {i}: {df.columns}")
                logger.error(f"Column mismatch details: {', '.join(column_sets)}")
                return None
        else:
            return None
        
    except Exception as e:
        logger.error(f"Failed to process directory {directory_id}: {e}")
        return None


def save_results_to_duckdb(results_df, next_id, con):
    """Save results DataFrame to DuckDB."""
    try:
        if results_df is None or len(results_df) == 0:
            return next_id
            
        # Assign sequential IDs
        num_rows = len(results_df)
        results_df = results_df.with_columns(
            pl.arange(next_id, next_id + num_rows).cast(pl.Int32).alias("result_id")
        )
        
        # Handle string-to-numeric conversion for option_votes and total_votes
        for col in ["option_votes", "total_votes"]:
            results_df = results_df.with_columns(
                pl.col(col).cast(pl.Int32).fill_null(0).alias(col)
            )
        
        # Handle option_percent conversion
        results_df = results_df.with_columns(
            pl.col("option_percent").cast(pl.Float64).fill_null(0.0).alias("option_percent")
        )
        
        # Convert to pandas for DuckDB
        pd_df = results_df.to_pandas()
        
        # Bulk insert using DuckDB's fast loading
        con.execute("INSERT INTO election_results SELECT * FROM pd_df")
        
        # Update next_id
        next_id += num_rows
        
        # Update the sequence
        con.execute("UPDATE sequence_values SET next_value = ? WHERE name = 'result_id'", [next_id])
        
        logger.info(f"Saved {num_rows} rows to database")
        return next_id
    except Exception as e:
        logger.error(f"Failed to save results to database: {e}")
        # Log additional debug info
        if results_df is not None:
            logger.error(f"DataFrame columns: {results_df.columns}")
            logger.error(f"Sample data: {results_df.head(1)}")
        raise

def create_views(con):
    """Create helpful views for querying the data."""
    try:
        # Create a view for election summary
        con.execute("""
            CREATE OR REPLACE VIEW election_summary AS
            SELECT 
                year,
                election_date,
                COUNT(DISTINCT contest_id) as contest_count,
                COUNT(DISTINCT precinct_id) as precinct_count
            FROM 
                election_results
            GROUP BY 
                year, election_date
            ORDER BY 
                year
        """)
        
        # Create a view for contest results
        con.execute("""
            CREATE OR REPLACE VIEW contest_summary AS
            SELECT 
                contest_id,
                contest_name,
                year,
                election_date,
                COUNT(DISTINCT precinct_id) as precinct_count,
                SUM(total_votes) as total_votes
            FROM 
                election_results
            GROUP BY 
                contest_id, contest_name, year, election_date
            ORDER BY 
                year, contest_id
        """)
        
        # Create a view for mapping
        con.execute("""
            CREATE OR REPLACE VIEW mapping_data AS
            SELECT 
                year,
                contest_id,
                contest_name,
                precinct_id,
                ward,
                precinct,
                option_name,
                SUM(option_votes) as total_option_votes,
                SUM(total_votes) as precinct_total_votes
            FROM 
                election_results
            GROUP BY 
                year, contest_id, contest_name, precinct_id, ward, precinct, option_name
            ORDER BY 
                year, contest_id, precinct_id, option_name
        """)
        
        logger.info("Created database views")
    except Exception as e:
        logger.error(f"Failed to create views: {e}")
        raise

def main():
    start_time = datetime.now()
    logger.info(f"Starting election data ETL process at {start_time}")
    
    try:
        # Load metadata
        metadata = load_metadata()
        
        # Initialize the database
        con = init_database()
        
        # Clear existing data to avoid duplicates
        con.execute("DELETE FROM election_results")
        con.execute("UPDATE sequence_values SET next_value = 1 WHERE name = 'result_id'")
        
        # Get the next available result_id
        next_id = con.execute("SELECT next_value FROM sequence_values WHERE name = 'result_id'").fetchone()[0]
        
        # Get list of all directories
        all_dirs = []
        for directory_name in os.listdir(CONFIG["base_dir"]):
            directory_path = os.path.join(CONFIG["base_dir"], directory_name)
            if os.path.isdir(directory_path) and directory_name.isdigit():
                all_dirs.append((int(directory_name), directory_path))
        
        # Sort by directory ID for organized processing
        all_dirs.sort()
        
        logger.info(f"Found {len(all_dirs)} directories to process")
        
        # Prepare arguments for parallel processing
        process_args = [(dir_id, dir_path, metadata) for dir_id, dir_path in all_dirs]
        
        # Process directories in parallel
        with ProcessPoolExecutor(max_workers=CONFIG["workers"]) as executor:
            results = list(executor.map(process_directory, process_args))
        
        # Save all results to the database
        total_results = 0
        for results_df in results:
            if results_df is not None and len(results_df) > 0:
                # Process in batches to manage memory
                for i in range(0, len(results_df), CONFIG["batch_size"]):
                    batch_df = results_df.slice(i, min(CONFIG["batch_size"], len(results_df) - i))
                    next_id = save_results_to_duckdb(batch_df, next_id, con)
                    total_results += len(batch_df)
        
        # Create helpful views
        create_views(con)
        
        # Get statistics
        elections = con.execute("SELECT COUNT(DISTINCT election_id) FROM election_results").fetchone()[0]
        contests = con.execute("SELECT COUNT(DISTINCT contest_id) FROM election_results").fetchone()[0]
        
        logger.info(f"ETL process complete:")
        logger.info(f"  - {elections} elections loaded")
        logger.info(f"  - {contests} contests loaded")
        logger.info(f"  - {total_results} result records created")
        
        # Close the connection
        con.close()
        
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"ETL process completed successfully in {duration}")
        
    except Exception as e:
        logger.error(f"Main process failed: {e}")
        end_time = datetime.now()
        duration = end_time - start_time
        logger.error(f"ETL process failed after {duration}: {e}")

if __name__ == "__main__":
    main()
