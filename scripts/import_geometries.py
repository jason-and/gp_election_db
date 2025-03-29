import duckdb
import geopandas as gpd
import polars as pl
from pathlib import Path

# Connect to your database
con = duckdb.connect("chicago_elections.db")

# Set up spatial extension
con.execute("INSTALL spatial;")
con.execute("LOAD spatial;")


# Function to import a GeoJSON file
def import_precinct_geojson(filepath, valid_from_year, valid_to_year=None):
    """
    Import a GeoJSON file of precincts into the precinct_geometries table.

    Args:
        filepath: Path to the GeoJSON file
        valid_from_year: Year from which these precincts are valid
        valid_to_year: Year until which these precincts are valid (None if still valid)
    """
    print(f"Importing precincts from {Path(filepath).name}...")

    # Read the GeoJSON with geopandas
    gdf = gpd.read_file(filepath)

    # Check the column names to find the precinct ID
    print(f"Available columns: {gdf.columns.tolist()}")

    # Look for precinct_id in properties
    precinct_id_field = None
    possible_fields = ["precinct_id", "PRECINCT_ID", "precinct", "PRECINCT", "ID"]

    for field in possible_fields:
        if field in gdf.columns:
            precinct_id_field = field
            break

    # If no direct field found, check if we need to combine ward and precinct
    if (
        precinct_id_field is None
        and "ward" in gdf.columns
        and "precinct" in gdf.columns
    ):
        print("Creating precinct_id from ward and precinct columns")
        # Combine ward and precinct (typically formats as WWPPP - 2 digit ward, 3 digit precinct)
        gdf["precinct_id"] = gdf["ward"].astype(str).str.zfill(2) + gdf[
            "precinct"
        ].astype(str).str.zfill(3)
    elif precinct_id_field:
        print(f"Using {precinct_id_field} as precinct_id")
        gdf["precinct_id"] = gdf[precinct_id_field].astype(str)
    else:
        raise ValueError("Could not find a suitable precinct ID column in the GeoJSON")

    # Ensure precinct_id is properly formatted
    # This ensures format like "01001" instead of "1001"
    if gdf["precinct_id"].str.len().min() < 5:
        # Check if they appear to be ward-precinct combinations
        if gdf["precinct_id"].str.isdigit().all():
            lengths = gdf["precinct_id"].str.len().value_counts()
            print(f"Precinct ID length distribution: {lengths.to_dict()}")

            # If most are 4 digits, likely need a leading zero
            if lengths.idxmax() == 4:
                gdf["precinct_id"] = gdf["precinct_id"].str.zfill(5)
                print("Padded precinct_ids to 5 digits")

    # CRITICAL: Check for empty or problematic precinct IDs
    print("\nChecking for empty or problematic precinct IDs...")

    # Fix: Use proper pandas comparison operators
    empty_ids = gdf["precinct_id"].isna() | (gdf["precinct_id"] == "")
    zero_ids = (gdf["precinct_id"] == "0") | (gdf["precinct_id"] == "00000")

    if empty_ids.any():
        empty_count = empty_ids.sum()
        print(f"WARNING: Found {empty_count} records with empty precinct IDs!")
        print("Assigning generated IDs to these records...")

        # Generate new IDs for empty precinct IDs (e.g., "GEN001", "GEN002", etc.)
        next_id = 1
        for idx in gdf[empty_ids].index:
            gdf.loc[idx, "precinct_id"] = f"GEN{next_id:03d}"
            next_id += 1

    if zero_ids.any():
        zero_count = zero_ids.sum()
        print(f"WARNING: Found {zero_count} records with '0' or '00000' precinct IDs!")
        print("Renaming these to avoid database constraint issues...")

        # Rename "00000" IDs to something like "ZERO001", "ZERO002", etc.
        next_id = 1
        for idx in gdf[zero_ids].index:
            gdf.loc[idx, "precinct_id"] = f"ZERO{next_id:03d}"
            next_id += 1

    # Check for duplicates after fixes
    duplicates = gdf["precinct_id"].duplicated()
    if duplicates.any():
        dup_count = duplicates.sum()
        print(f"WARNING: Still found {dup_count} duplicate precinct IDs after fixes!")

        # Get the first few duplicates to examine
        dup_ids = gdf.loc[duplicates, "precinct_id"].values[:5]
        print(f"Sample duplicate IDs: {dup_ids}")

        # Fix: Properly check for specific precinct ID
        zeros_check = gdf["precinct_id"] == "00000"
        if zeros_check.any():
            print(
                "CRITICAL: Found '00000' precinct ID which caused the previous error!"
            )
            dup_count = zeros_check.sum()
            print(f"Count of '00000' IDs: {dup_count}")

        # Remove duplicates to avoid constraint violation
        print("Removing duplicate records to avoid constraint violation...")
        gdf = gdf.drop_duplicates(subset=["precinct_id"], keep="first")

    # Get current max precinct_geometry_id
    result = con.execute(
        "SELECT COALESCE(MAX(precinct_geometry_id), 0) FROM precinct_geometries"
    ).fetchone()
    start_id = result[0] + 1

    # Convert geometry to WKT
    gdf["geometry_wkt"] = gdf.geometry.apply(lambda geom: geom.wkt)

    # Convert to Polars DataFrame
    data = []
    for i, (_, row) in enumerate(gdf.iterrows()):
        data.append(
            {
                "precinct_geometry_id": start_id + i,
                "precinct_id": row["precinct_id"],
                "valid_from_year": valid_from_year,
                "valid_to_year": valid_to_year,
                "geometry_wkt": row["geometry_wkt"],
            }
        )

    insert_df = pl.DataFrame(data)

    # First check if there are any existing records for this year range
    existing = con.execute(
        f"""
    SELECT precinct_id FROM precinct_geometries
    WHERE valid_from_year = {valid_from_year}
    """
    ).fetchall()

    if existing:
        print(f"Found {len(existing)} existing records for year {valid_from_year}")
        print(f"First few existing precinct IDs: {[e[0] for e in existing[:5]]}")

        # Delete existing records
        con.execute(
            f"""
        DELETE FROM precinct_geometries
        WHERE valid_from_year = {valid_from_year}
        """
        )
        print(f"Deleted existing records for year {valid_from_year}")

    # Check database for any '00000' records that might still exist
    zeros = con.execute(
        """
    SELECT precinct_id, valid_from_year FROM precinct_geometries
    WHERE precinct_id = '00000'
    """
    ).fetchall()

    if zeros:
        print(
            f"WARNING: Database still contains {len(zeros)} records with precinct_id '00000'!"
        )
        for pid, year in zeros:
            print(f"  Precinct '00000' exists for year {year}")

    # Use DuckDB's bulk insert capability for better performance
    # Create a temporary table
    con.execute(
        """
    CREATE TEMPORARY TABLE temp_precincts (
        precinct_geometry_id INTEGER,
        precinct_id VARCHAR,
        valid_from_year INTEGER,
        valid_to_year INTEGER,
        geometry_wkt VARCHAR
    )
    """
    )

    # Register the Polars DataFrame with DuckDB
    con.register("insert_df", insert_df)

    # Insert data into the temporary table
    con.execute("INSERT INTO temp_precincts SELECT * FROM insert_df")

    # Insert from temp table to main table with geometry conversion
    inserted = con.execute(
        """
    INSERT INTO precinct_geometries
    SELECT
        precinct_geometry_id,
        precinct_id,
        valid_from_year,
        valid_to_year,
        ST_GeomFromText(geometry_wkt)
    FROM temp_precincts
    """
    ).fetchone()[0]

    # Drop the temporary table
    con.execute("DROP TABLE temp_precincts")

    print(f"Successfully imported {inserted} precincts for year {valid_from_year}")
    return inserted


# Main execution
if __name__ == "__main__":
    con.execute(
        """
    CREATE TABLE IF NOT EXISTS precinct_geometries (
        precinct_geometry_id INTEGER PRIMARY KEY,
        precinct_id VARCHAR,
        valid_from_year INTEGER,
        valid_to_year INTEGER,
        geometry GEOMETRY
    );
    """
    )

    try:
        # Recreate table without the constraint if needed
        print("Checking if we need to recreate the table without the constraint...")
        constraints = con.execute(
            """
        SELECT sql FROM sqlite_master
        WHERE type='table' AND name='precinct_geometries'
        """
        ).fetchone()

        if constraints and "UNIQUE" in constraints[0]:
            print("Found UNIQUE constraint, recreating table without it...")

            # Create a backup table
            con.execute(
                "CREATE TABLE IF NOT EXISTS precinct_geometries_backup AS SELECT * FROM precinct_geometries;"
            )
            print("Created backup of existing data")

            # Drop the original table
            con.execute("DROP TABLE IF EXISTS precinct_geometries;")
            print("Dropped original table")

            # Create a new table without the constraint
            con.execute(
                """
            CREATE TABLE IF NOT EXISTS precinct_geometries (
                precinct_geometry_id INTEGER PRIMARY KEY,
                precinct_id VARCHAR,
                valid_from_year INTEGER,
                valid_to_year INTEGER,
                geometry GEOMETRY
            );
            """
            )
            print("Created new table without UNIQUE constraint")

            # Restore data if backup exists
            backup_count = con.execute(
                "SELECT COUNT(*) FROM precinct_geometries_backup"
            ).fetchone()[0]
            if backup_count > 0:
                con.execute(
                    """
                INSERT INTO precinct_geometries
                SELECT * FROM precinct_geometries_backup
                """
                )
                print(f"Restored {backup_count} records from backup")
        else:
            print("Table has no UNIQUE constraint or doesn't exist yet, proceeding...")

        # Import each GeoJSON with its valid year range
        total_imported = 0

        # Import files one at a time, committing after each one
        print("\n--- Importing 2010 precincts ---")
        con.execute("BEGIN TRANSACTION")
        total_imported += import_precinct_geojson(
            "precincts/2010_precincts.geojson", 2010, 2013
        )
        con.execute("COMMIT")

        print("\n--- Importing 2014 precincts ---")
        con.execute("BEGIN TRANSACTION")
        total_imported += import_precinct_geojson(
            "precincts/2014_precincts.geojson",
            2014,
            2021,
        )
        con.execute("COMMIT")

        print("\n--- Importing 2022 precincts ---")
        con.execute("BEGIN TRANSACTION")
        total_imported += import_precinct_geojson(
            "precincts/2022_precincts.geojson",
            2022,
            None,
        )
        con.execute("COMMIT")

        print(
            f"\nTotal of {total_imported} precinct geometries imported into the database"
        )

        # Create index on precinct_id for faster lookups
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_precinct_id ON precinct_geometries(precinct_id)"
        )

        # Create index on year ranges for faster temporal queries
        con.execute(
            """
        CREATE INDEX IF NOT EXISTS idx_year_range ON precinct_geometries(valid_from_year, valid_to_year)
        """
        )

        # Final verification
        result = con.execute(
            """
        SELECT
            valid_from_year,
            valid_to_year,
            COUNT(*) as precinct_count
        FROM
            precinct_geometries
        GROUP BY
            valid_from_year, valid_to_year
        ORDER BY
            valid_from_year
        """
        ).fetchall()

        print("\nPrecinct counts by year range:")
        for from_year, to_year, count in result:
            to_year_str = str(to_year) if to_year is not None else "present"
            print(f"Years {from_year}-{to_year_str}: {count} precincts")

        # Final check for any remaining "00000" records
        zeros = con.execute(
            """
        SELECT precinct_id, valid_from_year FROM precinct_geometries
        WHERE precinct_id = '00000'
        """
        ).fetchall()

        if zeros:
            print(
                f"\nWARNING: Database contains {len(zeros)} records with precinct_id '00000'!"
            )
            for pid, year in zeros:
                print(f"  Precinct '00000' exists for year {year}")
        else:
            print("\nNo problematic '00000' precinct IDs found in the database.")

    except Exception as e:
        # Rollback on error
        try:
            con.execute("ROLLBACK")
        except:
            pass
        print(f"Error during import: {str(e)}")
        print("Changes have been rolled back")

    finally:
        # Close connection
        con.close()
