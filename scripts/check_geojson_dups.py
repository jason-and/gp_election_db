from pathlib import Path

import geopandas as gpd
import pandas as pd


def check_duplicates(filepath):
    """
    Check for duplicate precinct IDs in a GeoJSON file.
    
    Args:
        filepath: Path to the GeoJSON file
    """
    print(f"Checking for duplicates in {Path(filepath).name}...")
    
    # Read the GeoJSON with geopandas
    gdf = gpd.read_file(filepath)
    
    # Print column names
    print(f"Available columns: {gdf.columns.tolist()}")
    
    # Look for precinct_id in properties
    precinct_id_field = None
    possible_fields = ['precinct_id', 'PRECINCT_ID', 'precinct', 'PRECINCT', 'ID']
    
    for field in possible_fields:
        if field in gdf.columns:
            precinct_id_field = field
            break
    
    # If no direct field found, check if we need to combine ward and precinct
    if precinct_id_field is None and 'ward' in gdf.columns and 'precinct' in gdf.columns:
        print("Creating precinct_id from ward and precinct columns")
        # Combine ward and precinct
        gdf['precinct_id'] = gdf['ward'].astype(str).str.zfill(2) + gdf['precinct'].astype(str).str.zfill(3)
    elif precinct_id_field:
        print(f"Using {precinct_id_field} as precinct_id")
        gdf['precinct_id'] = gdf[precinct_id_field].astype(str)
    else:
        raise ValueError("Could not find a suitable precinct ID column in the GeoJSON")
    
    # Check for duplicates
    duplicates = gdf['precinct_id'].duplicated(keep=False)
    duplicate_count = duplicates.sum()
    
    if duplicate_count > 0:
        print(f"Found {duplicate_count} duplicate precinct IDs!")
        
        # Get all duplicate IDs and their counts
        dup_values = gdf.loc[duplicates, 'precinct_id'].value_counts()
        print("\nDuplicate IDs and their counts:")
        for precinct_id, count in dup_values.items():
            print(f"  Precinct ID: {precinct_id}, Count: {count}")
        
        # Show a sample of the duplicate records
        print("\nSample of duplicate records:")
        for precinct_id in dup_values.index[:min(5, len(dup_values))]:  # Show details for first 5 duplicates
            print(f"\nDetails for duplicate precinct ID: {precinct_id}")
            dup_records = gdf[gdf['precinct_id'] == precinct_id]
            for idx, row in dup_records.iterrows():
                print(f"  Record {idx}:")
                for col in gdf.columns:
                    if col != 'geometry':  # Skip printing the full geometry
                        print(f"    {col}: {row[col]}")
    else:
        print("No duplicate precinct IDs found!")
    
    # Check for empty/null precinct IDs which might cause issues
    null_ids = gdf['precinct_id'].isnull() | (gdf['precinct_id'] == '')
    null_count = null_ids.sum()
    
    if null_count > 0:
        print(f"\nWARNING: Found {null_count} empty or null precinct IDs!")
        print("Sample of records with null/empty IDs:")
        print(gdf[null_ids].head())
    
    # Check for precinct ID "00000" specifically (mentioned in the error)
    zeros_ids = gdf['precinct_id'] == "00000"
    zeros_count = zeros_ids.sum()
    
    if zeros_count > 0:
        print(f"\nWARNING: Found {zeros_count} records with precinct ID '00000'!")
        print("Sample of these records:")
        print(gdf[zeros_ids].head())
    
    return dup_values if duplicate_count > 0 else pd.Series()
