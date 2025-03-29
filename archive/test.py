import duckdb as db
import geopandas as gpd
import matplotlib as plt

con = db.connect("test_elections.db")
con.sql("INSTALL spatial; LOAD spatial;")

con.sql("""CREATE TABLE test_election AS
SELECT 
    -- Convert numeric columns
    TRY_CAST("shape_leng" AS DOUBLE) AS shape_leng,
    TRY_CAST("shape_area" AS DOUBLE) AS shape_area,
    "ward_preci",
    TRY_CAST("ward" AS INTEGER) AS ward,
    TRY_CAST("precinct" AS INTEGER) AS precinct,
    -- Convert WKT or GeoJSON to geometry
    ST_GeomFromText("geometry") AS geometry,
    TRY_CAST("Registered Voters" AS INTEGER) AS registered_voters,
    TRY_CAST("Ballots Cast" AS INTEGER) AS ballots_cast,
    TRY_CAST("Turnout" AS DOUBLE) AS turnout,
    "ward_precinct"
FROM read_csv_auto('merged_turnout_data_cleaned.csv');""")



# Query your spatial data
query = """
SELECT ward, precinct, turnout, geometry 
FROM test_election
"""
# Convert to GeoPandas DataFrame
gdf = gpd.GeoDataFrame.from_postgis(query, con, geom_col='geometry')

# con = duckdb.connect(database=':memory:', read_only=False)
#
# # Option 1: Read the CSV file using SQL and create a table
# con.execute(f"CREATE TABLE my_table AS SELECT * FROM read_csv_auto('{csv_file_path}')")
#
# # Option 2: Read the CSV file directly into a Pandas DataFrame
# # import pandas as pd
# # df = con.execute(f"SELECT * FROM read_csv_auto('{csv_file_path}')").fetchdf()
#
# # Option 3: Read the CSV file using COPY statement
# # con.execute(f"CREATE TABLE my_table (col1 INTEGER, col2 VARCHAR)") # Ensure table schema matches CSV
# # con.execute(f"COPY my_table FROM '{csv_file_path}' (HEADER, DELIMITER ',')")
#
# # Verify the data has been read correctly
# result = con.execute("SELECT * FROM my_table").fetchall()
# print(result)

