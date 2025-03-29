import duckdb

# Connect to your database
con = duckdb.connect('/home/andrew/projects/cgp_election_data/election_results.duckdb')

# Drop tables (in the correct order to respect foreign key constraints)
con.execute("DROP TABLE IF EXISTS contest_boundaries")
con.execute("DROP TABLE IF EXISTS boundaries")
con.execute("DROP TABLE IF EXISTS boundary_sets")

print("Tables successfully dropped. The database is now ready for a fresh import.")
