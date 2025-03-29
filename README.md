# Database Create Instructions

Using [uv](https://github.com/astral-sh/uv)

```
uv synch
```

Will have to manually adjust your file paths in scripts

- 'import_elections.py'
- 'import_geometries.py'

```
uv run import_elections.py
```

```
uv run import_geometries.py
```

# PROCESS STEPS

## Elections

Chicago Board of Elections Website is harder to scrape now and only outputs data in hard to use excel files.

Chi Hack Night previously scraped this website for use in their Chicago Elections Archive Map

[chicago-elections-archive](https://github.com/chihacknight/chicago-elections-archive/tree/main/output)
sored in /output directory, along with the results-metadata.json that acts as a key for all of the csvs.

At this time, we do not have the latest election results as this will have to be wrangled in a semi-manual way from excel files.

The csvs had some header issues with some not having 6 headers despite 6 columns of data.
This was in most cases a result of an implicit "id" field. I named each of these fields "id" as a placeholder.

Each id was formatted differently and not in the Precinct 000 Ward 00 Ward+Precinct 00000. So this field was generated using the ward and precinct data in each csv named "precinct_id" using scripts/csv_id_proccessing.py . This field can now be used to join on geographies.

All elections were brought in using scripts/import_elections.py. Table is denormalized for now, it is just one large tall table that will need additional query magic to be useful.

Geojson needed to be standardized with precinct_ids, used unscripted jq package to do this. Imported geojson files using scripts/import_geometries.py .

# Duckdb database

file is chicago_elections.db. Still need to work out table creation and relations to allow for geographic analysis. This also may not work right now and may require reworking. When querying data using duckdb cli tool, remember to use

```
INSTALL SPATIAL;
LOAD SPATIAL;
```

To be able to use the geometries correctly.

# Geographies

- [Boundaries-Ward Precincts 2023](https://data.cityofchicago.org/Facilities-Geographic-Boundaries/Boundaries-Ward-Precincts-2023-/6piy-vbxa/about_data) First used in 2022

- [Boundaries - Ward Precincts (2013-2022)
  F](https://data.cityofchicago.org/Facilities-Geographic-Boundaries/Boundaries-Ward-Precincts-2013-2022-/nvke-umup/about_data) First used in 2014 through 2020
- [Precincts2010](https://data.cityofchicago.org/Facilities-Geographic-Boundaries/Precincts2010/2d4k-r48m/about_data) Used in 2010
