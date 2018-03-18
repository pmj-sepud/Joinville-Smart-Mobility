## ETL step by step:

### 1 - Collect raw data:
Run "python get_waze_rawdata.py" to download all data from MongoDB in the cloud, in batches to fit memory, and store them in json files in /data/raw/

### 2 - Tabulate data:
Run "python store_tabulation.py" to read json files and store them in PostgreSQL tables accordingly, also in batches to fit memory.

### 3 - Cross-reference data with local georreferenced street data:
Run "python store_jps.py" to read SQL data of traffic jams as well as official geospatial data of street sections, merge them and store them in a separate table.

All functions uses in the three modules above can be found in processing_func.py.
