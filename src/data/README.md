## ETL step by step:

### 1 - Collect raw data:
Run "python get_waze_rawdata.py --batchsize 20000 --update True" to download all data from MongoDB in the cloud, in batches of 20000 documents, and store them in json files in /data/raw/. If True, the "--update" option will delete all data that is currently in the /data/raw/ folder. If you wish to keep downloading from the last document in the folder, ignore this option or set it to False.

### 2 - Tabulate data:
Run "python store_tabulation.py" to read json files and store them in PostgreSQL tables accordingly, also in batches to fit in memory.

### 3 - Cross-reference data with local georreferenced street data:
Run "python store_jps.py" to read SQL data of traffic jams as well as official geospatial data of street sections, merge and store them in a separate table.

All functions used in the three modules above can be found in processing_func.py.
