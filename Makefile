data/raw/Waze_rawdata.csv: src/data/get_waze_rawdata.py
	python src/data/get_waze_rawdata.py

storage: data/raw/Waze_rawdata.csv
	python src/data/store_tabulation.py