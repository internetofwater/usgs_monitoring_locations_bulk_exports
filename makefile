run_test:
	TEST_MODE=true uv run src/main.py

run_full:
	uv run src/main.py

prek:
	prek install
	prek run --all-files

check_metadata:
	duckdb -c "SELECT * FROM read_parquet('joined.parquet') WHERE timeseries_metadata IS NOT NULL LIMIT 5;"
	duckdb -c "SELECT COUNT(*) FROM read_parquet('joined.parquet')"
