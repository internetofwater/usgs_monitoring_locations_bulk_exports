# usgs_monitoring_locations_bulk_export

The USGS monitoring locations API does not provide any way to bulk download the entire dataset. This script pages through the entire items/ endpoint and writes the results to a parquet file. It then adds the time series metadata to the parquet file.

The latest associated GeoParquet release can be found [here](https://github.com/internetofwater/usgs_monitoring_locations_bulk_exports/releases)
