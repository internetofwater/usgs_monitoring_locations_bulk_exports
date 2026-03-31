# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: MIT

from pathlib import Path
import requests
import pyarrow.parquet as pq
import argparse
import logging
import json
import shapely

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


def strip_fractional_seconds(dt: str | None) -> str | None:
    if not dt:
        return None
    # Split on "." and keep the first part (before fractional seconds)
    # Also preserves timezone if present (e.g. +00:00)
    if "." in dt:
        left, right = dt.split(".", 1)
        # If there's a timezone after the fraction, reattach it
        if "Z" in right:
            return left + "Z"
        if "+" in right or "-" in right:
            # find timezone start
            for i, c in enumerate(right):
                if c in "+-":
                    return left + right[i:]
        return left
    return dt


def row_to_jsonld(row: dict) -> dict:
    """
    Convert a single monitoring location row into JSON-LD
    compliant with LocationOrientedShape + DatasetShape
    """

    timeseries = row.get("timeseries_metadata")

    # base geometry
    wkb = row.get("geometry")
    assert isinstance(wkb, bytes)
    geometry_obj = shapely.from_wkb(wkb)

    # assert the geometry is a point
    assert isinstance(geometry_obj, shapely.Point)

    place = {
        "@context": {
            "@vocab": "https://schema.org/",
            "schema": "https://schema.org/",
            "gsp": "http://www.opengis.net/ont/geosparql#",
            "hyf": "https://www.opengis.net/def/schema/hy_features/hyf/",
            "dc": "http://purl.org/dc/terms/",
        },
        "@type": "schema:Place",
        "@id": row.get("id"),
        "schema:name": row.get("monitoring_location_name"),
        "hyf:HydroLocationType": row.get("site_type"),
        "schema:identifier": {
            "@type": "PropertyValue",
            "propertyID": "USGS site identifier",
            "value": row.get("monitoring_location_number"),
        },
        "schema:url": f"https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items/{row.get('id')}",
        "schema:provider": {
            "@type": "GovernmentOrganization",
            "name": row.get("agency_name"),
        },
        # schema.org geometry (SHACL required)
        "schema:geo": {
            "@type": "GeoCoordinates",
            "schema:latitude": geometry_obj.y,
            "schema:longitude": geometry_obj.x,
        },
        # GeoSPARQL geometry (SHACL required)
        "gsp:hasGeometry": {
            "@type": "http://www.opengis.net/ont/sf#Point",
            "gsp:asWKT": {"@type": "gsp:wktLiteral", "@value": geometry_obj.wkt},
            "gsp:crs": {"@id": "http://www.opengis.net/def/crs/OGC/1.3/CRS84"},
        },
    }
    if timeseries is None or len(timeseries) == 0:
        return place
    else:
        place["schema:subjectOf"] = []

    for ts in timeseries:
        parameter = ts["parameter_name"]
        unit = ts["unit_of_measure"]
        code = ts["parameter_code"]
        begin = ts["begin"]
        end = ts["end"]
        begin_clean = strip_fractional_seconds(begin)
        end_clean = strip_fractional_seconds(end)

        dataset = {
            "@type": "schema:Dataset",
            "schema:name": row.get("monitoring_location_number"),
            "schema:description": f"{parameter} at {row['monitoring_location_name']}",
            "schema:provider": {
                "@type": "GovernmentOrganization",
                "name": row.get("agency_name"),
                "url": "https://www.usgs.gov/",
            },
            "schema:url": f"https://api.waterdata.usgs.gov/ogcapi/v0/collections/time-series-metadata/items/{ts['id']}",
            "schema:variableMeasured": {
                "@type": "PropertyValue",
                "schema:name": parameter,
                "schema:description": f"{parameter} in {unit}",
                "schema:propertyID": str(code),
                "schema:unitText": unit,
                "schema:measurementTechnique": "observation",
                "schema:measurementMethod": {
                    "schema:name": f"{parameter} Measurements",
                    "schema:publisher": row.get("agency_name"),
                },
            },
        }
        if begin_clean and end_clean:
            dataset["schema:temporalCoverage"] = f"{begin_clean}/{end_clean}"
        elif begin_clean:
            dataset["schema:temporalCoverage"] = f"{begin_clean}/.."
        elif end_clean:
            dataset["schema:temporalCoverage"] = f"../{end_clean}"

        place["schema:subjectOf"].append(dataset)

    # remove nulls (SHACL cleanliness)
    place = {k: v for k, v in place.items() if v is not None}

    return place


def get_parquet_file(file_location) -> Path:
    if Path(file_location).exists():
        LOGGER.info("Found parquet file locally")
        return Path(file_location)
    else:
        LOGGER.info("Downloading parquet file")
        with requests.get(file_location, stream=True) as response:
            response.raise_for_status()

            download_path = Path("monitoring_locations.parquet")
            with open(download_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
        return download_path


def main(file_location):
    parquet_file = get_parquet_file(file_location)
    pf = pq.ParquetFile(parquet_file)

    batch_size = 5000

    for batch in pf.iter_batches(batch_size=batch_size):
        df = batch.to_pandas()
        LOGGER.info(f"Processing chunk with {len(df)} rows")

        jsonld_records: list[str] = []

        for _, row in df.iterrows():
            jsonld = row_to_jsonld(row.to_dict())
            jsonld_records.append(json.dumps(jsonld))

        print("\n".join(jsonld_records))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--parquet_file",
        type=str,
        default="https://github.com/internetofwater/usgs_monitoring_locations_bulk_exports/releases/latest/download/monitoring_locations_with_time_series_metadata.parquet",
    )

    args = parser.parse_args()
    main(args.parquet_file)
