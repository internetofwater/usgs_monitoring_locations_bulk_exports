# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: MIT

from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import cast
import requests
import pyarrow.parquet as pq
import argparse
import logging
import json
import shapely
import math


def clean(value):
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


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

    id = cast(str, row.get("id"))
    agency_name = clean(row.get("agency_name"))
    place = {
        "@context": {
            "@vocab": "https://schema.org/",
            "gsp": "http://www.opengis.net/ont/geosparql#",
            "hyf": "https://www.opengis.net/def/schema/hy_features/hyf/",
            "locType": "https://api.waterdata.usgs.gov/ogcapi/v0/collections/site-types/items/",
        },
        "@type": [
            "Place",
            "hyf:HY_HydrometricFeature",
            "hyf:HY_HydroLocation",
            f"locType:{row.get('site_type_code')}",
        ],
        "@id": f"https://geoconnex.us/usgs/monitoring-location/{id}",
        "name": row.get("monitoring_location_name"),
        "identifier": {
            "@type": "PropertyValue",
            "propertyID": "USGS site identifier",
            "value": clean(row.get("monitoring_location_number")),
        },
        "url": f"https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items/{id}",
        "provider": {
            "@type": "GovernmentOrganization",
            "name": agency_name,
        },
        "geo": {
            "@type": "GeoCoordinates",
            "latitude": geometry_obj.y,
            "longitude": geometry_obj.x,
        },
        "gsp:hasGeometry": {
            "@type": "http://www.opengis.net/ont/sf#Point",
            "gsp:asWKT": {"@type": "gsp:wktLiteral", "@value": geometry_obj.wkt},
            "gsp:crs": {"@id": "http://www.opengis.net/def/crs/OGC/1.3/CRS84"},
        },
    }
    if timeseries is None or len(timeseries) == 0:
        return place
    else:
        place["subjectOf"] = []

    for ts in timeseries:
        parameter = ts["parameter_name"]
        unit = ts["unit_of_measure"]
        code = ts["parameter_code"]
        begin = ts["begin"]
        end = ts["end"]
        begin_clean = strip_fractional_seconds(begin)
        end_clean = strip_fractional_seconds(end)

        # Points is continuous, Daily is daily
        match ts["computation_period_identifier"]:
            case "Points":
                usgs_collection_name = "continuous"
            case "Daily":
                usgs_collection_name = "daily"
            case _:
                usgs_collection_name = None

        dataset = {
            "@type": "Dataset",
            "name": row.get("monitoring_location_number"),
            "description": f"{parameter} at {row['monitoring_location_name']}",
            "provider": {
                "@type": "GovernmentOrganization",
                "name": agency_name,
                "url": "https://www.usgs.gov/",
            },
            "url": f"https://api.waterdata.usgs.gov/ogcapi/v0/collections/time-series-metadata/items/{ts['id']}",
            "variableMeasured": {
                "@type": "PropertyValue",
                "name": parameter,
                "description": f"{parameter} in {unit}",
                "propertyID": str(code),
                "unitText": unit,
                "measurementTechnique": "observation",
                "measurementMethod": {
                    "name": f"{parameter} Measurements",
                    "publisher": agency_name,
                },
            },
        }
        if begin_clean and end_clean:
            temporalCoverage = f"{begin_clean}/{end_clean}"
            dataset["temporalCoverage"] = temporalCoverage
        elif begin_clean:
            temporalCoverage = f"{begin_clean}/.."
            dataset["temporalCoverage"] = temporalCoverage
        elif end_clean:
            temporalCoverage = f"../{end_clean}"
            dataset["temporalCoverage"] = temporalCoverage
        else:
            temporalCoverage = None

        if usgs_collection_name:
            base_distrib_url = f"https://api.waterdata.usgs.gov/ogcapi/v0/collections/{usgs_collection_name}/items?monitoring_location_id={id}&parameter_code={code}"
            if temporalCoverage:
                base_distrib_url += f"&time={temporalCoverage}"
            # only add a distribution link if
            # there is a corresponding USGS collection for it
            distribution = (
                [
                    {
                        "@type": "DataDownload",
                        "name": f"USGS Continuous Values for {parameter} at location {id} as CSV",
                        "contentUrl": f"{base_distrib_url}&f=csv",
                        "encodingFormat": ["text/comma-separated-values"],
                    },
                    {
                        "@type": "DataDownload",
                        "name": f"USGS Daily Values for {parameter} at location {id} as JSON",
                        "contentUrl": f"{base_distrib_url}&f=json",
                        "encodingFormat": ["application/json"],
                    },
                    {
                        "@type": "DataDownload",
                        "name": f"USGS Daily Values for {parameter} at location {id} as HTML",
                        "contentUrl": f"{base_distrib_url}&f=html",
                        "encodingFormat": ["text/html"],
                    },
                ],
            )
            dataset["distribution"] = distribution

        place["subjectOf"].append(dataset)

    # remove nulls (SHACL cleanliness)
    place = {k: v for k, v in place.items() if v is not None}

    return place


def get_parquet_file(file_location) -> Path:
    if Path(file_location).exists():
        LOGGER.info("Found parquet file locally")
        return Path(file_location)
    else:
        LOGGER.info(f"Downloading parquet file from {file_location}")
        with requests.get(file_location, stream=True) as response:
            response.raise_for_status()

            download_path = Path(__file__).parent / "monitoring_locations.parquet"
            with open(download_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
        return download_path


def process_row(row: dict) -> str:
    try:
        jsonld = row_to_jsonld(row)
        return json.dumps(jsonld, allow_nan=False)
    except ValueError as e:
        raise ValueError(f"Failed to serialize jsonld from row: {row}") from e


def main(file_location):
    parquet_file = get_parquet_file(file_location)
    pf = pq.ParquetFile(parquet_file)

    batch_size = 50000

    # Use most cores, but leave 1 free
    num_workers = max(cpu_count() - 1, 1)

    with Pool(processes=num_workers) as pool:
        for batch in pf.iter_batches(batch_size=batch_size):
            rows = batch.to_pylist()

            # Parallel map
            jsonld_records = pool.map(process_row, rows, chunksize=1000)

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
