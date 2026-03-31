# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: MIT

import asyncio
import os
from typing import Any, Dict, Tuple, cast

import aiohttp
import shapely
import duckdb

from lib import (
    GeojsonFeature,
    MonitoringLocationProperties,
    ParquetFeatureWriter,
    TimeSeriesMetadataProperties,
    fetch_all_pages_of_oaf_endpoint,
)
from schemas import monitoring_locations_schema, timeseries_schema
from shapely.geometry import shape
from dotenv import load_dotenv


def monitoring_location_to_row(
    feature: GeojsonFeature,
) -> dict[str, str | float | bytes | None]:
    props = cast(MonitoringLocationProperties, feature["properties"])
    geometry = feature.get("geometry")
    if geometry:
        geometry = shape(geometry).wkb
    else:
        geometry = shapely.from_wkt("POINT EMPTY").wkb

    return {
        "id": props["id"],
        "agency_code": props["agency_code"],
        "agency_name": props["agency_name"],
        "monitoring_location_number": props["monitoring_location_number"],
        "monitoring_location_name": props["monitoring_location_name"],
        "district_code": props["district_code"],
        "country_code": props["country_code"],
        "country_name": props["country_name"],
        "state_code": props["state_code"],
        "state_name": props["state_name"],
        "county_code": props["county_code"],
        "county_name": props["county_name"],
        "minor_civil_division_code": props["minor_civil_division_code"],
        "site_type_code": props["site_type_code"],
        "site_type": props["site_type"],
        "hydrologic_unit_code": props["hydrologic_unit_code"],
        "basin_code": props["basin_code"],
        "altitude": props["altitude"],
        "drainage_area": props["drainage_area"],
        "contributing_drainage_area": props["contributing_drainage_area"],
        "time_zone_abbreviation": props["time_zone_abbreviation"],
        "uses_daylight_savings": props["uses_daylight_savings"],
        "contruction_date": props["construction_date"],
        "aquifer_code": props["aquifer_code"],
        "national_aquifer_code": props["national_aquifer_code"],
        "aquifer_type_code": props["aquifer_type_code"],
        "well_constructed_depth": props["well_constructed_depth"],
        "hole_constructed_depth": props["hole_constructed_depth"],
        "depth_source_code": props["depth_source_code"],
        "revision_note": props["revision_note"],
        "revision_created": props["revision_created"],
        "revision_modified": props["revision_modified"],
        "geometry": geometry,
    }


def timeseries_metadata_to_row(feature: Dict[str, Any]) -> dict[str, str]:
    props: TimeSeriesMetadataProperties = feature["properties"]
    return {
        "id": feature["id"],
        "monitoring_location_id": props["monitoring_location_id"],
        "unit_of_measure": props["unit_of_measure"],
        "parameter_name": props["parameter_name"],
        "parameter_code": props["parameter_code"],
        "statistic_id": props["statistic_id"],
        "hydrologic_unit_code": props["hydrologic_unit_code"],
        "state_name": props["state_name"],
        "last_modified": props["last_modified"],
        "begin": props["begin"],
        "end": props["end"],
        "begin_utc": props["begin_utc"],
        "end_utc": props["end_utc"],
    }


async def parquet_writer_worker(
    queue: asyncio.Queue,
    ml_writer: ParquetFeatureWriter,
    ts_writer: ParquetFeatureWriter,
):
    BATCH_SIZE = 5000
    buffer_ml: list[dict] = []
    buffer_ts: list[dict] = []

    while True:
        item = await queue.get()

        if item is None:
            break

        endpoint, features = item

        if endpoint == "monitoring_locations":
            buffer_ml.extend(monitoring_location_to_row(f) for f in features)

            if len(buffer_ml) >= BATCH_SIZE:
                ml_writer.write(buffer_ml)
                buffer_ml.clear()

        elif endpoint == "time_series_metadata":
            buffer_ts.extend(timeseries_metadata_to_row(f) for f in features)

            if len(buffer_ts) >= BATCH_SIZE:
                ts_writer.write(buffer_ts)
                buffer_ts.clear()
        else:
            raise ValueError(f"Unknown endpoint: {endpoint}")

        queue.task_done()

    if buffer_ml:
        ml_writer.write(buffer_ml)

    if buffer_ts:
        ts_writer.write(buffer_ts)

    ml_writer.close()
    ts_writer.close()
    print("Completed writing parquet files")


async def fetch_monitoring_locations(session, queue):
    url = "https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items"
    await fetch_all_pages_of_oaf_endpoint(session, queue, url, "monitoring_locations")


async def fetch_timeseries(session, queue):
    url = "https://api.waterdata.usgs.gov/ogcapi/v0/collections/time-series-metadata/items"
    await fetch_all_pages_of_oaf_endpoint(session, queue, url, "time_series_metadata")


def join_and_write_parquet(
    ml_parquet_path: str,
    ts_parquet_path: str,
    output_parquet_path: str,
):
    con = duckdb.connect()
    print(
        f"Joining parquet files {ml_parquet_path} and {ts_parquet_path} to {output_parquet_path}"
    )
    query = f"""
        COPY (
            WITH ml AS (
                SELECT *
                FROM read_parquet('{ml_parquet_path}')
            ),
            ts AS (
                SELECT *
                FROM read_parquet('{ts_parquet_path}')
            )

            SELECT
                ml.*,
                ts_agg.timeseries_metadata
            FROM ml
            LEFT JOIN (
                SELECT
                    monitoring_location_id,
                    list(
                        struct_pack(
                            id,
                            unit_of_measure,
                            parameter_name,
                            parameter_code,
                            statistic_id,
                            last_modified,
                            begin,
                            "end",
                            begin_utc,
                            end_utc
                        )
                    ) AS timeseries_metadata
                FROM ts
                GROUP BY monitoring_location_id
            ) ts_agg
            ON ml.id = ts_agg.monitoring_location_id
        )
        TO '{output_parquet_path}'
        (FORMAT PARQUET, COMPRESSION ZSTD);
    """

    con.execute(query)
    con.close()


async def main():
    queue: asyncio.Queue[None | Tuple[str, list[dict]]] = asyncio.Queue()

    ml_writer = ParquetFeatureWriter(
        "monitoring_locations.parquet",
        monitoring_locations_schema(),
    )

    ts_writer = ParquetFeatureWriter(
        "time_series_metadata.parquet",
        timeseries_schema(),
    )

    USGS_API_KEY = os.environ.get("USGS_API_KEY")
    if not USGS_API_KEY:
        print("WARNING: USGS_API_KEY not set in .env")

    async with aiohttp.ClientSession(
        headers={"X-Api-Key": USGS_API_KEY or ""}
    ) as session:
        producers = [
            asyncio.create_task(fetch_monitoring_locations(session, queue)),
            asyncio.create_task(fetch_timeseries(session, queue)),
        ]

        consumer = asyncio.create_task(
            parquet_writer_worker(queue, ml_writer, ts_writer)
        )

        await asyncio.gather(*producers)

        await queue.put(None)
        await consumer

    join_and_write_parquet(
        "monitoring_locations.parquet",
        "time_series_metadata.parquet",
        "joined.parquet",
    )


if __name__ == "__main__":
    load_dotenv()
    asyncio.run(main())
