import asyncio
import os
import aiohttp
import duckdb

from monitoring_locations import fetch_all_monitoring_locations, make_monitoring_locations_table
from timeseries_metadata import fetch_all_timeseries_metadata, make_timeseries_metadata_table

import json
import asyncio
import duckdb


async def writer(conn: duckdb.DuckDBPyConnection, queue: asyncio.Queue):
    while True:
        batch = await queue.get()
        if batch is None:
            break

        table_name, features = batch
        print(f"Writing {len(features)} rows to duckdb table {table_name}")

        json_str = json.dumps({"type": "FeatureCollection", "features": features})

        match table_name:
            case "monitoring_locations":
                await asyncio.to_thread(
                    conn.execute,
                    """
                    INSERT OR IGNORE INTO "monitoring_locations"
                    SELECT
                        feature.properties.id::VARCHAR,
                        feature.properties.agency_code::VARCHAR,
                        feature.properties.agency_name::VARCHAR,
                        feature.properties.monitoring_location_number::VARCHAR,
                        feature.properties.monitoring_location_name::VARCHAR,
                        feature.properties.district_code::VARCHAR,
                        feature.properties.country_code::VARCHAR,
                        feature.properties.country_name::VARCHAR,
                        feature.properties.state_code::VARCHAR,
                        feature.properties.state_name::VARCHAR,
                        feature.properties.county_code::VARCHAR,
                        feature.properties.county_name::VARCHAR,
                        feature.properties.minor_civil_division_code::VARCHAR,
                        feature.properties.site_type_code::VARCHAR,
                        feature.properties.site_type::VARCHAR,
                        feature.properties.hydrologic_unit_code::VARCHAR,
                        feature.properties.basin_code::VARCHAR,
                        feature.properties.altitude::DOUBLE,
                        feature.properties.altitude_accuracy::DOUBLE,
                        feature.properties.altitude_method_code::VARCHAR,
                        feature.properties.altitude_method_name::VARCHAR,
                        feature.properties.vertical_datum::VARCHAR,
                        feature.properties.vertical_datum_name::VARCHAR,
                        feature.properties.horizontal_positional_accuracy_code::VARCHAR,
                        feature.properties.horizontal_positional_accuracy::VARCHAR,
                        feature.properties.horizontal_position_method_code::VARCHAR,
                        feature.properties.horizontal_position_method_name::VARCHAR,
                        feature.properties.original_horizontal_datum::VARCHAR,
                        feature.properties.original_horizontal_datum_name::VARCHAR,
                        feature.properties.drainage_area::DOUBLE,
                        feature.properties.contributing_drainage_area::DOUBLE,
                        feature.properties.time_zone_abbreviation::VARCHAR,
                        feature.properties.uses_daylight_savings::VARCHAR,
                        feature.properties.construction_date::VARCHAR,
                        feature.properties.aquifer_code::VARCHAR,
                        feature.properties.national_aquifer_code::VARCHAR,
                        feature.properties.aquifer_type_code::VARCHAR,
                        feature.properties.well_constructed_depth::DOUBLE,
                        feature.properties.hole_constructed_depth::DOUBLE,
                        feature.properties.depth_source_code::VARCHAR,
                        feature.properties.revision_note::VARCHAR,
                        feature.properties.revision_created::TIMESTAMP,
                        feature.properties.revision_modified::TIMESTAMP,
                        CASE
                            WHEN feature.geometry IS NULL THEN NULL
                            ELSE feature.geometry::JSON
                        END
                    FROM read_json_auto(?) t,
                    UNNEST(t.features) AS f(feature)
                    """,
                    [json_str],
                )
            case "time-series-metadata":
                await asyncio.to_thread(
                conn.execute,
                    """
                    INSERT OR IGNORE INTO "time_series_metadata"
                    SELECT
                        feature.properties.id::VARCHAR,
                        feature.properties.unit_of_measure::VARCHAR,
                        feature.properties.parameter_name::VARCHAR,
                        feature.properties.parameter_code::VARCHAR,
                        feature.properties.statistic_id::VARCHAR,
                        feature.properties.hydrologic_unit_code::VARCHAR,
                        feature.properties.state_name::VARCHAR,
                        feature.properties.last_modified::TIMESTAMP,
                        feature.properties.begin::TIMESTAMP,
                        feature.properties.end::TIMESTAMP,
                        feature.properties.begin_utc::TIMESTAMPTZ,
                        feature.properties.end_utc::TIMESTAMPTZ,
                        feature.properties.computation_period_identifier::VARCHAR,
                        feature.properties.computation_identifier::VARCHAR,
                        feature.properties.thresholds::JSON,
                        feature.properties.sublocation_identifier::VARCHAR,
                        feature.properties.primary::VARCHAR,
                        feature.properties.monitoring_location_id::VARCHAR,
                        feature.properties.web_description::VARCHAR,
                        feature.properties.parameter_description::VARCHAR,
                        feature.properties.parent_time_series_id::VARCHAR,
                        CASE
                            WHEN feature.geometry IS NULL THEN NULL
                            ELSE feature.geometry::JSON
                        END
                    FROM read_json_auto(?) t,
                    UNNEST(t.features) AS f(feature)
                    """,
                    [json_str],
                )
            case _:
                raise ValueError(f"Unknown table name {table_name}")

        print(f"Finished writing to {table_name}")
        queue.task_done()


async def main():

    USGS_API_KEY = os.environ.get("USGS_API_KEY")
    assert USGS_API_KEY, (
        "The USGS API key must be set in the USGS_API_KEY environment variable"
    )

    # Use file DB for safety / spilling to disk
    with duckdb.connect("usgs.duckdb") as conn:
        make_monitoring_locations_table(conn)
        make_timeseries_metadata_table(conn)

        conn.sql("SET preserve_insertion_order = false;")

        print("Finished creating tables in duckdb")

        async with aiohttp.ClientSession(headers={"X-Api-Key": USGS_API_KEY}) as session:
            queue = asyncio.Queue(maxsize=100)

            writer_task = asyncio.create_task(writer(conn, queue))

            try: 
                await asyncio.gather(
                    fetch_all_monitoring_locations(session, queue),
                    fetch_all_timeseries_metadata(session, queue),
                    return_exceptions=True,
                )
            finally:
                await queue.put(None)
            await writer_task
            print("Done writing to duckdb tables")

if __name__ == "__main__":
    asyncio.run(main())
