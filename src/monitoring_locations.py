import asyncio

import aiohttp
import duckdb

from lib import fetch_all_pages_of_oaf_endpoint


def template_feature_to_jsonld(feature: dict) -> dict:
    url = feature.get("")

    base_template = {
        "@context": {
            "schema": "https://schema.org/",
        },
        "@id": feature["id"],
    }
    return base_template


def make_monitoring_locations_table(conn: duckdb.DuckDBPyConnection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS monitoring_locations (
            id                                  VARCHAR PRIMARY KEY,
            agency_code                         VARCHAR,
            agency_name                         VARCHAR,
            monitoring_location_number          VARCHAR,
            monitoring_location_name            VARCHAR,
            district_code                       VARCHAR,
            country_code                        VARCHAR,
            country_name                        VARCHAR,
            state_code                          VARCHAR,
            state_name                          VARCHAR,
            county_code                         VARCHAR,
            county_name                         VARCHAR,
            minor_civil_division_code           VARCHAR,
            site_type_code                      VARCHAR,
            site_type                           VARCHAR,
            hydrologic_unit_code                VARCHAR,
            basin_code                          VARCHAR,
            altitude                            DOUBLE,
            altitude_accuracy                   DOUBLE,
            altitude_method_code                VARCHAR,
            altitude_method_name                VARCHAR,
            vertical_datum                      VARCHAR,
            vertical_datum_name                 VARCHAR,
            horizontal_positional_accuracy_code VARCHAR,
            horizontal_positional_accuracy      VARCHAR,
            horizontal_position_method_code     VARCHAR,
            horizontal_position_method_name     VARCHAR,
            original_horizontal_datum           VARCHAR,
            original_horizontal_datum_name      VARCHAR,
            drainage_area                       DOUBLE,
            contributing_drainage_area          DOUBLE,
            time_zone_abbreviation              VARCHAR,
            uses_daylight_savings               VARCHAR,
            construction_date                   VARCHAR,
            aquifer_code                        VARCHAR,
            national_aquifer_code               VARCHAR,
            aquifer_type_code                   VARCHAR,
            well_constructed_depth              DOUBLE,
            hole_constructed_depth              DOUBLE,
            depth_source_code                   VARCHAR,
            revision_note                       VARCHAR,
            revision_created                    TIMESTAMP,
            revision_modified                   TIMESTAMP,
            geometry                            JSON
        );
    """)


async def fetch_all_monitoring_locations(
    session: aiohttp.ClientSession, queue: asyncio.Queue
):
    url = "https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items"
    await fetch_all_pages_of_oaf_endpoint(session, queue, url, "monitoring_locations")
