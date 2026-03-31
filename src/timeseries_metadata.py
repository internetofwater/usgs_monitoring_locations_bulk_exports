# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: MIT

import asyncio

import aiohttp
import duckdb

from lib import fetch_all_pages_of_oaf_endpoint


def make_timeseries_metadata_table(conn: duckdb.DuckDBPyConnection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS "time_series_metadata" (
            id                              VARCHAR PRIMARY KEY,
            unit_of_measure                 VARCHAR,
            parameter_name                  VARCHAR,
            parameter_code                  VARCHAR,
            statistic_id                    VARCHAR,
            hydrologic_unit_code            VARCHAR,
            state_name                      VARCHAR,
            last_modified                   TIMESTAMP,
            begin_time                      TIMESTAMP,
            end_time                        TIMESTAMP,
            begin_utc                       TIMESTAMPTZ,
            end_utc                         TIMESTAMPTZ,
            computation_period_identifier   VARCHAR,
            computation_identifier          VARCHAR,
            thresholds                      JSON,
            sublocation_identifier          VARCHAR,
            primary_flag                    VARCHAR,
            monitoring_location_id          VARCHAR,
            web_description                 VARCHAR,
            parameter_description           VARCHAR,
            parent_time_series_id           VARCHAR,
            geometry                        JSON
        );
    """)


async def fetch_all_timeseries_metadata(
    session: aiohttp.ClientSession, queue: asyncio.Queue
):
    url = "https://api.waterdata.usgs.gov/ogcapi/v0/collections/time-series-metadata/items"
    await fetch_all_pages_of_oaf_endpoint(session, queue, url, "time_series_metadata")
