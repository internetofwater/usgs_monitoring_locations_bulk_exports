# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: MIT

import hashlib
import inspect
import json
import os
from pathlib import Path
from typing import Callable, Literal, Optional, Tuple, TypedDict
import aiohttp
import asyncio
import pyarrow as pa
import pyarrow.parquet as pq
from schemas import MONITORING_LOCATION_FIELDS_TO_TYPE, TIMESERIES_FIELDS
import logging

LOGGER = logging.getLogger(__name__)


class MonitoringLocationProperties(TypedDict):
    id: str
    agency_code: str
    agency_name: str
    monitoring_location_number: str
    monitoring_location_name: str
    district_code: str
    country_code: str
    country_name: str
    state_code: str
    state_name: str
    county_code: str
    county_name: str

    minor_civil_division_code: Optional[str]

    site_type_code: str
    site_type: str

    hydrologic_unit_code: Optional[str]
    basin_code: Optional[str]

    altitude: Optional[float]
    altitude_accuracy: Optional[str]
    altitude_method_code: Optional[str]
    altitude_method_name: Optional[str]

    vertical_datum: Optional[str]
    vertical_datum_name: Optional[str]

    horizontal_positional_accuracy_code: Optional[str]
    horizontal_positional_accuracy: Optional[str]
    horizontal_position_method_code: Optional[str]
    horizontal_position_method_name: Optional[str]

    original_horizontal_datum: Optional[str]
    original_horizontal_datum_name: Optional[str]

    drainage_area: Optional[float]
    contributing_drainage_area: Optional[float]

    time_zone_abbreviation: str
    uses_daylight_savings: Literal["Y", "N"]

    construction_date: Optional[str]

    aquifer_code: Optional[str]
    national_aquifer_code: Optional[str]
    aquifer_type_code: Optional[str]

    well_constructed_depth: Optional[float]
    hole_constructed_depth: Optional[float]
    depth_source_code: Optional[str]

    revision_note: Optional[str]
    revision_created: Optional[str]
    revision_modified: Optional[str]


location_typed_dict_keys = inspect.get_annotations(MonitoringLocationProperties).keys()
assert len(location_typed_dict_keys) >= len(MONITORING_LOCATION_FIELDS_TO_TYPE)
for key in MONITORING_LOCATION_FIELDS_TO_TYPE:
    assert key in location_typed_dict_keys or key == "geometry", (
        f"{key} missing in typed dict"
    )


class TimeSeriesMetadataProperties(TypedDict):
    id: str
    monitoring_location_id: str
    unit_of_measure: str
    parameter_name: str
    parameter_code: str
    parameter_description: str
    computation_period_identifier: str
    computation_identifier: str
    statistic_id: str
    hydrologic_unit_code: str
    state_name: str
    last_modified: str
    begin: str
    end: str
    begin_utc: str
    end_utc: str


time_series_typed_dict_keys = inspect.get_annotations(
    TimeSeriesMetadataProperties
).keys()
assert len(time_series_typed_dict_keys) == len(TIMESERIES_FIELDS)
for key in time_series_typed_dict_keys:
    assert key in TIMESERIES_FIELDS


class GeojsonFeature(TypedDict):
    type: Literal["Feature"]
    geometry: dict
    properties: MonitoringLocationProperties | TimeSeriesMetadataProperties


class OafResponse(TypedDict):
    links: list[dict[str, str]]
    type: Literal["FeatureCollection"]
    features: list[GeojsonFeature]


def has_next_link(response: OafResponse) -> Tuple[bool, str]:
    """
    Tries to follow OGC API 'next' link if present.
    """
    for link in response["links"]:
        if link["rel"] == "next":
            return True, link["href"]
    return False, ""


def template_feature_collection(response: OafResponse, template_fn: Callable) -> str:
    templates: list[dict] = []
    for feature in response["features"]:
        templates.append(template_fn(feature))

    concatenated_features = ""
    for template in templates:
        as_str = json.dumps(template)
        concatenated_features += f"{as_str}\n"
    return concatenated_features


def _cache_key(url: str, params: dict) -> str:
    raw = url + "?" + "&".join(f"{k}={params[k]}" for k in sorted(params))
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def _cache_path(cache_dir: str, key: str) -> Path:
    return Path(cache_dir) / f"{key}.json"


async def fetch_all_pages_of_oaf_endpoint(
    session: aiohttp.ClientSession,
    queue: asyncio.Queue,
    base_url: str,
    endpoint_name: str,
):
    cache_dir = "/tmp/oaf_cache"
    os.makedirs(cache_dir, exist_ok=True)

    current_page = 1
    MAX_USGS_LIMIT = 50000
    url = base_url

    while True:
        params = {"limit": MAX_USGS_LIMIT, "f": "json"}

        cache_key = _cache_key(url, params)
        cache_file = _cache_path(cache_dir, cache_key)

        LOGGER.info(f"[{endpoint_name}] Fetching page {current_page}")

        if cache_file.exists():
            LOGGER.info(f"[{endpoint_name}] Cache hit -> {cache_file}")
            with open(cache_file, "r", encoding="utf-8") as f:
                data = json.load(f)
        else:
            async with session.get(url, params=params) as response:
                response.raise_for_status()
                data = await response.json()

            # write cache to disk for next time
            # if running in github actions skip this
            # since workers are ephemeral; in the future could use github actions cache
            # but for production we may want to fetch fresh each time anyways
            if not os.environ.get("GITHUB_ACTIONS"):
                with open(cache_file, "w", encoding="utf-8") as f:
                    json.dump(data, f)

        features = data.get("features", [])
        await queue.put((endpoint_name, features))

        has_next, next_url = has_next_link(data)

        TEST_MODE = os.environ.get("TEST_MODE")
        if TEST_MODE:
            LOGGER.warning("Fetching subset of data since env var TEST_MODE was set")
            break

        if not has_next or not next_url:
            break

        url = next_url
        current_page += 1

    LOGGER.info(f"[{endpoint_name}] Done fetching data")


class ParquetFeatureWriter:
    def __init__(self, path: str, schema: pa.Schema):
        self.path = path
        self.schema = schema
        self.writer = pq.ParquetWriter(
            self.path,
            self.schema,
            compression="snappy",
        )

    def write(self, rows: list[dict]):
        if not rows:
            return

        table = pa.Table.from_pylist(rows, schema=self.schema)
        self.writer.write_table(table)

    def close(self):
        if self.writer:
            self.writer.close()
