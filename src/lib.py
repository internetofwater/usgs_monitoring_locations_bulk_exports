# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: MIT

import inspect
import json
from typing import Callable, Literal, Optional, Tuple, TypedDict
import aiohttp
import asyncio
import pyarrow as pa
import pyarrow.parquet as pq
from schemas import MONITORING_LOCATION_FIELDS_TO_TYPE, TIMESERIES_FIELDS


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
    assert key in location_typed_dict_keys, f"{key} missing in typed dict"


class TimeSeriesMetadataProperties(TypedDict):
    id: str
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


async def fetch_all_pages_of_oaf_endpoint(
    session: aiohttp.ClientSession,
    queue: asyncio.Queue,
    base_url: str,
    endpoint_name: str,
):
    current_page = 1
    MAX_USGS_LIMIT = 50000
    url = base_url

    while True:
        params = {"limit": MAX_USGS_LIMIT, "f": "json"}

        print(f"[{endpoint_name}] Fetching page {current_page}")

        async with session.get(url, params=params) as response:
            response.raise_for_status()
            data = await response.json()

        features: dict = data.get("features")
        await queue.put((endpoint_name, features))

        has_next, next_url = has_next_link(data)
        break
        if not has_next or not next_url:
            break

        url = next_url
        current_page += 1

    print(f"[{endpoint_name}] DONE\n\n")


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
