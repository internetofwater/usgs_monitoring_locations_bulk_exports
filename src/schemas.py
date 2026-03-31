from typing import Final

import pyarrow as pa

MONITORING_LOCATION_FIELDS_TO_TYPE: dict[str, pa.DataType] = {
    "id": pa.string(),
    "agency_code": pa.string(),
    "agency_name": pa.string(),
    "monitoring_location_number": pa.string(),
    "monitoring_location_name": pa.string(),
    "district_code": pa.string(),
    "country_code": pa.string(),
    "country_name": pa.string(),
    "state_code": pa.string(),
    "state_name": pa.string(),
    "county_code": pa.string(),
    "county_name": pa.string(),
    "minor_civil_division_code": pa.string(),
    "site_type_code": pa.string(),
    "site_type": pa.string(),
    "hydrologic_unit_code": pa.string(),
    "basin_code": pa.string(),
    "altitude": pa.float64(),
    # Commented out some fields because they add extra columns but are not useful for our purposes
    # "altitude_accuracy": pa.string(),
    # "altitude_method_code": pa.string(),
    # "altitude_method_name": pa.string(),
    # "vertical_datum": pa.string(),
    # "vertical_datum_name": pa.string(),
    # "horizontal_positional_accuracy_code": pa.string(),
    # "horizontal_positional_accuracy": pa.string(),
    # "horizontal_position_method_code": pa.string(),
    # "horizontal_position_method_name": pa.string(),
    # "original_horizontal_datum": pa.string(),
    # "original_horizontal_datum_name": pa.string(),
    "drainage_area": pa.float64(),
    "contributing_drainage_area": pa.float64(),
    "time_zone_abbreviation": pa.string(),
    "uses_daylight_savings": pa.string(),
    "construction_date": pa.string(),
    "aquifer_code": pa.string(),
    "national_aquifer_code": pa.string(),
    "aquifer_type_code": pa.string(),
    "well_constructed_depth": pa.float64(),
    "hole_constructed_depth": pa.float64(),
    "depth_source_code": pa.string(),
    "revision_note": pa.string(),
    "revision_created": pa.string(),
    "revision_modified": pa.string(),
}


def monitoring_locations_schema() -> pa.Schema:
    fields = []
    for field in MONITORING_LOCATION_FIELDS_TO_TYPE:
        fields.append(pa.field(field, MONITORING_LOCATION_FIELDS_TO_TYPE[field]))

    return pa.schema(fields)


TIMESERIES_FIELDS: Final[list[str]] = [
    "id",
    "unit_of_measure",
    "parameter_name",
    "parameter_code",
    "parameter_description",
    "computation_period_identifier",
    "computation_identifier",
    "statistic_id",
    "hydrologic_unit_code",
    "state_name",
    "last_modified",
    "begin",
    "end",
    "begin_utc",
    "end_utc",
]


def timeseries_schema() -> pa.Schema:
    fields: list[pa.field] = []
    for field in TIMESERIES_FIELDS:
        fields.append(pa.field(field, pa.string()))
    return pa.schema(fields)
