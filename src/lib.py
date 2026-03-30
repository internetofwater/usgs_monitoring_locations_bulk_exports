import json
from typing import Callable, Literal, Tuple, TypedDict
import aiohttp
import asyncio


class OafResponse(TypedDict):
    links: list[dict[str, str]]
    type: Literal["FeatureCollection"]
    features: list[dict]


def has_next_link(response: OafResponse) -> Tuple[bool, str]:
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
    endpoint_name: Literal["monitoring_locations", "time_series_metadata"],
):
    current_page = 1
    MAX_USGS_LIMIT = 50000
    url = base_url
    while True:
        params = {"limit": MAX_USGS_LIMIT, "f": "json"}
        print(f"Fetching page {current_page} of {endpoint_name} with url {url} and params {params}")
        async with session.get(url, params=params) as response:
            response.raise_for_status()
            data: OafResponse = await response.json()

        features = data.get("features")
        assert features, f"No features returned from {url} with params {params}"

        # send to writer
        await queue.put((endpoint_name, features))

        break
        # pagination
        has_next, url = has_next_link(data)
        if not has_next:
            break
        current_page += 1
    print(f"Completed fetching all pages of {endpoint_name}")
