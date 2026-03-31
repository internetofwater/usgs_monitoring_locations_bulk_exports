# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: MIT


def template_feature_to_jsonld(feature: dict) -> dict:
    _ = feature.get("")

    base_template = {
        "@context": {
            "schema": "https://schema.org/",
        },
        "@id": feature["id"],
    }
    return base_template
