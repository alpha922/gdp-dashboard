import gzip
import json
import requests
import pandas as pd
from io import BytesIO


# Paste the "Download GeoJSON" URL from alltheplaces.xyz here
LATEST_GEOJSON_URL = "https://alltheplaces-data.openaddresses.io/runs/2026-01-31-13-32-31/output.zip"  # e.g. "https://results.alltheplaces.xyz/latest.geojson.gz"


def fetch_wetherspoons_locations() -> pd.DataFrame:
    """
    Download the latest AllThePlaces GeoJSON NDJSON.gz,
    filter for JD Wetherspoon locations, and return as a pandas DataFrame.
    """

    resp = requests.get(LATEST_GEOJSON_URL, stream=True)
    resp.raise_for_status()
    

    # Decompress gzipped NDJSON
    gz = gzip.GzipFile(fileobj=BytesIO(resp.content))

    rows = []

    for line in gz:
        if not line.strip():
            continue

        feature = json.loads(line)
        props = feature.get("properties", {})
        geom = feature.get("geometry", {})

        brand = props.get("brand", "") or ""
        name = props.get("name", "") or ""

        # Filter for JD Wetherspoon
        if "wetherspoon" not in brand.lower() and "wetherspoon" not in name.lower():
            continue

        coords = geom.get("coordinates") or [None, None]

        rows.append(
            {
                "name": name,
                "brand": brand,
                "brand_wikidata": props.get("brand:wikidata"),
                "street": props.get("addr:street"),
                "city": props.get("addr:city"),
                "postcode": props.get("addr:postcode"),
                "country": props.get("addr:country"),
                "latitude": coords[1],
                "longitude": coords[0],
                "raw_properties": props,
            }
        )

    df = pd.DataFrame(rows)
    return df


if __name__ == "__main__":
    df = fetch_wetherspoons_locations()
    print(df.head())
    print(f"Loaded {len(df)} Wetherspoons locations.")
