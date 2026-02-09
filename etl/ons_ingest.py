import requests
import pandas as pd

DATASET = "https://api.beta.ons.gov.uk/v1/datasets/drsi"

def get_latest_version_url():
    r = requests.get(DATASET)
    r.raise_for_status()
    dataset = r.json()

    edition_url = dataset["links"]["editions"][0]["href"]

    r = requests.get(edition_url)
    r.raise_for_status()
    edition = r.json()

    version_url = edition["links"]["latest_version"]["href"]
    return version_url

def get_retail_sectors(version_url: str):
    r = requests.get(f"{version_url}/dimensions")
    r.raise_for_status()
    dims = r.json()

    retail_dim = next(d for d in dims["items"] if d["name"] == "retailsector")

    r = requests.get(retail_dim["links"]["options"]["href"])
    r.raise_for_status()
    options = r.json()

    return {
        o["label"].lower(): o["id"]
        for o in options["items"]
    }

def fetch_series(version_url: str, sector_code: str):
    r = requests.get(
        f"{version_url}/observations",
        params={
            "time": "*",
            "retailsector": sector_code,
            "measure": "volume",
            "seasonaladjustment": "nonseasonallyadjusted",
        },
    )
    r.raise_for_status()
    data = r.json()

    return pd.DataFrame(
        {
            "month": o["dimensions"]["time"],
            "value": o["observation"],
            "sector": o["dimensions"]["retailsector"],
        }
        for o in data["observations"]
    )

def run():
    version_url = get_latest_version_url()
    print("Using:", version_url)

    sectors = get_retail_sectors(version_url)

    df = fetch_series(version_url, sectors["all retailing"])
    df["month"] = pd.to_datetime(df["month"])
    df["value"] = pd.to_numeric(df["value"])

    print(df.head())
    return df

if __name__ == "__main__":
    run()
