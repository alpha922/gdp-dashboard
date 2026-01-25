import requests
import pandas as pd

# All non-seasonally adjusted retail sales volume series
RSI_SERIES = {
    "all_retailing": "RSI",
    "food_stores": "RSI2",
    "non_food_stores": "RSI3",
    "household_goods": "RSI4",
    "clothing": "RSI5",
    "department_stores": "RSI6",
    "fuel": "RSI7",
    "non_store_retailing": "RSI8",
}

def fetch_rsi_series(series_id: str) -> pd.DataFrame:
    """
    Fetch a single RSI series from the ONS API.
    """
    url = f"https://api.ons.gov.uk/timeseries/{series_id}/dataset/rsi/data"
    response = requests.get(url)
    response.raise_for_status()

    data = response.json()

    # Extract monthly observations
    observations = data["months"]

    df = pd.DataFrame(observations)
    df["series_id"] = series_id

    return df


def clean_rsi(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardise column names and convert types.
    """
    df = df.rename(columns={
        "date": "month",
        "value": "volume",
    })

    df["month"] = pd.to_datetime(df["month"], format="%Y-%m")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")

    # Drop missing values
    df = df.dropna(subset=["volume"])

    return df


def run_pipeline():
    """
    Fetch all RSI categories and combine into one DataFrame.
    """
    all_frames = []

    for category, series_id in RSI_SERIES.items():
        print(f"Fetching {category} ({series_id})...")
        df_raw = fetch_rsi_series(series_id)
        df_clean = clean_rsi(df_raw)
        df_clean["category"] = category
        all_frames.append(df_clean)

    df_final = pd.concat(all_frames, ignore_index=True)

    print("Preview of combined dataset:")
    print(df_final.head())

    # TODO: load into Supabase
    # load_to_supabase(df_final)

    return df_final


if __name__ == "__main__":
    run_pipeline()

