"""@bruin

name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: VendorID
    type: integer
    description: "A code indicating the TPEP provider that provided the record"
  - name: tpep_pickup_datetime
    type: timestamp
    description: "The date and time when the meter was engaged"
  - name: tpep_dropoff_datetime
    type: timestamp
    description: "The date and time when the meter was disengaged"
  - name: passenger_count
    type: integer
    description: "The number of passengers in the vehicle"
  - name: trip_distance
    type: float
    description: "The elapsed trip distance in miles"
  - name: RatecodeID
    type: integer
    description: "The final rate code in effect at the end of the trip"
  - name: store_and_fwd_flag
    type: string
    description: "Y/N flag indicating whether trip was stored and forwarded"
  - name: PULocationID
    type: integer
    description: "Pickup location ID"
  - name: DOLocationID
    type: integer
    description: "Dropoff location ID"
  - name: payment_type
    type: integer
    description: "A numeric code signifying how the passenger paid"
  - name: fare_amount
    type: float
    description: "The time and distance fare"
  - name: extra
    type: float
    description: "Miscellaneous extras (rush hour, tolls, etc.)"
  - name: mta_tax
    type: float
    description: "MTA tax of $0.50"
  - name: tip_amount
    type: float
    description: "Tip amount for the ride"
  - name: tolls_amount
    type: float
    description: "Total amount of tolls paid"
  - name: total_amount
    type: float
    description: "Total amount charged to the passenger"
  - name: pickup_datetime
    type: timestamp
    description: "Normalized pickup datetime (alias for tpep_pickup_datetime)"
  - name: extracted_at
    type: timestamp
    description: "Timestamp when the data was extracted"

@bruin"""

import os
import json
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import requests
from io import BytesIO


def materialize():
    """
    Fetch NYC Taxi data from TLC public endpoint and return as DataFrame.

    The function:
    - Uses BRUIN_START_DATE and BRUIN_END_DATE to determine the date range
    - Reads taxi_types from BRUIN_VARS pipeline variable
    - Fetches parquet files from the TLC endpoint for each taxi type + month combination
    - Adds an extracted_at timestamp for lineage tracking
    - Returns raw data with minimal transformations (duplicates handled in staging)
    """

    # Get environment variables set by Bruin
    start_date_str = os.getenv("BRUIN_START_DATE")
    end_date_str = os.getenv("BRUIN_END_DATE")
    bruin_vars_json = os.getenv("BRUIN_VARS", "{}")

    # Parse dates
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    # Parse pipeline variables
    bruin_vars = json.loads(bruin_vars_json)
    taxi_types = bruin_vars.get("taxi_types", ["yellow"])

    # NYC Taxi data endpoint
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

    # Collect all DataFrames to concatenate
    dfs = []

    # Generate list of (year, month, taxi_type) tuples to fetch
    current_date = start_date
    while current_date <= end_date:
        year = current_date.year
        month = current_date.month

        for taxi_type in taxi_types:
            # Build the parquet file URL
            filename = f"{taxi_type}_tripdata_{year:04d}-{month:02d}.parquet"
            url = base_url + filename

            try:
                # Fetch the parquet file
                response = requests.get(url, timeout=30)
                response.raise_for_status()

                # Read parquet into DataFrame
                df = pd.read_parquet(BytesIO(response.content))

                # Normalize column names to lowercase for consistency
                df.columns = df.columns.str.lower()

                # Add taxi_type column if not already present
                if "taxi_type" not in df.columns:
                    df["taxi_type"] = taxi_type

                # Create a normalized pickup_datetime column (handles both yellow and green taxi naming)
                if "tpep_pickup_datetime" in df.columns:
                    df["pickup_datetime"] = df["tpep_pickup_datetime"]
                elif "lpep_pickup_datetime" in df.columns:
                    df["pickup_datetime"] = df["lpep_pickup_datetime"]
                    # Rename green taxi columns to match yellow taxi convention
                    df = df.rename(columns={
                        "lpep_pickup_datetime": "tpep_pickup_datetime",
                        "lpep_dropoff_datetime": "tpep_dropoff_datetime"
                    })

                # Add extraction timestamp for lineage
                df["extracted_at"] = datetime.utcnow()

                dfs.append(df)
                print(f"✓ Fetched {taxi_type} data for {year:04d}-{month:02d}: {len(df)} rows")

            except requests.exceptions.RequestException as e:
                print(f"⚠ Failed to fetch {filename}: {e}")
                continue

        # Move to next month
        current_date += relativedelta(months=1)

    # Concatenate all DataFrames
    if dfs:
        final_df = pd.concat(dfs, ignore_index=True)
        print(f"\nTotal rows fetched: {len(final_df)}")
        return final_df
    else:
        # Return empty DataFrame with expected schema if no data found
        return pd.DataFrame()


