/* @bruin

name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: date

columns:
  - name: trip_id
    type: varchar
    description: "Unique identifier for each trip (vendor_id + tpep_pickup_datetime + passenger_count hash)"
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: vendor_id
    type: integer
    description: "TPEP provider code (1=Creative Mobile, 2=VeriFone)"
    nullable: false
    checks:
      - name: not_null
  - name: tpep_pickup_datetime
    type: timestamp
    description: "Trip start time"
    nullable: false
    checks:
      - name: not_null
  - name: tpep_dropoff_datetime
    type: timestamp
    description: "Trip end time"
    nullable: false
    checks:
      - name: not_null
  - name: passenger_count
    type: integer
    description: "Number of passengers"
    nullable: false
    checks:
      - name: not_null
      - name: positive
  - name: trip_distance
    type: double
    description: "Trip distance in miles"
    nullable: false
    checks:
      - name: not_null
      - name: positive
  - name: ratecode_id
    type: integer
    description: "Final rate code"
    nullable: true
  - name: store_and_fwd_flag
    type: varchar
    description: "Store and forward flag (Y/N)"
    nullable: true
  - name: pulocation_id
    type: integer
    description: "Pickup location ID"
    nullable: true
  - name: dolocation_id
    type: integer
    description: "Dropoff location ID"
    nullable: true
  - name: payment_type
    type: integer
    description: "Payment type code"
    nullable: false
    checks:
      - name: not_null
  - name: payment_type_name
    type: varchar
    description: "Payment type name from lookup table"
    nullable: true
  - name: fare_amount
    type: double
    description: "Base fare amount"
    nullable: false
    checks:
      - name: not_null
      - name: non_negative
  - name: extra
    type: double
    description: "Extra charges (rush hour, tolls)"
    nullable: false
    checks:
      - name: not_null
      - name: non_negative
  - name: mta_tax
    type: double
    description: "MTA tax amount"
    nullable: false
    checks:
      - name: not_null
      - name: non_negative
  - name: tip_amount
    type: double
    description: "Tip amount"
    nullable: false
    checks:
      - name: not_null
      - name: non_negative
  - name: tolls_amount
    type: double
    description: "Tolls amount"
    nullable: false
    checks:
      - name: not_null
      - name: non_negative
  - name: total_amount
    type: double
    description: "Total amount charged"
    nullable: false
    checks:
      - name: not_null
      - name: positive
  - name: pickup_datetime
    type: timestamp
    description: "Normalized pickup datetime for time-windowing"
    nullable: false
    checks:
      - name: not_null
  - name: taxi_type
    type: varchar
    description: "Taxi type (yellow, green)"
    nullable: true
  - name: extracted_at
    type: timestamp
    description: "Timestamp when data was extracted"
    nullable: true

quality_checks:
  - name: not_null
    metric: null_count
    value: 0
  - name: positive
    metric: positive_count
    value: 0
  - name: non_negative
    metric: non_negative_count
    value: 0

custom_checks:
  - name: no_duplicate_trips
    description: "Ensures all trip_id values are unique (no duplicates)"
    query: |
      SELECT COUNT(*) = COUNT(DISTINCT trip_id) FROM staging.trips
      WHERE pickup_datetime >= '{{ start_datetime }}'
        AND pickup_datetime < '{{ end_datetime }}'
    value: 1

  - name: payment_type_coverage
    description: "Ensures most payment types are enriched with lookup names"
    query: |
      SELECT
        CAST(
          COUNT(CASE WHEN payment_type_name IS NOT NULL THEN 1 END) AS FLOAT
        ) / COUNT(*) > 0.95
      FROM staging.trips
      WHERE pickup_datetime >= '{{ start_datetime }}'
        AND pickup_datetime < '{{ end_datetime }}'
    value: 1

@bruin */

-- Staging: Clean, deduplicate, and enrich raw trip data
--
-- Key transformations:
-- 1. Deduplicate using ROW_NUMBER (keep first occurrence by pickup_datetime)
-- 2. Filter to valid records (required fields not null)
-- 3. Join with payment_lookup to enrich payment_type with names
-- 4. Generate trip_id as primary key
-- 5. Filter to the time window for incremental processing

WITH deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY vendorid, tpep_pickup_datetime, passenger_count
      ORDER BY extracted_at DESC
    ) AS rn
  FROM ingestion.trips
  WHERE pickup_datetime >= '{{ start_datetime }}'
    AND pickup_datetime < '{{ end_datetime }}'
    -- Filter out invalid records
    AND vendorid IS NOT NULL
    AND tpep_pickup_datetime IS NOT NULL
    AND tpep_dropoff_datetime IS NOT NULL
    AND passenger_count IS NOT NULL
    AND passenger_count > 0
    AND trip_distance IS NOT NULL
    AND trip_distance > 0
    AND payment_type IS NOT NULL
    AND total_amount IS NOT NULL
    AND total_amount > 0
)

SELECT
  -- Generate unique trip identifier
  CONCAT(
    CAST(vendorid AS VARCHAR), '_',
    STRFTIME(tpep_pickup_datetime, '%Y%m%d%H%M%S'), '_',
    CAST(CAST(passenger_count AS INT) AS VARCHAR)
  ) AS trip_id,

  -- Core trip data (use correct lowercase column names)
  CAST(vendorid AS INTEGER) AS vendor_id,
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  CAST(passenger_count AS INTEGER) AS passenger_count,
  CAST(trip_distance AS DOUBLE) AS trip_distance,
  CAST(ratecodeid AS INTEGER) AS ratecode_id,
  store_and_fwd_flag,
  CAST(pulocationid AS INTEGER) AS pulocation_id,
  CAST(dolocationid AS INTEGER) AS dolocation_id,

  -- Payment data with enrichment
  CAST(deduped.payment_type AS INTEGER) AS payment_type,
  COALESCE(pl.payment_type_name, 'unknown') AS payment_type_name,

  -- Fare breakdown
  CAST(fare_amount AS DOUBLE) AS fare_amount,
  CAST(extra AS DOUBLE) AS extra,
  CAST(mta_tax AS DOUBLE) AS mta_tax,
  CAST(tip_amount AS DOUBLE) AS tip_amount,
  CAST(tolls_amount AS DOUBLE) AS tolls_amount,
  CAST(total_amount AS DOUBLE) AS total_amount,

  -- Normalized timestamps
  pickup_datetime,
  taxi_type,
  extracted_at
FROM deduped
LEFT JOIN ingestion.payment_lookup pl
  ON CAST(deduped.payment_type AS INTEGER) = pl.payment_type_id
WHERE deduped.rn = 1
ORDER BY tpep_pickup_datetime DESC
