/* @bruin

name: reports.trips_report
type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_date
  time_granularity: date

columns:
  - name: pickup_date
    type: date
    description: "Trip pickup date (aggregation key)"
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: taxi_type
    type: varchar
    description: "Type of taxi (yellow, green)"
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: payment_type_name
    type: varchar
    description: "Payment type from lookup table"
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: trip_count
    type: bigint
    description: "Number of trips in this aggregation"
    nullable: false
    checks:
      - name: not_null
      - name: positive
  - name: total_fare
    type: double
    description: "Total fare amount for all trips"
    nullable: false
    checks:
      - name: not_null
      - name: non_negative
  - name: total_tip
    type: double
    description: "Total tip amount for all trips"
    nullable: false
    checks:
      - name: not_null
      - name: non_negative
  - name: total_amount
    type: double
    description: "Total amount charged for all trips"
    nullable: false
    checks:
      - name: not_null
      - name: non_negative
  - name: avg_fare
    type: double
    description: "Average fare per trip"
    nullable: false
    checks:
      - name: not_null
      - name: non_negative
  - name: avg_tip
    type: double
    description: "Average tip per trip"
    nullable: false
    checks:
      - name: not_null
      - name: non_negative
  - name: avg_trip_distance
    type: double
    description: "Average trip distance in miles"
    nullable: false
    checks:
      - name: not_null
      - name: positive
  - name: avg_passenger_count
    type: double
    description: "Average number of passengers per trip"
    nullable: false
    checks:
      - name: not_null
      - name: positive
  - name: max_fare
    type: double
    description: "Maximum fare amount in this aggregation"
    nullable: false
    checks:
      - name: not_null
      - name: non_negative
  - name: min_fare
    type: double
    description: "Minimum fare amount in this aggregation"
    nullable: false
    checks:
      - name: not_null
      - name: non_negative

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
  - name: no_duplicate_aggregations
    description: "Ensures all rows are unique (no duplicate aggregations)"
    query: |
      SELECT COUNT(*) = COUNT(DISTINCT CONCAT(CAST(pickup_date AS VARCHAR), '_', taxi_type, '_', payment_type_name))
      FROM reports.trips_report
      WHERE pickup_date >= '{{ start_date }}'
        AND pickup_date < '{{ end_date }}'
    value: 1

  - name: positive_trip_counts
    description: "Validates that all aggregations have positive trip counts"
    query: |
      SELECT COUNT(*) = SUM(CASE WHEN trip_count > 0 THEN 1 ELSE 0 END)
      FROM reports.trips_report
      WHERE pickup_date >= '{{ start_date }}'
        AND pickup_date < '{{ end_date }}'
    value: 1

@bruin */

-- Reports: Aggregate cleaned trip data by date, taxi type, and payment type
--
-- Purpose:
-- - Summarize trip metrics at daily granularity for analytics
-- - Calculate aggregations: counts, totals, averages, min/max
-- - Enable dashboards and business intelligence queries
-- - Incremental processing: only reprocess modified date windows

SELECT
  -- Aggregation key: date, taxi type, payment type
  CAST(pickup_datetime AS DATE) as pickup_date,
  COALESCE(taxi_type, 'unknown') as taxi_type,
  COALESCE(payment_type_name, 'unknown') as payment_type_name,

  -- Trip count
  COUNT(*) as trip_count,

  -- Totals
  SUM(fare_amount) as total_fare,
  SUM(tip_amount) as total_tip,
  SUM(total_amount) as total_amount,

  -- Averages
  AVG(fare_amount) as avg_fare,
  AVG(tip_amount) as avg_tip,
  AVG(trip_distance) as avg_trip_distance,
  AVG(passenger_count) as avg_passenger_count,

  -- Min/Max
  MAX(fare_amount) as max_fare,
  MIN(fare_amount) as min_fare

FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY
  CAST(pickup_datetime AS DATE),
  COALESCE(taxi_type, 'unknown'),
  COALESCE(payment_type_name, 'unknown')
ORDER BY pickup_date DESC, taxi_type, payment_type_name


