# Data Engineering Zoom Camp Homework Module 1

## Question 1. Understanding Docker images

Run docker with the `python:3.13` image. Use an entrypoint `bash` to interact with the container.

What's the version of `pip` in the image?

- 25.3

### Code for Q1:

Running Docker Container:

```bash
docker run -it --rm --entrypoint /bin/bash
```

Find version of pip:

```bash
pip --version
```

## Question 2. Understanding Docker networking and docker-compose

- postgres:5433

## SQL Questions

> ingest_data.py and ingest_taxi_zone_lookup.py have code for ingestion of data referred to in the questions.

## Question 3. Counting short trips

For the trips in November 2025 (lpep_pickup_datetime between '2025-11-01' and '2025-12-01', exclusive of the upper bound), how many trips had a `trip_distance` of less than or equal to 1 mile?

- 8,007
### SQL Query for Q3:

```sql
SELECT COUNT(*)
FROM green_taxi_data
WHERE
    lpep_pickup_datetime BETWEEN '2025-11-01 00:00:01' AND '2025-11-30 23:59:59'
AND
    trip_distance <= 1;
```

## Question 4. Longest trip for each day

Which was the pick up day with the longest trip distance? Only consider trips with `trip_distance` less than 100 miles (to exclude data errors).

Use the pick up time for your calculations.

- 2025-11-14

### SQL Query for Q4:

```sql
SELECT
    CAST(lpep_pickup_datetime AS date) AS day,
    trip_distance
FROM
    green_taxi_data
WHERE
    trip_distance <= 100
ORDER BY
    trip_distance DESC
LIMIT 1;
```

## Question 5. Biggest pickup zone

Which was the pickup zone with the largest `total_amount` (sum of all trips) on November 18th, 2025?

- East Harlem North

### SQL Query for Q5:

```sql
SELECT
    z."Zone",
    SUM(g.fare_amount) AS total_fare_amount
FROM
    green_taxi_data g
JOIN
    zone_lookup z
ON
    g."PULocationID" = z."LocationID"
WHERE
    CAST(g.lpep_pickup_datetime AS DATE) = '2025-11-18'
GROUP BY
    z."Zone"
ORDER BY
    total_fare_amount DESC
LIMIT 1;
```

## Question 6. Largest tip

For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip?

Note: it's `tip` , not `trip`. We need the name of the zone, not the ID.

- Yorkville West

### SQL Query for Q6:

```sql
SELECT
    zdo."Zone" AS dropoff_zone,
    MAX(g.tip_amount) AS largest_tip
FROM
    green_taxi_data g
JOIN
    zone_lookup zpu ON g."PULocationID" = zpu."LocationID"
JOIN
    zone_lookup zdo ON g."DOLocationID" = zdo."LocationID"
WHERE
    zpu."Zone" = 'East Harlem North'
    AND g.lpep_pickup_datetime >= '2025-11-01 00:00:00'
    AND g.lpep_pickup_datetime < '2025-12-01 00:00:00'
GROUP BY
    zdo."Zone"
ORDER BY
    largest_tip DESC
LIMIT 1;
```
