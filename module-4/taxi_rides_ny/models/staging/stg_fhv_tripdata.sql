with source as (
    select * from {{ source('raw', 'fhv_tripdata') }}
),

renamed as (
    select
        -- identifiers
        cast(dispatching_base_num as string) as dispatching_base_num,
        cast(affiliated_base_number as string) as affiliated_base_number,
        cast(pulocationid as integer) as pickup_location_id,
        cast(dolocationid as integer) as dropoff_location_id,

        -- timestamps
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropoff_datetime as timestamp) as dropoff_datetime,

        -- trip info
        cast(sr_flag as string) as sr_flag
    from source
    -- Filter out records with null dispatching_base_num (data quality requirement)
    where dispatching_base_num is not null
)

select * from renamed

-- Sample records for dev environment using deterministic date filter
{% if target.name == 'dev' %}
where pickup_datetime >= '{{ var("dev_start_date") }}' and pickup_datetime < '{{ var("dev_end_date") }}'
{% endif %}
