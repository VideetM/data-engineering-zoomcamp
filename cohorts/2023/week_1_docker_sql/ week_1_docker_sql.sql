-- Question 3
select
    count(*)
from
    yellow_taxi_trips_2019
where
    lpep_pickup_datetime :: date = '2019-01-15'
    and lpep_dropoff_datetime :: date = '2019-01-15';


-- Question 4
select
    distinct lpep_pickup_datetime :: date,
    max(trip_distance) over(
        partition by lpep_pickup_datetime :: date
        order by
            trip_distance desc
    )
from
    yellow_taxi_trips_2019
where
    lpep_pickup_datetime :: date in (
        '2019-01-18',
        '2019-01-28',
        '2019-01-15',
        '2019-01-10'
    )
order by
    2 desc
limit
    1;
    
-- Question 5
select
    count(
        case
            when passenger_count = 2 then 1
        end
    ) as two_pass,
    count(
        case
            when passenger_count = 3 then 1
        end
    ) as three_pass
from
    yellow_taxi_trips_2019
where
    lpep_pickup_datetime :: date = '2019-01-01';

-- Question 6
with max_tip as (
    select
        "DOLocationID",
        max(tip_amount) as max_tip
    from
        yellow_taxi_trips_2019 a
        inner join taxi_zone_lookup b on "PULocationID" = "LocationID"
    where
        "Zone" = 'Astoria'
    group by
        "DOLocationID"
    order by
        2 desc
    limit
        1
)
select
    b."Zone" as max_tip_zone
from
    max_tip a
    inner join taxi_zone_lookup b on "DOLocationID" = "LocationID";