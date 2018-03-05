-- Compute some interesting insights from the dedup_data table and load them into the insights one

drop table baseline;
drop table scoring;

-- The table baseline contain the average value of mileage, price and year for each (make, model) pair
create temp table baseline(
make,
model,
avg_mileage,
avg_price,
avg_year
)
as select
make,
model,
avg_mileage,
avg_price,
avg_year
from
(
    select
        make,
        model,
        -- We exclude the outliers value from the computation of the average
        -- otherwise the average values would be biased and much lower than what they must be
        avg(nullif(mileage, 0)) as avg_mileage,
        avg(nullif(price, 0.0)) as avg_price,
        avg(nullif(year, 0)) as avg_year
    from
        trovit.dedup_data
    group by
        make, model
)
;


-- The table scoring attributes a score to each unique_id by comparing them to our baseline means
create temp table scoring(
unique_id,
mileage_score,
price_score,
year_score
)
as select
unique_id,
mileage_score,
price_score,
year_score
from
(
    select
        unique_id,
        -- We coalesce the avg values and the value of the raw because some are null and would bias the scoring.
        -- If, for some of the scoring field, wether the (make, model) is lacking the average value, or the row is lacking a value, this
        -- scoring field gets attributed a 0. This keeps us from pushing wrong information to our users.
        case when (coalesce(avg_mileage, 0) - coalesce(nullif(mileage, 0), 800000)) > 50000 then 1 else 0 end as mileage_score,
        case when (coalesce(avg_price, 0.0) - coalesce(nullif(price, 0.0), 100000.00)) > 4000.00 then 1 else 0 end as price_score,
        case when (year - coalesce(avg_year, 3000)) > 3 then 1 else 0 end as year_score
    from
        trovit.dedup_data
    left join
        baseline
    on
        trovit.dedup_data.make = baseline.make and
        trovit.dedup_data.model = baseline.model
)
;

-- Last step: Gather our scores in a "deal" columns specifying how good (or bad) the offer is for each classified ad
insert into trovit.insights
select
    city,
    country,
    date,
    make,
    mileage,
    model,
    price,
    region,
    unique_id,
    year,
    doors,
    fuel,
    car_type,
    transmission,
    color,
    deal
from
(
    select
        city,
        country,
        date,
        make,
        mileage,
        model,
        price,
        region,
        trovit.dedup_data.unique_id as unique_id,
        year,
        doors,
        fuel,
        car_type,
        transmission,
        color,
        case
            when (mileage_score + price_score + year_score) = 3 then 'Good'
            when (mileage_score + price_score + year_score) = 2 then 'Interesting'
            when (mileage_score + price_score + year_score) = 1 then 'Average'
            when (mileage_score + price_score + year_score) = 0 then 'Bad'
        end as deal
    from
        trovit.dedup_data
    left join
        scoring
    on
        trovit.dedup_data.unique_id = scoring.unique_id
)
;

