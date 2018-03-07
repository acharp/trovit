-- This script removes the duplicated unique_id from the table raw_data and insert the result in dedup_data
insert into trovit.dedup_data
select
    city,
    content_chunk,
    country,
    date,
    make,
    mileage,
    model,
    price,
    region,
    title_chunk,
    unique_id,
    url_anonymized,
    year,
    doors,
    fuel,
    car_type,
    transmission,
    color
from
(
    select distinct
        city,
        content_chunk,
        country,
        date,
        make,
        mileage,
        model,
        price,
        region,
        title_chunk,
        unique_id,
        url_anonymized,
        year,
        doors,
        fuel,
        car_type,
        transmission,
        color,
        row_number() over (
            partition by
                unique_id
            -- We window over the date
            order by date desc) as ad_number
    from
        trovit.raw_data
) labelled_data
where
    -- We keep the most recent version of the duplicate, assuming it is the most
    -- likely to have the freshest data
    labelled_data.ad_number = 1
;
