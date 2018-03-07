-- Initializes the database and ingest the dataset from aws s3 to the raw_data table

-- Contains the raw data as they are in the initial dataset
create table if not exists trovit.raw_data(
city varchar(60),
content_chunk varchar(max),
country varchar(10),
date date,
make varchar(30),
mileage int,
model varchar(30),
price float,
region varchar(60),
title_chunk varchar(200),
unique_id varchar(200) not null,
url_anonymized varchar(200),
year int,
doors int,
fuel varchar(20),
car_type varchar(30),
transmission varchar(20),
color varchar(20),
-- Unfortunately Redshift does not enforce any constraint on the PK so we will have to deal with the duplicates ourselves
primary key(unique_id))
distkey(unique_id)
compound sortkey(unique_id, date)
;

-- Contains the raw data with the uniqueId deduplicated
create table if not exists trovit.dedup_data(like trovit.raw_data);

-- Contains the final result table with the clean dataset and the insights of the users
-- The insight is just one extra column specifying if the ad is a good or bad deal.
-- We also took off some fields like url_anonymized, content_chunk and title_chunk which the users probably don't care about.
create table if not exists trovit.insights(
city varchar(60),
country varchar(10),
date date,
make varchar(30),
mileage int,
model varchar(30),
price float,
region varchar(60),
unique_id varchar(200) not null,
year int,
doors int,
fuel varchar(20),
car_type varchar(30),
transmission varchar(20),
color varchar(20),
deal varchar(20),
primary key(unique_id))
distkey(unique_id)
compound sortkey(unique_id)
;

-- Ingest data from our json file stored in AWS S3 to our raw_data table
copy trovit.raw_data
from 's3://arnaud/trovit/cars.json.gz'
    iam_role 'MY_AWS_IAM_ROLE_CREDENTIALS'
    blanksasnull
    emptyasnull
    truncatecolumns
    gzip
    statupdate on
    compupdate on
    json
    's3://arnaud/trovit/cars.jsonpath'
;
