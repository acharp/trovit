-- Here we still have the duplicates, we can see that as the sum_ids is higher than distinct_ids
select count(distinct(unique_id)) as distinct_ids, count(unique_id) as sum_ids from trovit.raw_data;

-- Here the number of distinct_ids and the sum of total unique_ids match. There are no duplicates anymore.
select count(distinct(unique_id)) as distinct_ids, count(unique_id) as sum_ids from trovit.dedup_data;
