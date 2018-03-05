-- Unload our final result from the insights table to a csv file in S3
-- Unfortunately Redshift does not support unloading to json format so we take care of it ourselves with the format_output.py script

unload (
    'select * from trovit.insights'
)
to 's3://arnaud/trovit/insights.csv'
iam_role 'MY_AWS_IAM_ROLE_CREDENTIALS'
DELIMITER AS ','
GZIP
ALLOWOVERWRITE
PARALLEL OFF
ESCAPE;
