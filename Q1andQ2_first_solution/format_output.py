import csv
import json
import gzip
import logging
import tempfile

import boto3
import botocore


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# When running inside an aws lambda function these values are automatically ingested from the s3 path where the file was dumped
S3_BUCKET = "arnaud"
S3_KEY = "trovit/insights.csv000.gz"

# In a production application the fieldnames and type_mapping would be directy read wether from the aws redshift schema or even better
# from the ORM (such as sqlalchemy for example) if we are using one
FIELDNAMES = ("city", "country", "date", "make", "mileage", "model",
    "price", "region", "unique_id", "year", "doors", "fuel", "carType",
    "transmision", "color", "deal")
TYPE_MAPPING = {
        "year": int,
        "mileage": int,
        "price": float,
        "doors": int}


def download_file_from_s3(bucket, key, otpt_path):
    """Download file from S3."""
    client = boto3.client("s3")
    logger.info("Downloading file from s3://%s/%s" % (bucket, key))
    try:
        client.download_file(bucket, key, otpt_path)
    except botocore.exceptions.ClientError as err:
        logger.exception(err)
        return ""
    return otpt_path


def csv_to_dicts(path, fieldnames, compressed=False):
    """Convert a csv file to a list of dicts."""
    out = []
    if compressed:
        csv_file = gzip.open(path, "rt")
    else:
        csv_file = open(path, "rt")
    for row in csv.DictReader(csv_file, delimiter=",", fieldnames=fieldnames):
        out.append(row)
    csv_file.close()
    return out


def dicts_to_json(dicts, otpt_path, encoding="utf-8"):
    """Dump a list of dicts to a json file."""
    with open(otpt_path, "wt", encoding=encoding) as otpt_json_file:
        for dic in dicts:
            json.dump(dic, otpt_json_file, ensure_ascii=False)
    return otpt_path


def coerce_type(dic, type_mapping):
    """Coerce the types of a dict."""
    for k, v in dic.items():
        # If the key needs to be converted, we apply the matching type_mapping
        if k in type_mapping.keys():
            dic[k] = type_mapping[k](dic[k])
    return dic


def scrub(dic, null_field=""):
    """Replace chosen empty values of a nested dict by a proper None value."""
    # ret = copy.deepcopy(x)
    if isinstance(dic, dict):
        for k,v in dic.items():
            dic[k] = scrub(v)
    # Handle null field
    if dic == null_field:
        dic = None
    return dic


# Entry point of the aws lambda function
if __name__ == "__main__":
    gz_path = download_file_from_s3(S3_BUCKET, S3_KEY, otpt_path="insights.csv.gz")
    dicts = csv_to_dicts(gz_path, FIELDNAMES, compressed=True)
    # We sanitize the null field and type our dicts
    clean_dicts = [coerce_type(scrub(dic), TYPE_MAPPING) for dic in dicts]
    dicts_to_json(clean_dicts, otpt_path="insights.json")
