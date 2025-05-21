from datetime import datetime, timedelta
from airflow.models import Variable
import logging
import json
import glob
import time
import pandas as pd
import re
import boto3
import datetime as dt
from botocore.exceptions import ClientError
import gzip


# import helpers.redshift as redshift_aetna


def get_uncompressed_size_by_streaming_s3(boto3_client, s3_path):

    try:
        s3 = boto3_client
        # Extract bucket name and key from S3 path (assumes full path is provided)
        bucket = s3_path.split("/")[2]
        key = "/".join(s3_path.split("/")[3:])

        # Fetch the object from S3
        response = s3.get_object(Bucket=bucket, Key=key)

        # Use the response['Body'] stream directly without loading it into memory
        compressed_stream = response["Body"]

        uncompressed_size = 0
        chunk_size = 1024 * 1024  # 1 MB chunks

        # Decompress the stream on the fly without loading it fully into memory
        with gzip.GzipFile(fileobj=compressed_stream, mode="rb") as gz:
            while True:
                chunk = gz.read(chunk_size)
                if not chunk:
                    break
                uncompressed_size += len(chunk)

        return uncompressed_size
    except Exception as e:
        print(f"Got Exception during uncompress size calculation: {e}")


def skip_task_check(task_id, list_of_tasks):
    if list_of_tasks:
        return str(task_id).lower() in map(str.lower, list_of_tasks)
    return False


def get_ssm_parameters(parameter_name, region_name, decryption):
    """
    Fetch a parameter from AWS Systems Manager (SSM) Parameter Store.

    Parameters:
    - parameter_name (str): The name of the parameter to fetch.
    - region_name (str): The AWS region where the parameter is stored. Default is 'us-east-1'.

    Returns:
    - str: The value of the parameter.

    Example:
    >>> get_ssm_parameter('password', 'us-east-2')
    'testd123'
    """
    region_name = "us-east-2"
    ssm_client = boto3.client("ssm", region_name)
    try:
        response = ssm_client.get_parameter(
            Name=parameter_name, WithDecryption=decryption
        )

        parameter_value = response["Parameter"]["Value"]
        return parameter_value
    except ssm_client.exceptions.ParameterNotFound:
        raise ValueError("Parameter not found: {}".format(parameter_name))


def get_secret(secret_name, region_name):
    region_name = "us-east-2"
    secret_client = boto3.client("secretsmanager", region_name)
    try:
        response = secret_client.get_secret_value(SecretId=secret_name)
        secret = json.loads(response["SecretString"])
        return secret
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            raise ValueError("Secret not found: {}".format(secret_name))


def get_dag_params(bucket_s3, json_name):
    print("params called")
    s3_bucket = bucket_s3
    key = "json_config/"
    param_json = s3_client.get_object(Bucket=s3_bucket, Key=key + json_name)
    print("thru")
    content = param_json["Body"]
    json_data = json.loads(content.read())
    return json_data


def get_json_config(json_path):
    with open(f"{json_path}", "r") as file:
        data = file.read()
        json_data = json.loads(data)
    return json_data


def get_aws_account_id():
    """
    Retrieves the AWS account ID associated with the current AWS credentials.

    This function uses the Security Token Service (STS) to get information
    about the caller identity and extracts the AWS account ID from the response.

    Returns:
        str: The AWS account ID.

    """
    sts_client = boto3.client("sts")
    account_id = sts_client.get_caller_identity()["Account"]
    return account_id


def generate_exec_id(req_type):
    now = datetime.now()
    year = now.strftime("%y")
    julian_day = now.strftime("%j")
    hour = now.strftime("%H")
    v_exec_id = year + julian_day + hour
    v_batch_id = now.strftime("%y%m%d%H") + str(req_type)
    return v_exec_id, v_batch_id


def generate_file_suffix(pattern):
    now = datetime.now()
    if pattern == "yyyyMMdd":
        return now.strftime("%Y%m%d")
    elif pattern == "yyyyMMddhhmmss":
        return now.strftime("%Y%m%d%H%M%S")
    else:
        return None


def get_batch_id(request_id):
    # Get the current date and time
    current_datetime = datetime.now()

    # Format the date as YYDDDHH24MI
    formatted_date = current_datetime.strftime("%Y%m%d")

    # Create BATCH_ID
    str_batch_id = formatted_date + str(request_id)

    # Convert the formatted date to an integer
    BATCH_ID = int(str_batch_id)
    return BATCH_ID
