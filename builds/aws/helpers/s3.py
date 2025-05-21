import boto3
import awswrangler as wr
import os
import fnmatch
import logging
import re
import sys
import socket
from botocore.exceptions import ClientError
from datetime import datetime
from urllib.parse import urlsplit
from airflow.providers.amazon.aws.operators.datasync import DataSyncOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import get_current_context
import time
from helpers.common import *
import subprocess
from airflow.operators.python import get_current_context


def upload_file(file_name, bucket, object_name=None):
    """upload a file to s3 bucket
    param file name : file to upload
    param bucket : bucket to upload to
    param object name : s3 object name . if not specified then file name is used
    return True if file uploaded ,else False
    """
    session = boto3.session.Session()
    if object_name is None:
        object_name = os.basename(file_name)
    try:
        s3_client = session.client("s3")
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def delete_bucket_object(bucket, object_name):
    s3_client = boto3.client("s3")
    try:
        response = s3_client.delete_object(Bucket=bucket, Key=object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def del_all_object_from_s3_folder(bucket, object_name):
    session = boto3.Session()
    s3_client = session.client("s3")
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=object_name)
    #  to Handle keyerror for response['Contents'] if bucket is already empty --Done
    if "Contents" in response:
        try:
            files_in_folder = response["Contents"]
        except ClientError as e:
            raise e

            return True
        files_to_delete = []
        for f in files_in_folder:
            files_to_delete.append({"Key": f["Key"]})

            # print(files_to_delete)
            # This will delete all files in a folder
        try:
            response = s3_client.delete_objects(
                Bucket=bucket, Delete={"Objects": files_to_delete}
            )
        except ClientError as e:
            logging.error(e)
            return False
        return True
    else:
        print(f"No Such key found - {object_name}")


def prefix_exits_s3_folder(bucket, prefix):
    s3_client = boto3.client("s3")
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return response["KeyCount"]


def move_and_delete_s3_file(
    source_bucket, source_key, destination_bucket, destination_key
):
    """
    Move a file within folders in Amazon S3 and delete the old file.

    Args:
        source_bucket (str): The name of the source S3 bucket.
        source_key (str): The key (i.e., path) of the source file within the source bucket.
        destination_bucket (str): The name of the destination S3 bucket.
        destination_key (str): The key (i.e., path) of the destination file within the destination bucket.
        using aws cli instead of boto3 sdk methods to automatically do multi part upload
    """

    # # Initialize the S3 client
    # s3_client = boto3.client("s3")
    #
    # # Copy the file to the destination folder
    # s3_client.copy_object(
    #     Bucket=destination_bucket,
    #     Key=destination_key,
    #     CopySource={"Bucket": source_bucket, "Key": source_key},
    # )
    #
    # # Delete the old file
    # try:
    #     s3_client.delete_object(Bucket=source_bucket, Key=source_key)
    # except ClientError as e:
    #     raise e
    #

    s3 = boto3.client("s3")
    try:
        # Check if the source file exists
        s3.head_object(Bucket=source_bucket, Key=source_key)
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            print(
                f"Source file {source_key} not found in bucket {source_bucket}. Skipping move operation."
            )
            return

    command = "aws s3 mv s3://{}/{} s3://{}/{}".format(
        source_bucket, source_key, destination_bucket, destination_key
    )
    print(command)

    s3_mv_operator = BashOperator(task_id="s3_move_task", bash_command=command)

    s3mvcontext = get_current_context()
    result = s3_mv_operator.execute(context=s3mvcontext)

    print(f"File '{source_key}' successfully moved to '{destination_key}' ")


def read_s3_obj_content(source_bucket, file_to_read):
    """
    Read the content of an S3 object using Amazon S3 Select.

    Parameters:
    - source_bucket (str): The name of the source S3 bucket.
    - file_to_read (str): The key (path) of the S3 object to be read.

    Returns:
    dict: A dictionary containing the response from the S3 Select operation.

    Note:
    - This function uses Amazon S3 Select to execute a SQL query on the specified S3 object.
    - The SQL query in the example is a simple count(*), but it can be modified as needed.
    - Input and output serialization are set for CSV format by default.

    Example Usage:
    ```
    source_bucket = "your_source_bucket"
    file_to_read = "path/to/your/object.csv"
    response = read_s3_obj_content(source_bucket, file_to_read)
    print(response)
    ```

    AWS SDK Documentation:
    - S3 Select: https://docs.aws.amazon.com/AmazonS3/latest/API/API_SelectObjectContent.html
    """
    try:
        s3_client = boto3.client("s3")
        expression_type = "SQL"
        query = """SELECT count(*) FROM S3Object"""
        input_serialization = {
            # "CSV": {"FileHeaderInfo": "USE", "AllowQuotedRecordDelimiter": True}
            "CSV": {"AllowQuotedRecordDelimiter": True}
        }

        output_serialization = {"CSV": {}}

        response = s3_client.select_object_content(
            Bucket=source_bucket,
            Key=file_to_read,
            ExpressionType=expression_type,
            Expression=query,
            InputSerialization=input_serialization,
            OutputSerialization=output_serialization,
        )
        return response
    except Exception as e:
        print(
            "S3 select error occurred for file {} with error {}:".format(
                file_to_read, e
            )
        )
        return None


def get_s3Filelist_using_wr(s3path):
    """
    Get a list of files from an S3 path using the `wr.s3.list_objects` function.

    Parameters:
    - s3path (str): The S3 path to list files from. Should follow the S3 URL format,
                   e.g., 's3://myBucket/raw/client/Hist/2017/*/*/Tracking_*.zip'.

    Returns:
    list: A list of file paths matching the specified S3 path.

    Example:
    ```
    s3path = 's3://myBucket/raw/client/Hist/2017/*/*/Tracking_*.zip'
    files = get_s3filelist_using_wr(s3path)
    print(files)
    ```

    Note:
    - This function uses the `wr.s3.list_objects` function from the 'awswrangler' library.
    - The S3 path supports wildcard characters (*) for flexible file matching.
    - Ensure the 'awswrangler' library is installed before using this function.
      You can install it using: `pip install awswrangler`.
    - For more details on 'awswrangler' and `wr.s3.list_objects`, refer to the documentation:
      https://github.com/awslabs/aws-data-wrangler

    AWS Data Wrangler Documentation:
    - https://aws-data-wrangler.readthedocs.io/en/stable/

    Addtnl Params
    last_modified_begin – Filter the s3 files by the Last modified date of the object. The filter is applied only after list all s3 files.
    last_modified_end (datetime, optional) – Filter the s3 files by the Last modified date of the object. The filter is applied only after list all s3 files.
    ignore_empty (bool) – Ignore files with 0 bytes.

    Also sorts the list based on file recency date
    """
    s3_objects = wr.s3.list_objects(s3path)
    sorted_sb_objects = sorted(
        s3_objects, key=lambda x: x.split(".")[0].split("_")[-1], reverse=True
    )
    return sorted_sb_objects


def split_s3_path(s3_url):
    """
    Splits s3 url into bucket and key
    """
    components = urlsplit(s3_url)
    return components.netloc, components.path.lstrip("/")


def copy_s3_object(
    copy_source_bucket, copy_source_path, copy_target_bucket, copy_target_path
):
    s3_resource = boto3.resource("s3")
    copy_source = {"Bucket": copy_source_bucket, "Key": copy_source_path}
    logging.info(f"Copying {copy_source} to {copy_target_bucket}/{copy_target_path}")
    try:
        s3_resource.meta.client.copy(copy_source, copy_target_bucket, copy_target_path)
    except ClientError as e:
        raise e


# def copy_cross_region_s3_object(
#     copy_source_bucket,
#     copy_source_path,
#     destination_bucket,
#     destination_object_key,
#     src_region,
#     tgt_region,
# ):
#     s3_src_client = boto3.client("s3", region_name=src_region)
#     s3_tgt_client = boto3.client("s3", region_name=tgt_region)
#     copy_source = {"Bucket": copy_source_bucket, "Key": copy_source_path}
#     try:
#         s3_tgt_client.copy_object(
#             Bucket=destination_bucket,
#             Key=destination_object_key,
#             CopySource=copy_source,
#         )
#     except ClientError as e:
#         raise e


def copy_cross_region_s3_object(
    source_bucket,
    source_key,
    destination_bucket,
    destination_key,
    source_region,
    destination_region,
):
    """
    Copy a file within folders in Amazon S3.

    Args:
        source_bucket (str): The name of the source S3 bucket.
        source_key (str): The key (i.e., path) of the source file within the source bucket.
        destination_bucket (str): The name of the destination S3 bucket.
        destination_key (str): The key (i.e., path) of the destination file within the destination bucket.
        source_region (str): The AWS region of the source bucket.
        destination_region (str): The AWS region of the destination bucket.
    """

    s3 = boto3.client("s3", region_name=source_region)
    try:
        # Check if the source file exists
        s3.head_object(Bucket=source_bucket, Key=source_key)
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            print(
                f"Source file {source_key} not found in bucket {source_bucket}. Skipping copy operation."
            )
            raise e

    copy_command = (
        "aws s3 cp s3://{}/{} s3://{}/{} --source-region {} --region {}".format(
            source_bucket,
            source_key,
            destination_bucket,
            destination_key,
            source_region,
            destination_region,
        )
    )
    print(copy_command)

    s3_cp_operator = BashOperator(task_id="s3_copy_task", bash_command=copy_command)

    context = get_current_context()
    cp_result = s3_cp_operator.execute(context=context)
    print(
        f"File '{source_key}' successfully copied to '{destination_key}' in bucket '{destination_bucket}'"
    )


def get_existing_location(datasync_client, bucket_arn, subdirectory):
    """Check if a DataSync location already exists for the given bucket and subdirectory."""
    locations = datasync_client.list_locations()["Locations"]
    print("locations: ", str(locations))
    print("location uri: ", str(f"s3://{bucket_arn}{subdirectory}/"))
    for location in locations:
        if location["LocationUri"] == f"s3://{bucket_arn}{subdirectory}/":
            return location["LocationArn"]
    return None
    

def copy_cross_region_s3_datasync(
    copy_source_bucket,
    copy_source_prefix,
    destination_bucket,
    destination_prefix,
    src_region,
    tgt_region,
    specific_file=None,
    cross_account_cross_region=None,
    exclude_file=None,
    specific_wildcard=None
):
    datasync_source = boto3.client("datasync", region_name=src_region)
    datasync_destination = boto3.client("datasync", region_name=tgt_region)

    aet_account_id = get_aws_account_id()

    print("Creating S3 Source Location...")

    # Define S3 source location configuration
    source_location_config = {
        "S3BucketArn": f"arn:aws:s3:::{copy_source_bucket}",
        "S3Config": {
            "BucketAccessRoleArn": "arn:aws:iam::{}:role/da-mz-datasync-role".format(
                aet_account_id
            )
        },
    }

    print(source_location_config)

    destination_location_config = {
        "S3BucketArn": f"arn:aws:s3:::{destination_bucket}",
        "S3Config": {
            "BucketAccessRoleArn": "arn:aws:iam::{}:role/da-mz-datasync-role".format(
                aet_account_id
            )
        },
    }

    print(destination_location_config)

    # Process source prefix
    processed_source_prefix = "/"
    if copy_source_prefix and copy_source_prefix.strip():
        processed_source_prefix = copy_source_prefix.strip().rstrip("/")
        if not processed_source_prefix.startswith("/"):
            processed_source_prefix = "/" + processed_source_prefix

    # Process destination prefix
    processed_dest_prefix = "/"
    if destination_prefix and destination_prefix.strip():
        processed_dest_prefix = destination_prefix.strip().rstrip("/")
        if not processed_dest_prefix.startswith("/"):
            processed_dest_prefix = "/" + processed_dest_prefix

    print("processed_source_prefix :", processed_source_prefix)
    print("processed_dest_prefix :", processed_dest_prefix)

    source_location_arn = get_existing_location(datasync_source,copy_source_bucket,processed_source_prefix)
    destination_location_arn = get_existing_location(datasync_destination,destination_bucket,processed_dest_prefix)

    print("source_location_arn: ", str(source_location_arn))
    print("destination_location_arn: ",str(destination_location_arn))
    if not source_location_arn:
        print("Creating new source location...")
        # Create S3 source location
        response = datasync_source.create_location_s3(
            S3BucketArn=source_location_config["S3BucketArn"],
            Subdirectory=processed_source_prefix,
            S3Config=source_location_config["S3Config"],
        )
        source_location_arn = response["LocationArn"]
        print(f"S3 Source Source Location created: {source_location_arn}")
    else:
        print(f"Using existing Source Location: {source_location_arn}")

    if not destination_location_arn:
        print("Creating new destination location...")

        # Create S3 destination location
        response = datasync_destination.create_location_s3(
            S3BucketArn=destination_location_config["S3BucketArn"],
            Subdirectory=processed_dest_prefix,
            S3Config=destination_location_config["S3Config"],
        )
        destination_location_arn = response["LocationArn"]
        print(f"S3 Source Destination Location created: {destination_location_arn}")
    else:
        print(f"Using existing Destination Location: {destination_location_arn}")

    print("Creating DataSync Task...")

    # Define base task configuration
    task_args = {
        "SourceLocationArn": source_location_arn,
        "DestinationLocationArn": destination_location_arn,
        "Name": "XRegionDataSyncTask",
        "Options": {
            "VerifyMode": "NONE",
            "OverwriteMode": "ALWAYS",
        },
    }

    includes_filter = None
    # Add includes filter if specific file is provided
    if specific_file:
        #Process the specific file pattern to ensure it starts with '/'
        if not specific_file.startswith("/"):
            specific_file = "/" + specific_file

        includes_filter = specific_file
    elif specific_wildcard:
        #includes_filter = "/part-*"
        includes_filter = "/" + f"{specific_wildcard}"
    if includes_filter:
        task_args["Includes"] = [
            {"FilterType": "SIMPLE_PATTERN", "Value": includes_filter}
        ]
        print(f"Configured to transfer specific file with pattern: {includes_filter}")

    if exclude_file:

        task_args["Excludes"] = [
            {"FilterType":"SIMPLE_PATTERN","Value":"*/_SUCCESS*|*/_committed_*|*/_started_*|*/.aws-datasync/*"},

        ]

    # Create task
    datasync_reg = datasync_destination if cross_account_cross_region else datasync_source
    response = datasync_reg.create_task(**task_args)
    task_arn = response["TaskArn"]
    print(f"Task created: {task_arn}")

    ## Run Task
    print("Running DataSync Task...")
    response = datasync_reg.start_task_execution(TaskArn=task_arn)
    task_execution_arn = response["TaskExecutionArn"]
    print(f"Task execution started: {task_execution_arn}")

    ## Track Task Progress
    try:
        while True:
            response = datasync_reg.describe_task_execution(
                TaskExecutionArn=task_execution_arn
            )
            status = response["Status"]
            print(f"Task execution status: {status}")

            if status in ["SUCCESS"]:
                print("Aetna Xregion DataSync Script Completed.")
                break
            elif status in ["ERROR"]:
                raise Exception("Aetna Xregion DataSync Script Failed, please review.")
            time.sleep(60)  # Wait for 1 minute before checking again

    finally:
        # Delete the task after execution completes or if there's an error
        print(f"Deleting DataSync task: {task_arn}")
        datasync_reg.delete_task(TaskArn=task_arn)
        print(f"DataSync task {task_arn} deleted.")

    print("Task execution completed.")


def list_zero_byte_objects(bucket, object_name):
    session = boto3.Session()
    s3_client = session.client("s3")
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=object_name)
    content = response["Contents"]
    zero_byte_list = []
    for obj in content:
        if obj["Size"] == 0:
            zero_byte_list.append("s3://" + bucket + "/" + obj["Key"])
    return zero_byte_list


def get_s3_file_record_count(bucket, key):
    # function returns no of records from s3 file excluding header
    bucket = bucket
    key = key
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read().decode("utf-8")

    # Assuming the file is a CSV
    records = body.splitlines()
    num_records = len(records)  # subtract 1 for the header
    return num_records


def read_s3_file(bucket_name, file_key):
    """
    Reads the content of a file from an S3 bucket.

    :param bucket_name: The name of the S3 bucket
    :param file_key: The key (path) of the file in the S3 bucket
    :return: The content of the file as a string
    """
    # Initialize the S3 client
    s3_client = boto3.client("s3")

    try:
        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        body = obj["Body"].read().decode("utf-8")
        return body
    except Exception as e:
        print(f"Error reading {file_key} from bucket {bucket_name}: {e}")
        return None


def s3_put_object(bucket_name, key, body):
    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket_name, Key=key, Body=body)


def getsingleobjectlist(bucketName, prefix_key, maxKeys, separator):
    s3 = boto3.client("s3")
    content = s3.list_objects_v2(
        Bucket=bucketName, Prefix=prefix_key, MaxKeys=maxKeys, Delimiter=separator
    ).get("Contents")
    result = []
    for c in content:
        result.append(c.get("Key"))
    return result


def copy_cross_account_s3_obj(
    cfg,
    da_ingestion_bucket_name,
    bus_bucket,
    bus_path,
    processed_pattern,
    content_type,
    destination_bucket,
    redshift_files=None,
):
    # Initialize S3 client in the destination account
    s3_client = boto3.client("s3", region_name="us-east-2")

    if len(bus_path) > 0:
        source_prefix = f"{bus_path}/{processed_pattern}*"
    else:
        source_prefix = f"{processed_pattern}*"
    complete_path_iter = "s3://{}/{}".format(bus_bucket, source_prefix)
    try:
        session = boto3.Session()

        # s3_objects = get_s3Filelist_using_wr(complete_path_iter)
        s3_objects = wr.s3.list_objects(complete_path_iter, boto3_session=session)
        sorted_sb_objects = sorted(
            s3_objects, key=lambda x: x.split(".")[0].split("_")[-1], reverse=True
        )
        print("files:", str(sorted_sb_objects))

        if sorted_sb_objects:
            if content_type == "Full":
                print("File type is Full")
                latest_file = sorted_sb_objects[0] if sorted_sb_objects else None
                if latest_file:
                    print(f"Copying latest file: {latest_file}")
                    s3_key = latest_file.replace("s3://", "").split("/", 1)[1]
                    print(f"S3 Key: {s3_key}")
                    copy_source = {"Bucket": bus_bucket, "Key": s3_key}
                    destination_key = f"{cfg['ingestion_path']}/{s3_key.split('/')[-1]}"
                    s3_client.copy(copy_source, destination_bucket, destination_key)
                    print(
                        f"Copied {latest_file} to {destination_bucket}/{destination_key}"
                    )

                else:
                    print(
                        f"No files found for pattern {processed_pattern} in {bus_bucket}."
                    )
            elif content_type == "Incremental":
                print("File type is Incremental")
                new_files = []
                for file in sorted_sb_objects:
                    file_name = file.replace("s3://", "").split("/")[-1]

                    if file_name not in redshift_files:
                        new_files.append(file)

                if new_files:
                    # Copy only the new files to the destination bucket
                    for new_file in new_files:
                        print(f"Copying new file: {new_file}")
                        s3_key = new_file.replace("s3://", "").split("/", 1)[1]
                        destination_key = (
                            f"{cfg['ingestion_path']}/{s3_key.split('/')[-1]}"
                        )
                        copy_source = {"Bucket": bus_bucket, "Key": s3_key}
                        s3_client.copy(copy_source, destination_bucket, destination_key)
                        print(
                            f"Copied {new_file} to {destination_bucket}/{destination_key}"
                        )

                        # Update max_timestamp with the last modified time of the new file

                    print("Incremental file transfer finished....")
                else:
                    print(f"No new files found for pattern {processed_pattern}.")
        else:
            print(f"No files found for pattern {processed_pattern} in {bus_bucket}.")
    except ClientError as e:
        raise Exception(
            f"Error processing {processed_pattern} in bucket {bus_bucket}: {e}"
        )
