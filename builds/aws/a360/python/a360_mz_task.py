import helpers.redshift as redshift_a360
import helpers.s3 as s3_a360
import helpers.athena as athena_a360
import logging
import time
import boto3
import json
from typing import Optional, List
from helpers.AwsDaxOperators import AwsStgOperator as AwsStgOperator
from helpers.AwsExtractOperator import AwsExtractOperator as AwsExtractOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
import pandas as pd
from botocore.exceptions import ClientError
import helpers.send_email as email
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.models import DagRun
import os
from datetime import datetime
from helpers.AwsDPOperator import AwsDPOperator as AwsDPOperator
import helpers.postgres as a360_pg
from helpers.s3 import *
from git import Repo
import shutil
import requests


def dp_process_setup(
    step_set, step_name, cfg, secret_cfg, src_code, bucket_name, redshift_iam_role
):
    s3_bucket = bucket_name
    outbound_dp = cfg["dp_config"][f"{src_code}"]["outbound"]
    inbound_dp = cfg["dp_config"][f"{src_code}"]["inbound"]
    query_type = ""
    sql = (
        f"select * from ga360_core.tmdm_config_sql where step_set = '{step_set}' "
        f"and step_name = '{step_name}'  order by process_order"
        ""
    )

    return_val = redshift_a360.redshift_query_execute(sql, cfg, secret_cfg, "query")
    step_name_value = return_val[0][1]
    step_sql = return_val[0][5]

    if step_name_value == "count":
        sql_string = step_sql
        print(sql_string)

        response = redshift_a360.redshift_query_execute(
            sql_string, cfg, secret_cfg, query_type
        )
        record_count = response[0][0]  # count return value from tuple
        logging.info(
            f"Files unloaded successfully to bucket {s3_bucket} at {outbound_dp}"
        )
        print(f"send counts email to dp {record_count}")
    if step_name_value == "unload":
        print(step_name_value)
        del_obj = s3_a360.del_all_object_from_s3_folder(s3_bucket, outbound_dp)
        sql_string = step_sql
        sql_string = sql_string.replace("<iam>", redshift_iam_role)
        sql_string = sql_string.replace("<s3path>", f"s3://{s3_bucket}/{outbound_dp}")
        print(sql_string)
        response = redshift_a360.redshift_query_execute(
            sql_string, cfg, secret_cfg, query_type
        )
        # print("record unloaded {} ".format(response[0][0]))
    if step_name_value == "copy":
        print(step_name_value)
        ### First Truncate the dpoutput table before running copy command ###
        step_name = "truncate"
        sql = (
            f"select * from a360_core.tmdm_config_sql where step_set = '{step_set}' "
            f"and step_name = '{step_name}'  order by process_order"
            ""
        )
        return_val = redshift_a360.redshift_query_execute(sql, cfg, secret_cfg, "query")
        step_sql_truncate = return_val[0][5]
        print(step_sql_truncate)
        response = redshift_a360.redshift_query_execute(
            step_sql_truncate, cfg, secret_cfg, query_type
        )
        print("Truncate command successful")
        ### First Truncate the dpoutput table before running copy command ###
        sql_string = step_sql
        sql_string = sql_string.replace("<iam>", redshift_iam_role)
        sql_string = sql_string.replace("<s3path>", f"s3://{s3_bucket}/{inbound_dp}")
        print(sql_string)
        response = redshift_a360.redshift_query_execute(
            sql_string, cfg, secret_cfg, query_type
        )
    print("Copy Command Successful")
    return response


def query_athena_table(cfg, table_name, bucket_name, athena_database):
    database = athena_database
    athena_result_path = "athena_query_results"
    s3_bucket = bucket_name
    query = f'select count(1) as rec_count from "{database}".{table_name}'
    print(query)
    session = boto3.session.Session()
    output_path = f"s3://{s3_bucket}/{athena_result_path}/"
    result_athena = athena_a360.executequery(
        boto3_session=session, sql=query, database=database, output_path=output_path
    )
    return result_athena.head()


def dp_send_file_process(cfg, src_code, bucket_name, dps_bucket_name):
    s3_bucket = bucket_name
    s3_bucket_dp = dps_bucket_name
    # athena_db = athena_database
    outbound_path_src = cfg["dp_config"][f"{src_code}"]["outbound"]
    inbound_path_src = cfg["dp_config"][f"{src_code}"]["inbound"]
    outbound_dp = cfg["dp_config"][f"{src_code}"]["dp_outbound"]
    inbound_dp = cfg["dp_config"][f"{src_code}"]["dp_inbound"]
    dp_reg = cfg["aws_reg_dp"]
    non_dp_reg = cfg["aws_reg_rs"]

    s3path_source_file = "s3://" + s3_bucket + "/" + outbound_path_src
    # Handle below deletion of files form incoming currently deletes entire folder when empty
    # del_obj = s3_a360.del_all_object_from_s3_folder(s3_bucket_dp, inbound_dp)
    # del_obj = s3_a360.del_all_object_from_s3_folder(s3_bucket_dp, outbound_dp)
    list_zero_size = s3_a360.list_zero_byte_objects(s3_bucket, outbound_path_src)
    list_files = s3_a360.get_s3Filelist_using_wr(s3path_source_file)
    list_file_final = list(filter(lambda x: x not in list_zero_size, list_files))
    source_file_count = len(list_file_final)
    print("DP {} file count :{}".format(src_code, source_file_count))
    for obj in list_file_final:
        bucket, s3_file = s3_a360.split_s3_path(obj)
        copy_dp_files_s3 = s3_a360.copy_cross_region_s3_object(
            s3_bucket,
            s3_file,
            s3_bucket_dp,
            outbound_dp + s3_file.split("/")[-1],
            non_dp_reg,
            dp_reg,
        )
    return source_file_count


# def dp_send_file_process_datasync(cfg, src_code, bucket_name, dps_bucket_name): commented for datasync issue to be worked later
#     s3_bucket = bucket_name
#     s3_bucket_dp = dps_bucket_name
#     # athena_db = athena_database
#     outbound_path_src = cfg["dp_config"][f"{src_code}"]["outbound"]
#     inbound_path_src = cfg["dp_config"][f"{src_code}"]["inbound"]
#     outbound_dp = cfg["dp_config"][f"{src_code}"]["dp_outbound"]
#     inbound_dp = cfg["dp_config"][f"{src_code}"]["dp_inbound"]
#     dp_reg = cfg["aws_reg_dp"]
#     non_dp_reg = cfg["aws_reg_rs"]
#
#     s3path_source_file = "s3://" + s3_bucket + "/" + outbound_path_src
#     # Handle below deletion of files form incoming currently deletes entire folder when empty
#     del_obj = s3_a360.del_all_object_from_s3_folder(s3_bucket_dp, inbound_dp)
#     # del_obj = s3_a360.del_all_object_from_s3_folder(s3_bucket_dp, outbound_dp)
#     list_zero_size = s3_a360.list_zero_byte_objects(s3_bucket, outbound_path_src)
#     for obj in list_zero_size:
#         bucket, s3_file = s3_a360.split_s3_path(obj)
#         s3_a360.delete_bucket_object(bucket, s3_file)
#     print("0 byte files deleted ")
#     list_files = s3_a360.get_s3Filelist_using_wr(s3path_source_file)
#     # list_file_final = list(filter(lambda x: x not in list_zero_size, list_files))
#     source_file_count = len(list_files)
#     print("DP {} file count :{}".format(src_code, source_file_count))
#     # for obj in list_file_final:
#     # bucket, s3_file = s3_a360.split_s3_path(obj)
#     copy_dp_files_s3 = (
#         s3_a360.copy_cross_region_s3_datasync(  # copy_cross_region_s3_object(
#             s3_bucket,
#             outbound_path_src,
#             s3_bucket_dp,
#             outbound_dp,
#             non_dp_reg,
#             dp_reg,
#         )
#     )
#     return source_file_count


def dp_receive_file_process(
    cfg, src_code, bucket_name, dps_bucket_name, athena_database, sent_file_count
):
    s3_bucket = bucket_name
    s3_bucket_dp = dps_bucket_name
    athena_db = athena_database
    athena_tab_to_dp = cfg["dp_config"][f"{src_code}"]["athena_tab_to_dp"]
    athena_tab_from_dp = cfg["dp_config"][f"{src_code}"]["athena_tab_from_dp"]
    outbound_path_src = cfg["dp_config"][f"{src_code}"]["outbound"]
    inbound_path_src = cfg["dp_config"][f"{src_code}"]["inbound"]
    outbound_dp = cfg["dp_config"][f"{src_code}"]["dp_outbound"]
    inbound_dp = cfg["dp_config"][f"{src_code}"]["dp_inbound"]
    dp_reg = cfg["aws_reg_dp"]
    non_dp_reg = cfg["aws_reg_rs"]

    del_obj = s3_a360.del_all_object_from_s3_folder(s3_bucket, inbound_path_src)

    sent_df = query_athena_table(cfg, athena_tab_to_dp, bucket_name, athena_database)
    sent_data_count = sent_df.head()["rec_count"][0]

    received_data_count = 0

    while received_data_count != sent_data_count:
        received_df = query_athena_table(
            cfg, athena_tab_from_dp, bucket_name, athena_database
        )
        received_data_count = received_df.head()["rec_count"][0]
        logging.info("Waiting for {} dp files to be received ".format(src_code))
        time.sleep(300)

    s3path_files_from_dp = "s3://" + s3_bucket_dp + "/" + inbound_dp
    list_files_frm_dp = s3_a360.get_s3Filelist_using_wr(s3path_files_from_dp)
    received_file_count = len(list_files_frm_dp)

    if (
        received_data_count == sent_data_count
        and received_file_count == sent_file_count
    ):
        print("counts matched")
        copy_dp_files_s3 = (
            s3_a360.copy_cross_region_s3_datasync(  # copy_cross_region_s3_object(
                s3_bucket_dp,
                inbound_dp,
                s3_bucket,
                inbound_path_src,
                dp_reg,
                non_dp_reg,
            )
        )
        # for obj in list_files_frm_dp:
        # #     bucket, s3_file = s3_a360.split_s3_path(obj)
        # copy_dp_files_s3 = s3_a360.copy_cross_region_s3_object(
        #         s3_bucket_dp,
        #         s3_file,
        #         s3_bucket,
        #         inbound_path_src + s3_file.split("/")[-1],
        #         non_dp_reg,
        #         dp_reg,
        #     )
        return True
    else:
        return False


def extract_reject_files(**kwargs):
    # Mandatory parameters
    cfg = kwargs.get("cfg")
    bucket_name = kwargs.get("bucket_name")
    mft_bucket_name = kwargs.get("mft_bucket_name")
    secrets_config = kwargs.get("secrets_config")
    athena_database = kwargs.get("athena_database")
    temp_path_athena = kwargs.get("temp_path_athena")
    pulled_list = kwargs.get("pulled_list")
    reject_path = kwargs.get("reject_path")
    batch_id = kwargs.get("batch_id")
    redshift_iam_role = kwargs.get("redshift_iam_role")
    redshift_db=cfg["redshift_db"]
    print("Starting reject process")
    print("pulled_list: ", pulled_list)

    #   1 - Check If source_cd present in pulled list.
    session = boto3.session.Session()
    s3_client = boto3.client("s3")
    # for src_cd, source_data in cfg["list_config"].items():
    for src_cd in pulled_list:
        athena_reject_table = f"stg_{src_cd}_reject"
        input_filename_col = f"filename_dax_{src_cd}"
        query = f"select distinct {input_filename_col} from {athena_reject_table}"
        print("src_cd: ", src_cd)

        df_input_list = athena_a360.executequery(
            boto3_session=session,
            sql=query,
            database=athena_database,
            output_path=temp_path_athena,
        )

        for index, row in df_input_list.iterrows():
            # Access individual row values using row['column_name']
            input_filename = row[input_filename_col.lower()]
            reject_filename = input_filename + ".reject"
            reject_file_with_path = reject_path + batch_id + "/" + reject_filename

            # Unload reject file to reject folder first. uSE REDSHIFT SPECTRUM DB instead of athena
            UNLOAD_SQL = f"""UNLOAD ('SELECT * FROM spectrum_{redshift_db}_stg.{athena_reject_table} WHERE {input_filename_col} = ''{input_filename}''') 
                            TO '{reject_file_with_path}'
                            IAM_ROLE '{redshift_iam_role}'
                            GZIP
                            ALLOWOVERWRITE 
                            PARALLEL OFF"""

            # Execute Unload
            result_unload = redshift_a360.redshift_query_execute(
                UNLOAD_SQL, cfg, secrets_config, ""
            )

            if "reject_mailbox" in cfg["list_config"][src_cd]:
                reject_mailbox = cfg["list_config"][src_cd]["reject_mailbox"]
                for file_prefix, mailbox_list in reject_mailbox.items():
                    if file_prefix in reject_filename.upper():
                        source_bucket = bucket_name
                        source_path = (
                            cfg["s3_key_reject_path"]
                            + batch_id
                            + "/"
                            + reject_filename
                            + "000.gz"
                        )
                        target_bucket = mft_bucket_name
                        for mailbox in mailbox_list:
                            archive_path = (
                                cfg["s3_key_reject_path"]
                                + batch_id
                                + "/"
                                + mailbox
                                + "/"
                                + reject_filename.replace(".reject", ".reject.gz")
                            )
                            target_path = (
                                cfg["s3_key_mft_outbound"]
                                + mailbox
                                + "/"
                                + reject_filename.replace(".reject", ".reject.gz")
                            )
                            # Two passes are required, first pass will transfer to mft bucket, second pass will copy to archive path
                            # First Pass - Copy to MFT bucket (us-east-1)

                            s3_a360.copy_cross_region_s3_object(
                                source_bucket=source_bucket,
                                source_key=source_path,
                                destination_bucket=target_bucket,
                                destination_key=target_path,
                                source_region=cfg["aws_reg_rs"],
                                destination_region=cfg["aws_reg_mft"],
                            )
                            # Second Pass - Copy to archive path
                            s3_a360.copy_s3_object(
                                copy_source_bucket=source_bucket,
                                copy_source_path=source_path,
                                copy_target_bucket=source_bucket,
                                copy_target_path=archive_path,
                            )
                        # Delete only after file copied to all mailboxes. This needs to be outside mailbox_list loop
                        s3_client.delete_object(Bucket=source_bucket, Key=source_path)

        query2 = (
            "select "
            + input_filename_col
            + ",count (*) as rej_count from "
            + athena_reject_table
            + " group by "
            + input_filename_col
        )
        print(query2)
        df_input_list2 = athena_a360.executequery(
            boto3_session=session,
            sql=query2,
            database=athena_database,
            output_path=temp_path_athena,
        )
        print("df_input_list2_value", df_input_list2)
        for index, row in df_input_list2.iterrows():
            file_name = row[input_filename_col.lower()]
            rej_count_t = row["rej_count"]
            # rej_count_t=rej_count
            update_sql = f"""UPDATE A360_CORE.TMDM_SOURCE_FILES
                            SET reject_file_count = {rej_count_t},
                                reject_file_name = '{file_name}.reject',
                                processed_flag = 'Y'
                            WHERE file_name = '{file_name}'"""
            print(update_sql)
            result_update = redshift_a360.redshift_query_execute(
                update_sql, cfg, secrets_config, ""
            )


def run_stg_job(**kwargs):
    cfg = kwargs.get("cfg")
    bucket_name = kwargs.get("bucket_name")
    default_args = kwargs.get("default_args")
    source_cd = kwargs.get("source_cd")
    secrets_config = kwargs.get("secrets_config")
    v_batchid = kwargs.get("batch_id")
    file_present_ind = kwargs.get("file_present_ind")
    redshift_iam_role = kwargs.get("redshift_iam_role")
    print("file_present_ind for source {}:{} ".format(source_cd, file_present_ind))
    run_stg_operator = AwsStgOperator(
        task_id="STG_FCVRL_{}".format(source_cd),
        source_cd="{}".format(source_cd),
        email_to=default_args["email_to"],
        email_subject=default_args["email_subject"],
        email_body=default_args["email_subject"],
        cfg=cfg,
        output_path=default_args["output_path"].replace("{}", bucket_name),
        file_present_ind=file_present_ind,
        script_path=default_args["script_path"].replace("{}", bucket_name),
        format_file_path_glue=default_args["format_file_path_glue"].replace(
            "{}", bucket_name
        ),
        iam_glue_role=default_args["iam_role_glue"],
        iam_redshift_role=redshift_iam_role,
        timeout_glue_job=default_args["timeout_glue_job"],
        maxretries_glue_job=default_args["maxretries_glue_job"],
        common_settings_glue=default_args["common_settings_glue"].replace(
            "{}", bucket_name
        ),
        additional_python_modules_glue=default_args["additional_python_modules_glue"],
        glue_version=default_args["glue_version"],
        extra_py_files_glue=default_args["extra_py_files_glue"].replace(
            "{}", bucket_name
        ),
        connections_glue=default_args["connections_glue"],
        secrets_config_redshift=secrets_config,
        batch_id=v_batchid,
        bucket_name=bucket_name,
        # **kwargs
    )

    run_stg_operator.execute(context=kwargs)


def run_ods_job(gv_checkpoint_code, **kwargs):
    cfg = kwargs.get("cfg")
    bucket_name = kwargs.get("bucket_name")
    default_args = kwargs.get("default_args")
    source_cd = kwargs.get("source_cd")
    secrets_config = kwargs.get("secrets_config")
    v_batchid = kwargs.get("batch_id")
    v_cur_date = kwargs.get("v_cur_date")
    v_cur_user = kwargs.get("v_cur_user")

    procedure_call = "CALL {}({},'{}','{}','{}')".format(
        cfg["list_config"][source_cd]["ods_load_config"]["redshift_procedure"],
        v_batchid,
        gv_checkpoint_code,
        v_cur_date,
        v_cur_user,
    )
    print(procedure_call)
    redshift_a360.redshift_query_execute(
        sql=procedure_call,
        cfg=cfg,
        secrets_config=secrets_config,
        query_type="",
    )


def get_processing_list(cfg, secrets_config, batchid):
    tuple_data = redshift_a360.redshift_query_execute(
        sql="""SELECT distinct t1.source_typ_code FROM A360_core.tmdm_source t1 JOIN A360_core.tmdm_source_files t2 
        ON t2.file_name   LIKE CONCAT(t1.file_name_format, '%') where t2.err_msg is null and t2.batch_id={}""".format(
            batchid
        ),
        cfg=cfg,
        secrets_config=secrets_config,
        query_type="query",
    )
    pulled_list = [x for lst in tuple_data for x in lst]
    print(pulled_list)
    return pulled_list


def get_dupfile_threshold_reject_list(cfg, secrets_config, batchid):
    tuple_data = redshift_a360.redshift_query_execute(
        sql="""with process_temp as (
            select  case when substring(file_name,11,3) in ('E09','E10','E15','E16','E19','E50','E22','E23') THEN 'CC_INTR'
              when substring(file_name,11,3)='E20' THEN 'DIGT_INTR'
              when substring(file_name,11,3)='TDL' THEN 'A30'
              else substring(file_name,11,3) end as fileprefix ,err_msg ,file_name  
                 from mzb_aet_core.tmdm_source_files  
                 where batch_id ={} 
                 )
                 select distinct fileprefix from process_temp a where err_msg is not null
                 and not exists ( select 1 from process_temp where a.fileprefix=fileprefix
                 and err_msg is null)""".format(
            batchid
        ),
        cfg=cfg,
        secrets_config=secrets_config,
        query_type="query",
    )
    pulled_list = [x for lst in tuple_data for x in lst]
    print(pulled_list)
    return pulled_list


def generate_excel_report(
    bucket_name,
    cfg,
    secrets_config,
    report_path,
    file_name,
    step_set,
    batchid,
    **kwargs,
):
    if "sql_str" not in kwargs:
        v_sql = (
            "select * from a360_core.tmdm_config_sql "
            "where step_set = '{}' order by process_order"
        ).format(step_set)
        sql = redshift_a360.redshift_query_execute(v_sql, cfg, secrets_config, "query")
    else:
        sql = kwargs["sql_str"]
        print(sql)

    file_path = report_path + "/" + batchid + "/" + file_name
    s3_path = "s3://" + bucket_name + "/" + file_path

    excel_writer = pd.ExcelWriter("tmp/{}".format(file_name), engine="xlsxwriter")
    for rec in sql:
        if "sql_str" not in kwargs:
            step_name_value = rec[1]
            step_sql = rec[5]
            sheet_name = step_name_value

        if "sql_str" in kwargs:
            step_sql = rec[0]
            sheet_name = rec[1]

        exec_sql, column_list = redshift_a360.redshift_query_execute(
            step_sql, cfg, secrets_config, "query", col_names="Y"
        )

        df1 = pd.DataFrame.from_records(exec_sql, columns=column_list)
        print(df1.shape)
        df1.to_excel(excel_writer, sheet_name=sheet_name, header=True, index=False)
        wb = excel_writer.book
        ws = excel_writer.sheets[f"{sheet_name}"]
        cell_color = "#50D092"
        header_format = wb.add_format({"bold": True, "fg_color": cell_color})
        for col_num, value in enumerate(df1.columns.values):
            ws.write(0, col_num, value, header_format)

    excel_writer.close()
    s3_a360.upload_file("tmp/{}".format(file_name), bucket_name, file_path)
    os.remove("tmp/{}".format(file_name))

    return s3_path


def mcd_run_process(py_name, cfg, mdm_bucket_name, application_id, emr_iam_role):
    session = boto3.Session()
    client = session.client("emr-serverless", region_name=cfg["aws_reg_rs"])
    # v_client = "demo"
    v_client = "qa"
    response = client.start_job_run(
        applicationId=application_id,
        executionRoleArn=emr_iam_role,
        jobDriver={
            "sparkSubmit": {
                "entryPoint": f"s3://{mdm_bucket_name}/scripts/{py_name}",
                "entryPointArguments": ["--env", "Prod", "--client", v_client],
                "sparkSubmitParameters": f"--conf spark.submit.pyFiles=s3://{mdm_bucket_name}/scripts/mcdschema.py --conf spark.archives=s3://{mdm_bucket_name}/scripts/pyspark_venv.tar.gz#environment --conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python --conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python --conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://axle-elevance-mdm/glue_catalog/ --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.jars=/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar  --conf spark.driver.maxResultSize=12g --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
            },
        },
        # --conf spark.sql.catalog.glue_catalog.lock - impl = org.apache.iceberg.aws.glue.DynamoLockManager - -conf spark.sql.catalog.glue_catalog.lock.table = unprocessed
        executionTimeoutMinutes=60,
        name="a360_{}".format(py_name.split(".")[0]),
    )

    job_run_id = response["jobRunId"]

    job_run_flag = "start"

    while job_run_flag == "start":
        time.sleep(300)
        response = client.get_job_run(applicationId=application_id, jobRunId=job_run_id)
        job_run_status = response["jobRun"]["state"]
        if job_run_status == "SUCCESS":
            logging.info("A360 Mcd Script {} Process Success ".format(py_name))
            break
        elif job_run_status in ("FAILED", "CANCELLING", "CANCELLED"):
            logging.info(
                "A360 Mcd Script {} Process Status: {} ".format(py_name, job_run_status)
            )
            raise Exception("A360 Mcd {} Script Failed, please review.".format(py_name))


def get_queue_url(cfg, account_id):
    reg_name = cfg["aws_reg_rs"]
    session = boto3.session.Session()
    sqs_client = session.client("sqs", region_name=reg_name)
    v_client = "demo"
    queue_name = "axle-{}-ret-sqs".format(v_client)

    try:
        response = sqs_client.get_queue_url(
            QueueName=queue_name, QueueOwnerAWSAccountId=account_id
        )
        logging.info("Queue url".format(response["QueueUrl"]))
        return response["QueueUrl"]
    except ClientError as e:
        raise e


def get_queue_message(p_queue_url, cfg):
    reg_name = cfg["aws_reg_rs"]
    session = boto3.session.Session()
    sqs_client = session.client("sqs", region_name=reg_name)
    try:
        response = sqs_client.receive_message(
            QueueUrl=p_queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10,
        )
    except ClientError as e:
        raise e
    logging.info(f"Number of messages received: {len(response.get('Messages', []))}")
    return len(response.get("Messages", []))


def purge_mcdret_queue(cfg, account_id):
    session = boto3.session.Session()
    reg_name = cfg["aws_reg_rs"]
    sqs_client = session.client("sqs", region_name=reg_name)
    v_queue_url = get_queue_url(cfg, account_id)
    print(v_queue_url)
    try:
        response = sqs_client.purge_queue(
            QueueUrl=v_queue_url,
        )
        logging.info(response)
    except ClientError as e:
        raise e


def mcd_match_indicator(cfg, account_id):
    v_queue_url = get_queue_url(cfg, account_id)

    job_run_flag = "start"

    while job_run_flag == "start":
        v_msg_count = get_queue_message(p_queue_url=v_queue_url, cfg=cfg)
        if v_msg_count > 0:
            logging.info("On Premise match Job Complete")
            break
        else:
            logging.info("Sleeping for 5 mins")
            time.sleep(300)


def get_html_format(sql, cfg, secrets_config):
    query_data, column_list = redshift_a360.redshift_query_execute(
        sql, cfg, secrets_config, "query", col_names="Y"
    )
    df = pd.DataFrame.from_records(query_data, columns=column_list)
    html_table = df.to_html(index=False)

    html_table = html_table.replace(
        '<table border="1" class="dataframe">',
        '<table style="width:100% ; border-collapse:collapse; border:1px">',
    )
    html_table = html_table.replace("text-align: right", "text-align: left")
    html_table = html_table.replace(
        "<th>",
        '<th style="width:30%;border:1px solid black ;border-collapse:collapse ; text-align:left ;color:black;font-family: verdana; font-size: 10pt;font-weight:bold; background-color:#669999">',
    )
    html_table = html_table.replace(
        "<td>",
        '<td style="width:20%;border:1px solid black ;border-collapse:collapse ; text-align:left ;color:black;font-family: verdana; font-size: 7.5pt;font-weight:bold; background-color:#D4D4D4">',
    )
    print(html_table)
    return html_table


def send_file_stats(step_set, email_subject, cfg, secrets_config):
    sql = f"select * from mzb_aet_core.tmdm_config_sql where step_set = '{step_set}' "
    exec_sql = redshift_a360.redshift_query_execute(sql, cfg, secrets_config, "query")
    # cnt_step_sql = exec_sql[0][5]
    query_step_sql = exec_sql[0][5]
    # v_cnt = redshift_a360.redshift_query_execute(
    #     cnt_step_sql, cfg, secrets_config, "query"
    # )
    # print("Query to be executed {} and count returned {}".format(cnt_step_sql, v_cnt))

    current_env = Variable.get("var-env", "local")
    if current_env == "Prod":
        email_business_users_list = ["DL-A360-Team@data-axle.com"]
    else:
        email_business_users_list = ["DL-A360-Team@data-axle.com"]

    mail_body = get_html_format(query_step_sql, cfg, secrets_config)
    print("Step : {} Mail body {}".format(step_set, mail_body))
    email.send_email(
        client="A360",
        email_address="DL-A360-Team@data-axle.com",
        # to be configured via json -- email list
        email_business_users=email_business_users_list,
        email_subject=email_subject,
        email_message=mail_body,
    )


def run_extract_task(**kwargs):
    cfg = kwargs.get("cfg")
    bucket_name = kwargs.get("bucket_name")
    default_args = kwargs.get("default_args")
    task_name = kwargs.get("task_name")
    secrets_config_redshift = kwargs.get("secrets_config_redshift")
    batch_id = kwargs.get("batch_id")
    current_dt = kwargs.get("current_dt")
    extracts_path = default_args.get("extracts_path")
    extract_config = cfg["extract_configs"].get(task_name)
    mft_bucket_name = kwargs.get("mft_bucket_name")
    redshift_iam_role = kwargs.get("redshift_iam_role")
    cfg["insert_query"] = cfg["insert_query"].replace("<batch_id>", batch_id).replace("<request_type>", cfg["request_type"])
    extract_operator = AwsExtractOperator(
        extract_config=extract_config,
        secrets_config_redshift=secrets_config_redshift,
        batch_id=batch_id,
        current_dt=current_dt,
        cfg=cfg,
        task_name=task_name,
        iam_redshift_role=redshift_iam_role,
        extracts_path=extracts_path.replace("{}", bucket_name),
        iam_glue_role=default_args["iam_role_glue"],
        script_path=default_args["script_path"].replace("{}", bucket_name),
        glue_version=default_args["glue_version"],
        connections_glue=default_args["connections_glue"],
        timeout_glue_job=default_args["timeout_glue_job"],
        extra_py_files_glue=default_args["extra_py_files_glue"].replace(
            "{}", bucket_name
        ),
        bucket=bucket_name,
        mft_bucket_name=mft_bucket_name,
        audit_schema=default_args["audit_schema"],
        audit_table=default_args["audit_table"],
    )
    extract_operator.execute(context=kwargs)


def generate_match_rpt(athena_path, athena_database):
    session = boto3.session.Session()

    query = "SELECT * FROM vw_mcd_match_rpt"
    query_response = athena_a360.executequery(
        boto3_session=session,
        sql=query,
        database="demomdm",
        output_path=athena_path,
    )

    query_response.columns = query_response.columns.str.upper()
    html_table = query_response.to_html(index=False)
    html_table = html_table.replace(
        '<table border="1" class="dataframe">',
        '<table border="1" class="dataframe" width="60%">',
    )
    html_table = html_table.replace("text-align: right", "text-align: centre")
    html_table = html_table.replace(
        "<th>", '<th style="background-color: lightblue; font-size: 16px;">'
    )
    return html_table


def load_crc_prev_process(
    pulled_list, cfg, athena_database, athena_result_path, bucket_name, secrets_config
):
    sec_name = cfg["secret_name"]
    region_name = cfg["aws_reg_rs"]

    for i in pulled_list:
        s3_object_path = "process_fcvrl/athena_glue/stg_" + str(i).lower() + "_crc_prev"
        print(s3_object_path)
        del_obj = s3_a360.del_all_object_from_s3_folder(bucket_name, s3_object_path)
    logging.info(" crc prev folders cleaned up")
    step_set = "crc_prev_load"
    sql = (
        f"select * from a360_core.tmdm_config_sql where step_set = '{step_set}'"
        f"order by process_order"
    )
    return_val = redshift_a360.redshift_query_execute(sql, cfg, secrets_config, "query")
    for row in return_val:
        step_table = row[3]
        step_sql = row[5]
        if step_table in pulled_list:
            print(f"Match found for step_table: {step_table}")
            print(step_sql)
            # Create Athena Boto3 client
            athena_client = boto3.client("athena", region_name)
            output_path = athena_result_path
            success = athena_a360.executenonquery(
                boto3_client=athena_client,
                sql=step_sql,
                database=athena_database,
                output_path=output_path,
            )
            if success:
                print("Query execution successful!")
            else:
                print("Query execution failed!")


def get_file_record_count(
    batch_id,
    incoming_path,
    redshift_config_path,
    s3_bucket,
    table_source_file_stats,
    **kwargs,
):
    extra_jars = kwargs.get("script_path") + "splittablegzip-1.3.jar"
    submit_glue_job = GlueJobOperator(
        task_id=kwargs.get("glue_job_name"),
        job_name=kwargs.get("glue_job_name"),
        script_location=kwargs.get("script_path") + "job_get_file_record_count.py",
        iam_role_name=kwargs.get("iam_glue_role"),
        update_config=True,
        create_job_kwargs={
            "GlueVersion": "4.0",
            "NumberOfWorkers": 5,
            "WorkerType": "G.1X",
            "Timeout": 90,
            "MaxRetries": 0,
            "DefaultArguments": {  # Default arguments for the job
                "--additional-python-modules": "h3==3.7.6,smart-open==6.3.0,openpyxl==3.1.2",
                "--batch_id": batch_id,
                "--incoming_path": incoming_path,
                "--redshift_config_path": redshift_config_path,
                "--s3_bucket": s3_bucket,
                "--table_source_file_stats": table_source_file_stats,
                "--enable-metrics": "true",
                "--enable-glue-datacatalog": "true",
                "--extra-jars": extra_jars,
                # Additional arguments to pass to the job script
            },
            "Connections": {"Connections": [kwargs.get("connections_glue")]},
        },
    )

    gluecontext = get_current_context()
    submit_glue_job.execute(context=gluecontext)


def get_most_recent_dag_run(dag_id):
    dag_runs = DagRun.find(dag_id)
    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    if dag_runs:
        most_recent_dag_run = dag_runs[0]
        print(
            "The most recent DagRun for dag id {} was executed at {}".format(
                dag_id, most_recent_dag_run.execution_date
            )
        )
        return most_recent_dag_run
    else:
        print("No DAG runs found for dag id {}".format(dag_id))
        return None


def a360_gen_extract(
    cfg,
    bucket_name,
    secrets_config,
    redshift_iam_role,
    v_execid,
    p_cur_user,
    v_cur_date,
    p_freq,
):
    json_var_val = cfg
    bucket_s3 = bucket_name
    separator = "|"
    file_ts = v_cur_date
    datetimefolder = datetime.now().strftime("%Y-%m-%d")
    # p_freq = 'DAILY'
    pr_ob_extracts_path = (
        json_var_val["process_outbound_extracts"] + "/" + str(datetimefolder)
    )
    bucket_s3_path = "s3://" + bucket_s3 + "/" + pr_ob_extracts_path

    sql = f"call a360_load.prc_a360_extracts_trkg({v_execid}, 'Outbound Extracts':: character varying, '{p_cur_user}'::character varying, 'R', '{p_freq}'::character varying, 'X'::character varying)"

    print(sql)
    redshift_a360.redshift_query_execute(
        sql=sql,
        cfg=cfg,
        secrets_config=secrets_config,
        query_type="",
    )

    logging.info("prc_a360_extracts_trkg for 'R' Complete")

    record_set = redshift_a360.redshift_query_execute(
        sql=f"""select view_name FROM a360_ods.a360_outbound_extracts_hist where frequency = '{p_freq}' and exec_id = {v_execid} and """
        f"""process_status = 'R' """,
        cfg=cfg,
        secrets_config=secrets_config,
        query_type="query",
    )

    view_names = [result[0] for result in record_set]

    for vw in view_names:
        v_unload_sql = f"""
                        unload ('select * from a360_dwh.{vw}')
                        to '{bucket_s3_path}/{vw}|{file_ts}.txt'
                        IAM_ROLE '{redshift_iam_role}'
                        DELIMITER '{separator}'
                        HEADER
                        ALLOWOVERWRITE
                        PARALLEL ON;
                        """

        print(v_unload_sql)
        redshift_a360.redshift_query_execute(
            sql=v_unload_sql,
            cfg=cfg,
            secrets_config=secrets_config,
            query_type="",
        )
        logging.info(" A360 dwh views Unload Complete")

        procedure_call = "CALL a360_load.prc_a360_extracts_trkg ({},'{}','{}','{}','{}','{}')".format(
            v_execid, "Outbound Extracts", p_cur_user, "C", p_freq, vw
        )
        print(procedure_call)
        redshift_a360.redshift_query_execute(
            sql=procedure_call,
            cfg=cfg,
            secrets_config=secrets_config,
            query_type="",
        )


def rename_outbound_extract(cfg, bucket_name, p_cur_date):
    logging.info("Renaming extracts")
    json_var_val = cfg
    # bucket_s3 = json_var_val["bucket"]
    bucket_s3 = bucket_name
    datetimefolder = datetime.now().strftime("%Y-%m-%d")
    source_folder = (
        json_var_val["process_outbound_extracts"] + "/" + str(datetimefolder)
    )
    destination_folder = json_var_val["outbound_extracts"]
    maxKeys = 1000
    fname_list = ["File_Name|File_Count\n"]
    delimiter = "/"
    objects = s3_a360.getsingleobjectlist(
        bucket_s3, source_folder + "/", maxKeys, delimiter
    )
    keys = list(filter(lambda k: ".txt" in k, objects))
    file_ts = p_cur_date
    print("keys : ", str(keys))
    for file_to_rename in keys:
        # folder, file_name = file_to_rename.split("/")
        file_nm = file_to_rename.split("/")
        file_name = file_nm[3]

        file_name_with_time, part_name = file_name.split(".txt")
        view_name, file_timestamp = file_name_with_time.split("|")

        destination_file_name = (
            destination_folder
            + "/"
            + view_name
            + "_"
            + part_name
            + "_"
            + file_timestamp
            + ".txt"
        )

        response = s3_a360.read_s3_obj_content(bucket_s3, file_to_rename)

        for event in response["Payload"]:
            if "Records" in event:
                count = int(event["Records"]["Payload"].decode("utf-8"))

                if count > 1:
                    file_data = (
                        destination_file_name.split("/")[-1]
                        + "|"
                        + event["Records"]["Payload"].decode("utf-8")
                    )
                    fname_list.append(file_data)
                    # copy_source = {"Bucket": bucket_s3, "Key": file_to_rename}
                    s3_a360.copy_s3_object(
                        bucket_s3, file_to_rename, bucket_s3, destination_file_name
                    )
    logging.info("Files with content is written to s3.")

    trigger_data = "".join(fname_list)
    s3_a360.s3_put_object(
        bucket_name=bucket_s3,
        key=f"{destination_folder}/A360_OUTBOUND_EXTRACTS_CTL_{file_ts}.txt",
        body=trigger_data.encode(),
    )
    logging.info("Final Report file placed in S3.")

    s3_path = f"s3://{bucket_s3}/{destination_folder}/A360_OUTBOUND_EXTRACTS_CTL_{file_ts}.txt"

    return s3_path


def da_source_files_stats(**kwargs):
    bucket_name = kwargs.get("bucket_name")
    cfg = kwargs.get("cfg")
    folder_prefix = kwargs.get("folder_prefix")
    file_pattern = kwargs.get("file_pattern")
    v_batchid = kwargs.get("v_batchid")
    v_cur_date = kwargs.get("v_cur_date")
    secrets_config = kwargs.get("secrets_config")
    default_args = kwargs.get("default_args")
    count_update = kwargs.get("count_update")

    if count_update:
        print("Glue job starting ....")
        get_file_record_count(
            batch_id=v_batchid,
            incoming_path=folder_prefix,
            redshift_config_path=default_args["redshift_config_paths"].replace(
                "{}", bucket_name
            ),
            s3_bucket=bucket_name,
            table_source_file_stats="stg_source_file_stats",
            script_path=default_args["script_path"].replace("{}", bucket_name),
            iam_glue_role=default_args["iam_role_glue"],
            connections_glue=default_args["connections_glue"],
        )

        print("a360_core.prc_check_dup_files proc loading starts....")
        procedure_call = "CALL a360_core.prc_check_dup_files ({},'{}')".format(
            v_batchid, v_cur_date
        )
        print(procedure_call)
        redshift_a360.redshift_query_execute(
            sql=procedure_call,
            cfg=cfg,
            secrets_config=secrets_config,
            query_type="",
        )

        return None

    df = pd.DataFrame(columns=["file_name", "batch_id", "file_record_count"])
    file_present_ind = "N"
    complete_path_iter = "s3://{}/{}/{}".format(
        bucket_name,
        folder_prefix,
        file_pattern,
    )
    print("complete_path_iter: ", str(complete_path_iter))
    filelist = s3_a360.get_s3Filelist_using_wr(complete_path_iter)
    print("filelist: ", str(filelist))
    for file in filelist:
        bucket, key = s3_a360.split_s3_path(file)
        print("file: ", file)
        print("bucket: ", bucket)
        print("key: ", key)
        file_name = key.split("/")[1]
        count = -1
        file_iter_df = pd.DataFrame(
            {
                "file_name": [key.split("/")[-1]],
                "batch_id": v_batchid,
                "file_record_count": pd.Series([count], dtype="int64"),
            }
        )
        df = pd.concat([df, file_iter_df], ignore_index=True)

    print(df.dtypes)
    engine = redshift_a360.get_redshift_sqlalchemy_conn(
        cfg["aws_reg_rs"], cfg["redshift_cluster"], cfg["redshift_db"], secrets_config
    )
    if not df.empty:
        file_present_ind = "Y"
        df.to_sql(
            "stg_source_file_stats",
            con=engine,
            if_exists="append",
            index=False,
            schema="a360a360_stg",
        )

    return file_present_ind


def run_dp_job(**kwargs):
    task_name = kwargs.get("task_name")
    cfg = kwargs.get("cfg")
    bucket_name = kwargs.get("bucket_name")
    request_id = kwargs.get("request_id")
    secrets_config = kwargs.get("secrets_config")
    v_batchid = kwargs.get("batch_id")
    athena_database = kwargs.get("athena_database")
    dps_bucket_name = kwargs.get("dps_bucket_name")
    redshift_iam_role = kwargs.get("redshift_iam_role")
    email_to = kwargs.get("email_to")
    client = kwargs.get("client")
    print("Executing task for DP Request {}:{} ".format(request_id, task_name))
    run_dp_operator = AwsDPOperator(
        task_id="run_dp_job_{}".format(request_id),
        task_name=task_name,
        cfg=cfg,
        iam_redshift_role=redshift_iam_role,
        secrets_config_redshift=secrets_config,
        batch_id=v_batchid,
        request_id=request_id,
        athena_database=athena_database,
        bucket_name=bucket_name,
        dps_bucket_name=dps_bucket_name,
        email_to=email_to,
        client=client,
    )

    run_dp_operator.execute(context=kwargs)


def athenaRejectCount(table_name, filename):
    query = f"""WITH data AS (SELECT 
                                        CAST(json_parse(reject_json) AS map<varchar, varchar>) AS json_map
                                    FROM 
                                        "{athena_database}".stg_{table_name}_reject where replace(lower(filename_infogroup_{table_name}),'.gz','') = replace(lower('{filename}'),'.gz','')
                                )
                                SELECT
                                    substring(kv_key,8) AS attribute,
                                    count(*) as count 
                                FROM
                                    data,
                                    UNNEST(json_map) AS t(kv_key, kv_value)
                                group by kv_key"""
    print(query)
    athena_result_path = "athena_query_results"
    session = boto3.session.Session()
    output_path = f"s3://{bucket_name}/{athena_result_path}/"
    result_athena = athena_a360.executequery(
        boto3_session=session,
        sql=query,
        database=athena_database,
        output_path=output_path,
    )
    df_header_reject = result_athena[result_athena.iloc[:, 0] == "header"]
    if not df_header_reject.empty:
        return df_header_reject

    return result_athena


#def business_to_internal_ingestion_integration(
#    cfg, secret_cfg, postgres_sec_config, destination_bucket, feed_types=None
#):
#    feed_types_str = f"({', '.join([repr(ft) for ft in feed_types])})"
#    rds_query = f"""SELECT a.data_ingestion_id, a.data_source_type, data_source_object, uploaded_file_name, feed, 
#    company_id, content_type, feed_type, priority_processing, b.cloud_region FROM mz360.data_ingestion a join 
#    mz360.connector b on a.connector_id = b.connector_id WHERE a.feed_type IN {feed_types_str} and a.is_enabled = 
#    'True' and a.data_source_type = 'S3'"""

def business_to_internal_ingestion_integration(
    cfg, secret_cfg, postgres_sec_config, destination_bucket, data_ingestion_names=None
):
    data_ingestion_names_str = f"({', '.join([repr(ft) for ft in data_ingestion_names])})"
    rds_query = f"""SELECT a.data_ingestion_id, a.data_source_type, data_source_object, uploaded_file_name, feed, 
    company_id, content_type, feed_type, priority_processing, b.cloud_region FROM mz360.data_ingestion a join 
    mz360.connector b on a.connector_id = b.connector_id WHERE a.name IN {data_ingestion_names_str} and a.is_enabled = 
    'True' and a.data_source_type = 'S3'"""
    print(f"Executing SQL: {rds_query}")
    data, column_names = a360_pg.pg_query_execute(
        rds_query, cfg, postgres_sec_config, "query", col_names="Y"
    )

    if not data:
        print("No data found in the result.")

    # da_ingestion_path = cfg['ingestion_path']
    print("data :", str(data))
    for row in data:
        data_source_object = row[column_names.index("data_source_object")]
        upload_file_location = data_source_object.replace("s3://", "")
        bus_bucket = upload_file_location.split("/")[0]
        bus_path = "/".join(upload_file_location.split("/")[1:])
        src_reg = row[column_names.index("cloud_region")]
        file_patterns = row[
            column_names.index("uploaded_file_name")
        ]  # Assuming each row has a single file pattern
        content_type = row[column_names.index("content_type")]
        # src_reg = 'us-east-1'  #row[column_names.index('src_reg')]

        print(
            f"Upload File Location: {upload_file_location}, Bus Bucket: {bus_bucket}, Bus Path: {bus_path}, File "
            f"Patterns: {file_patterns}, Content Type: {content_type}"
        )

        segments = file_patterns.split("_")
        processed_pattern = "_".join(segments[:2])
        redshift_files = None

        if len(bus_path) > 0:
            source_key = f"{bus_path}/{processed_pattern}*"
            source_prefix = bus_path
        else:
            source_key = f"{processed_pattern}*"
            source_prefix = ""

        complete_path_iter = f"s3://{bus_bucket}/{source_key}"

        s3_objects = get_s3Filelist_using_wr(complete_path_iter)

        sorted_sb_objects = sorted(
            s3_objects, key=lambda x: x.split(".")[0].split("_")[-1], reverse=True
        )
        print("files:", str(sorted_sb_objects))
        destination_prefix = cfg["ingestion_path"]

        if sorted_sb_objects:
            if content_type == "Full":
                print("File type is Full")
                latest_file = sorted_sb_objects[0] if sorted_sb_objects else None
                if latest_file:
                    file_copy = latest_file.replace("s3://", "").split("/")[-1]
                    print("source_prefix", source_prefix)
                    print("file_copy", file_copy)
                    copy_bus_files_s3 = s3_a360.copy_cross_region_s3_datasync(  # copy_cross_region_s3_object(
                        bus_bucket,
                        source_prefix,
                        destination_bucket,
                        destination_prefix,
                        src_reg,
                        cfg["aws_reg_rs"],
                        file_copy,
                    )
            if content_type == "Incremental":
                sql = f"select file_name from a360_core.tmdm_source_files WHERE file_name LIKE '{processed_pattern}%' "
                print(sql)
                print("cfg: ", str(cfg))
                return_val = redshift_a360.redshift_query_execute(
                    sql, cfg, secret_cfg, "query"
                )
                redshift_files = [row[0] for row in return_val] if return_val else []
                print("redshift_files: ", str(redshift_files))

                new_files = []
                for file in sorted_sb_objects:
                    file_name = file.replace("s3://", "").split("/")[-1]

                    if file_name not in redshift_files:
                        new_files.append(file)

                if new_files:
                    # Copy only the new files to the destination bucket
                    file_list = []
                    for new_file in new_files:
                        print(f"Copying new file: {new_file}")
                        s3_key = new_file.replace("s3://", "").split("/")[-1]
                        print("source_prefix", source_prefix)
                        print("s3_key", s3_key)
                        copy_bus_files_s3 = s3_a360.copy_cross_region_s3_datasync(  # copy_cross_region_s3_object(
                            bus_bucket,
                            source_prefix,
                            destination_bucket,
                            destination_prefix,
                            src_reg,
                            cfg["aws_reg_rs"],
                            s3_key,
                        )


def unload_redshift_datashare_data(unload_bucket, file_pattern, src_tab,incremental_field,feed, cfg, redshift_iam_role, secrets_config,datashare_name):

    #fetch consumer_database 
    sql = (
        f"select * from SVV_DATASHARES where share_name = '{datashare_name}'"
    )
    return_val = redshift_a360.redshift_query_execute(
        sql, cfg, secrets_config, "query"
    )  
    
    consumer_database=""
    for row in return_val:
        consumer_database = row[4]  
        
    if incremental_field is not None:
        incremental_fields = [f.strip() for f in incremental_field.split(",")]
                 
        # Get last unloaded values
        sql = f"SELECT incremental_field_value FROM a360_core.tmdm_source WHERE source_typ_code = '{feed}'"
        return_val = redshift_a360.redshift_query_execute(sql, cfg, secrets_config, "query")  
        
        
        last_unloaded_value = ""
        for row in return_val:
            last_unloaded_value = row[0]  # Assuming a comma-separated string for two incremental_fields
    
        print(f"last_unloaded_value : {last_unloaded_value}" )
        if  last_unloaded_value =="":
            last_unloaded_values="1900-01-01"
        else:
            last_unloaded_values = [v.strip() for v in last_unloaded_value.split(",")]



        
        if len(incremental_fields) == 1:
            sql = f"SELECT MAX({incremental_fields[0]}) FROM {consumer_database}.{src_tab} "
        elif len(incremental_fields) == 2:
            sql = (
                f"SELECT MAX({incremental_fields[0]}), MAX({incremental_fields[1]}) "
                f"FROM {consumer_database}.{src_tab} "
            )
        return_val = redshift_a360.redshift_query_execute(sql, cfg, secrets_config, "query")  
        
        max_incremental_field = []
        for row in return_val:
            max_incremental_field = [str(v) for v in row]

        
        # Update the tmdm_source table
        max_val_str = ",".join(max_incremental_field)
        print(f"max_val_str : {max_val_str}")
        sql = (
            f"UPDATE a360_core.tmdm_source SET incremental_field_value='{max_val_str}' "
            f"WHERE source_typ_code = '{feed}'"
        )
        print(sql)
        redshift_a360.redshift_query_execute(sql, cfg, secrets_config, "")

        # Construct where for UNLOAD
        #below two if conditions are for batch process
        if len(incremental_fields) == 1 and last_unloaded_values!='1900-01-01':
            where_clause = (
                f"WHERE {incremental_fields[0]} > ''{last_unloaded_values[0]}'' AND {incremental_fields[0]} <= ''{max_incremental_field[0]}'' "
            )
        elif len(incremental_fields) == 2 and last_unloaded_values!='1900-01-01':
            
            where_clause = (
                f"WHERE (({incremental_fields[0]} > ''{last_unloaded_values[0]}'' AND {incremental_fields[0]} <= ''{max_incremental_field[0]}'') "
                f"OR ({incremental_fields[1]} > ''{last_unloaded_values[1]}'' AND {incremental_fields[1]} <= ''{max_incremental_field[1]}'')) "
            ) 
        #below two if conditions are for validation process --first time run    
        elif len(incremental_fields) == 1 and last_unloaded_values=='1900-01-01':
            where_clause = (
                f"WHERE {incremental_fields[0]} > ''{last_unloaded_values}'' AND {incremental_fields[0]} <= ''{max_incremental_field[0]}'' "
            )
        elif len(incremental_fields) == 2 and last_unloaded_values=='1900-01-01':
            
            where_clause = (
                f"WHERE (({incremental_fields[0]} > ''{last_unloaded_values}'' AND {incremental_fields[0]} <= ''{max_incremental_field[0]}'') "
                f"OR ({incremental_fields[1]} > ''{last_unloaded_values}'' AND {incremental_fields[1]} <= ''{max_incremental_field[1]}'')) "
            )             
                                   
            

        # UNLOAD command
        v_sql = f"""UNLOAD ('SELECT * FROM {consumer_database}.{src_tab} {where_clause}')
                    TO 's3://{unload_bucket}/incoming/{file_pattern}'
                    IAM_ROLE '{redshift_iam_role}'
                    NULL AS ''
                    ALLOWOVERWRITE
                    DELIMITER '|'
                    MAXFILESIZE 2 GB
                    HEADER
                    PARALLEL OFF
                    EXTENSION 'TXT'
                """         
        
        print(v_sql)

        redshift_a360.redshift_query_execute(
            sql=v_sql, cfg=cfg, secrets_config=secrets_config, query_type="unload"
        )                          
        
        
    else:
        
        v_sql = f"""UNLOAD ('select * from {consumer_database}.{src_tab}')
                        TO 's3://{unload_bucket}/incoming/{file_pattern}'
                        IAM_ROLE '{redshift_iam_role}'
                        NULL AS ''
                        ALLOWOVERWRITE
                        DELIMITER '|'
                        MAXFILESIZE 2 GB
                        HEADER
                        PARALLEL OFF
                        EXTENSION 'TXT'
                    """        

        print(v_sql)

        redshift_a360.redshift_query_execute(
            sql=v_sql, cfg=cfg, secrets_config=secrets_config, query_type="unload"
        )
    
        
    
    
        


def dag_git_push_consolidated(file_type_mapping, git_secrets_config):
    print("file_type_mapping :", str(file_type_mapping))
    access_token = git_secrets_config["github_access_token"]
    repo_name = git_secrets_config["repo_name"]
    base_branch_name = git_secrets_config["base_branch"]
    repo_url = f"https://{access_token}@github.com/{repo_name}.git"
    clone_path = "/tmp/a360"
    commit_message = "Added/Updated DAG and Config files"
    pr_title = "Add or Modify DAG and Config Files"
    pr_body = "This PR adds or updates the DAG and Config files."

    if os.path.exists(clone_path):
        print(f"Removing existing directory at {clone_path}...")
        shutil.rmtree(clone_path)

    try:
        # Clone the repository
        print(f"Cloning repository to {clone_path}...")
        repo = Repo.clone_from(repo_url, clone_path)
        print("Repository cloned successfully.")

        # Checkout the base branch
        print(f"Checking out the base branch '{base_branch_name}'...")
        repo.git.checkout(base_branch_name)

        # Create a unique branch for all DAGs and Config files
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        new_branch_name = f"feature/dags_configs_{timestamp}"
        print(f"Creating and checking out branch '{new_branch_name}'...")
        new_branch = repo.create_head(new_branch_name)
        new_branch.checkout()

        # Stage and write all DAG and Config files based on the type
        file_paths = []
        for file_type, files in file_type_mapping.items():
            if file_type == "dag":
                folder = "dags"
            elif file_type == "config":
                folder = "config"
            else:
                print(f"Skipping unknown file type: {file_type}")
                continue

            for dag_name, dag_content in files.items():
                file_path = os.path.join(
                    clone_path, "builds", "aws", "a360", folder, dag_name
                )
                print(f"Adding content to {file_path}...")
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                if isinstance(dag_content, dict):
                    dag_content = json.dumps(dag_content, indent=4)

                with open(file_path, "w") as f:
                    f.write(dag_content)
                file_paths.append(file_path)

        # Add all changes to Git
        print("Staging all changes...")
        repo.git.add(file_paths)

        # Commit all changes
        print("Committing all changes...")
        repo.index.commit(commit_message)

        # Push the changes to the remote repository
        print(f"Pushing changes to branch '{new_branch_name}'...")
        repo.remote(name="origin").push(new_branch_name)

        # Create Pull Request and Merge
        print("Creating a Pull Request...")
        headers = {"Authorization": f"token {access_token}"}
        pr_url = f"https://api.github.com/repos/{repo_name}/pulls"
        pr_data = {
            "title": pr_title,
            "body": pr_body,
            "head": new_branch_name,  # Source branch
            "base": base_branch_name,  # Target branch
        }
        response = requests.post(pr_url, json=pr_data, headers=headers)

        if response.status_code == 201:
            pr_response = response.json()
            pr_number = pr_response["number"]
            print(f"Pull Request created successfully: {pr_response['html_url']}")

            # Merging the Pull Request
            print(f"Merging Pull Request #{pr_number}...")
            merge_url = (
                f"https://api.github.com/repos/{repo_name}/pulls/{pr_number}/merge"
            )
            merge_response = requests.put(merge_url, headers=headers)

            if merge_response.status_code == 200:
                print("Pull Request merged successfully.")
            else:
                print(
                    f"Failed to merge Pull Request: {merge_response.status_code} - {merge_response.json()}"
                )
                raise Exception(
                    "Failed to merge Pull Request"
                )
        else:
            print(
                f"Failed to create Pull Request: {response.status_code} - {response.json()}"
            )

    except Exception as e:
        print(f"Error during Git operations: {e}")
        raise e


def create_dag_config(
    cfg, bucket_name, secret_name, list_config_inputs, frequency, priority_key=None
):
    config = {
        "bucket_name": cfg["bucket_name"],
        "dps_bucket_name": cfg["dps_bucket_name"],
        "aws_reg_rs": cfg["aws_reg_rs"],
        "aws_reg_dp": cfg["aws_reg_dp"],
        "aws_reg_mft": cfg["aws_reg_mft"],
        "a360_global_param": cfg["a360_global_param"],
        "git_repo_secret_name": cfg["git_repo_secret_name"],
        "request_type": cfg["request_type"],
        "s3_key_reject_path": cfg["s3_key_reject_path"],
        "metric_report_path": cfg["metric_report_path"],
        "s3_key_extracts_path": cfg["s3_key_extracts_path"],
        "mft_bucket": cfg["mft_bucket"],
        "s3_key_mft_outbound": cfg["s3_key_mft_outbound"],
        "process_outbound_extracts": "extracts/process_outbound_extracts",
        "outbound_extracts": "extracts/outbound_extracts",
        "split_file_maxsize": "2 GB",
        "ctl_file_path": "archive/outbound/{}/ctl_file",
        "extract_metadata_tbl": "a360_extract_metadata",
        "insert_query": "insert into {}.{} select path as FILE_NAME,start_time,end_time,line_count as FILE_COUNT,transfer_size as FILE_SIZE, '<batch_id>' as batch_id, <request_type> as request_id from stl_unload_log where path ilike '%{}%';",
        "email_business_users_list": "DL-A360-Team@Data-Axle.com",
        "ingestion_path": cfg["ingestion_path"],
        "s3_key_dp_config": cfg["s3_key_dp_config"],
        "s3_key_athena_glue": cfg["s3_key_athena_glue"],
        "s3_key_athena_validation_glue": cfg["s3_key_athena_validation_glue"],
        "company_id": 6,
        "dp_config": {},
        "list_config": {},
        "extract_configs": {},
    }
    print(f"frequency: {frequency}")
    if frequency:
        config["dp_config"][f"{frequency.lower()}"] = cfg["dp_config"][
            f"{frequency.lower()}"
        ]

    print(list_config_inputs)
    for input_item in list_config_inputs:
        input_file = ""
        if priority_key:
            format_key = input_item["key"] + "_priority"
        else:
            format_key = input_item["key"]
        key = input_item["key"]
        fullfile = input_item["fullfile"]
        input_file = input_item["file_pattern"]
        if fullfile:
            input_file += f",stg_{key.lower()}_crc_prev,stg_{key.lower()}_crc_pii_prev"
        if input_item["frequency"] == frequency:
            config["list_config"][key.upper()] = {
                "key_prefix": "incoming",
                "file_pattern": input_item["file_pattern"],
                "header": input_item["header"],
                "fullfile": fullfile,
                "stg_load_config": {
                    "task_name": f"STG_{key}",
                    "glue_job": f"STG_FCVRL_{key}",
                    "write_to_parquet": input_item.get("write_to_parquet", False),
                    "redshift_procedure": "",
                    "redshift_procedure_params": "",
                    "table_name": f"STG_{key}",
                    "schema_name": "SPECTRUM_DEMO_ODS",
                    "glue_job_parameters": {
                        "WorkerType": "G.1X",
                        "NumberOfWorkers": 5,
                        "JobBookmark": True,
                        "ScriptFilename": "FCVRL.py",
                        "JobParameters": {
                            "input_file": input_file,
                            "additional-python-modules": "h3==3.7.6,smart-open==6.3.0,openpyxl==3.1.2",
                            "format_file": f"dax_{format_key.lower()}.json",
                        },
                    },
                    "dependencies": [],
                },
                "ods_load_config": {
                    "task_name": f"ODS_{key}",
                    "redshift_procedure": f"A360_ODS.PRC_ODS_{key}",
                },
                "reject_mailbox": {f"DAX_{key}": input_item["reject_mailboxes"]},
            }

            if priority_key is False or priority_key is None:
                config["extract_configs"][f"extract_{key.lower()}"] = {
                    "taskName": f"extract_{key.lower()}",
                    "schemaName": "a360_dwh",
                    "viewName": f"vw_extract_{key.lower()}",
                    "FileNamePrefix": f"DAX_EXTRACT_{key.upper()}_",
                    "FileNameSuffixPattern": "yyyyMMddhhmmss",
                    "Delimiter": "|",
                    "Extension": "DAT",
                    "MailBoxes": input_item["extract_mailboxes"],
                    "parallel": False,
                    "Incremental": not fullfile,
                    "header": True,
                    "quotes": False,
                    "generate_ctl_file": True,
                    "split_file": True,
                    "delay": input_item["extract_mft_delay"],
                }

    return config


def fetch_dag_gen_parameters(
    feed_type, frequency, cfg, pg_secrets_config, company_id, priority_flag=None
):
    feed_category_mapping = {
        "Customer Data": "Customer_Data_Processing",
        "Prospect Data": "Customer_Data_Processing",
        "Suppression Data": "Customer_Data_Processing",
        "Transaction Data": "Transaction_Data_Processing",
        "3rd Party Data Enrichment": "Enrichment_Data_Processing",
        "Campaign Data": "CampaignAndLookup_Data_Processing",
        "Lookup Data": "CampaignAndLookup_Data_Processing",
    }

    data_category = feed_category_mapping.get(feed_type)
    feed_type_list = [
        feed_type
        for feed_type, category in feed_category_mapping.items()
        if category == data_category
    ]
    if len(feed_type_list) == 1:
        sql_filter = "('" + " ".join(feed_type_list) + "')"
    else:
        sql_filter = tuple(feed_type_list)
    if priority_flag == "Priority_Data_Processing":
        data_category = "Priority_Data_Processing"
        sql = (
            f"select * from mz360.data_ingestion where priority_processing = 'True' and "
            f"is_enabled = 'True' and company_id = {company_id} "
        )
    else:
        sql = f"select * from mz360.data_ingestion where feed_type in {sql_filter} and is_enabled = 'True' and company_id = {company_id} and coalesce(upper(frequency),'') = '{frequency.upper()}' "
    print(f"Executing SQL: {sql}")
    data, column_names = a360_pg.pg_query_execute(
        sql, cfg, pg_secrets_config, "query", col_names="Y"
    )
    return data, column_names, data_category


def get_api_access_token(base_url, secret_config, auth_endpoint, auth_header):
    try:
        auth_payload = {
            "username": secret_config["a360_username"],
            "password": secret_config["a360_password"],
            "remember_me": False,
        }
        auth_response = requests.post(
            base_url + auth_endpoint,
            json=auth_payload,
            headers=auth_header,
            verify=False,
        )
        if auth_response.status_code != 200:
            logging.error(
                f"Failed to get access token: {auth_response.status_code}, {auth_response.text}"
            )
            return

        auth_response_json = auth_response.json()

        access_token = auth_response.json().get("access")
        if not access_token:
            logging.error("Access token not found in response.")
            raise Exception("Access token not found in response.")
        logging.info(f"Authentication Response, access token retrieved")
        return access_token
    except requests.RequestException as e:
        logging.exception("Request failed:", exc_info=e)
        raise
    except Exception as e:
        logging.exception("An unexpected error occurred:", exc_info=e)
        raise


def a360_api_call(
    url: str = None,
    method: str = "GET",
    secret_config: str = None,
    a360_global_params: str = None,
    header: dict = None,
    api_endpoint: str = None,
    params: dict = None,
    data: dict = None,
    json: dict = None,
    timeout: int = 50,
    client: int = None,
) -> dict:
    """
    Makes an API call and returns the response as a dictionary.
    API Header creation handled for A360 web app api calls.
    If not used for A360 Web app , then please use get_api_access_token() first to prepare header, before calling this method.

    :param url: API endpoint
    :param method: HTTP method (GET, POST, PUT, DELETE, etc.)
    :param headers: Optional headers
    :param params: Optional query parameters
    :param data: Optional form data (for POST, PUT, PATCH)
    :param json: Optional JSON payload (for POST, PUT, PATCH)
    :param timeout: Request timeout in seconds (default: 50)
    :return: API response as a dictionary
    """
    try:
        api_call_header = header
        api_url = url
        api_endpoint = api_endpoint
        if a360_global_params is not None:
            api_url = a360_global_params["a360_base_url"]
            access_token = get_api_access_token(
                base_url=api_url,
                secret_config=secret_config,
                auth_endpoint=a360_global_params["a360_auth_token_endpoint"],
                auth_header=a360_global_params["a360_auth_header"],
            )

            api_call_header = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {access_token}",
                "Client": f"{client}",
            }

            api_endpoint = a360_global_params["a360_notification_api_endpoint"]

        response = requests.request(
            method=method.upper(),
            url=api_url + api_endpoint,
            headers=api_call_header,
            params=params,
            data=data,
            json=json,
            timeout=timeout,
        )
        response.raise_for_status()
        return response.json()  # Convert response to JSON and return
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}


def get_dupfile_threshold_reject_list(cfg, secrets_config, batchid):
    tuple_data = redshift_a360.redshift_query_execute(
        sql="""with process_temp as (
           SELECT SUBSTRING(file_name,REGEXP_INSTR(file_name,'_',1)+1,REGEXP_INSTR(file_name,'_',1,2)-REGEXP_INSTR(file_name,'_',1,1)-1) AS fileprefix
            ,err_msg ,file_name  
                 from a360_core.tmdm_source_files  
                 where batch_id = {}
                 )
                 select distinct fileprefix from process_temp a where err_msg is not null
                 and not exists ( select 1 from process_temp where a.fileprefix=fileprefix
                 and err_msg is null)""".format(
            batchid
        ),
        cfg=cfg,
        secrets_config=secrets_config,
        query_type="query",
    )
    pulled_list = [x for lst in tuple_data for x in lst]
    print(pulled_list)
    return pulled_list


def get_name_field_from_json_metadata(file_path: str) -> Optional[List[str]]:
    logging.basicConfig(level=logging.INFO)
    try:
        # Open and parse the JSON file
        with open(file_path, "r") as file:
            data = json.load(file)

        # Extract 'list_config'
        list_config = data.get("list_config", {})
        if not isinstance(list_config, dict):
            logging.error("Error: 'list_config' is not a dictionary.")
            return None

        # Extract and return the keys as a list
        id_list = list(list_config.keys())
        print("Extracted IDs:", id_list)
        return id_list

    except FileNotFoundError:
        logging.error(f"Error: The file at '{file_path}' was not found.")
    except json.JSONDecodeError:
        logging.error(f"Error: The file at '{file_path}' is not a valid JSON.")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

    return None

def get_source_cd(file_name):
    source_cd = file_name.split("_")[1]
    return source_cd
