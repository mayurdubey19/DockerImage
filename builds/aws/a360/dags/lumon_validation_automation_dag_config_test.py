import airflow.utils.dates
from datetime import datetime
from airflow.exceptions import AirflowSkipException
from airflow.configuration import conf
from airflow.models import Variable
import os
import logging
from helpers.send_email import *
from airflow.decorators import dag, task
from a360.python.a360_mz_task import *
from a360.python.a360_dag_template import *

# from aetna.python.mz_task import *
from helpers.common import *
from helpers.s3 import *
from helpers.redshift import *
import helpers.athena as athena_a360
from helpers.AwsExtractOperator import *
from helpers.send_email import *
import logging
from airflow.models import Variable
import helpers.postgres as pg

# from airflow.timetables.trigger import CronTriggerTimetable
from airflow.configuration import conf
from airflow.operators.bash import BashOperator
import boto3
import sys
import json

default_args = {
    "owner": "A360",
    "start_date": airflow.utils.dates.days_ago(2),
    "depends_on_past": False,
    "current_path": os.path.abspath(os.path.dirname(__file__)),
    "config_path": os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "config")
    ),
    "retries": 0,
    "email_to": "mayur.dubey@data-axle.com;shreya.sharma@data-axle.com;shreyash.nigam@data-axle.com;pradeep.ingle@data-axle.com",
    "email_subject": "Lumon Validation and Automation Processing Job: ",
    "email_on_failure": True,
    "email_on_retry": False,
    "email_body": "Executing Stage Job ",
    "incoming_path": "s3://{}/incoming/",
    "output_path": "s3://{}/process_fcvrl/parquet_glue/",
    "script_path": "s3://{}/scripts/python/",
    "format_file_path_glue": "s3://{}/config/pyspark_json/",
    "athena_files_path_glue": "s3://{}/process_fcvrl/athena_glue/",
    "athena_files_path_validation_glue": "s3://{}/process_fcvrl/athena_validation_glue/",
    "redshift_config_paths": "s3://{}/config/common/glue_redshift_config.json",
    "iam_role_glue": "da-mz-glue-role",
    "timeout_glue_job": 90,
    "maxretries_glue_job": 0,
    "common_settings_glue": "s3://{}/config/common/pysparkcommonsettings.json",
    "additional_python_modules_glue": "h3==3.7.6,smart-open==6.3.0,openpyxl==3.1.2",
    "glue_version": "4.0",
    "extra_py_files_glue": "s3://{}/scripts/python/ValidateJsonFormatFile.py,s3://{}/scripts/python/FCVRL_UDFS.py,s3://{}/scripts/python/common.py",
    "connections_glue": "Lumon Redshift Connection",
    "temp_path_athena": "s3://{}/athena_query_results/",
    "mcd_input_table": "MZB_MCD_FILE_INCR",
    "reject_path": "s3://{}/archive/outbound/rejects/",
    "extracts_path": "s3://{}/archive/outbound/extracts/",
    "ods_user": "A360_ODS",
    "audit_schema": "A360_CORE",
    "audit_table": "TMDM_OBJECT",
}


@dag(
    dag_id="lumon_validation_automation_dag_config_test",
    access_control={"Lumon": {"can_read", "can_edit", "can_delete"}},
    default_args=default_args,
    description="Lumon Validation and Automation Data Pipeline",
    # schedule=CronTriggerTimetable("0 11 * * sun", timezone="America/Chicago"),
    schedule_interval=None,
    max_active_runs=1,
    params={"run_time_params": {"task_to_skip": []}},
    catchup=False,
)
def lumon_validation_automation():
    current_env = Variable.get("var-env", "local")
    if current_env == "Prod":
        json_config_path = default_args["config_path"] + "/a360_daily_config_prod.json"
    else:
        json_config_path = (
            default_args["config_path"] + "/lumon_validation_automation_config.json"
        )

    cfg = get_json_config(json_config_path)
    sec_name = cfg["secret_name"]
    region_name = cfg["aws_reg_rs"]
    secrets_config = get_secret(sec_name, region_name)
    a360_account_id = get_aws_account_id()
    bucket_name = cfg["bucket_name"].replace("{}", a360_account_id[-4:])
    mdm_bucket_name = cfg["bucket_name"].replace("{}", a360_account_id[-4:]) + "-mdm"
    dps_bucket_name = cfg["dps_bucket_name"].replace("{}", a360_account_id[-4:])
    mft_bucket_name = cfg["mft_bucket"].replace("{}", a360_account_id[-4:])
    redshift_iam_role = "arn:aws:iam::{}:role/da-mz-redshift-role".format(
        a360_account_id
    )
    emr_iam_role = "arn:aws:iam::{}:role/da-mz-emr-serverless-role".format(
        a360_account_id
    )
    athena_database = get_ssm_parameters("a360_athena_database", region_name, False)
    athena_validation_database = get_ssm_parameters(
        "a360_athena_validation_database", region_name, False
    )
    mcd_emr_application_id = get_ssm_parameters("a360_mcd_emr", region_name, False)
    v_execid, v_batchid = generate_exec_id(cfg["request_type"])
    reject_path = default_args["reject_path"].replace("{}", bucket_name)
    extracts_path = default_args["extracts_path"].replace("{}", bucket_name)
    temp_path_athena = default_args["temp_path_athena"].replace("{}", bucket_name)
    athena_files_path_glue = default_args["athena_files_path_glue"].replace(
        "{}", bucket_name
    )
    athena_files_path_validation_glue = default_args[
        "athena_files_path_validation_glue"
    ].replace("{}", bucket_name)
    format_file_path_glue = default_args["format_file_path_glue"].replace(
        "{}", bucket_name
    )
    common_settings_glue = default_args["common_settings_glue"].replace(
        "{}", bucket_name
    )
    extra_py_files_glue = default_args["extra_py_files_glue"].replace("{}", bucket_name)
    script_path = default_args["script_path"].replace("{}", bucket_name)
    v_cur_date = datetime.now().strftime("%Y%m%d%H%M%S")
    p_cur_user = "awsuser"
    # ctl_file_path = cfg["ctl_file_path"].format(v_batchid)
    pg_secret_name = cfg["mz360_postgres_secret_name"]
    pg_secrets_config = get_secret(pg_secret_name, region_name)
    git_secrets_name = cfg["git_repo_secret_name"]
    git_secrets_config = get_secret(git_secrets_name, region_name)
    

    def get_task_log(task_instance):
        base_log_folder = conf.get("logging", "BASE_LOG_FOLDER")

        # Adjust attempt number to reflect the actual try number
        adjusted_attempt_number = task_instance.try_number

        log_file_path = os.path.join(
            base_log_folder,
            f"dag_id={task_instance.dag_id}",
            f"run_id={task_instance.run_id}",
            f"task_id={task_instance.task_id}",
            f"attempt={adjusted_attempt_number}.log",
        )
        print(log_file_path)

        if os.path.exists(log_file_path):
            with open(log_file_path, "r") as log_file:
                return log_file.read()
        return "Log file not found. Generated Log file path : {}".format(log_file_path)

    def send_success_email(context):
        task_instance = context["task_instance"]
        log_content = get_task_log(task_instance)

        # subject = f"Task {context['task_instance_key_str']} Failed"
        html_content = f"""
          <h3>Task: {context['task_instance_key_str']}</h3>
          <p>Succeeded on: {datetime.now()}</p>
          <h4>Log Content:</h4>
          <pre>{log_content}</pre>
          """

        # Extract the task_id from the task_instance_key_str
        task_instance_key_str = context["task_instance_key_str"]
        task_id = task_instance_key_str.split("__")[1]
        send_email(
            email_address=default_args["email_to"],
            email_message=html_content,
            email_subject="Success :{}{}".format(
                default_args["email_subject"], task_id
            ),
            client="lumon",
        )

    def send_failure_email(context, data_details):
        task_instance = context["task_instance"]
        log_content = get_task_log(task_instance)

        # subject = f"Task {context['task_instance_key_str']} Failed"
        html_content = f"""
          <h3>Task: {context['task_instance_key_str']}</h3>
          <p>Failed on: {datetime.now()}</p>
          <h4>Log Content:</h4>
          <pre>{log_content}</pre>
          """

        # Extract the task_id from the task_instance_key_str
        task_instance_key_str = context["task_instance_key_str"]
        task_id = task_instance_key_str.split("__")[1]

        send_email(
            email_address=default_args["email_to"],
            email_message=html_content,
            email_subject="Failure :{}{}".format(
                default_args["email_subject"], task_id
            ),
            client="lumon",
        )

        # Add logic to send notificaions and update Validation status
        logging.info(
            f"Failure in task. Data ingestion ID: {data_details['data_ingestion_id']}"
        )

        if task_id in ("get_data_ingestion_details", "branch_func"):
            stage = "Fetching Configuration Details"
        elif task_id in (
            "unload_data_from_client_redshift",
            "copy_file_from_client_bucket",
            "copy_sample_file_manual_upload",
            "copy_ingestion_config",
        ):
            stage = "Copying Data and Configuration Files"
        elif task_id in (
            "generate_code_and_run_stg_load",
            "generate_code_and_run_ods_load",
        ):
            stage = "Validation Step"
        elif task_id in (
            "generate_data_pipeline_for_orchestration",
            "generate_metadata_for_orchestration",
            "generate_github_feature_branch",
        ):
            stage = "Automation Step"
        else:
            stage = "Deployment Step"

        if "Threshold check failed. Reject Count exceeds Threshold" in log_content:
            logging.info("Update not required to mz360.data_ingestion")
        else:
            data = {
                "status": "Error",
                "stage": stage,
                "payload": {
                    "message": "Process Failed. Operations Team looking into issue"
                },
            }

        json_string = json.dumps(data, indent=4)
        logging.info(json_string)

        if "Threshold check failed. Reject Count exceeds Threshold" in log_content:
            logging.info("Update not required to mz360.data_ingestion")
        else:
            # Connect to PostgreSQL database
            sql = """
                UPDATE mz360.data_ingestion a set validation_status = 'ERROR',validation_result='{}'
                WHERE a.data_ingestion_id = '{}'
            """.format(
                json_string, data_details["data_ingestion_id"]
            )

            query_type = ""
            logging.info(sql)

            pg_result = pg.pg_query_execute(sql, cfg, pg_secrets_config, query_type)

    def get_glue_job_failure_reason(job_name, job_run_id):
        """Fetch Glue job failure reason if available."""
        client = boto3.client("glue")

        try:
            response = client.get_job_run(JobName=job_name, RunId=job_run_id)
            job_status = response["JobRun"]["JobRunState"]

            if job_status in ["FAILED", "TIMEOUT"]:
                error_message = response["JobRun"].get(
                    "ErrorMessage", "Unknown error occurred"
                )
                return error_message
            return None  # Job did not fail
        except Exception as e:
            return f"Failed to fetch Glue job status: {str(e)}"

    def athenaRejectCount(table_name, filename):

        query = f"""WITH data AS (SELECT 
                                        CAST(json_parse(reject_json) AS map<varchar, varchar>) AS json_map
                                    FROM 
                                        "{athena_validation_database}".stg_{table_name}_reject where replace(lower(filename_dax_{table_name}),'.gz','') = replace(lower('{filename}'),'.gz','')
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
            database=athena_validation_database,
            output_path=output_path,
        )
        df_header_reject = result_athena[result_athena.iloc[:, 0] == "header"]
        if not df_header_reject.empty:
            return df_header_reject

        return result_athena

    def create_athena_table_with_s3_copy(
        data_details, bucket_name, cfg, athena_db, table_type
    ):
        """
        Creates Athena tables and copies necessary S3 objects for full feeds.
        """
        if athena_db == athena_validation_database:
            athena_file_path = athena_files_path_validation_glue
            s3_key = cfg["s3_key_athena_validation_glue"]
        else:
            athena_file_path = athena_files_path_glue
            s3_key = cfg["s3_key_athena_glue"]

        if table_type == "crc_prev":
            copy_source_path = s3_key + "template_crc_prev/crc_prev_parquet"
            tablename = f"stg_{data_details['feed'].lower()}_crc_prev"
            copy_target_path = s3_key + tablename + "/crc_prev_parquet"
        elif table_type == "crc_pii_prev":
            copy_source_path = s3_key + "template_crc_pii_prev/crc_pii_prev_parquet"
            tablename = f"stg_{data_details['feed'].lower()}_crc_pii_prev"
            copy_target_path = s3_key + tablename + "/crc_pii_prev_parquet"

        athena_client = boto3.client("athena", region_name)

        # Copy data from template folder to target folder
        copy_s3_object(
            copy_source_bucket=bucket_name,
            copy_source_path=f"{copy_source_path}",
            copy_target_bucket=bucket_name,
            copy_target_path=f"{copy_target_path}",
        )
        logging.info(
            f"Successfully copied {copy_source_path} to {copy_target_path} folder"
        )

        sql = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {tablename} (
            crc_prev STRING
        )
        STORED AS PARQUET
        LOCATION '{athena_file_path}{tablename}/'
        """

        success = athena_a360.executenonquery(
            boto3_client=athena_client,
            sql=sql,
            database=athena_db,
            output_path=temp_path_athena,
        )

        if success:
            logging.info(
                f"Athena {athena_db}.{tablename} Create Table execution successful!"
            )
        else:
            logging.error(f"Query execution failed!: {sql}")

    @task(
        on_success_callback=send_success_email,
        on_failure_callback=lambda context: send_failure_email(
            context, context["task_instance"].xcom_pull(key="data_details")
        ),
        trigger_rule="none_failed",
    )
    def get_data_ingestion_details(**kwargs):
        """Fetch data ingestion details from PostgreSQL database."""
        # Access DAG run configuration from kwargs
        print("sys.executable:", sys.executable)
        dag_run_conf = kwargs["dag_run"].conf

        # Check for task skip condition
        if dag_run_conf.get("run_time_params") and skip_task_check(
            "get_data_ingestion_details",
            dag_run_conf.get("run_time_params").get("task_to_skip"),
        ):
            print("Skipping task.")
            raise AirflowSkipException

        # Fetch data ingestion ID from DAG run configuration
        data_ingestion_id = dag_run_conf.get("data_ingestion_id")
        # secret_name = cfg["mz360_postgres_secret_name"]
        if not data_ingestion_id:
            raise ValueError("data_ingestion_id not found in DAG run configuration.")

        try:
            data = {
                "status": "In Progress",
                "stage": "Fetching Configuration details",
                "payload": {"remainingTime": 15, "stageNumber": 1},
            }

            json_string = json.dumps(data, indent=4)
            logging.info(json_string)
            # Connect to PostgreSQL database
            sql = """
               UPDATE mz360.data_ingestion a set validation_status = 'IN_PROGRESS',validation_result='{}'
                WHERE a.data_ingestion_id = '{}'
            """.format(
                json_string, data_ingestion_id
            )

            query_type = ""
            logging.info(sql)

            pg_result = pg.pg_query_execute(sql, cfg, pg_secrets_config, query_type)

            sql = """
                SELECT c.company_name,a.frequency,a.data_ingestion_id, a.data_source_type, data_source_object, uploaded_file_name, feed, a.company_id, content_type, feed_type, config, priority_processing, b.cloud_region
                FROM mz360.data_ingestion a 
                join mz360.connector b
                on a.connector_id = b.connector_id
                join mz360.company c
                on a.company_id  = c.company_id 
                WHERE a.data_ingestion_id = '{}'
            """.format(
                data_ingestion_id
            )

            query_type = "query"

            pg_result, column_names = pg.pg_query_execute(
                sql, cfg, pg_secrets_config, query_type, col_names="Y"
            )

            if pg_result:
                for row in pg_result:
                    data_details = dict(zip(column_names, row))
                    logging.info(f"Data details: {data_details}")
                    kwargs["ti"].xcom_push(key="data_details", value=data_details)
            else:
                raise ValueError(
                    f"No data found for data_ingestion_id: {data_ingestion_id}"
                )

        except Exception as e:
            logging.error(f"Error while querying PostgreSQL: {str(e)}")
            raise e

    @task(
        on_success_callback=send_success_email,
        on_failure_callback=lambda context: send_failure_email(
            context, context["task_instance"].xcom_pull(key="data_details")
        ),
        trigger_rule="none_failed",
    )
    def unload_data_from_client_redshift(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "unload_data_from_client_redshift",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException

        task_instance = kwargs["ti"]
        # Pull data details from XCom
        data_details = task_instance.xcom_pull(
            task_ids="get_data_ingestion_details", key="data_details"
        )
        if not data_details:
            raise ValueError("No data details found in XCom.")

        if not data_details.get("data_source_object"):
            raise ValueError(
                "Datashare object (table name) is required for Redshift source type."
            )

        data = {
            "status": "In Progress",
            "stage": "Copying Data and Configuration Files",
            "payload": {"remainingTime": 14, "stageNumber": 2},
        }

        json_string = json.dumps(data, indent=4)
        logging.info(json_string)
        # Connect to PostgreSQL database
        sql = """
            UPDATE mz360.data_ingestion a set validation_status = 'IN_PROGRESS',validation_result='{}'
            WHERE a.data_ingestion_id = '{}'
        """.format(
            json_string, data_details["data_ingestion_id"]
        )

        query_type = ""
        logging.info(sql)

        pg_result = pg.pg_query_execute(sql, cfg, pg_secrets_config, query_type)
        # Generate S3 path and unload query
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        filename = f"DAX_{data_details['feed'].upper()}_{timestamp}"

        s3_path = f"s3://{bucket_name}/incoming/{filename}"

        unload_redshift_datashare_data(
            unload_bucket=bucket_name,
            file_pattern=filename,
            src_tab=data_details["data_source_object"],
            cfg=cfg,
            redshift_iam_role=redshift_iam_role,
            secrets_config=secrets_config,
        )

        # Log the success
        logging.info(f"Successfully unloaded data to {s3_path}")
        kwargs["ti"].xcom_push(key="filename_redshift", value=filename)

    # # Task 3: Copy S3 files (for manual_upload and s3 source types)
    # copy_s3_files = S3ToS3Operator(
    #     task_id="copy_s3_files",
    #     source_bucket_key='sample_files/{{ task_instance.xcom_pull(task_ids="get_data_ingestion_details", key="data_details")["filename"] }}',
    #     source_bucket_name="XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
    #     dest_bucket_key='sample_files/{{ task_instance.xcom_pull(task_ids="get_data_ingestion_details", key="data_details")["filename"] }}',
    #     dest_bucket_name="XXXXXXXXXXXXXXXXXXXX",
    #     aws_conn_id="aws_default",
    #     dag=dag,
    # )

    @task(
        on_success_callback=send_success_email,
        on_failure_callback=lambda context: send_failure_email(
            context, context["task_instance"].xcom_pull(key="data_details")
        ),
        trigger_rule="none_failed",
    )
    def copy_file_from_client_bucket(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "copy_file_from_client_bucket",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException

        ti = kwargs["ti"]
        data_details = ti.xcom_pull(
            task_ids="get_data_ingestion_details", key="data_details"
        )
        data = {
            "status": "In Progress",
            "stage": "Copying Data and Configuration Files",
            "payload": {"remainingTime": 14, "stageNumber": 2},
        }

        json_string = json.dumps(data, indent=4)
        logging.info(json_string)
        # Connect to PostgreSQL database
        sql = """
            UPDATE mz360.data_ingestion a set validation_status = 'IN_PROGRESS',validation_result='{}'
            WHERE a.data_ingestion_id = '{}'
        """.format(
            json_string, data_details["data_ingestion_id"]
        )

        query_type = ""
        logging.info(sql)

        pg_result = pg.pg_query_execute(sql, cfg, pg_secrets_config, query_type)

        # Split the path and get the client bucket name and path after removing s3:// prefix
        s3_path = data_details["data_source_object"].replace("s3://", "")
        client_bucket_name = s3_path.split("/")[0]
        client_bucket_path = "/".join(s3_path.split("/")[1:])
        client_bucket_region = data_details["cloud_region"]
        logging.info(f"client_bucket_name: {client_bucket_name}")
        logging.info(f"client_bucket_path: {client_bucket_path}")
        # client_bucket_region = data_details["cloud_region"]

        # Check if client bucket region is same as A360 bucket region
        if region_name == client_bucket_region:
            copy_s3_object(
                copy_source_bucket=client_bucket_name,
                copy_source_path=f"{client_bucket_path}/{data_details['uploaded_file_name']}",
                copy_target_bucket=bucket_name,
                copy_target_path=f"incoming/{data_details['uploaded_file_name']}",
            )
        else:
            copy_cross_region_s3_datasync(
                client_bucket_name,
                client_bucket_path,
                bucket_name,
                "incoming/",
                client_bucket_region,
                region_name,
                specific_file=data_details["uploaded_file_name"],
            )

    @task(
        on_success_callback=send_success_email,
        on_failure_callback=lambda context: send_failure_email(
            context, context["task_instance"].xcom_pull(key="data_details")
        ),
        trigger_rule="none_failed",
    )
    def copy_sample_file_manual_upload(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "copy_sample_file_manual_upload",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException

        ti = kwargs["ti"]
        data_details = ti.xcom_pull(
            task_ids="get_data_ingestion_details", key="data_details"
        )
        data = {
            "status": "In Progress",
            "stage": "Copying Data and Configuration Files",
            "payload": {"remainingTime": 14, "stageNumber": 2},
        }

        json_string = json.dumps(data, indent=4)
        logging.info(json_string)
        # Connect to PostgreSQL database
        sql = """
            UPDATE mz360.data_ingestion a set validation_status = 'IN_PROGRESS',validation_result='{}'
            WHERE a.data_ingestion_id = '{}'
        """.format(
            json_string, data_details["data_ingestion_id"]
        )

        query_type = ""
        logging.info(sql)

        pg_result = pg.pg_query_execute(sql, cfg, pg_secrets_config, query_type)

        # Manually uploaded sample files are stored in s3 bucket of web app
        s3_bucket_name = cfg["aws_webapp_bucket_name"]
        s3_bucket_path = "sample_files/"
        s3_bucket_region = cfg["aws_reg_webapp"]

        copy_cross_region_s3_datasync(
            s3_bucket_name,
            s3_bucket_path,
            bucket_name,
            "incoming/",
            s3_bucket_region,
            region_name,
            specific_file=data_details["uploaded_file_name"],
        )

    # Define task dependencies with conditional execution
    @task.branch(
        on_success_callback=send_success_email,
        on_failure_callback=lambda context: send_failure_email(
            context, context["task_instance"].xcom_pull(key="data_details")
        ),
        trigger_rule="none_failed",
    )
    def branch_func(**context):
        data_details = context["task_instance"].xcom_pull(
            task_ids="get_data_ingestion_details", key="data_details"
        )

        data = {
            "status": "In Progress",
            "stage": "Copying Data and Configuration Files",
            "payload": {"remainingTime": 14, "stageNumber": 2},
        }

        json_string = json.dumps(data, indent=4)
        logging.info(json_string)
        # Connect to PostgreSQL database
        sql = """
            UPDATE mz360.data_ingestion a set validation_status = 'IN_PROGRESS',validation_result='{}'
            WHERE a.data_ingestion_id = '{}'
        """.format(
            json_string, data_details["data_ingestion_id"]
        )

        query_type = ""
        logging.info(sql)

        pg_result = pg.pg_query_execute(sql, cfg, pg_secrets_config, query_type)

        source_type = data_details["data_source_type"].lower()

        if source_type == "redshift":
            return "unload_data_from_client_redshift"
        elif source_type == "s3":
            return "copy_file_from_client_bucket"
        elif source_type == "manual_upload":
            return "copy_sample_file_manual_upload"
        else:
            raise Exception("Invalid data_source_type")

    @task(
        on_success_callback=send_success_email,
        on_failure_callback=lambda context: send_failure_email(
            context, context["task_instance"].xcom_pull(key="data_details")
        ),
        trigger_rule="none_failed",
    )
    def copy_ingestion_config(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "copy_ingestion_config",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException
        """Copy Ingestion config from Webapp bucket to config_path"""

        ti = kwargs["ti"]
        data_details = ti.xcom_pull(
            task_ids="get_data_ingestion_details", key="data_details"
        )

        data = {
            "status": "In Progress",
            "stage": "Copying Data and Configuration Files",
            "payload": {"remainingTime": 14, "stageNumber": 2},
        }

        json_string = json.dumps(data, indent=4)
        logging.info(json_string)
        # Connect to PostgreSQL database
        sql = """
            UPDATE mz360.data_ingestion a set validation_status = 'IN_PROGRESS',validation_result='{}'
            WHERE a.data_ingestion_id = '{}'
        """.format(
            json_string, data_details["data_ingestion_id"]
        )

        query_type = ""
        logging.info(sql)

        pg_result = pg.pg_query_execute(sql, cfg, pg_secrets_config, query_type)

        s3_bucket_name = cfg["aws_webapp_bucket_name"]
        s3_bucket_path = f"config_files/{data_details['company_id']}/"
        s3_bucket_region = cfg["aws_reg_webapp"]

        # Check if a360 webapp bucket region is same as A360 ETL bucket region
        if region_name == s3_bucket_region:
            copy_s3_object(
                copy_source_bucket=s3_bucket_name,
                copy_source_path=f"{s3_bucket_path}{data_details['data_ingestion_id']}_config.json",
                copy_target_bucket=bucket_name,
                copy_target_path=f"config/pyspark_json/{data_details['data_ingestion_id']}_config.json",
            )
        else:
            copy_cross_region_s3_datasync(
                s3_bucket_name,
                s3_bucket_path,
                bucket_name,
                "/config/pyspark_json/",
                s3_bucket_region,
                region_name,
                specific_file=f"{data_details['data_ingestion_id']}_config.json",
            )

        # Rename config file
        move_and_delete_s3_file(
            bucket_name,
            f"config/pyspark_json/{data_details['data_ingestion_id']}_config.json",
            bucket_name,
            f"config/pyspark_json/dax_{data_details['feed'].lower()}.json",
        )

    @task(
        on_success_callback=send_success_email,
        on_failure_callback=lambda context: send_failure_email(
            context, context["task_instance"].xcom_pull(key="data_details")
        ),
        trigger_rule="none_failed",
    )
    def generate_code_and_run_stg_load(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "generate_code_and_run_stg_load",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException
        """Run FCVRL Glue Job"""

        ti = kwargs["ti"]
        data_details = ti.xcom_pull(
            task_ids="get_data_ingestion_details", key="data_details"
        )

        data = {
            "status": "In Progress",
            "stage": "Validation Step",
            "payload": {"remainingTime": 12, "stageNumber": 3},
        }

        json_string = json.dumps(data, indent=4)
        logging.info(json_string)
        # Connect to PostgreSQL database
        sql = """
            UPDATE mz360.data_ingestion a set validation_status = 'IN_PROGRESS',validation_result='{}'
            WHERE a.data_ingestion_id = '{}'
        """.format(
            json_string, data_details["data_ingestion_id"]
        )

        query_type = ""
        logging.info(sql)

        pg_result = pg.pg_query_execute(sql, cfg, pg_secrets_config, query_type)
        athena_client = boto3.client("athena", region_name)

        # Prepare Inputs to FCVRL based on feed type and content type
        if data_details["content_type"].lower() == "full" and data_details[
            "feed_type"
        ].lower() in (
            "customer data",
            "prospect data",
            "suppression data",
            "transaction data",
        ):
            # 1. Create crc_prev and crc_pii_prev input athena tables required for Full feeds
            create_athena_table_with_s3_copy(
                data_details=data_details,
                bucket_name=bucket_name,
                cfg=cfg,
                athena_db=athena_validation_database,
                table_type="crc_prev",
            )

            create_athena_table_with_s3_copy(
                data_details=data_details,
                bucket_name=bucket_name,
                cfg=cfg,
                athena_db=athena_validation_database,
                table_type="crc_pii_prev",
            )

            # 2. Define input_files
            crc_prev_tablename = tablename = (
                f"stg_{data_details['feed'].lower()}_crc_prev"
            )
            crc_pii_prev_tablename = tablename = (
                f"stg_{data_details['feed'].lower()}_crc_pii_prev"
            )
            input_file = f"DAX_{data_details['feed'].upper()}*,{crc_prev_tablename},{crc_pii_prev_tablename}"

            # 3. Define format file
            format_file = (
                f"{format_file_path_glue}dax_{data_details['feed'].lower()}.json"
            )

        if data_details["content_type"].lower() == "incremental" or data_details[
            "feed_type"
        ].lower() in ("3rd party data enrichment", "campaign data", "lookup data"):
            # 2. Define input_files
            input_file = f"DAX_{data_details['feed'].upper()}*"
            # 3. Define format file
            format_file = (
                f"{format_file_path_glue}dax_{data_details['feed'].lower()}.json"
            )
            NumberOfWorkers = 10
            WorkerType = "G.1X"
        else:
            NumberOfWorkers = 5
            WorkerType = "G.2X"

        logging.info("starting glue stage job")
        # Add string value default_args["connections_glue"] to list and assign to connection_glue
        connection_glue = [default_args["connections_glue"]]

        extra_jars = script_path + "splittablegzip-1.3.jar"
        submit_glue_job = GlueJobOperator(
            task_id="run_fcvrl",
            job_name="STG_FCVRL_" + data_details["feed"],
            script_location=script_path + "FCVRL.py",
            iam_role_name=default_args["iam_role_glue"],
            update_config=True,
            create_job_kwargs={
                "GlueVersion": default_args["glue_version"],
                "NumberOfWorkers": NumberOfWorkers,
                "WorkerType": WorkerType,
                "Timeout": default_args["timeout_glue_job"],
                "MaxRetries": default_args["maxretries_glue_job"],
                "DefaultArguments": {  # Default arguments for the job
                    "--input_file": input_file,
                    "--format_file": format_file,
                    "--common_settings": common_settings_glue,
                    "--additional-python-modules": default_args[
                        "additional_python_modules_glue"
                    ],
                    "--enable-metrics": "true",
                    "--extra-py-files": extra_py_files_glue,
                    "--enable-glue-datacatalog": "true",
                    "--extra-jars": extra_jars,
                    "--run_type": "validation",
                    # Additional arguments to pass to the job script
                },
                "Connections": {"Connections": connection_glue},
            },
        )

        try:
            gluecontext = get_current_context()
            job_name = "STG_FCVRL_" + data_details["feed"]
            submit_glue_job.execute(context=gluecontext)

        except Exception as e:
            # Log the exception
            # Retrieve JobRunId from boto3 after submitting the job
            glue_client = boto3.client("glue")
            job_name = "STG_FCVRL_" + data_details["feed"]
            # Assuming the job run is initiated, you would use a function to retrieve the job run details:
            response = glue_client.get_job_runs(JobName=job_name, MaxResults=1)
            job_run_id = response["JobRuns"][0].get("Id")
            error_message = f"Error while running Glue job: {str(e)}"
            logging.info(f"JobRunId is {job_run_id}")

            # If job_run_id exists, fetch Glue failure reason
            if "job_run_id" in locals():
                failure_reason = get_glue_job_failure_reason(job_name, job_run_id)
                logging.info(failure_reason)

                if failure_reason:
                    error_message += f" | Glue failure reason: {failure_reason}"

                    # Check if the failure message matches the threshold check failure
                    if (
                        "Threshold check failed. Reject Count exceeds Threshold"
                        in failure_reason
                    ):
                        table_name = data_details["feed"]
                        file_name = (
                            ti.xcom_pull(
                                task_ids="unload_data_from_client_redshift",
                                key="filename_redshift",
                            )
                            if data_details["data_source_type"].lower() == "redshift"
                            else data_details["uploaded_file_name"]
                        )
                        result_athena = athenaRejectCount(
                            table_name, filename=file_name
                        )
                        logging.info(result_athena)

                        # Convert to dictionary
                        reject_dict = {
                            f"{row.attribute}": int(row.count)
                            for row in result_athena.itertuples()
                        }

                        # Get Total record count and reject count
                        query = f"select file_total_count,reject_total_count from stg_{data_details['feed']}_count"
                        logging.info(query)

                        session = boto3.session.Session()

                        df_count = athena_a360.executequery(
                            boto3_session=session,
                            sql=query,
                            database=athena_validation_database,
                            output_path=temp_path_athena,
                        )

                        for index, row in df_count.iterrows():
                            # Access individual row values using row['column_name']
                            total_count = row["file_total_count"]
                            reject_count = row["reject_total_count"]

                        # Create dedupe_key list
                        # JSON string input
                        json_str = data_details["config"]

                        # Parse the JSON string
                        data_config = json.loads(json_str)

                        # Initialize the list to store fields where dedupe_key is true
                        dedupe_fields = []

                        # Traverse the 'in' section to extract fields
                        if "in" in data_config:
                            for entry in data_config["in"]:
                                if "fields" in entry:
                                    for field in entry["fields"]:
                                        if field.get("dedupe_key"):
                                            dedupe_fields.append(field["name"])

                        # Convert the list to a comma-separated string
                        dedupe_fields_str = "-".join(dedupe_fields)
                        # Fetch error_threhold
                        error_threshold = data_config["error_threshold"]

                        # Check if "DUPE" exists and rename it
                        if "DUPE" in reject_dict:
                            new_key = f"DUPE_{dedupe_fields_str}"
                            reject_dict[new_key] = reject_dict.pop("DUPE")

                        # Convert to JSON string (if needed)
                        # reject_json = json.dumps(reject_dict, indent=4)

                        logging.info(f"Updated reject details: {reject_dict}")

                        # {"stage": "Validation Step", "status": "Error", "payload": {"message": "More than 20% records rejected. Please check reject reasons and correct ingestion configuration", "rejectCount": 1800, "totalRecords": 2000, "rejectDetails": {"NULLABLE_AWN": 95, "DUPE_COLUMN1-COLUMN2": 111, "LEN_BROKERCONTACTEMAIL": 1, "CREGEX_INTERACTION_DATE": 1}}}
                        data = {
                            "status": "Error",
                            "stage": "Validation Step",
                            "payload": {
                                "message": f"More than {error_threshold}% records rejected. Please check reject reasons and correct ingestion configuration",
                                "rejectCount": reject_count,
                                "totalRecords": total_count,
                                "rejectDetails": reject_dict,
                            },
                        }

                        json_string = json.dumps(data, indent=4)
                        logging.info(json_string)
                        # Connect to PostgreSQL database
                        sql = """
                            UPDATE mz360.data_ingestion a set validation_status = 'ERROR',validation_result='{}'
                            WHERE a.data_ingestion_id = '{}'
                        """.format(
                            json_string, data_details["data_ingestion_id"]
                        )

                        query_type = ""
                        logging.info(sql)

                        pg_result = pg.pg_query_execute(
                            sql, cfg, pg_secrets_config, query_type
                        )

            # Create Notofication for UI - Commented for now - Need API Call as per UI requirement
            # query_type = "insert"
            # sql = """insert into mz360.notification
            #      (subject,body,created_at,company_id,incoming_category,status)
            #      values
            #      ('Lumon Validation and Automation','Validation Failed for feed {} at '||current_date,current_timestamp,{},'INGESTION','ERROR')
            #    """.format(
            #     data_details["feed"], data_details["company_id"]
            # )
            # pg_result = pg.pg_query_execute(sql, cfg, pg_secrets_config, query_type)
            # Additional actions (e.g., sending notifications)
            # raise the exception to mark the task as failed
            raise

    @task(
        on_success_callback=send_success_email,
        on_failure_callback=lambda context: send_failure_email(
            context, context["task_instance"].xcom_pull(key="data_details")
        ),
        trigger_rule="none_failed",
    )
    def generate_code_and_run_ods_load(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "generate_code_and_run_ods_load",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException
        """Run ODS Load"""

        ti = kwargs["ti"]
        data_details = ti.xcom_pull(
            task_ids="get_data_ingestion_details", key="data_details"
        )

        data = {
            "status": "In Progress",
            "stage": "Validation Step",
            "payload": {"remainingTime": 5, "stageNumber": 3},
        }

        json_string = json.dumps(data, indent=4)
        logging.info(json_string)
        # Connect to PostgreSQL database
        sql = """
            UPDATE mz360.data_ingestion a set validation_status = 'IN_PROGRESS',validation_result='{}'
            WHERE a.data_ingestion_id = '{}'
        """.format(
            json_string, data_details["data_ingestion_id"]
        )

        query_type = ""
        logging.info(sql)

        pg_result = pg.pg_query_execute(sql, cfg, pg_secrets_config, query_type)

        # JSON string input
        json_str = data_details["config"]

        # Parse the JSON string
        data = json.loads(json_str)

        # Initialize the list to store fields where dedupe_key is true
        dedupe_fields = []

        # Traverse the 'in' section to extract fields
        if "in" in data:
            for entry in data["in"]:
                if "fields" in entry:
                    for field in entry["fields"]:
                        if field.get("dedupe_key"):
                            dedupe_fields.append(field["name"])

        # Convert the list to a comma-separated string
        dedupe_fields_str = ",".join(dedupe_fields)

        if data_details["priority_processing"]:
            priority_processing = "Y"
        else:
            priority_processing = "N"
        ### Call Redshift procedure to configure and Validate ODS Load ###
        procedure_call = (
            "CALL a360_load.prc_configure_ods_dwh ('{}','{}','{}','{}', '{}')".format(
                data_details["feed"],
                data_details["feed_type"],
                data_details["content_type"],
                dedupe_fields_str,
                priority_processing,
            )
        )
        print(procedure_call)
        redshift_query_execute(
            sql=procedure_call,
            cfg=cfg,
            secrets_config=secrets_config,
            query_type="",
        )

    @task(
        on_success_callback=send_success_email,
        on_failure_callback=lambda context: send_failure_email(
            context, context["task_instance"].xcom_pull(key="data_details")
        ),
        trigger_rule="none_failed",
    )
    def data_pipeline_dag_generation(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "data_pipeline_dag_generation",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException

        FEED_TYPE_TO_CATEGORY = {
            "Customer Data": "Customer_Data_Processing",
            "Prospect Data": "Customer_Data_Processing",
            "Suppression Data": "Customer_Data_Processing",
            "Transaction Data": "Transaction_Data_Processing",
            "3rd Party Data Enrichment": "Enrichment_Data_Processing",
            "Campaign Data": "CampaignAndLookup_Data_Processing",
            "Lookup Data": "CampaignAndLookup_Data_Processing",
        }
        file_type_mapping = {"dag": {}, "config": {}}

        task_instance = kwargs["ti"]

        # Pull data details from XCom
        data_details = task_instance.xcom_pull(
            task_ids="get_data_ingestion_details", key="data_details"
        )

        if not data_details:
            raise ValueError("No data details found in XCom.")

        data = {
            "status": "In Progress",
            "stage": "Automation Step",
            "payload": {"remainingTime": 3, "stageNumber": 4},
        }

        json_string = json.dumps(data, indent=4)
        logging.info(json_string)
        # Connect to PostgreSQL database
        sql = """
                    UPDATE mz360.data_ingestion a set validation_status = 'IN_PROGRESS',validation_result='{}'
                    WHERE a.data_ingestion_id = '{}'
                """.format(
            json_string, data_details["data_ingestion_id"]
        )

        query_type = ""
        logging.info(sql)

        pg_result = pg.pg_query_execute(sql, cfg, pg_secrets_config, query_type)

        feed_type = data_details.get("feed_type", "").strip()
        company_name = data_details.get("company_name", "").strip().capitalize()
        frequency = data_details.get("frequency", "").capitalize()

        data_category = FEED_TYPE_TO_CATEGORY.get(feed_type)
        if not data_category:
            raise ValueError(f"Unknown feed type: {feed_type}")

        formatted_dag = generate_dag(
            data_category, company_name, FEED_TYPE_TO_CATEGORY, frequency
        )

        config_file_path = "/tmp"

        dag_file_name = f"{company_name}_{data_category}_{frequency}.py"
        file_type_mapping["dag"][dag_file_name] = formatted_dag

        file_path = os.path.join(config_file_path,dag_file_name)
        with open(file_path,"w") as file:
            file.write(json.dumps(formatted_dag,indent=2))

        print("priority_processing:", data_details.get("priority_processing"))

        if data_details.get("priority_processing", ""):
            data_category = "Priority_Data_Processing"
            formatted_dag = generate_dag(
                data_category, company_name, FEED_TYPE_TO_CATEGORY, frequency
            )
            dag_file_name = f"{company_name}_Priority_Data_Processing.py"
            file_type_mapping["dag"][dag_file_name] = formatted_dag

            file_path = os.path.join(config_file_path,dag_file_name)
            with open(file_path,"w") as file:
                file.write(json.dumps(formatted_dag,indent=2))

        print("file_type_mapping: ", file_type_mapping)
        task_instance.xcom_push(key="file_type_mapping", value=file_type_mapping)
        
    @task(
        on_success_callback=send_success_email,
        on_failure_callback=lambda context: send_failure_email(
            context, context["task_instance"].xcom_pull(key="data_details")
        ),
        trigger_rule="none_failed",
    )
    def generate_metadata_for_orchestration(**kwargs):
        config = []
        task_instance = kwargs["ti"]
        data_details = task_instance.xcom_pull(
            task_ids="get_data_ingestion_details", key="data_details"
        )

        data = {
            "status": "In Progress",
            "stage": "Automation Step",
            "payload": {"remainingTime": 2, "stageNumber": 4},
        }

        json_string = json.dumps(data, indent=4)
        logging.info(json_string)
        # Connect to PostgreSQL database
        sql = """
            UPDATE mz360.data_ingestion a set validation_status = 'IN_PROGRESS',validation_result='{}'
            WHERE a.data_ingestion_id = '{}'
        """.format(
            json_string, data_details["data_ingestion_id"]
        )

        query_type = ""
        logging.info(sql)

        pg_result = pg.pg_query_execute(sql, cfg, pg_secrets_config, query_type)
        list_config_inputs = []
        feed_name_list = []
        frequency = data_details.get("frequency", "")
        feed_type = data_details.get("feed_type", "").strip()
        company = data_details.get("company_name", "").strip()
        pg_result, col_names, data_category = fetch_dag_gen_parameters(feed_type, frequency, cfg, pg_secrets_config)

        json_template = {
            "key": "<source_cd>",
            "file_pattern": "DAX_<source_code>*",
            "header": True,
            "fullfile": "<full_file_flag>",
            "reject_mailboxes": [],
            "write_to_parquet": False,
            "frequency": "<frequency>"
        }

        for recs in pg_result:
            if recs[col_names.index('content_type')] == 'Full':
                full_file_flag = True
            else:
                full_file_flag = False
            source_cd = recs[col_names.index('feed')].upper()
            feed_name_list.append(recs[col_names.index('feed')])
            print(feed_name_list)
            feed_json = json_template.copy()
            feed_json["key"] = f"{source_cd}"
            feed_json["fullfile"] = full_file_flag
            feed_json["frequency"] = f"{frequency}"
            feed_json["file_pattern"] = feed_json["file_pattern"].replace("<source_code>", source_cd.upper())
            # list_config_inputs.append(json.dumps(feed_json, indent=4, cls=CustomEncoder))
            list_config_inputs.append(feed_json)

        config = create_dag_config(bucket_name, sec_name, list_config_inputs, frequency)

        print(" ------Printing Daily Config ------ \n {} ".format(json.dumps(config, indent=2)))

        config_file_path = "/tmp"
        config_file_name = f"{company}_{data_category}_{frequency}.json"
        file_type_mapping = task_instance.xcom_pull(
            task_ids="data_pipeline_dag_generation",key="file_type_mapping"
        )

        file_type_mapping["config"][config_file_name] = config
        print("file_type_mapping: ",file_type_mapping)
        file_path = os.path.join(config_file_path, config_file_name)
        with open(file_path, "w") as file:
            file.write(json.dumps(config, indent=2))
        task_instance.xcom_push(key="file_type_mapping",value=file_type_mapping)

        if data_details.get("priority_processing", ""):
            priority_flag = "Priority_Data_Processing"
            pg_result,col_names,data_category = fetch_dag_gen_parameters(feed_type,frequency,cfg,pg_secrets_config,priority_flag)
            list_config_inputs = []
            feed_name_list = []
            json_template = {
                "key":"<source_cd>",
                "file_pattern":"DAX_<source_code>*",
                "header":True,
                "fullfile":"<full_file_flag>",
                "reject_mailboxes":[],
                "write_to_parquet":False,
                "frequency":"<frequency>"
            }

            for recs in pg_result:
                if recs[col_names.index('content_type')]=='Full':
                    full_file_flag = True
                else:
                    full_file_flag = False
                source_cd = recs[col_names.index('feed')].upper()
                feed_name_list.append(recs[col_names.index('feed')])
                print(feed_name_list)
                feed_json = json_template.copy()
                feed_json["key"] = f"{source_cd}"
                feed_json["fullfile"] = full_file_flag
                feed_json["frequency"] = f"{frequency}"
                feed_json["file_pattern"] = feed_json["file_pattern"].replace("<source_code>",source_cd.upper())
                # list_config_inputs.append(json.dumps(feed_json, indent=4, cls=CustomEncoder))
                list_config_inputs.append(feed_json)

            config = create_dag_config(bucket_name,sec_name,list_config_inputs,frequency)

            print(" ------Printing Daily Config ------ \n {} ".format(json.dumps(config,indent=2)))

            config_file_path = "/tmp"
            config_file_name = f"{company}_Priority_Data_Processing.json"
            
            file_type_mapping["config"][config_file_name] = config
            print("file_type_mapping: ",file_type_mapping)
            file_path = os.path.join(config_file_path,config_file_name)
            with open(file_path,"w") as file:
                file.write(json.dumps(config,indent=2))
            task_instance.xcom_push(key="file_type_mapping",value=file_type_mapping)


    @task(
        on_success_callback=send_success_email,
        on_failure_callback=lambda context: send_failure_email(
            context, context["task_instance"].xcom_pull(key="data_details")
        ),
        trigger_rule="none_failed",
    )
    def dag_config_git_integration(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "generate_github_feature_branch",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException

        task_instance = kwargs["ti"]

        # Pull data details from XCom
        data_details = task_instance.xcom_pull(
            task_ids="get_data_ingestion_details", key="data_details"
        )

        if not data_details:
            raise ValueError("No data details found in XCom.")

        file_type_mapping = task_instance.xcom_pull(
            task_ids="generate_metadata_for_orchestration",key="file_type_mapping"
        )
        logging.info("Starting DAG and Config changes with Git integration...")
        dag_git_push_consolidated(file_type_mapping,git_secrets_config)

    @task(
        on_success_callback=send_success_email,
        on_failure_callback=lambda context: send_failure_email(
            context, context["task_instance"].xcom_pull(key="data_details")
        ),
        trigger_rule="none_failed",
    )
    def generate_and_share_validation_report(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "generate_and_share_validation_report",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException

        task_instance = kwargs["ti"]

        # Pull data details from XCom
        data_details = task_instance.xcom_pull(
            task_ids="get_data_ingestion_details", key="data_details"
        )

        if not data_details:
            raise ValueError("No data details found in XCom.")

        logging.info("Generating Validation Report")

    @task(
        on_success_callback=send_success_email,
        on_failure_callback=lambda context: send_failure_email(
            context, context["task_instance"].xcom_pull(key="data_details")
        ),
        trigger_rule="none_failed",
    )
    def deploy_to_production(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "deploy_to_production",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException

        task_instance = kwargs["ti"]

        # Pull data details from XCom
        data_details = task_instance.xcom_pull(
            task_ids="get_data_ingestion_details", key="data_details"
        )

        if not data_details:
            raise ValueError("No data details found in XCom.")

        data = {
            "status": "In Progress",
            "stage": "Deployment Step",
            "payload": {"remainingTime": 1, "stageNumber": 5},
        }

        json_string = json.dumps(data, indent=4)
        logging.info(json_string)
        # Connect to PostgreSQL database
        sql = """
            UPDATE mz360.data_ingestion a set validation_status = 'IN_PROGRESS',validation_result='{}'
            WHERE a.data_ingestion_id = '{}'
        """.format(
            json_string, data_details["data_ingestion_id"]
        )

        query_type = ""
        logging.info(sql)

        pg_result = pg.pg_query_execute(sql, cfg, pg_secrets_config, query_type)

        logging.info("Deploying to Production")

        if data_details["content_type"].lower() == "full" and data_details[
            "feed_type"
        ].lower() in (
            "customer data",
            "prospect data",
            "suppression data",
            "transaction data",
        ):
            # 1. Create crc_prev and crc_pii_prev input athena tables required for Full feeds
            create_athena_table_with_s3_copy(
                data_details=data_details,
                bucket_name=bucket_name,
                cfg=cfg,
                athena_db=athena_database,
                table_type="crc_prev",
            )

            create_athena_table_with_s3_copy(
                data_details=data_details,
                bucket_name=bucket_name,
                cfg=cfg,
                athena_db=athena_database,
                table_type="crc_pii_prev",
            )

        # JSON string input
        json_str = data_details["config"]

        # Parse the JSON string
        data = json.loads(json_str)

        # Initialize the list to store fields where dedupe_key is true
        dedupe_fields = []

        # Traverse the 'in' section to extract fields
        if "in" in data:
            for entry in data["in"]:
                if "fields" in entry:
                    for field in entry["fields"]:
                        if field.get("dedupe_key"):
                            dedupe_fields.append(field["name"])

        # Convert the list to a comma-separated string
        dedupe_fields_str = ",".join(dedupe_fields)

        ### Call Redshift procedure to Deploy ODS table, view, DWH view, ODS load, Metadata tables ###
        procedure_call = "CALL a360_load.prc_deploy_ods_dwh ('{}','{}','{}')".format(
            data_details["feed"],
            data_details["feed_type"],
            dedupe_fields_str,
        )
        print(procedure_call)
        redshift_query_execute(
            sql=procedure_call,
            cfg=cfg,
            secrets_config=secrets_config,
            query_type="",
        )

        data = {}

        json_string = json.dumps(data, indent=4)
        logging.info(json_string)
        # Connect to PostgreSQL database
        sql = """
            UPDATE mz360.data_ingestion a set validation_status = 'SUCCESS',validation_result='{}'
            WHERE a.data_ingestion_id = '{}'
        """.format(
            json_string, data_details["data_ingestion_id"]
        )

        query_type = ""
        logging.info(sql)

        pg_result = pg.pg_query_execute(sql, cfg, pg_secrets_config, query_type)

    data_details = get_data_ingestion_details()
    branch = branch_func()
    # join = join_task()

    # Connect the tasks
    (
        data_details
        >> branch
        >> [
            unload_data_from_client_redshift(),
            copy_file_from_client_bucket(),
            copy_sample_file_manual_upload(),
        ]
        >> copy_ingestion_config()
        >> generate_code_and_run_stg_load()
        >> generate_code_and_run_ods_load()
        >> data_pipeline_dag_generation()
        >> generate_metadata_for_orchestration()
        >> dag_config_git_integration()
        >> generate_and_share_validation_report()
        >> deploy_to_production()
    )


start = lumon_validation_automation()
