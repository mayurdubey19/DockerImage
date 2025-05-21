import os
import boto3
import json
import pandas as pd
import redshift_connector
from datetime import datetime
from airflow import DAG
from airflow.decorators import dag, task
from sqlalchemy import create_engine
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.exceptions import AirflowException, AirflowSkipException
import airflow.utils.dates
from helpers.AwsExtractOperator import *
from helpers.redshift import *
from helpers.s3 import *
from helpers.common import *
from aetna.python.mz_task import *
from aetna.python.mz_task import run_stg_job as run_aet_stg_job
from aetna.python.mz_task import run_ods_job as run_aet_ods_job
from helpers.send_email import *
import logging
from airflow.models import Variable
# from airflow.timetables.trigger import CronTriggerTimetable
from airflow.utils.db import provide_session
from airflow.models import TaskInstance
from airflow.configuration import conf
import sqlparse
default_args = {
    "owner": "Pratik.Patil",
    "start_date": airflow.utils.dates.days_ago(2),
    "depends_on_past": False,
    "current_path": os.path.abspath(os.path.dirname(__file__)),
    "config_path": os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "config")
    ),
    "retries": 0,
    "email_to": "pratik.patil@data-axle.com",
    "email_subject": "A360 db export Processing Job: ",
    "email_on_failure": True,
    "email_on_retry": False,
    "email_body": "Executing Stage Job ",
    "incoming_path": "s3://{}/incoming/",
    "output_path": "s3://{}/process_fcvrl/parquet_glue/",
    "script_path": "s3://{}/scripts/python/",
    "format_file_path_glue": "s3://{}/config/pyspark_json/",
    "redshift_config_paths": "s3://{}/config/common/glue_redshift_config.json",
    "iam_role_glue": "axle-a360-qa-glue-role",
    "timeout_glue_job": 90,
    "maxretries_glue_job": 0,
    "common_settings_glue": "s3://{}/config/common/pysparkcommonsettings.json",
    "additional_python_modules_glue": "h3==3.7.6,smart-open==6.3.0,openpyxl==3.1.2",
    "glue_version": "4.0",
    "extra_py_files_glue": "s3://{}/scripts/python/ValidateJsonFormatFile.py,s3://{}/scripts/python/FCVRL_UDFS.py,s3://{}/scripts/python/common.py",
    "connections_glue": "axle-a360-qa-redshift-connector",
    "temp_path_athena": "s3://{}/athena_query_results/",
    "mcd_input_table": "MZB_MCD_FILE_INCR",
    "reject_path": "s3://{}/archive/outbound/rejects/",
    "extracts_path": "s3://{}/archive/outbound/extracts/",
    "ods_user": "A360_ODS",
    "audit_schema": "A360_CORE",
    "audit_table": "TMDM_OBJECT",
}

@dag(
    dag_id="a360_export_db_objects",
    default_args=default_args,
    description="a360 Export DB Objects Pipeline",
    # schedule=CronTriggerTimetable("0 15 * * mon-thu", timezone="America/Chicago"),
    schedule_interval=None,
    max_active_runs=1,
    params={"run_time_params": {"task_to_skip": []}},
    catchup=False,
)
def aetna_export_db_objects():
    current_env = Variable.get("var-env", "local")
    if current_env == "Prod":
        json_config_path = default_args["config_path"] + "/Monarch-Solutions_Customer_Data_Processing_Daily_prod.json"
    else:
        json_config_path = default_args["config_path"] + "/Monarch-Solutions_Customer_Data_Processing_Daily.json"
        default_args["email_to"] = "DL-Aetna-Offshore-Support@data-axle.com"
       
        
    cfg = get_json_config(json_config_path)
    sec_name = cfg["secret_name"]
    region_name = cfg["aws_reg_rs"]
    secrets_config = get_secret(sec_name, region_name)
    aet_account_id = get_aws_account_id()
    bucket_name = cfg["bucket_name"].replace("{}", aet_account_id[-4:])
    mdm_bucket_name = cfg["bucket_name"].replace("{}", aet_account_id[-4:]) + "-mdm"
    dps_bucket_name = cfg["dps_bucket_name"].replace("{}", aet_account_id[-4:])
    mft_bucket_name = cfg["mft_bucket"].replace("{}", aet_account_id[-4:])
    redshift_iam_role = "arn:aws:iam::{}:role/da-mz-redshift-role".format(
        aet_account_id
    )
    emr_iam_role = "arn:aws:iam::{}:role/da-mz-emr-serverless-role".format(
        aet_account_id
    )
    athena_database = get_ssm_parameters("aet_athena_database", region_name, False)
    mcd_emr_application_id = get_ssm_parameters("aet_mcd_emr", region_name, False)
    v_execid, v_batchid = generate_exec_id(cfg["request_type"])
    reject_path = default_args["reject_path"].replace("{}", bucket_name)
    extracts_path = default_args["extracts_path"].replace("{}", bucket_name)
    temp_path_athena = default_args["temp_path_athena"].replace("{}", bucket_name)
    v_cur_date = datetime.now().strftime("%Y%m%d%H%M%S")
    def get_task_log(task_instance):
        base_log_folder = conf.get("logging", "BASE_LOG_FOLDER")
        # Adjust attempt number to reflect the actual try number
        adjusted_attempt_number = task_instance.try_number - 1
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
            client="aetna",
        )
    def send_failure_email(context):
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

        email_to = default_args["email_to"] + ",MzSupportTier1@infogroup.com"

        send_email(
            email_address=email_to,
            email_message=html_content,
            email_subject="Failure :{}{}".format(
                default_args["email_subject"], task_id
            ),
            client="aetna",
        )


    @task(
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email,
        trigger_rule="none_failed",
    )
    def export_db_objects_views_ddl_ods(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "export_db_objects_views_ddl",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException
        
        sql = """SELECT viewname,schemaname,definition
                FROM pg_views
                WHERE schemaname  IN ('a360_ods')   ;   
                """

        print(sql)
        return_val = redshift_query_execute(
            sql,
            cfg=cfg,
            secrets_config=secrets_config,
            query_type="query",
        )

    # Initialize S3 client
        s3_client = boto3.client('s3')        
        for row in return_val:
            viewname = row[0]
            schema_name = row[1]
            definition = row[2]
            vw_definition = sqlparse.format(definition, reindent=True, keyword_case='lower')
            vw_definition = vw_definition.lower()
            # Create file content
            file_content = vw_definition
            
            # File name and S3 path
            file_name = f"{viewname}.sql"
            s3_key = f"temp/a360_ods/views/{file_name}"
            
            # Upload file to S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=file_content
            )
            print(f"Uploaded {file_name} to s3://{bucket_name}/{s3_key}")        
    
    @task(
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email,
        trigger_rule="none_failed",
    )
    def export_db_objects_proc_ods(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "export_db_objects_proc_ods",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException
        
        sql = """SELECT 
                    proname AS procedure_name,
                    nspname AS schema_name,
                    pg_get_functiondef(pg_proc.oid) AS definition
                FROM pg_proc
                JOIN pg_namespace ON pg_proc.pronamespace = pg_namespace.oid
                WHERE nspname NOT IN ('pg_catalog', 'information_schema','public') and nspname in ('a360_ods') ;"""
        print(sql)
        return_val = redshift_query_execute(
            sql,
            cfg=cfg,
            secrets_config=secrets_config,
            query_type="query",
        )

    # Initialize S3 client
        s3_client = boto3.client('s3')        
        for row in return_val:
            procedure_name = row[0]
            schema_name = row[1]
            definition = row[2]
            # Create file content
            file_content = definition
            
            # File name and S3 path
            file_name = f"{procedure_name}.sql"
            s3_key = f"temp/a360_ods/stored_procedures/{file_name}"
            
            # Upload file to S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=file_content
            )
            print(f"Uploaded {file_name} to s3://{bucket_name}/{s3_key}")
    @task(
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email,
        trigger_rule="none_failed",
    )
    def export_db_objects_table_ddl_ods(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "export_db_objects_table_ddl_ods",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException
        # Query to fetch all distinct table names
        fetch_tables_query = """
            SELECT DISTINCT tablename
            FROM public.v_generate_tbl_ddl where schemaname='a360_ods' ;
        """
        
        # Execute query to fetch table names
        table_names = redshift_query_execute(
            fetch_tables_query,
            cfg=cfg,
            secrets_config=secrets_config,
            query_type="query"
        )
        table_names = [row[0] for row in table_names]  # Extract table names
        s3_client = boto3.client('s3')
        for table_name in table_names:
            # Query to fetch the DDL for the table
            fetch_ddl_query = f"""
                SELECT ddl
                FROM public.v_generate_tbl_ddl
                WHERE tablename = '{table_name}' and schemaname='a360_ods'
                ORDER BY seq
            """
            
            # Execute query to fetch DDL
            ddl_result = redshift_query_execute(
                fetch_ddl_query,
                cfg=cfg,
                secrets_config=secrets_config,
                query_type="query"
            )
            
            # Combine DDL statements into a single string
            full_ddl = "\n".join([row[0] for row in ddl_result])
            
            # Create file name and S3 key
            file_name = f"{table_name}.sql"
            s3_key = f"temp/a360_ods/tables/{file_name}"
            
            # Upload to S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=full_ddl
            )
            print(f"Uploaded {file_name} to s3://{bucket_name}/{s3_key}")         

    @task(
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email,
        trigger_rule="none_failed",
    )
    def export_db_objects_function_ddl_ods(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "export_db_objects_function_ddl_ods",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException
        # Query to fetch all distinct table names
        fetch_tables_query = """
            SELECT DISTINCT udfname
            FROM public.v_generate_udf_ddl where schemaname='a360_ods' ;
        """
        
        # Execute query to fetch table names
        table_names = redshift_query_execute(
            fetch_tables_query,
            cfg=cfg,
            secrets_config=secrets_config,
            query_type="query"
        )
        table_names = [row[0] for row in table_names]  # Extract table names
        s3_client = boto3.client('s3')
        for table_name in table_names:
            # Query to fetch the DDL for the table
            fetch_ddl_query = f"""
                SELECT ddl
                FROM public.v_generate_udf_ddl
                WHERE udfname = '{table_name}' and schemaname='a360_ods'
                ORDER BY seq
            """
            
            # Execute query to fetch DDL
            ddl_result = redshift_query_execute(
                fetch_ddl_query,
                cfg=cfg,
                secrets_config=secrets_config,
                query_type="query"
            )
            
            # Combine DDL statements into a single string
            full_ddl = "\n".join([row[0] for row in ddl_result])
            
            # Create file name and S3 key
            file_name = f"{table_name}.sql"
            s3_key = f"temp/a360_ods/functions/{file_name}"
            
            # Upload to S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=full_ddl
            )
            print(f"Uploaded {file_name} to s3://{bucket_name}/{s3_key}")      

    @task(
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email,
        trigger_rule="none_failed",
    )
    def export_db_objects_views_ddl_stg(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "export_db_objects_views_ddl_stg",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException
        
        sql = """SELECT viewname,schemaname,definition
                FROM pg_views
                WHERE schemaname  IN ('a360_stg')  ;   
                """

        print(sql)
        return_val = redshift_query_execute(
            sql,
            cfg=cfg,
            secrets_config=secrets_config,
            query_type="query",
        )

    # Initialize S3 client
        s3_client = boto3.client('s3')        
        for row in return_val:
            viewname = row[0]
            schema_name = row[1]
            definition = row[2]
            vw_definition = sqlparse.format(definition, reindent=True, keyword_case='lower')
            vw_definition = vw_definition.lower()
            # Create file content
            file_content = vw_definition
            
            # File name and S3 path
            file_name = f"{viewname}.sql"
            s3_key = f"temp/a360_stg/views/{file_name}"
            
            # Upload file to S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=file_content
            )
            print(f"Uploaded {file_name} to s3://{bucket_name}/{s3_key}")        
    
    @task(
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email,
        trigger_rule="none_failed",
    )
    def export_db_objects_proc_stg(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "export_db_objects_proc_stg",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException
        
        sql = """SELECT 
                    proname AS procedure_name,
                    nspname AS schema_name,
                    pg_get_functiondef(pg_proc.oid) AS definition
                FROM pg_proc
                JOIN pg_namespace ON pg_proc.pronamespace = pg_namespace.oid
                WHERE nspname NOT IN ('pg_catalog', 'information_schema','public') and nspname in ('a360_stg') ;"""
        print(sql)
        return_val = redshift_query_execute(
            sql,
            cfg=cfg,
            secrets_config=secrets_config,
            query_type="query",
        )

    # Initialize S3 client
        s3_client = boto3.client('s3')        
        for row in return_val:
            procedure_name = row[0]
            schema_name = row[1]
            definition = row[2]
            # Create file content
            file_content = definition
            
            # File name and S3 path
            file_name = f"{procedure_name}.sql"
            s3_key = f"temp/a360_stg/stored_procedures/{file_name}"
            
            # Upload file to S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=file_content
            )
            print(f"Uploaded {file_name} to s3://{bucket_name}/{s3_key}")
    @task(
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email,
        trigger_rule="none_failed",
    )
    def export_db_objects_table_ddl_stg(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "export_db_objects_table_ddl_stg",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException
        # Query to fetch all distinct table names
        fetch_tables_query = """
            SELECT DISTINCT tablename
            FROM public.v_generate_tbl_ddl where schemaname='a360_stg' ;
        """
        
        # Execute query to fetch table names
        table_names = redshift_query_execute(
            fetch_tables_query,
            cfg=cfg,
            secrets_config=secrets_config,
            query_type="query"
        )
        table_names = [row[0] for row in table_names]  # Extract table names
        s3_client = boto3.client('s3')
        for table_name in table_names:
            # Query to fetch the DDL for the table
            fetch_ddl_query = f"""
                SELECT ddl
                FROM public.v_generate_tbl_ddl
                WHERE tablename = '{table_name}' and schemaname='a360_stg'
                ORDER BY seq
            """
            
            # Execute query to fetch DDL
            ddl_result = redshift_query_execute(
                fetch_ddl_query,
                cfg=cfg,
                secrets_config=secrets_config,
                query_type="query"
            )
            
            # Combine DDL statements into a single string
            full_ddl = "\n".join([row[0] for row in ddl_result])
            
            # Create file name and S3 key
            file_name = f"{table_name}.sql"
            s3_key = f"temp/a360_stg/tables/{file_name}"
            
            # Upload to S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=full_ddl
            )
            print(f"Uploaded {file_name} to s3://{bucket_name}/{s3_key}")         

    @task(
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email,
        trigger_rule="none_failed",
    )
    def export_db_objects_function_ddl_stg(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "export_db_objects_function_ddl_stg",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException
        # Query to fetch all distinct table names
        fetch_tables_query = """
            SELECT DISTINCT udfname
            FROM public.v_generate_udf_ddl where schemaname='a360_stg' ;
        """
        
        # Execute query to fetch table names
        table_names = redshift_query_execute(
            fetch_tables_query,
            cfg=cfg,
            secrets_config=secrets_config,
            query_type="query"
        )
        table_names = [row[0] for row in table_names]  # Extract table names
        s3_client = boto3.client('s3')
        for table_name in table_names:
            # Query to fetch the DDL for the table
            fetch_ddl_query = f"""
                SELECT ddl
                FROM public.v_generate_udf_ddl
                WHERE udfname = '{table_name}' and schemaname='a360_stg'
                ORDER BY seq
            """
            
            # Execute query to fetch DDL
            ddl_result = redshift_query_execute(
                fetch_ddl_query,
                cfg=cfg,
                secrets_config=secrets_config,
                query_type="query"
            )
            
            # Combine DDL statements into a single string
            full_ddl = "\n".join([row[0] for row in ddl_result])
            
            # Create file name and S3 key
            file_name = f"{table_name}.sql"
            s3_key = f"temp/a360_stg/functions/{file_name}"
            
            # Upload to S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=full_ddl
            )
            print(f"Uploaded {file_name} to s3://{bucket_name}/{s3_key}") 

       
    @task(
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email,
        trigger_rule="none_failed",
    )
    def export_db_objects_views_ddl_dwh(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "export_db_objects_views_ddl_dwh",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException
        
        sql = """SELECT viewname,schemaname,definition
                FROM pg_views
                WHERE schemaname  IN ('a360_dwh')  ;   
                """

        print(sql)
        return_val = redshift_query_execute(
            sql,
            cfg=cfg,
            secrets_config=secrets_config,
            query_type="query",
        )

    # Initialize S3 client
        s3_client = boto3.client('s3')        
        for row in return_val:
            viewname = row[0]
            schema_name = row[1]
            definition = row[2]
            vw_definition = sqlparse.format(definition, reindent=True, keyword_case='lower')
            vw_definition = vw_definition.lower()
            # Create file content
            file_content = vw_definition
            
            # File name and S3 path
            file_name = f"{viewname}.sql"
            s3_key = f"temp/a360_dwh/views/{file_name}"
            
            # Upload file to S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=file_content
            )
            print(f"Uploaded {file_name} to s3://{bucket_name}/{s3_key}")        
    
    @task(
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email,
        trigger_rule="none_failed",
    )
    def export_db_objects_proc_dwh(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "export_db_objects_proc_dwh",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException
        
        sql = """SELECT 
                    proname AS procedure_name,
                    nspname AS schema_name,
                    pg_get_functiondef(pg_proc.oid) AS definition
                FROM pg_proc
                JOIN pg_namespace ON pg_proc.pronamespace = pg_namespace.oid
                WHERE nspname NOT IN ('pg_catalog', 'information_schema','public') and nspname in ('a360_dwh') ;"""
        print(sql)
        return_val = redshift_query_execute(
            sql,
            cfg=cfg,
            secrets_config=secrets_config,
            query_type="query",
        )

    # Initialize S3 client
        s3_client = boto3.client('s3')        
        for row in return_val:
            procedure_name = row[0]
            schema_name = row[1]
            definition = row[2]
            # Create file content
            file_content = definition
            
            # File name and S3 path
            file_name = f"{procedure_name}.sql"
            s3_key = f"temp/a360_dwh/stored_procedures/{file_name}"
            
            # Upload file to S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=file_content
            )
            print(f"Uploaded {file_name} to s3://{bucket_name}/{s3_key}")
			
     

    @task(
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email,
        trigger_rule="none_failed",
    )
    def export_db_objects_function_ddl_dwh(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "export_db_objects_function_ddl_dwh",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException
        # Query to fetch all distinct table names
        fetch_tables_query = """
            SELECT DISTINCT udfname
            FROM public.v_generate_udf_ddl where schemaname='a360_dwh' ;
        """
        
        # Execute query to fetch table names
        table_names = redshift_query_execute(
            fetch_tables_query,
            cfg=cfg,
            secrets_config=secrets_config,
            query_type="query"
        )
        table_names = [row[0] for row in table_names]  # Extract table names
        s3_client = boto3.client('s3')
        for table_name in table_names:
            # Query to fetch the DDL for the table
            fetch_ddl_query = f"""
                SELECT ddl
                FROM public.v_generate_udf_ddl
                WHERE udfname = '{table_name}' and schemaname='a360_dwh'
                ORDER BY seq
            """
            
            # Execute query to fetch DDL
            ddl_result = redshift_query_execute(
                fetch_ddl_query,
                cfg=cfg,
                secrets_config=secrets_config,
                query_type="query"
            )
            
            # Combine DDL statements into a single string
            full_ddl = "\n".join([row[0] for row in ddl_result])
            
            # Create file name and S3 key
            file_name = f"{table_name}.sql"
            s3_key = f"temp/a360_dwh/functions/{file_name}"
            
            # Upload to S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=full_ddl
            )
            print(f"Uploaded {file_name} to s3://{bucket_name}/{s3_key}") 


    @task(
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email,
        trigger_rule="none_failed",
    )
    def export_db_objects_views_ddl_core(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "export_db_objects_views_ddl_core",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException
        
        sql = """SELECT viewname,schemaname,definition
                FROM pg_views
                WHERE schemaname  IN ('a360_core')   ;   
                """

        print(sql)
        return_val = redshift_query_execute(
            sql,
            cfg=cfg,
            secrets_config=secrets_config,
            query_type="query",
        )

    # Initialize S3 client
        s3_client = boto3.client('s3')        
        for row in return_val:
            viewname = row[0]
            schema_name = row[1]
            definition = row[2]
            vw_definition = sqlparse.format(definition, reindent=True, keyword_case='lower')
            vw_definition = vw_definition.lower()
            # Create file content
            file_content = vw_definition
            
            # File name and S3 path
            file_name = f"{viewname}.sql"
            s3_key = f"temp/a360_core/views/{file_name}"
            
            # Upload file to S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=file_content
            )
            print(f"Uploaded {file_name} to s3://{bucket_name}/{s3_key}")        
    
    @task(
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email,
        trigger_rule="none_failed",
    )
    def export_db_objects_proc_core(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "export_db_objects_proc_core",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException
        
        sql = """SELECT 
                    proname AS procedure_name,
                    nspname AS schema_name,
                    pg_get_functiondef(pg_proc.oid) AS definition
                FROM pg_proc
                JOIN pg_namespace ON pg_proc.pronamespace = pg_namespace.oid
                WHERE nspname NOT IN ('pg_catalog', 'information_schema','public') and nspname in ('a360_core') ;"""
        print(sql)
        return_val = redshift_query_execute(
            sql,
            cfg=cfg,
            secrets_config=secrets_config,
            query_type="query",
        )

    # Initialize S3 client
        s3_client = boto3.client('s3')        
        for row in return_val:
            procedure_name = row[0]
            schema_name = row[1]
            definition = row[2]
            # Create file content
            file_content = definition
            
            # File name and S3 path
            file_name = f"{procedure_name}.sql"
            s3_key = f"temp/a360_core/stored_procedures/{file_name}"
            
            # Upload file to S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=file_content
            )
            print(f"Uploaded {file_name} to s3://{bucket_name}/{s3_key}")
			
    @task(
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email,
        trigger_rule="none_failed",
    )
    def export_db_objects_table_ddl_core(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "export_db_objects_table_ddl_core",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException
        # Query to fetch all distinct table names
        fetch_tables_query = """
            SELECT DISTINCT tablename
            FROM public.v_generate_tbl_ddl where schemaname='a360_core' ;
        """
        
        # Execute query to fetch table names
        table_names = redshift_query_execute(
            fetch_tables_query,
            cfg=cfg,
            secrets_config=secrets_config,
            query_type="query"
        )
        table_names = [row[0] for row in table_names]  # Extract table names
        s3_client = boto3.client('s3')
        for table_name in table_names:
            # Query to fetch the DDL for the table
            fetch_ddl_query = f"""
                SELECT ddl
                FROM public.v_generate_tbl_ddl
                WHERE tablename = '{table_name}' and schemaname='a360_core'
                ORDER BY seq
            """
            
            # Execute query to fetch DDL
            ddl_result = redshift_query_execute(
                fetch_ddl_query,
                cfg=cfg,
                secrets_config=secrets_config,
                query_type="query"
            )
            
            # Combine DDL statements into a single string
            full_ddl = "\n".join([row[0] for row in ddl_result])
            
            # Create file name and S3 key
            file_name = f"{table_name}.sql"
            s3_key = f"temp/a360_core/tables/{file_name}"
            
            # Upload to S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=full_ddl
            )
            print(f"Uploaded {file_name} to s3://{bucket_name}/{s3_key}")         

    @task(
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email,
        trigger_rule="none_failed",
    )
    def export_db_objects_views_ddl_load(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "export_db_objects_views_ddl",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException
        
        sql = """SELECT viewname,schemaname,definition
                FROM pg_views
                WHERE schemaname  IN ('a360_load')  ;   
                """

        print(sql)
        return_val = redshift_query_execute(
            sql,
            cfg=cfg,
            secrets_config=secrets_config,
            query_type="query",
        )

    # Initialize S3 client
        s3_client = boto3.client('s3')        
        for row in return_val:
            viewname = row[0]
            schema_name = row[1]
            definition = row[2]
            vw_definition = sqlparse.format(definition, reindent=True, keyword_case='lower')
            vw_definition = vw_definition.lower()
            # Create file content
            file_content = vw_definition
            
            # File name and S3 path
            file_name = f"{viewname}.sql"
            s3_key = f"temp/a360_load/views/{file_name}"
            
            # Upload file to S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=file_content
            )
            print(f"Uploaded {file_name} to s3://{bucket_name}/{s3_key}")        
    
    @task(
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email,
        trigger_rule="none_failed",
    )
    def export_db_objects_proc_load(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "export_db_objects_proc_load",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException
        
        sql = """SELECT 
                    proname AS procedure_name,
                    nspname AS schema_name,
                    pg_get_functiondef(pg_proc.oid) AS definition
                FROM pg_proc
                JOIN pg_namespace ON pg_proc.pronamespace = pg_namespace.oid
                WHERE nspname NOT IN ('pg_catalog', 'information_schema','public') and nspname in ('a360_load') ;"""
        print(sql)
        return_val = redshift_query_execute(
            sql,
            cfg=cfg,
            secrets_config=secrets_config,
            query_type="query",
        )

    # Initialize S3 client
        s3_client = boto3.client('s3')        
        for row in return_val:
            procedure_name = row[0]
            schema_name = row[1]
            definition = row[2]
            # Create file content
            file_content = definition
            
            # File name and S3 path
            file_name = f"{procedure_name}.sql"
            s3_key = f"temp/a360_load/stored_procedures/{file_name}"
            
            # Upload file to S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=file_content
            )
            print(f"Uploaded {file_name} to s3://{bucket_name}/{s3_key}")
    @task(
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email,
        trigger_rule="none_failed",
    )
    def export_db_objects_table_ddl_load(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "export_db_objects_table_ddl_load",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException
        # Query to fetch all distinct table names
        fetch_tables_query = """
            SELECT DISTINCT tablename
            FROM public.v_generate_tbl_ddl where schemaname='a360_load' ;
        """
        
        # Execute query to fetch table names
        table_names = redshift_query_execute(
            fetch_tables_query,
            cfg=cfg,
            secrets_config=secrets_config,
            query_type="query"
        )
        table_names = [row[0] for row in table_names]  # Extract table names
        s3_client = boto3.client('s3')
        for table_name in table_names:
            # Query to fetch the DDL for the table
            fetch_ddl_query = f"""
                SELECT ddl
                FROM public.v_generate_tbl_ddl
                WHERE tablename = '{table_name}' and schemaname='a360_load'
                ORDER BY seq
            """
            
            # Execute query to fetch DDL
            ddl_result = redshift_query_execute(
                fetch_ddl_query,
                cfg=cfg,
                secrets_config=secrets_config,
                query_type="query"
            )
            
            # Combine DDL statements into a single string
            full_ddl = "\n".join([row[0] for row in ddl_result])
            
            # Create file name and S3 key
            file_name = f"{table_name}.sql"
            s3_key = f"temp/a360_load/tables/{file_name}"
            
            # Upload to S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=full_ddl
            )
            print(f"Uploaded {file_name} to s3://{bucket_name}/{s3_key}")         


    @task(
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email,
        trigger_rule="none_failed",
    )
    def export_db_objects_views_ddl_idms(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "export_db_objects_views_ddl",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException
        
        sql = """SELECT viewname,schemaname,definition
                FROM pg_views
                WHERE schemaname  IN ('mzb_aet_idms')  ;   
                """

        print(sql)
        return_val = redshift_query_execute(
            sql,
            cfg=cfg,
            secrets_config=secrets_config,
            query_type="query",
        )

    # Initialize S3 client
        s3_client = boto3.client('s3')        
        for row in return_val:
            viewname = row[0]
            schema_name = row[1]
            definition = row[2]
            vw_definition = sqlparse.format(definition, reindent=True, keyword_case='lower')
            vw_definition = vw_definition.lower()
            # Create file content
            file_content = vw_definition
            
            # File name and S3 path
            file_name = f"{viewname}.sql"
            s3_key = f"temp/mzb_aet_idms/views/{file_name}"
            
            # Upload file to S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=file_content
            )
            print(f"Uploaded {file_name} to s3://{bucket_name}/{s3_key}")        
    
 			
    @task(
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email,
        trigger_rule="none_failed",
    )
    def export_db_objects_views_ddl_mc(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "export_db_objects_views_ddl",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException
        
        sql = """SELECT viewname,schemaname,definition
                FROM pg_views
                WHERE schemaname  IN ('mzb_aet_mc')  ;   
                """

        print(sql)
        return_val = redshift_query_execute(
            sql,
            cfg=cfg,
            secrets_config=secrets_config,
            query_type="query",
        )

    # Initialize S3 client
        s3_client = boto3.client('s3')        
        for row in return_val:
            viewname = row[0]
            schema_name = row[1]
            definition = row[2]
            vw_definition = sqlparse.format(definition, reindent=True, keyword_case='lower')
            vw_definition = vw_definition.lower()
            # Create file content
            file_content = vw_definition
            
            # File name and S3 path
            file_name = f"{viewname}.sql"
            s3_key = f"temp/mzb_aet_mc/views/{file_name}"
            
            # Upload file to S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=file_content
            )
            print(f"Uploaded {file_name} to s3://{bucket_name}/{s3_key}")        
    
    @task(
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email,
        trigger_rule="none_failed",
    )
    def export_db_objects_proc_mc(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "export_db_objects_proc_mc",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException
        
        sql = """SELECT 
                    proname AS procedure_name,
                    nspname AS schema_name,
                    pg_get_functiondef(pg_proc.oid) AS definition
                FROM pg_proc
                JOIN pg_namespace ON pg_proc.pronamespace = pg_namespace.oid
                WHERE nspname NOT IN ('pg_catalog', 'information_schema','public') and nspname in ('mzb_aet_mc')  ;"""
        print(sql)
        return_val = redshift_query_execute(
            sql,
            cfg=cfg,
            secrets_config=secrets_config,
            query_type="query",
        )

    # Initialize S3 client
        s3_client = boto3.client('s3')        
        for row in return_val:
            procedure_name = row[0]
            schema_name = row[1]
            definition = row[2]
            # Create file content
            file_content = definition
            
            # File name and S3 path
            file_name = f"{procedure_name}.sql"
            s3_key = f"temp/mzb_aet_mc/stored_procedures/{file_name}"
            
            # Upload file to S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=file_content
            )
            print(f"Uploaded {file_name} to s3://{bucket_name}/{s3_key}")
    @task(
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email,
        trigger_rule="none_failed",
    )
    def export_db_objects_table_ddl_mc(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "export_db_objects_table_ddl_mc",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException
        # Query to fetch all distinct table names
        fetch_tables_query = """
            SELECT DISTINCT tablename
            FROM public.v_generate_tbl_ddl where schemaname='mzb_aet_mc';
        """
        
        # Execute query to fetch table names
        table_names = redshift_query_execute(
            fetch_tables_query,
            cfg=cfg,
            secrets_config=secrets_config,
            query_type="query"
        )
        table_names = [row[0] for row in table_names]  # Extract table names
        s3_client = boto3.client('s3')
        for table_name in table_names:
            # Query to fetch the DDL for the table
            fetch_ddl_query = f"""
                SELECT ddl
                FROM public.v_generate_tbl_ddl
                WHERE tablename = '{table_name}' and schemaname='mzb_aet_mc'
                ORDER BY seq
            """
            
            # Execute query to fetch DDL
            ddl_result = redshift_query_execute(
                fetch_ddl_query,
                cfg=cfg,
                secrets_config=secrets_config,
                query_type="query"
            )
            
            # Combine DDL statements into a single string
            full_ddl = "\n".join([row[0] for row in ddl_result])
            
            # Create file name and S3 key
            file_name = f"{table_name}.sql"
            s3_key = f"temp/mzb_aet_mc/tables/{file_name}"
            
            # Upload to S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=full_ddl
            )
            print(f"Uploaded {file_name} to s3://{bucket_name}/{s3_key}")         

    @task(
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email,
        trigger_rule="none_failed",
    )
    def export_db_objects_views_ddl_medstg(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "export_db_objects_views_ddl",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException
        
        sql = """SELECT viewname,schemaname,definition
                FROM pg_views
                WHERE schemaname  IN ('mzb_aet_medstg')  ;   
                """

        print(sql)
        return_val = redshift_query_execute(
            sql,
            cfg=cfg,
            secrets_config=secrets_config,
            query_type="query",
        )

    # Initialize S3 client
        s3_client = boto3.client('s3')        
        for row in return_val:
            viewname = row[0]
            schema_name = row[1]
            definition = row[2]
            vw_definition = sqlparse.format(definition, reindent=True, keyword_case='lower')
            vw_definition = vw_definition.lower()
            # Create file content
            file_content = vw_definition
            
            # File name and S3 path
            file_name = f"{viewname}.sql"
            s3_key = f"temp/mzb_aet_medstg/views/{file_name}"
            
            # Upload file to S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=file_content
            )
            print(f"Uploaded {file_name} to s3://{bucket_name}/{s3_key}")        
    
    @task(
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email,
        trigger_rule="none_failed",
    )
    def export_db_objects_proc_medstg(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "export_db_objects_proc_medstg",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException
        
        sql = """SELECT 
                    proname AS procedure_name,
                    nspname AS schema_name,
                    pg_get_functiondef(pg_proc.oid) AS definition
                FROM pg_proc
                JOIN pg_namespace ON pg_proc.pronamespace = pg_namespace.oid
                WHERE nspname NOT IN ('pg_catalog', 'information_schema','public') and nspname in ('mzb_aet_medstg') ;"""
        print(sql)
        return_val = redshift_query_execute(
            sql,
            cfg=cfg,
            secrets_config=secrets_config,
            query_type="query",
        )

    # Initialize S3 client
        s3_client = boto3.client('s3')        
        for row in return_val:
            procedure_name = row[0]
            schema_name = row[1]
            definition = row[2]
            # Create file content
            file_content = definition
            
            # File name and S3 path
            file_name = f"{procedure_name}.sql"
            s3_key = f"temp/mzb_aet_medstg/stored_procedures/{file_name}"
            
            # Upload file to S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=file_content
            )
            print(f"Uploaded {file_name} to s3://{bucket_name}/{s3_key}")
    @task(
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email,
        trigger_rule="none_failed",
    )
    def export_db_objects_table_ddl_medstg(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "export_db_objects_table_ddl_medstg",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException
        # Query to fetch all distinct table names
        fetch_tables_query = """
            SELECT DISTINCT tablename
            FROM public.v_generate_tbl_ddl where schemaname='mzb_aet_medstg' ;
        """
        
        # Execute query to fetch table names
        table_names = redshift_query_execute(
            fetch_tables_query,
            cfg=cfg,
            secrets_config=secrets_config,
            query_type="query"
        )
        table_names = [row[0] for row in table_names]  # Extract table names
        s3_client = boto3.client('s3')
        for table_name in table_names:
            # Query to fetch the DDL for the table
            fetch_ddl_query = f"""
                SELECT ddl
                FROM public.v_generate_tbl_ddl
                WHERE tablename = '{table_name}' and  schemaname='mzb_aet_medstg'
                ORDER BY seq
            """
            
            # Execute query to fetch DDL
            ddl_result = redshift_query_execute(
                fetch_ddl_query,
                cfg=cfg,
                secrets_config=secrets_config,
                query_type="query"
            )
            
            # Combine DDL statements into a single string
            full_ddl = "\n".join([row[0] for row in ddl_result])
            
            # Create file name and S3 key
            file_name = f"{table_name}.sql"
            s3_key = f"temp/mzb_aet_medstg/tables/{file_name}"
            
            # Upload to S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=full_ddl
            )
            print(f"Uploaded {file_name} to s3://{bucket_name}/{s3_key}")         

    @task(
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email,
        trigger_rule="none_failed",
    )
    def export_db_objects_views_ddl_medods(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "export_db_objects_views_ddl",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException
        
        sql = """SELECT viewname,schemaname,definition
                FROM pg_views
                WHERE schemaname  IN ('mzb_aet_medods')  ;   
                """

        print(sql)
        return_val = redshift_query_execute(
            sql,
            cfg=cfg,
            secrets_config=secrets_config,
            query_type="query",
        )

    # Initialize S3 client
        s3_client = boto3.client('s3')        
        for row in return_val:
            viewname = row[0]
            schema_name = row[1]
            definition = row[2]
            vw_definition = sqlparse.format(definition, reindent=True, keyword_case='lower')
            vw_definition = vw_definition.lower()
            # Create file content
            file_content = vw_definition
            
            # File name and S3 path
            file_name = f"{viewname}.sql"
            s3_key = f"temp/mzb_aet_medods/views/{file_name}"
            
            # Upload file to S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=file_content
            )
            print(f"Uploaded {file_name} to s3://{bucket_name}/{s3_key}")        
    
    @task(
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email,
        trigger_rule="none_failed",
    )
    def export_db_objects_proc_medods(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "export_db_objects_proc_medods",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException
        
        sql = """SELECT 
                    proname AS procedure_name,
                    nspname AS schema_name,
                    pg_get_functiondef(pg_proc.oid) AS definition
                FROM pg_proc
                JOIN pg_namespace ON pg_proc.pronamespace = pg_namespace.oid
                WHERE nspname NOT IN ('pg_catalog', 'information_schema','public') and nspname in ('mzb_aet_medods')  ;"""
        print(sql)
        return_val = redshift_query_execute(
            sql,
            cfg=cfg,
            secrets_config=secrets_config,
            query_type="query",
        )

    # Initialize S3 client
        s3_client = boto3.client('s3')        
        for row in return_val:
            procedure_name = row[0]
            schema_name = row[1]
            definition = row[2]
            # Create file content
            file_content = definition
            
            # File name and S3 path
            file_name = f"{procedure_name}.sql"
            s3_key = f"temp/mzb_aet_medods/stored_procedures/{file_name}"
            
            # Upload file to S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=file_content
            )
            print(f"Uploaded {file_name} to s3://{bucket_name}/{s3_key}")
    @task(
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email,
        trigger_rule="none_failed",
    )
    def export_db_objects_table_ddl_medods(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "export_db_objects_table_ddl_medods",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException
        # Query to fetch all distinct table names
        fetch_tables_query = """
            SELECT DISTINCT tablename
            FROM public.v_generate_tbl_ddl where schemaname='mzb_aet_medods'  ;
        """
        
        # Execute query to fetch table names
        table_names = redshift_query_execute(
            fetch_tables_query,
            cfg=cfg,
            secrets_config=secrets_config,
            query_type="query"
        )
        table_names = [row[0] for row in table_names]  # Extract table names
        s3_client = boto3.client('s3')
        for table_name in table_names:
            # Query to fetch the DDL for the table
            fetch_ddl_query = f"""
                SELECT ddl
                FROM public.v_generate_tbl_ddl
                WHERE tablename = '{table_name}' and schemaname='mzb_aet_medods' 
                ORDER BY seq
            """
            
            # Execute query to fetch DDL
            ddl_result = redshift_query_execute(
                fetch_ddl_query,
                cfg=cfg,
                secrets_config=secrets_config,
                query_type="query"
            )
            
            # Combine DDL statements into a single string
            full_ddl = "\n".join([row[0] for row in ddl_result])
            
            # Create file name and S3 key
            file_name = f"{table_name}.sql"
            s3_key = f"temp/mzb_aet_medods/tables/{file_name}"
            
            # Upload to S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=full_ddl
            )
            print(f"Uploaded {file_name} to s3://{bucket_name}/{s3_key}")         


    @task(
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email,
        trigger_rule="none_failed",
    )
    def export_db_objects_views_ddl_uusr(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "export_db_objects_views_ddl_uusr",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException
        
        sql = """SELECT viewname,schemaname,definition
                FROM pg_views
                WHERE schemaname  IN ('mzb_aet_uusr')   ;   
                """

        print(sql)
        return_val = redshift_query_execute(
            sql,
            cfg=cfg,
            secrets_config=secrets_config,
            query_type="query",
        )

    # Initialize S3 client
        s3_client = boto3.client('s3')        
        for row in return_val:
            viewname = row[0]
            schema_name = row[1]
            definition = row[2]
            vw_definition = sqlparse.format(definition, reindent=True, keyword_case='lower')
            vw_definition = vw_definition.lower()
            # Create file content
            file_content = vw_definition
            
            # File name and S3 path
            file_name = f"{viewname}.sql"
            s3_key = f"temp/mzb_aet_uusr/views/{file_name}"
            
            # Upload file to S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=file_content
            )
            print(f"Uploaded {file_name} to s3://{bucket_name}/{s3_key}")

    
    @task(
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email,
        trigger_rule="none_failed",
    )
    def export_db_objects_proc_uusr(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "export_db_objects_proc_uusr",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException
        
        sql = """SELECT 
                    proname AS procedure_name,
                    nspname AS schema_name,
                    pg_get_functiondef(pg_proc.oid) AS definition
                FROM pg_proc
                JOIN pg_namespace ON pg_proc.pronamespace = pg_namespace.oid
                WHERE nspname NOT IN ('pg_catalog', 'information_schema','public') and nspname in ('mzb_aet_uusr')  ;"""
        print(sql)
        return_val = redshift_query_execute(
            sql,
            cfg=cfg,
            secrets_config=secrets_config,
            query_type="query",
        )

    # Initialize S3 client
        s3_client = boto3.client('s3')        
        for row in return_val:
            procedure_name = row[0]
            schema_name = row[1]
            definition = row[2]
            # Create file content
            file_content = definition
            
            # File name and S3 path
            file_name = f"{procedure_name}.sql"
            s3_key = f"temp/mzb_aet_uusr/stored_procedures/{file_name}"
            
            # Upload file to S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=file_content
            )
            print(f"Uploaded {file_name} to s3://{bucket_name}/{s3_key}")


    @task(
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email,
        trigger_rule="none_failed",
    )
    def export_db_objects_views_ddl_public(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "export_db_objects_views_ddl_public",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException
        
        sql = """SELECT viewname,schemaname,definition
                FROM pg_views
                WHERE schemaname  IN ('public') and   viewname ilike 'tbl%' ; 
                """

        print(sql)
        return_val = redshift_query_execute(
            sql,
            cfg=cfg,
            secrets_config=secrets_config,
            query_type="query",
        )

    # Initialize S3 client
        s3_client = boto3.client('s3')        
        for row in return_val:
            viewname = row[0]
            schema_name = row[1]
            definition = row[2]
            vw_definition = sqlparse.format(definition, reindent=True, keyword_case='lower')
            vw_definition = vw_definition.lower()
            # Create file content
            file_content = vw_definition
            
            # File name and S3 path
            file_name = f"{viewname}.sql"
            s3_key = f"temp/public/views/{file_name}"
            
            # Upload file to S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=file_content
            )
            print(f"Uploaded {file_name} to s3://{bucket_name}/{s3_key}")
      

    @task(
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email,
        trigger_rule="none_failed",
    )
    def export_db_objects_table_ddl_public(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "export_db_objects_table_ddl_public",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException
        # Query to fetch all distinct table names
        fetch_tables_query = """
            SELECT DISTINCT tablename
            FROM public.v_generate_tbl_ddl where schemaname='public' and tablename='tblmain'  ;
        """
        
        # Execute query to fetch table names
        table_names = redshift_query_execute(
            fetch_tables_query,
            cfg=cfg,
            secrets_config=secrets_config,
            query_type="query"
        )
        table_names = [row[0] for row in table_names]  # Extract table names
        s3_client = boto3.client('s3')
        for table_name in table_names:
            # Query to fetch the DDL for the table
            fetch_ddl_query = f"""
                SELECT ddl
                FROM public.v_generate_tbl_ddl
                WHERE tablename = '{table_name}' and schemaname='public' 
                ORDER BY seq
            """
            
            # Execute query to fetch DDL
            ddl_result = redshift_query_execute(
                fetch_ddl_query,
                cfg=cfg,
                secrets_config=secrets_config,
                query_type="query"
            )
            
            # Combine DDL statements into a single string
            full_ddl = "\n".join([row[0] for row in ddl_result])
            
            # Create file name and S3 key
            file_name = f"{table_name}.sql"
            s3_key = f"temp/public/tables/{file_name}"
            
            # Upload to S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=full_ddl
            )
            print(f"Uploaded {file_name} to s3://{bucket_name}/{s3_key}")  


    # ( export_db_objects_proc_ods() >>export_db_objects_table_ddl_ods()>> export_db_objects_function_ddl_ods()>>export_db_objects_views_ddl_ods() )
    # ( export_db_objects_proc_stg() >>export_db_objects_table_ddl_stg()>> export_db_objects_function_ddl_stg()>>export_db_objects_views_ddl_stg() )
    # ( export_db_objects_proc_dwh() >> export_db_objects_function_ddl_dwh()>>export_db_objects_views_ddl_dwh() )
    # ( export_db_objects_proc_core() >>export_db_objects_table_ddl_core() >>export_db_objects_views_ddl_core() )
    # ( export_db_objects_proc_load() >>export_db_objects_table_ddl_load()>>export_db_objects_views_ddl_load() )
    # ( export_db_objects_views_ddl_idms() )
    # ( export_db_objects_proc_mc() >>export_db_objects_table_ddl_mc()>>export_db_objects_views_ddl_mc() )
    # ( export_db_objects_proc_medstg() >>export_db_objects_table_ddl_medstg()>>export_db_objects_views_ddl_medstg() )
    # ( export_db_objects_proc_medods() >>export_db_objects_table_ddl_medods()>>export_db_objects_views_ddl_medods() )
    # ( export_db_objects_proc_uusr() >>export_db_objects_views_ddl_uusr() )
    # (export_db_objects_views_ddl_public() >> export_db_objects_table_ddl_public())
    
    ################################################################3
    # (export_db_objects_proc_stg() >> export_db_objects_proc_core() >> export_db_objects_proc_load() >> export_db_objects_proc_dwh())

    ( export_db_objects_table_ddl_ods()>>export_db_objects_views_ddl_ods() )
    ( export_db_objects_table_ddl_stg()>>export_db_objects_views_ddl_stg() )
    (  export_db_objects_views_ddl_dwh() )
    ( export_db_objects_table_ddl_core() >>export_db_objects_views_ddl_core() )
    ( export_db_objects_table_ddl_load()>>export_db_objects_views_ddl_load() )    
    

# Instantiate the DAG object
dag = aetna_export_db_objects()