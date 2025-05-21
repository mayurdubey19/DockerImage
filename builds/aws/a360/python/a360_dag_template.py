import json
import logging
import os
import psycopg2

# import helpers.common as cmn
from botocore.exceptions import ClientError
import boto3
import autopep8
import argparse

# from a360.python.a360_mz_task import *


region_name = "us-east-2"

cfg = {
    "dag_definition": '@dag(\n    dag_id="<dag_name>",\n    default_args=default_args,\n    description="<client> <dag_type> Data Pipeline",\n    schedule_interval=None,\n    max_active_runs=1,\n    params={"run_time_params": {"task_to_skip": []}},\n    access_control={\n        "<client>_admin": {"can_read", "can_edit", "can_delete"},\n        "<client>_ops": {"can_read", "can_edit", "can_delete"},\n        "<client>_a360user": {"can_read"}\n    },\n    catchup=False\n)',
    "default_args": 'default_args = {\n    "owner": "A360",\n    "start_date": airflow.utils.dates.days_ago(2),\n    "depends_on_past": False,\n    "current_path": os.path.abspath(os.path.dirname(__file__)),\n    "config_path": os.path.abspath(\n        os.path.join(os.path.dirname(__file__), "..", "config")\n    ),\n    "retries": 0,\n    "email_to": "DL-A360-Team@data-axle.com",\n    "email_subject": "A360 Daily Processing Job: ",\n    "email_on_failure": True,\n    "email_on_retry": False,\n    "email_body": "Executing Stage Job ",\n    "incoming_path": "s3://{}/incoming/",\n    "output_path": "s3://{}/process_fcvrl/parquet_glue/",\n    "script_path": "s3://{}/scripts/python/",\n    "format_file_path_glue": "s3://{}/config/pyspark_json/",\n    "redshift_config_paths": "s3://{}/config/common/glue_redshift_config.json",\n    "timeout_glue_job": 90,\n    "maxretries_glue_job": 0,\n    "common_settings_glue": "s3://{}/config/common/pysparkcommonsettings.json",\n    "additional_python_modules_glue": "h3==3.7.6,smart-open==6.3.0,openpyxl==3.1.2",\n    "glue_version": "4.0",\n    "extra_py_files_glue": "s3://{}/scripts/python/ValidateJsonFormatFile.py,s3://{}/scripts/python/FCVRL_UDFS.py,s3://{}/scripts/python/common.py",\n    "temp_path_athena": "s3://{}/athena_query_results/",\n    "mcd_input_table": "MZB_MCD_FILE_INCR",\n    "reject_path": "s3://{}/archive/outbound/rejects/",\n    "extracts_path": "s3://{}/archive/outbound/extracts/",\n    "ods_user": "A360_ODS",\n    "audit_schema": "A360_CORE",\n    "audit_table": "TMDM_OBJECT",\n}',
    "global_variables": '    current_env = Variable.get("var-env", "local")\n    if current_env == "Prod":\n        json_config_path = default_args["config_path"] + "/<dag_name>_prod.json"\n    else:\n        json_config_path = default_args["config_path"] + "/<dag_name>.json"\n\n    cfg = get_json_config(json_config_path)\n    region_name = cfg["aws_reg_rs"]\n    global_param_config = get_ssm_parameters("a360_<global_param_env>_global_param_config", region_name, True)\n    global_param_config = json.loads(global_param_config)\n    cfg.update(global_param_config)\n    sec_name = cfg["secret_name"]\n    secrets_config = get_secret(sec_name, region_name)\n    a360_account_id = get_aws_account_id()\n    bucket_name = cfg["bucket_name"].replace("{}", a360_account_id[-4:])\n    mdm_bucket_name = cfg["bucket_name"].replace("{}", a360_account_id[-4:]) + "-mdm"\n    dps_bucket_name = cfg["dps_bucket_name"].replace("{}", a360_account_id[-4:])\n    mft_bucket_name = cfg["mft_bucket"].replace("{}", a360_account_id[-4:])\n    redshift_iam_role = "arn:aws:iam::{}:role/{}".format(a360_account_id, cfg["redshift_iam_role"])\n    emr_iam_role = "arn:aws:iam::{}:role/{}".format(a360_account_id, cfg["emr_iam_role"])\n    athena_database = cfg["athena_database"]\n    mcd_emr_application_id = cfg["mcd_emr_application_id"]\n    default_args["iam_role_glue"] = cfg["iam_role_glue"]\n    default_args["connections_glue"] = cfg["connections_glue"]\n    v_execid, v_batchid = generate_exec_id(cfg["request_type"])\n    reject_path = default_args["reject_path"].replace("{}", bucket_name)\n    extracts_path = default_args["extracts_path"].replace("{}", bucket_name)\n    temp_path_athena = default_args["temp_path_athena"].replace("{}", bucket_name)\n    v_cur_date = datetime.now().strftime("%Y%m%d%H%M%S")\n    p_cur_user = "awsuser"\n    ctl_file_path = cfg["ctl_file_path"].format(v_batchid)\n    pg_secrets_config = get_secret(cfg["postgres_secret_name"], region_name)',
    "import_libraries": """import airflow.utils.dates   
from helpers.AwsExtractOperator import *
from helpers.redshift import *
from helpers.s3 import *
from helpers.common import *
from <client_folder>.python.a360_mz_task import *
from <client_folder>.python.a360_mz_task import run_stg_job as run_a360_stg_job
from airflow.decorators import dag, task
from helpers.send_email import *
import logging
import os
from airflow.models import Variable
from airflow.configuration import conf
from airflow.exceptions import AirflowException, AirflowSkipException
from datetime import datetime
import helpers.postgres as pg
import pandas as pd
import json
import requests""",
    "logging_tasks": {
        "task_body": '    def get_task_log(task_instance):\n        base_log_folder = conf.get("logging", "BASE_LOG_FOLDER")\n\n        # Adjust attempt number to reflect the actual try number\n        adjusted_attempt_number = task_instance.try_number\n\n        log_file_path = os.path.join(\n            base_log_folder,\n            f"dag_id={task_instance.dag_id}",\n            f"run_id={task_instance.run_id}",\n            f"task_id={task_instance.task_id}",\n            f"attempt={adjusted_attempt_number}.log",\n        )\n        print(log_file_path)\n\n        if os.path.exists(log_file_path):\n            with open(log_file_path, "r") as log_file:\n                return log_file.read()\n        return "Log file not found. Generated Log file path : {}".format(log_file_path)\n\n    def send_success_email(context):\n        task_instance = context["task_instance"]\n        log_content = get_task_log(task_instance)\n        task_status = task_instance.state\n        if task_status == "success":\n            v_msg1 = "Succeeded"\n            v_msg2 = "Success"\n        if task_status == "skipped":\n            v_msg1 = "Skipped"\n            v_msg2 = "Skipped"\n\n        # subject = f"Task {context[\'task_instance_key_str\']} Failed"\n        html_content = f"""\n          <h3>Task: {context[\'task_instance_key_str\']}</h3>\n          <p>{v_msg1} on: {datetime.now()}</p>\n          <h4>Log Content:</h4>\n          <pre>{log_content}</pre>\n          """\n\n        # Extract the task_id from the task_instance_key_str\n        task_instance_key_str = context["task_instance_key_str"]\n        task_id = task_instance_key_str.split("__")[1]\n        if current_env != "local":\n            send_email(\n                email_address=default_args["email_to"],\n                email_message=html_content,\n                email_subject="{} :{}{}".format(\n                    v_msg2, default_args["email_subject"], task_id\n                ),\n                email_business_users=[],\n                client="geha",\n            )\n\n    def send_failure_email(context):\n        task_instance = context["task_instance"]\n        log_content = get_task_log(task_instance)\n\n        # subject = f"Task {context[\'task_instance_key_str\']} Failed"\n        html_content = f"""\n          <h3>Task: {context[\'task_instance_key_str\']}</h3>\n          <p>Failed on: {datetime.now()}</p>\n          <h4>Log Content:</h4>\n          <pre>{log_content}</pre>\n          """\n\n        # Extract the task_id from the task_instance_key_str\n        task_instance_key_str = context["task_instance_key_str"]\n        task_id = task_instance_key_str.split("__")[1]\n\n        if current_env != "local":\n            send_email(\n                email_address=default_args["email_to"],\n                email_message=html_content,\n                email_subject="Failure :{}{}".format(\n                    default_args["email_subject"], task_id\n                ),\n                email_business_users=[],\n                client="geha",\n            )\n',
        "task_dependencies": [""],
    },
    "mcd_process": {
        "dps_extract_mcd_incr_demo": {
            "task_body": "    @task(\n        on_success_callback=send_success_email,\n        on_failure_callback=send_failure_email,\n        trigger_rule=\"none_failed\",\n    )\n    def dps_extract_mcd_incr_demo(**kwargs):\n        if kwargs[\"dag_run\"].conf.get(\"run_time_params\") and skip_task_check(\n            \"dps_extract_mcd_incr_demo\",\n            kwargs[\"dag_run\"].conf.get(\"run_time_params\").get(\"task_to_skip\"),\n        ):\n            # send_email()\n            print(\"SKip\")\n            raise AirflowSkipException\n        del_obj = del_all_object_from_s3_folder(bucket_name, \"mcd_input/\")\n        logging.info(\" Mcd Input Files cleaned up\")\n\n        ti = kwargs[\"ti\"]\n        v_execid = ti.xcom_pull(task_ids=\"source_file_stats\", key=\"exec_id\")\n        procedure_call = (\n            \"CALL a360_load.prc_dps_extract_mcd_incr_demo\"\n            + \"(\"\n            + str(v_execid)\n            + \" :: bigint)\"\n        )\n        print(procedure_call)\n        redshift_query_execute(\n            sql=procedure_call,\n            cfg=cfg,\n            secrets_config=secrets_config,\n            query_type=\"\",\n        )\n        logging.info(\" prc_dps_extract_mcd_incr_demo Completed\")\n        unload_sql = \"\"\"UNLOAD (' SELECT REPLACE(sequence_number,'';'','''') AS sequence_number,\n                        REPLACE(file_id,'';'','''') AS file_id,\n                        REPLACE(run_id,'';'','''') AS run_id,\n                        REPLACE(enterprise_id,'';'','''') AS enterprise_id,\n                        REPLACE(collection_id,'';'','''') AS collection_id,\n                        REPLACE(channel_id,'';'','''') AS channel_id,\n                        REPLACE(title,'';'','''') AS title,\n                        REPLACE(first_name,'';'','''') AS first_name,\n                        REPLACE(middle,'';'','''') AS middle,\n                        REPLACE(last_name,'';'','''') AS last_name,\n                        REPLACE(suffix,'';'','''') AS suffix,\n                        REPLACE(gender,'';'','''') AS gender,\n                        REPLACE(birth_year,'';'','''') AS birth_year,\n                        REPLACE(address_1,'';'','''') AS address_1,\n                        REPLACE(address_2,'';'','''') AS address_2,\n                        REPLACE(address_3,'';'','''') AS address_3,\n                        REPLACE(city,'';'','''') AS city,\n                        REPLACE(state,'';'','''') AS state,\n                        REPLACE(zip,'';'','''') AS zip,\n                        REPLACE(zip4,'';'','''') AS zip4,\n                        REPLACE(account_number,'';'','''') AS account_number,\n                        REPLACE(phone,'';'','''') AS phone,\n                        REPLACE(email,'';'','''') AS email,\n                        REPLACE(company,'';'','''') AS company,\n                        REPLACE(division,'';'','''') AS division,\n                        REPLACE(cust_since,'';'','''') AS cust_since,\n                        REPLACE(address_mailability,'';'','''') AS address_mailability,\n                        REPLACE(delimiter,'';'','''') AS delimiter,\n                        REPLACE(extra01,'';'','''') AS extra01,\n                        REPLACE(extra02,'';'','''') AS extra02,\n                        REPLACE(extra03,'';'','''') AS extra03,\n                        REPLACE(extra04,'';'','''') AS extra04,\n                        REPLACE(extra05,'';'','''') AS extra05,\n                        REPLACE(extra06,'';'','''') AS extra06,\n                        REPLACE(extra07,'';'','''') AS extra07  FROM a360_ods.{}')\n                        TO 's3://{}/mcd_input/mcd.dat'\n                        IAM_ROLE '{}'\n                        DELIMITER ';'\n                        allowoverwrite\n                        parallel on \"\"\".format(\n            default_args[\"mcd_input_table\"], bucket_name, redshift_iam_role\n        )\n        print(unload_sql)\n        redshift_query_execute(\n            sql=unload_sql,\n            cfg=cfg,\n            secrets_config=secrets_config,\n            query_type=\"\",\n        )\n        logging.info(\" Mcd files Unload Complete\")\n",
            "task_dependencies": [""],
        },
        "mcd_candidate": {
            "task_body": '    @task(\n        on_success_callback=send_success_email,\n        on_failure_callback=send_failure_email,\n        trigger_rule="none_failed",\n    )\n    def mcd_candidate(**kwargs):\n        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(\n            "mcd_candidate",\n            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),\n        ):\n            # send_email()\n            print("SKip")\n            raise AirflowSkipException\n\n        logging.info("Mcd candidate Process")\n        mcd_run_process(\n            "candidate.py", cfg, mdm_bucket_name, mcd_emr_application_id, emr_iam_role\n        )',
            "task_dependencies": [""],
        },
        "mcd_matchai_file_send": {
            "task_body": '    @task(\n        on_success_callback=send_success_email,\n        on_failure_callback=send_failure_email,\n        trigger_rule="none_failed",\n    )\n    def mcd_matchai_file_send(**kwargs):\n        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(\n                "mcd_matchai_file_send",\n                kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),\n        ):\n            # send_email()\n            print("Skip")\n            raise AirflowSkipException\n\n        task_instance = kwargs["ti"]\n\n        logging.info("Mcd MatchAI File Transfer Process")\n        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")\n        task_instance.xcom_push(key="timestamp", value=timestamp)\n        match_ai_src_prefix = f\'{cfg["match_ai_src_prefix"].rstrip("/")}/{timestamp}/\'\n\n        copy_cross_region_s3_datasync(\n            mdm_bucket_name,\n            cfg["mcd_consolidate_prefix"],\n            cfg["match_ai_bucket"],\n            match_ai_src_prefix,\n            cfg["aws_reg_rs"],\n            cfg["match_ai_reg"],\n            cross_account_cross_region=True,\n            exclude_file=True\n        )\n\n        s3_file = f\'{cfg["mcd_consolidate_prefix"].rstrip("/")}/_SUCCESS\'\n        copy_cross_region_s3_object(\n            mdm_bucket_name,\n            s3_file,\n            cfg["match_ai_bucket"],\n            match_ai_src_prefix,\n            cfg["aws_reg_rs"],\n            cfg["match_ai_reg"],\n        )\n',
            "task_dependencies": [""],
        },
        "mcd_part1_match_indicator": {
            "task_body": '    @task(\n        on_success_callback=send_success_email,\n        on_failure_callback=send_failure_email,\n        trigger_rule="none_failed",\n    )\n    def mcd_part1_match_indicator(**kwargs):\n        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(\n            "mcd_part1_match_indicator",\n            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),\n        ):\n            # send_email()\n            print("SKip")\n            raise AirflowSkipException\n        logging.info("Mcd On prem Match Indicator")\n        mcd_match_indicator(cfg, a360_account_id)\n',
            "task_dependencies": [""],
        },
        "mcd_matchai_file_receive": {
            "task_body": '    @task(\n        on_success_callback=send_success_email, on_failure_callback=send_failure_email\n    )\n    def mcd_matchai_file_receive(**kwargs):\n        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(\n                "mcd_matchai_file_receive",\n                kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),\n        ):\n            # send_email()\n            print("Skip")\n            raise AirflowSkipException\n\n        task_instance = kwargs["ti"]\n\n        logging.info("MatchAI A360 File Receive Process")\n\n        timestamp = task_instance.xcom_pull(task_ids="mcd_matchai_file_send", key="timestamp")\n        # timestamp = "202412314892"\n        folders = [\'ClusterOutput\', \'CorrelationOutput\']\n\n        for folder in folders:\n            match_ai_src_prefix = f\'{cfg["match_ai_outfile_prefix"].rstrip("/")}/{timestamp}/{folder}/\'\n            a360_prefix = f\'{cfg["a360_matchai_correlate_prefix"].rstrip("/")}/{folder}/\'\n\n            a360_path = f"s3://{mdm_bucket_name}/{a360_prefix}"\n            wr.s3.delete_objects(path=a360_path)\n\n            copy_cross_region_s3_datasync(\n                cfg["match_ai_bucket"],\n                match_ai_src_prefix,\n                mdm_bucket_name,\n                a360_prefix,\n                cfg["match_ai_reg"],\n                cfg["aws_reg_rs"],\n                specific_wildcard="part-*",\n            )\n',
            "task_dependencies": [""],
        },
        "mcd_workmatched": {
            "task_body": '    @task(\n        on_success_callback=send_success_email,\n        on_failure_callback=send_failure_email,\n        trigger_rule="none_failed",\n    )\n    def mcd_workmatched(**kwargs):\n        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(\n            "mcd_workmatched",\n            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),\n        ):\n            # send_email()\n            print("SKip")\n            raise AirflowSkipException\n\n        logging.info("Mcd Work Matched Process")\n        mcd_run_process(\n            "workmatched.py", cfg, mdm_bucket_name, mcd_emr_application_id, emr_iam_role\n        )\n',
            "task_dependencies": [""],
        },
        "mcd_part2": {
            "task_body": '    @task(\n        on_success_callback=send_success_email,\n        on_failure_callback=send_failure_email,\n        trigger_rule="none_failed",\n    )\n    def mcd_part2(**kwargs):\n        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(\n            "mcd_part2",\n            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),\n        ):\n            # send_email()\n            print("Skip")\n            raise AirflowSkipException\n\n        logging.info("Mcd Part 2 Unprocessed/Pool Update Process")\n        mcd_run_process(\n            "mcd_hist_update.py",\n            cfg,\n            mdm_bucket_name,\n            mcd_emr_application_id,\n            emr_iam_role,\n        )\n',
            "task_dependencies": [""],
        },
        "mcd_purge_mcdret_queue": {
            "task_body": '    @task(\n        on_success_callback=send_success_email,\n        on_failure_callback=send_failure_email,\n        trigger_rule="none_failed",\n    )\n    def mcd_purge_mcdret_queue(**kwargs):\n        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(\n            "mcd_purge_mcdret_queue",\n            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),\n        ):\n            # send_email()\n            print("Skip")\n            raise AirflowSkipException\n\n        logging.info("Purge mcd Return Queue")\n        purge_mcdret_queue(cfg, a360_account_id)\n',
            "task_dependencies": [""],
        },
    },
    "extracts": {
        "task_template": {
            "task_body": '    @task(\n        on_success_callback=send_success_email,\n        on_failure_callback=send_failure_email,\n        trigger_rule="none_failed",\n    )\n    def <task_name>(**kwargs):\n        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(\n            "<task_name>",\n            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),\n        ):\n            # send_email()\n            print("Skip")\n            raise AirflowSkipException\n        ti = kwargs["ti"]\n        v_batchid = ti.xcom_pull(task_ids="source_file_stats", key="batch_id")\n        v_cur_date = ti.xcom_pull(task_ids="source_file_stats", key="batch_timestamp")\n        task_name = "<task_name>"\n        run_extract_task(\n            cfg=cfg,\n            task_name=task_name,\n            default_args=default_args,\n            redshift_iam_role=redshift_iam_role,\n            current_dt=v_cur_date,\n            bucket_name=bucket_name,\n            secrets_config_redshift=secrets_config,\n            mft_bucket_name=mft_bucket_name,\n            batch_id=v_batchid,\n        )\n',
            "task_dependencies": [""],
        }
    },
    "control_file_task": {
        "task_template": {
            "task_body": '    @task(\n        on_success_callback=send_success_email,\n        on_failure_callback=send_failure_email,\n        trigger_rule="none_failed",\n    )\n    def generate_ctl_file(**kwargs):\n        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(\n            "generate_ctl_file",\n            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),\n        ):\n            # send_email()\n            print("SKip")\n            raise AirflowSkipException\n\n        ti = kwargs["ti"]\n        v_batchid = ti.xcom_pull(task_ids="source_file_stats", key="batch_id")\n        v_cur_date = ti.xcom_pull(task_ids="source_file_stats", key="batch_timestamp")\n\n        file_ts = v_cur_date\n        sql = f"""\n                   SELECT file_name, file_count FROM geha_core.geha_extract_metadata where batch_id = \'{v_batchid}\'\n                """\n        print(sql)\n        return_val = redshift_query_execute(\n            sql, cfg, secrets_config, "query"\n        )\n\n        fname_list = ["File_Name|File_Count\\n"]\n\n        for row in return_val:\n            file_name = row[0]\n            file_name = str(file_name.split("/")[-1]).strip()\n            record_count = row[1]\n            if record_count > 1:\n                file_data = file_name + "|" + str(record_count) + "\\n"\n                fname_list.append(file_data)\n        trigger_data = "".join(fname_list)\n        print(trigger_data)\n        s3_put_object(\n            bucket_name=bucket_name,\n            key=f"{ctl_file_path}/GEHA_EXTRACTS_CTL_{file_ts}.txt",\n            body=trigger_data.encode(),\n        )\n        logging.info("generate_ctl_file job completed")\n\n        send_email(\n            client="GEHA",\n            email_address=default_args["email_to"],\n            # to be configured via json -- email list\n            email_business_users=default_args["email_to"],\n            email_subject=f"GEHA extracts control file",\n            email_message=f"Please review attached report control file.",\n            report_list=[\n                f"s3://{bucket_name}/{ctl_file_path}/GEHA_EXTRACTS_CTL_{file_ts}.txt"\n            ],\n        )',
            "task_dependencies": [""],
        }
    },
    "ingestion": {
        "task_template": {
            "task_body": '    @task(\n        on_success_callback=send_success_email,\n        on_failure_callback=send_failure_email,\n        trigger_rule="none_failed",\n    )\n    def <task_name>(**kwargs):\n        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(\n            "<task_name>",\n            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),\n        ):\n            # send_email()\n            print("SKip")\n            raise AirflowSkipException\n        # for src_cd, source_data in cfg["list_config"].items():\n        ti = kwargs["ti"]\n        v_batchid = ti.xcom_pull(task_ids="source_file_stats", key="batch_id")\n        pulled_list = (\n            []\n            if ti.xcom_pull(task_ids="source_file_stats", key="process_list_key")\n            is None\n            else ti.xcom_pull(task_ids="source_file_stats", key="process_list_key")\n        )\n        print("Pulled List for XCOM : {}".format(pulled_list))\n        if pulled_list == []:\n            pulled_list = get_processing_list(cfg, secrets_config, v_batchid)\n            print("Pulled List for Redshift for current Batch : {}".format(pulled_list))\n\n        if "<source_cd>" in pulled_list:\n            file_present_ind = True\n        else:\n            file_present_ind = False\n        src_cd = "<source_cd>"\n        run_a360_stg_job(\n            cfg=cfg,\n            source_cd=src_cd,\n            bucket_name=bucket_name,\n            secrets_config=secrets_config,\n            batch_id=v_batchid,\n            default_args=default_args,\n            file_present_ind=file_present_ind,\n            redshift_iam_role=redshift_iam_role,\n        )',
            "task_dependencies": [""],
        }
    },
    "pre_mcd": {
        "dps_mcd_hist_bkp": {
            "task_body": '    @task(\n        on_success_callback=send_success_email,\n        on_failure_callback=send_failure_email,\n        trigger_rule="none_failed",\n    )\n    def dps_mcd_hist_bkp(**kwargs):\n        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(\n            "dps_mcd_hist_bkp",\n            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),\n        ):\n            # send_email()\n            print("SKip")\n            raise AirflowSkipException\n        ##### Daily  Backups ######\n        step_set = "mcd_hist_bkup_<freq_val>"\n        procedure_call = "CALL a360_ods.prc_dps_mcd_hist_bkup(\'{}\',{})".format(\n            step_set, v_execid\n        )\n        print(procedure_call)\n        redshift_query_execute(\n            sql=procedure_call,\n            cfg=cfg,\n            secrets_config=secrets_config,\n            query_type="",\n        )\n        # Truncate DPInput Athena Table by deleting files from Table location\n        s3_key = f"{cfg[\'s3_key_athena_glue\']}{cfg[\'dp_config\'][\'<freq_val>\'][\'athena_partitioned_table\']}/"\n        del_all_object_from_s3_folder(bucket=bucket_name, object_name=s3_key)\n        \n        \n        tuple_data = redshift_query_execute(\n            sql="""select param_value  from a360_core.tmdm_param_config where context = \'ARCHIVE_FILES_ON_HOLD\'""",\n            cfg=cfg,\n            secrets_config=secrets_config,\n            query_type="query",\n        )\n        file_prefix_list = [x for lst in tuple_data for x in lst]\n        logging.info(file_prefix_list)\n        for file_pattern_iter in file_prefix_list:\n            complete_path_iter = "s3://{}/incoming/{}".format(\n                bucket_name,\n                file_pattern_iter,\n            )\n            file_archive_list = get_s3Filelist_using_wr(complete_path_iter)\n            print(file_archive_list)\n            for file in file_archive_list:\n                key = "incoming/{}".format(file.split("/")[-1])\n                move_and_delete_s3_file(\n                    bucket_name,\n                    key,\n                    bucket_name,\n                    "incoming_archive/{}".format(file.split("/")[-1]),\n                )',
            "task_dependencies": [""],
        },
        "business_to_internal_s3_file_transfer": {
            "task_body": '    @task(\n        on_success_callback=send_success_email,\n        on_failure_callback=send_failure_email,\n        trigger_rule="none_failed",\n    )\n    def business_to_internal_s3_file_transfer(**kwargs):\n        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(\n                "business_to_internal_s3_file_transfer",\n                kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),\n        ):\n            # send_email()\n            print("SKip")\n            raise AirflowSkipException\n\n        print("pg_secrets_config: ",str(pg_secrets_config))\n        logging.info("business to internal s3 file transfer Process")\n        file_path = json_config_path\n        data_ingestion_names = get_name_field_from_json_metadata(file_path)\n        business_to_internal_ingestion_integration(\n            cfg,secrets_config,pg_secrets_config,bucket_name,data_ingestion_names\n        )',
            "task_dependencies": [""],
        },
        "prepare_incoming_file_from_db": {
            "task_body": '    @task(\n        on_success_callback=send_success_email,\n        on_failure_callback=send_failure_email,\n        trigger_rule="none_failed",\n    )\n    def prepare_incoming_file_from_db(**kwargs):\n        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(\n                "prepare_incoming_file_from_db",\n                kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),\n        ):\n            # send_email()\n            print("SKip")\n            raise AirflowSkipException\n\n        query_type = "query"\n        file_path = json_config_path\n        data_ingestion_names = get_name_field_from_json_metadata(file_path)\n        data_ingestion_names_str = f"({\', \'.join([repr(ft) for ft in data_ingestion_names])})"\n        sql = (f"select * from mz360.data_ingestion where name IN {data_ingestion_names_str} and is_enabled = \'True\' and "\n               f"data_source_type = \'Redshift\'")\n        pg_result, column_names = pg.pg_query_execute(\n            sql, cfg, pg_secrets_config, query_type, col_names="Y")\n\n        print(pg_result)\n        if not pg_result:\n            print("No data sets found for redshift in the pg_result.")\n        else:\n            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")\n\n            for data in pg_result:\n                filename = f"DAX_{data[column_names.index(\'feed\')]}_{timestamp}"\n                src_tab = data[column_names.index(\'data_source_object\')]\n                incremental_field = data[column_names.index(\'incremental_field\')]\n                feed = data[column_names.index(\'feed\')]\n                datashare_name = data[column_names.index(\'datashare_name\')]\n\n                unload_redshift_datashare_data(\n                    bucket_name, filename, src_tab, incremental_field, feed, cfg, redshift_iam_role, secrets_config,datashare_name)\n',
            "task_dependencies": [""],
        },
        "source_file_stats": {
            "task_body": '    @task(\n        on_success_callback=send_success_email,\n        on_failure_callback=send_failure_email,\n        trigger_rule="none_failed",\n    )\n    def source_file_stats(**kwargs):\n        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(\n            "source_file_stats",\n            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),\n        ):\n            # send_email()\n            print("SKip")\n            raise AirflowSkipException\n        print(default_args["current_path"])\n        print(redshift_iam_role)\n        print(athena_database)\n        # Create an empty DataFrame\n        df = pd.DataFrame(columns=["file_name", "batch_id", "file_record_count"])\n        # Create an empty List for source cd to be processed today\n        source_cd_list = []\n        file_present_list = []\n        file_src_cd_mapping = {}\n        env = "dev"\n        if env == "dev":\n            print(default_args["config_path"])\n            with open(json_config_path, "r") as file:\n                data = file.read()\n            list_info = json.loads(data)\n            list_info_lookup = list_info["list_config"]\n            for source_cd, source_det in list_info_lookup.items():\n                # print(source_cd,source_det)\n                # print(\n                #     " File pattern {} , header {} ,fullfile {}".format(\n                #         source_det["file_pattern"],\n                #         source_det["header"],\n                #         source_det["fullfile"],\n                #     )\n                # )\n                folder_prefix = source_det["key_prefix"]\n                file_pattern = source_det["file_pattern"]\n                if len(file_pattern.split(",")) > 1:\n                    for file_pattern_iter in file_pattern.split(","):\n                        complete_path_iter = "s3://{}/{}/{}".format(\n                            bucket_name,\n                            folder_prefix,\n                            file_pattern_iter,\n                        )\n                        filelist = get_s3Filelist_using_wr(complete_path_iter)\n                        if filelist:\n                            file_present_list.extend(filelist)\n                            if source_cd not in source_cd_list:\n                                source_cd_list.append(source_cd)\n                            # mapping each file with its source_cd\n                            file_src_cd_mapping.update(\n                                {file: source_cd for file in filelist}\n                            )\n                else:\n                    complete_path_iter = "s3://{}/{}/{}".format(\n                        bucket_name, folder_prefix, file_pattern\n                    )\n                    file_dupe_list = get_s3Filelist_using_wr(complete_path_iter)\n                    # Handle for individual files if full file is True then keep the latest and archive file duplicates\n                    if file_dupe_list:\n                        if len(file_dupe_list) > 1 and source_det["fullfile"]:\n                            file_present_list.extend([file_dupe_list[0]])\n                            if source_cd not in source_cd_list:\n                                source_cd_list.append(source_cd)\n                            # mapping each file with its source_cd\n                            file_src_cd_mapping[file_dupe_list[0]] = source_cd\n                            ### Files to be archived into the current batchid folder\n                            logging.info(\n                                "Full Files to be Deleted : {}".format(\n                                    file_dupe_list[1:]\n                                )\n                            )\n                            for file in file_dupe_list[1:]:\n                                bucket, key = split_s3_path(file)\n                                move_and_delete_s3_file(\n                                    bucket,\n                                    key,\n                                    bucket,\n                                    "archive/{}/incoming/{}".format(\n                                        v_batchid, key.split("/")[-1]\n                                    ),\n                                )\n                        else:\n                            file_present_list.extend(file_dupe_list)\n                            if source_cd not in source_cd_list:\n                                source_cd_list.append(source_cd)\n                            # mapping each file with its source_cd\n                            file_src_cd_mapping.update(\n                                {file: source_cd for file in file_dupe_list}\n                            )\n            logging.info(\n                "file name with src cd mapping : {}".format(file_src_cd_mapping)\n            )\n            for file in file_present_list:\n                bucket, key = split_s3_path(file)\n                print("file: ", file)\n                print("key: ", key)\n                # Get source_cd from filename to read header flag from config\n                file_name = key.split("/")[1]\n                source_cd = file_src_cd_mapping[file]\n                print("file name: ", file_name)\n                print("source code: ", source_cd)\n                if cfg["list_config"][source_cd]["header"]:\n                    count = -1\n                else:\n                    count = 0\n                # Avoiding S3select for full files , running glue job instead\n                if cfg["list_config"][source_cd]["fullfile"]:\n                    response = None\n                else:\n                    response = read_s3_obj_content(bucket, key)\n                if response is not None:\n                    try:\n                        for event in response["Payload"]:\n                            if "Records" in event:\n                                count = count + int(\n                                    event["Records"]["Payload"].decode("utf-8")\n                                )\n                                print("File Name {} , File Count {}".format(key, count))\n                                file_iter_df = pd.DataFrame(\n                                    {\n                                        "file_name": [key.split("/")[1]],\n                                        "batch_id": v_batchid,\n                                        "file_record_count": [count],\n                                    }\n                                )\n                                # Concatenate the DataFrames\n                                df = pd.concat([df, file_iter_df], ignore_index=True)\n                    except Exception as e:\n                        print(\n                            "S3 select error occurred for file {} with error {}:".format(\n                                key.split("/")[1], e\n                            )\n                        )\n                        file_iter_df = pd.DataFrame(\n                            {\n                                "file_name": [key.split("/")[1]],\n                                "batch_id": v_batchid,\n                                "file_record_count": [count],\n                            }\n                        )\n                        # Concatenate the DataFrames\n                        df = pd.concat([df, file_iter_df], ignore_index=True)\n                else:\n                    file_iter_df = pd.DataFrame(\n                        {\n                            "file_name": [key.split("/")[1]],\n                            "batch_id": v_batchid,\n                            "file_record_count": [count],\n                        }\n                    )\n                    # Concatenate the DataFrames\n                    df = pd.concat([df, file_iter_df], ignore_index=True)\n            logging.info(df)\n            # Source code Lists processed today #\n            logging.info(source_cd_list)\n            kwargs["ti"].xcom_push(key="process_list_key", value=source_cd_list)\n            kwargs["ti"].xcom_push(key="batch_id", value=v_batchid)\n            kwargs["ti"].xcom_push(key="batch_timestamp", value=v_cur_date)\n            kwargs["ti"].xcom_push(key="exec_id", value=v_execid)\n            engine = get_redshift_sqlalchemy_conn(\n                "us-east-2", cfg["redshift_cluster"], cfg["redshift_db"], secrets_config\n            )\n            # in case of empty df it is overwriting stg_source_file_stats_<category_init> making file_record_count col to varchar\n            if not df.empty:\n                df.to_sql(\n                    "stg_source_file_stats_<category_init>",\n                    con=engine,\n                    if_exists="replace",\n                    index=False,\n                    schema="a360_stg",\n                )\n            # Get file_record_count for files with S3 select failure, capture in tmp table mzb_aet_stg.stg_source_file_stats_<category_init>_tmp\n            if file_present_list == []:\n                bucket = bucket_name\n            glue_job_keyword =  "_".join(kwargs["dag"].dag_id.split(\'_\')[:2]) \n            get_file_record_count(\n                batch_id=v_batchid,\n                incoming_path=folder_prefix,\n                redshift_config_path=default_args["redshift_config_paths"].replace(\n                    "{}", bucket_name\n                ),\n                s3_bucket=bucket,\n                table_source_file_stats="stg_source_file_stats_<category_init>",\n                 glue_job_name=f"{glue_job_keyword}_source_file_stats",\n                script_path=default_args["script_path"].replace("{}", bucket_name),\n                iam_glue_role=cfg["iam_role_glue"],\n                connections_glue=cfg["connections_glue"],\n            )\n            ### Call Redshift procedure to handle checks for duplicate files received that were processed earlier ###\n            procedure_call = "CALL a360_core.prc_check_dup_files_<category_init> ({},\'{}\')".format(\n                v_batchid, v_cur_date\n            )\n            print(procedure_call)\n            redshift_query_execute(\n                sql=procedure_call,\n                cfg=cfg,\n                secrets_config=secrets_config,\n                query_type="",\n            )\n            ### Archive the output data from redshift stored processed procedure from above step to archive the processed duplicate files ###\n            tuple_data = redshift_query_execute(\n                sql="""select file_name  from a360_core.tmdm_source_files where err_msg is not null and batch_id={}""".format(\n                    v_batchid\n                ),\n                cfg=cfg,\n                secrets_config=secrets_config,\n                query_type="query",\n            )\n            file_archive_list = [x for lst in tuple_data for x in lst]\n            logging.info(file_archive_list)\n            for file in file_archive_list:\n                key = "incoming/{}".format(file)\n                move_and_delete_s3_file(\n                    bucket_name,\n                    key,\n                    bucket_name,\n                    "archive/{}/incoming/{}".format(v_batchid, file),\n                )\n',
            "task_dependencies": [""],
        },
    },
    "dp_job": {
        "dps_daily_send_to_dp": {
            "task_body": '    @task(\n        on_success_callback=send_success_email,\n        on_failure_callback=send_failure_email,\n        trigger_rule="none_failed",\n    )\n    def dps_daily_send_to_dp(**kwargs):\n        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(\n            "dps_daily_send_to_dp",\n            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),\n        ):\n            # send_email()\n            print("SKip")\n            raise AirflowSkipException\n        run_dp_job(\n        task_name=\'send_to_dp\',\n        cfg=cfg,\n        bucket_name=bucket_name,\n        request_id=\'<freq_val>\',\n        secrets_config=secrets_config,\n        batch_id=v_batchid,\n        athena_database=athena_database,\n        dps_bucket_name=dps_bucket_name,\n        redshift_iam_role=redshift_iam_role,\n        email_to=default_args["email_to"],\n        client=\'FirstParty\'\n    )',
            "task_dependencies": [""],
        },
        "dps_daily_load_dpout": {
            "task_body": '    @task(\n        on_success_callback=send_success_email,\n        on_failure_callback=send_failure_email,\n        trigger_rule="none_failed",\n    )\n    def dps_daily_load_dpout(**kwargs):\n        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(\n            "dps_daily_load_dpout",\n            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),\n        ):\n            # send_email()\n            print("SKip")\n            raise AirflowSkipException\n        run_dp_job(\n        task_name=\'load_dpout\',\n        cfg=cfg,\n        bucket_name=bucket_name,\n        request_id=\'<freq_val>\',\n        secrets_config=secrets_config,\n        batch_id=v_batchid,\n        athena_database=athena_database,\n        dps_bucket_name=dps_bucket_name,\n        redshift_iam_role=redshift_iam_role,\n        email_to=default_args["email_to"],\n        client=\'FirstParty\'\n    )',
            "task_dependencies": [""],
        },
    },
    "ods": {
        "load_transactional_tables": {
            "task_body": '    @task(\n        on_success_callback=send_success_email,\n        on_failure_callback=send_failure_email,\n        trigger_rule="none_failed",\n    )\n    def load_transactional_tables(**kwargs):\n        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(\n                "load_transactional_tables",\n                kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),\n        ):\n            print("Skip")\n            raise AirflowSkipException\n        ti = kwargs["ti"]\n        v_execid = ti.xcom_pull(task_ids="source_file_stats", key="exec_id")\n        v_cur_date = ti.xcom_pull(\n            task_ids="source_file_stats", key="batch_timestamp")\n        pulled_list = (\n            []\n            if ti.xcom_pull(task_ids="source_file_stats", key="process_list_key")\n               is None\n            else ti.xcom_pull(task_ids="source_file_stats", key="process_list_key")\n        )\n        if pulled_list:\n            sql_src_cd_filter = f"({\' ,\'.join(repr(data) for data in pulled_list)})"\n            sql_src_cd_filter = sql_src_cd_filter.replace("\'", "\'\'")\n        else:\n            sql_src_cd_filter = "1=1"\n        if any(category in kwargs["dag"].dag_id.lower() for category in ["campaignandlookup"]):\n            proc_param = f",\'{redshift_iam_role}\' :: character varying)"\n        else:\n            proc_param = " ) "\n        logging.info("Load transaction files to ODS-RedShift")\n        procedure_call = f"""CALL a360_load.prc_a360_ods_<wrapper_type>_wrapper(\n        {v_execid},\n        \'ODS_TXN_Load\'::character varying,\n        \'{v_cur_date}\'::character varying,\n        \'{default_args["ods_user"]}\'::character varying,\n        \'{sql_src_cd_filter}\'::character varying \n         {proc_param} """\n        print(procedure_call)\n\n        redshift_query_execute(\n            sql=procedure_call,\n            cfg=cfg,\n            secrets_config=secrets_config,\n            query_type="",\n        )',
            "task_dependencies": [""],
        }
    },
    "load_individual_aggs": {
        "load_individual_aggs": {
            "task_body": '    @task(\n        on_success_callback=send_success_email,\n        on_failure_callback=send_failure_email,\n        trigger_rule="none_failed",\n    )\n    def load_individual_aggs(**kwargs):\n        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(\n            "load_individual_dim",\n            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),\n        ):\n            print("Skip")\n            raise AirflowSkipException\n\n        ti = kwargs["ti"]\n        v_execid = ti.xcom_pull(task_ids="source_file_stats", key="exec_id")\n        v_cur_date = ti.xcom_pull(task_ids="source_file_stats", key="batch_timestamp")\n\n        logging.info("Invoking Procedure to load Individual Dim")\n\n        procedure_call = f"""CALL a360_load.prc_a360_load_agg_indiv(\n            {v_execid},\n            \'INDIV_AGGS_LOAD\'::character varying,\n            \'{v_cur_date}\'::character varying,\n            \'{default_args["ods_user"]}\'::character varying\n        );"""\n\n        print(procedure_call)\n\n        redshift_query_execute(\n            sql=procedure_call,\n            cfg=cfg,\n            secrets_config=secrets_config,\n            query_type="",\n        )\n',
            "task_dependencies": [""],
        }
    },
    "priority_processing_pre_ingestion": {
        "business_to_internal_s3_file_transfer": {
            "task_body": '    @task(\n        on_success_callback=send_success_email,\n        on_failure_callback=send_failure_email,\n        trigger_rule="none_failed",\n    )\n    def business_to_internal_s3_file_transfer(**kwargs):\n        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(\n                "business_to_internal_s3_file_transfer",\n                kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),\n        ):\n            # send_email()\n            print("SKip")\n            raise AirflowSkipException\n\n        print("pg_secrets_config: ",str(pg_secrets_config))\n        logging.info("business to internal s3 file transfer Process")\n        feed_types = <feed_types>\n        business_to_internal_ingestion_integration(\n            cfg,secrets_config,pg_secrets_config,bucket_name,feed_types\n        )',
            "task_dependencies": [""],
        },
        "prepare_incoming_file_from_db": {
            "task_body": '    @task(\n        on_success_callback=send_success_email,\n        on_failure_callback=send_failure_email,\n        trigger_rule="none_failed",\n    )\n    def prepare_incoming_file_from_db(**kwargs):\n        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(\n                "prepare_incoming_file_from_db",\n                kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),\n        ):\n            # send_email()\n            print("SKip")\n            raise AirflowSkipException\n\n        query_type = "query"\n        file_path = json_config_path\n        data_ingestion_names = get_name_field_from_json_metadata(file_path)\n        data_ingestion_names_str = f"({\', \'.join([repr(ft) for ft in data_ingestion_names])})"\n        sql = (f"select * from mz360.data_ingestion where name IN {data_ingestion_names_str} and is_enabled = \'True\' and "\n               f"data_source_type = \'Redshift\'")\n        pg_result, column_names = pg.pg_query_execute(\n            sql, cfg, pg_secrets_config, query_type, col_names="Y")\n\n        print(pg_result)\n        if not pg_result:\n            print("No data sets found for redshift in the pg_result.")\n        else:\n            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")\n\n            for data in pg_result:\n                filename = f"DAX_{data[column_names.index(\'feed\')]}_{timestamp}"\n                src_tab = data[column_names.index(\'data_source_object\')]\n                incremental_field = data[column_names.index(\'incremental_field\')]\n                feed = data[column_names.index(\'feed\')]\n                datashare_name = data[column_names.index(\'datashare_name\')]\n\n                unload_redshift_datashare_data(\n                    bucket_name, filename, src_tab, incremental_field, feed, cfg, redshift_iam_role, secrets_config,datashare_name)\n',
            "task_dependencies": [""],
        },
    },
    "priority_process_ingestion": {
        "task_template": {
            "task_body": '    @task(\n    on_success_callback=send_success_email,\n    on_failure_callback=send_failure_email,\n    trigger_rule="none_failed",\n    )\n    def <task_name>(**kwargs):\n        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(\n            "<task_name>",\n        kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),\n        ):\n            # send_email()\n            print("SKip")\n            raise AirflowSkipException\n        src_cd = "<source_cd>"\n        file_present_ind = True\n        run_a360_stg_job(\n        cfg=cfg,\n        source_cd=src_cd,\n        bucket_name=bucket_name,\n        secrets_config=secrets_config,\n        batch_id=v_batchid,\n        default_args=default_args,\n        file_present_ind=file_present_ind,\n        redshift_iam_role=redshift_iam_role,\n    )',
            "task_dependencies": [""],
        }
    },
    "priority_process_prematch_data": {
        "process_prematch_data": {
            "task_body": '    @task(\n        on_success_callback=send_success_email,\n        on_failure_callback=send_failure_email,\n        trigger_rule="none_failed",\n    )\n    def process_prematch_data(**kwargs):\n        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(\n            "process_prematch_data",\n            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),\n        ):\n            # send_email()\n            print("Skip")\n            raise AirflowSkipException\n\n        p_cur_date = v_cur_date\n        gv_batchexecid = v_execid\n        gv_checkpoint = "prc_a360_proc_caller"\n        p_process_name = "PRIORITY_PROCESS"\n        procedure_call = f"""CALL a360_load.prc_a360_proc_caller({gv_batchexecid},\'{gv_checkpoint}\',\'{p_cur_date}\',\'{p_cur_user}\',\'{p_process_name}\');"""\n        print(procedure_call)\n        redshift_query_execute(\n            sql=procedure_call,\n            cfg=cfg,\n            secrets_config=secrets_config,\n            query_type="",\n        )\n',
            "task_dependencies": [""],
        }
    },
    "post_mcd": {
        "post_mcd_process": {
            "task_body": '    @task(\n        on_success_callback=send_success_email,\n        on_failure_callback=send_failure_email,\n        trigger_rule="none_failed",\n    )\n    def post_mcd_process(**kwargs):\n        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(\n            "post_mcd_process",\n            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),\n        ):\n            # send_email()\n            print("Skip")\n            raise AirflowSkipException\n        ti = kwargs["ti"]\n        v_execid = ti.xcom_pull(task_ids="source_file_stats", key="exec_id")\n        v_cur_date = ti.xcom_pull(task_ids="source_file_stats", key="batch_timestamp")\n\n        procedure_call = "CALL a360_load.prc_a360_post_mcd({}, \'{}\')".format(\n            v_execid, v_cur_date\n        )\n        print(procedure_call)\n        redshift_query_execute(\n            sql=procedure_call,\n            cfg=cfg,\n            secrets_config=secrets_config,\n            query_type="",\n        )\n',
            "task_dependencies": [""],
        }
    },
    "archive_incoming_files": {
        "archive_incoming_files": {
            "task_body": '    @task(\n        on_success_callback=send_success_email,\n        on_failure_callback=send_failure_email,\n        trigger_rule="none_failed",\n    )\n    def archive_incoming_files(**kwargs):\n        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(\n            "archive_incoming_files",\n            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),\n        ):\n            # send_email()\n            print("SKip")\n            raise AirflowSkipException\n        ti = kwargs["ti"]\n        v_batchid = ti.xcom_pull(task_ids="source_file_stats", key="batch_id")\n        list_config = cfg["list_config"]\n        tuple_data = redshift_query_execute(\n            sql="""select file_name  from a360_core.tmdm_source_files where err_msg is  null  and batch_id={}""".format(\n                v_batchid\n            ),\n            cfg=cfg,\n            secrets_config=secrets_config,\n            query_type="query",\n        )\n        file_archive_list = [x for lst in tuple_data for x in lst]\n        logging.info(file_archive_list)\n        for file in file_archive_list:\n            source_code = get_source_cd(file)\n            feed_type = "<feed_type>"\n            move_and_delete_s3_file(\n                bucket_name,\n                "incoming/{}".format(file),\n                bucket_name,\n                "archive/incoming/{}/{}".format(feed_type, file),\n            )\n',
            "task_dependencies": [""],
        }
    },
    "job_load_crc_prev": {
        "job_load_crc_prev": {
            "task_body": '    @task(\n        on_success_callback=send_success_email,\n        on_failure_callback=send_failure_email,\n        trigger_rule="none_failed",\n    )\n    def job_load_crc_prev(**kwargs):\n        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(\n            "job_load_crc_prev",\n            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),\n        ):\n            # send_email()\n            print("SKip")\n            raise AirflowSkipException\n        ti = kwargs["ti"]\n        pulled_list = ti.xcom_pull(task_ids="source_file_stats", key="process_list_key")\n\n        print(pulled_list)\n        load_crc_prev_process(\n            pulled_list,\n            cfg,\n            athena_database,\n            temp_path_athena,\n            bucket_name,\n            secrets_config,\n        )\n',
            "task_dependencies": [""],
        }
    },
    "prc_extract_reject_files": {
        "prc_extract_reject_files": {
            "task_body": '    @task(\n        on_success_callback=send_success_email,\n        on_failure_callback=send_failure_email,\n        trigger_rule="none_failed",\n    )\n    def prc_extract_reject_files(**kwargs):\n        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(\n            "prc_extract_reject_files",\n            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),\n        ):\n            # send_email()\n            logging.info("SKip")\n            raise AirflowSkipException\n        logging.info("Starting Reject file generation and transfer process")\n        # Send email - Job started\n        ti = kwargs["ti"]\n        v_batchid = ti.xcom_pull(task_ids="source_file_stats", key="batch_id")\n        pulled_list = ti.xcom_pull(task_ids="source_file_stats", key="process_list_key")\n        \n        # If both pulls return None, initialize as an empty list\n        if pulled_list is None:\n            pulled_list = []\n        \n        logging.info("Pulled List for XCOM : {}".format(pulled_list))\n        \n        if len(pulled_list) > 0:\n            print("Pulled List for Redshift for current Batch : {}".format(pulled_list))\n            extract_reject_files(\n                cfg=cfg,\n                bucket_name=bucket_name,\n                mft_bucket_name=mft_bucket_name,\n                secrets_config=secrets_config,\n                athena_database=athena_database,\n                temp_path_athena=temp_path_athena,\n                pulled_list=pulled_list,\n                reject_path=reject_path,\n                batch_id=str(v_batchid),\n                redshift_iam_role=redshift_iam_role,\n            )\n            logging.info("Completed Reject file generation and transfer process")\n        else:\n            logging.info("No Reject extracts for feeds in this cycle")\n',
            "task_dependencies": [""],
        }
    },
}


def get_secret(secret_name, region_name):
    region_name = "us-east-2"
    secret_client = boto3.client("secretsmanager", region_name)
    try:
        response = secret_client.get_secret_value(SecretId=secret_name)
        secret = json.loads(response["SecretString"])
        print("secrets:", secret)
        return secret
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            raise ValueError("Secret not found: {}".format(secret_name))


def create_pg_connection(secrets):
    print(secrets)
    dbname = secrets["database"]
    user = secrets["username"]
    password = secrets["password"]
    host = secrets["host"]
    port = secrets["port"]

    try:
        # Establish a connection to the PostgreSQL database
        conn = psycopg2.connect(
            dbname=dbname, user=user, password=password, host=host, port=port
        )
        return conn

    except psycopg2.Error as e:
        print("Error connecting to PostgreSQL database:", e)


def pg_query_execute(sql, cfg, secrets_config, query_type, **kwargs):
    conn = create_pg_connection(secrets_config)
    cursor = conn.cursor()
    try:
        cursor.execute(f"{sql}")
        if query_type == "query":
            if "col_names" in kwargs:
                result = cursor.fetchall()
                column_names = [col[0] for col in cursor.description]
                cursor.close()
                conn.close()
                return result, column_names
            else:
                result = cursor.fetchall()

        else:
            result = "Success"
        cursor.close()
        conn.close()
    except psycopg2.Error as e:
        print("Error connecting to PostgreSQL database:", e)
    return result


def get_json_config(json_path):
    with open(f"{json_path}", "r") as file:
        data = file.read()
        json_data = json.loads(data)
    return json_data


def dag_generation():
    logging.info("Start here")
    json_task_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "config")
    )
    metadata_config_path = json_task_path + "\\fpdm_dag_metadata.json"
    airflow_task_metadata = get_json_config(metadata_config_path)
    print(airflow_task_metadata)


def data_category_config():
    category_config = {
        "data_category": {
            "Customer_Data_Processing": [
                "pre_mcd",
                "ingestion",
                "dp_job",
                "mcd_process",
                "post_mcd",
                "ods",
                "load_individual_aggs",
                "archive_incoming_files",
                "job_load_crc_prev",
                "prc_extract_reject_files",
                "extracts",
            ],
            "Transaction_Data_Processing": [
                "pre_mcd",
                "ingestion",
                "dp_job",
                "mcd_process",
                "post_mcd",
                "ods",
                "load_individual_aggs",
                "archive_incoming_files",
                "job_load_crc_prev",
                "prc_extract_reject_files",
                "extracts",
            ],
            "Enrichment_Data_Processing": [
                "pre_mcd",
                "ingestion",
                "ods",
                "load_individual_aggs",
                "archive_incoming_files",
                "job_load_crc_prev",
                "prc_extract_reject_files",
                "extracts",
            ],
            "CampaignAndLookup_Data_Processing": [
                "pre_mcd",
                "ingestion",
                "ods",
                "archive_incoming_files",
                "prc_extract_reject_files",
                "extracts",
            ],
            "Priority_Data_Processing": [
                "pre_mcd",
                "priority_processing_pre_ingestion",
                "priority_process_ingestion",
                "priority_process_prematch_data",
                "ods",
                "prc_extract_reject_files",
            ],
        }
    }
    return category_config


# print(cfg["mcd_process"]["mcd_candidate"]["task_body"])


def generate_dag(
    data_category,
    company_id,
    client_name,
    FEED_TYPE_TO_CATEGORY,
    frequency,
    param_env,
    dag_cfg=None,
):
    print("data_category: ", data_category)
    print("param_env: ", param_env)
    data_category_steps = data_category_config()
    default_arguments = str(cfg["default_args"])
    dag_name = f"{client_name}_{data_category}{f'_{frequency}' if frequency else ''}"
    # dag_category_initial is used in replacing the placeholder in source_file_stats task of pre mcd
    dag_category_initial = data_category.lower().split("_")[0]
    feed_name_list = []
    priority_processing_feed_name_list = []
    # frequency_set = set()

    pg_secrets_config = get_secret(dag_cfg["mz360_postgres_secret_name"], "us-east-2")
    print("pg_secrets_config:", pg_secrets_config)

    feed_types = [
        ftype
        for ftype, category in FEED_TYPE_TO_CATEGORY.items()
        if category == data_category
    ]
    print("feed_types: ", str(feed_types))
    # if len(feed_types)==1:
    #     # If there's only one item, no need for a comma
    #     feed_types_str = f"'{feed_types[0]}'"
    # else:
    #     # For multiple items, join them with commas
    #     feed_types_str = ", ".join(f"'{ft}'" for ft in feed_types)
    feed_types_str = f"({', '.join([repr(ft) for ft in feed_types])})"
    print(feed_types_str)
    query_type = "query"
    if data_category == "Priority_Data_Processing":
        sql = (
            f"select name,priority_processing from mz360.data_ingestion where priority_processing = 'True' and "
            f"is_enabled = 'True' and company_id = {company_id}"
        )
        # process_priority_data = True
        dag_name = f"{client_name}_Priority_Data_Processing"
        # The variable wrapper_type is used for replacing place holder in dag task to call respective procedues
        wrapper_type = "priority"
    else:
        # sql = f"select name,priority_processing from mz360.data_ingestion where feed_type in {feed_type} "
        sql = f"""
        SELECT name, priority_processing, frequency
        FROM mz360.data_ingestion
        WHERE feed_type IN {feed_types_str} and is_enabled = 'True' and company_id = {company_id} and coalesce(upper(frequency),'') = '{frequency.upper()}'
        """
        # process_priority_data = False
        # The variable wrapper_type is used for replacing place holder in dag task to call respective procedues
        if data_category == "CampaignAndLookup_Data_Processing":
            wrapper_type = "lkp_campgn"
        else:
            wrapper_type = "txn"

    print(f"Executing SQL: {sql}")
    pg_result, col_names = pg_query_execute(
        sql, cfg, pg_secrets_config, query_type, col_names="Y"
    )
    for recs in pg_result:
        feed_name_list.append(recs[col_names.index("name")])
        priority_processing_feed_name_list.append(recs[col_names.index("name")])
        # if not process_priority_data:
        #     frequency_set.add(recs[col_names.index('frequency')])

    # if process_priority_data:
    #     dag_name = "Priority_Data_Processing"
    # else:
    #     if len(frequency_set) > 1:
    #         raise Exception("Multiple frequency defined for a single processing type in data ingestion table..")
    #     else:
    #         frequency = frequency_set.pop()
    #         dag_name = f"{client_name}_{data_category}_{frequency}"

    print(feed_name_list)
    dag_start_text = (
        cfg["import_libraries"].replace("<client_folder>", dag_cfg["client_folder"])
        + "\n"
        + cfg["dag_definition"]
        .replace("<dag_name>", dag_name)
        .replace("<client>", client_name)
        .replace("<dag_type>", data_category)
        + "\n"
    )

    task_final_body = []
    task_body = []
    dag_tasks_name = []
    ingestion_tasks_name = []
    extracts_tasks_name = []
    for tasks in data_category_steps["data_category"][f"{data_category}"]:
        task_steps = cfg[tasks]

        for task, detail in task_steps.items():
            if tasks in ("mcd_process", "dp_job", "pre_mcd", "post_mcd"):
                task_name = task
                task_body.append(
                    detail["task_body"]
                    .replace("<feed_types>", f"{feed_types}")
                    .replace("<freq_val>", f"{frequency.lower()}")
                    .replace("<category_init>", dag_category_initial)
                )
                dag_tasks_name.append(task_name + "()")

            elif tasks == "ingestion":
                ingestion_tasks_list = []

                for name in feed_name_list:
                    task_name = f"stg_a360_{name.lower()}"
                    ingestion_tasks_name.append(task_name + "()")
                    ingestion_tasks_list.append(
                        detail["task_body"]
                        .replace("<task_name>", task_name)
                        .replace("<source_cd>", name.upper())
                    )
                task_body.extend(ingestion_tasks_list)
                dag_tasks_name.extend(ingestion_tasks_name)

            elif tasks == "ods":
                task_name = task
                task_body.append(
                    detail["task_body"].replace("<wrapper_type>", wrapper_type)
                )
                dag_tasks_name.append(task_name + "()")

            elif tasks == "extracts":
                extracts_tasks_list = []

                for name in feed_name_list:
                    task_name = f"extract_{name.lower()}"
                    extracts_tasks_name.append(task_name + "()")
                    extracts_tasks_list.append(
                        detail["task_body"]
                        .replace("<task_name>", task_name)
                        .replace("<source_cd>", name.upper())
                    )
                task_body.extend(extracts_tasks_list)
                dag_tasks_name.extend(extracts_tasks_name)

            elif tasks == "priority_processing_pre_ingestion":
                print("task: ", task)
                task_name = task
                task_body.append(
                    detail["task_body"].replace(
                        "<feed_types>", f"{priority_processing_feed_name_list}"
                    )
                )
                dag_tasks_name.append(task_name + "()")

            elif tasks == "priority_process_ingestion":
                ingestion_tasks_list = []

                for name in feed_name_list:
                    task_name = f"stg_a360_{name.lower()}"
                    ingestion_tasks_name.append(task_name + "()")
                    ingestion_tasks_list.append(
                        detail["task_body"]
                        .replace("<task_name>", task_name)
                        .replace("<source_cd>", name.upper())
                    )
                task_body.extend(ingestion_tasks_list)
                dag_tasks_name.extend(ingestion_tasks_name)

            elif tasks == "priority_process_prematch_data":
                task_name = task
                task_body.append(detail["task_body"])
                dag_tasks_name.append(task_name + "()")
            elif tasks == "archive_incoming_files":
                print("task: ", task)
                task_name = task
                feed_type = data_category.split("_")[0]
                task_body.append(detail["task_body"].replace("<feed_type>", feed_type))
                dag_tasks_name.append(task_name + "()")
            else:
                task_name = tasks
                task_body.append(detail["task_body"].replace("<task_name>", task_name))
                dag_tasks_name.append(task_name + "()")

            task_final_body = "\n".join(task_body)

    if "mcd_part2()" in dag_tasks_name:
        dag_tasks_name.remove("mcd_part2()")

    if data_category in ("Customer_Data_Processing", "Transaction_Data_Processing"):
        post_mcd_task = "    post_mcd_process_task = post_mcd_process()"
        parallel_dependency = "    post_mcd_process_task >> mcd_part2()"

    else:
        post_mcd_task = ""
        parallel_dependency = ""

    if data_category in (
        "CampaignAndLookup_Data_Processing",
        "Priority_Data_Processing",
    ):
        if "dps_mcd_hist_bkp()" in dag_tasks_name:
            dag_tasks_name.remove("dps_mcd_hist_bkp()")

    task_execution_string = "\n    >> ".join(dag_tasks_name).replace(
        "post_mcd_process()", "post_mcd_process_task"
    )
    initial_logging_function = cfg["logging_tasks"]["task_body"]

    final_dag = (
        default_arguments
        + "\n"
        + dag_start_text
        + "\n"
        + "def pipeline_function():"
        + "\n"
        + cfg["global_variables"]
        .replace("<dag_name>", dag_name)
        .replace("<global_param_env>", param_env)
        + "\n"
        + initial_logging_function
        + "\n"
        + task_final_body
        + "\n"
        + post_mcd_task
        + "\n"
        + f"    ({task_execution_string})"
        + "\n"
        + parallel_dependency
        + "\n\n"
        + "start = pipeline_function()"
    )

    formatted_dag = autopep8.fix_code(final_dag)
    # formatted_dag = final_dag
    # script_dir = os.path.dirname(os.path.abspath(__file__))
    # output_dir = os.path.join(script_dir,"..","dags")
    # os.makedirs(output_dir,exist_ok=True)
    # output_file = os.path.join(output_dir,f"{dag_name}.py")

    # output_file_new = os.path.join("/tmp/MarketZone/builds/aws/a360/dags/",f"a360_{data_category.lower()}.py")

    # with open(output_file,"w") as file:
    #    file.write(formatted_dag)
    # print(f"DAG successfully generated and written to {os.path.abspath(output_file)}")
    # with open(output_file_new,"w") as file:
    # file.write(formatted_dag)
    # dag_file_name = f"a360_{data_category.lower()}.py"
    # dag_git_push(dag_file_name, formatted_dag)
    return formatted_dag


# generate_dag("Transaction_Data_Processing","Lumon", {
#             "Customer Data":"Customer_Data_Processing",
#             "Prospect Data":"Customer_Data_Processing",
#             "Suppression Data":"Customer_Data_Processing",
#             "Transaction Data":"Transaction_Data_Processing",
#             "3rd Party Data Enrichment":"Enrichment_Data_Processing",
#             "Campaign Data":"CampaignAndLookup_Data_Processing",
#             "Lookup Data":"CampaignAndLookup_Data_Processing",
#         }, "Daily")
