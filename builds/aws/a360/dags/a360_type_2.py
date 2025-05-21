import airflow.utils.dates
import pandas as pd
import helpers.postgres as pg
from datetime import datetime
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.configuration import conf
from airflow.models import Variable
import os
import logging
from helpers.send_email import *
from airflow.decorators import dag, task
from a360.python.a360_mz_task import run_stg_job as run_a360_stg_job
from a360.python.a360_mz_task import *
from helpers.common import *
from helpers.s3 import *
from helpers.redshift import *
from helpers.AwsExtractOperator import *
default_args = {
    "owner": "A360",
    "start_date": airflow.utils.dates.days_ago(2),
    "depends_on_past": False,
    "current_path": os.path.abspath(os.path.dirname(__file__)),
    "config_path": os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "config")
    ),
    "retries": 0,
    "email_to": "DL-A360-Team@data-axle.com",
    "email_subject": "A360 Daily Processing Job: ",
    "email_on_failure": True,
    "email_on_retry": False,
    "email_body": "Executing Stage Job ",
    "incoming_path": "s3://{}/incoming/",
    "output_path": "s3://{}/process_fcvrl/parquet_glue/",
    "script_path": "s3://{}/scripts/python/",
    "format_file_path_glue": "s3://{}/config/pyspark_json/",
    "redshift_config_paths": "s3://{}/config/common/glue_redshift_config.json",
    "iam_role_glue": "da-mz-glue-role",
    "timeout_glue_job": 90,
    "maxretries_glue_job": 0,
    "common_settings_glue": "s3://{}/config/common/pysparkcommonsettings.json",
    "additional_python_modules_glue": "h3==3.7.6,smart-open==6.3.0,openpyxl==3.1.2",
    "glue_version": "4.0",
    "extra_py_files_glue": "s3://{}/scripts/python/ValidateJsonFormatFile.py,s3://{}/scripts/python/FCVRL_UDFS.py,s3://{}/scripts/python/common.py",
    "connections_glue": "GEHA Redshift connection",
    "temp_path_athena": "s3://{}/athena_query_results/",
    "mcd_input_table": "MZB_MCD_FILE_INCR",
    "reject_path": "s3://{}/archive/outbound/rejects/",
    "extracts_path": "s3://{}/archive/outbound/extracts/",
    "ods_user": "A360_ODS",
    "audit_schema": "A360_CORE",
    "audit_table": "TMDM_OBJECT",
}


@dag(
    dag_id="a360_TYPE_2",
    default_args=default_args,
    description="a360 TYPE_2 Data Pipeline",
    schedule_interval=None,
    max_active_runs=1,
    params={"run_time_params": {"task_to_skip": []}},
)
def pipeline_function():
    current_env = Variable.get("var-env", "local")
    if current_env == "Prod":
        json_config_path = default_args["config_path"] + \
            "/a360_daily_config_prod.json"
    else:
        json_config_path = default_args["config_path"] + \
            "/a360_type1_config.json"

    cfg = get_json_config(json_config_path)
    sec_name = cfg["secret_name"]
    region_name = cfg["aws_reg_rs"]
    secrets_config = get_secret(sec_name, region_name)
    a360_account_id = get_aws_account_id()
    bucket_name = cfg["bucket_name"].replace("{}", a360_account_id[-4:])
    mdm_bucket_name = cfg["bucket_name"].replace(
        "{}", a360_account_id[-4:]) + "-mdm"
    dps_bucket_name = cfg["dps_bucket_name"].replace(
        "{}", a360_account_id[-4:])
    mft_bucket_name = cfg["mft_bucket"].replace("{}", a360_account_id[-4:])
    redshift_iam_role = "arn:aws:iam::{}:role/da-mz-redshift-role".format(
        a360_account_id
    )
    emr_iam_role = "arn:aws:iam::{}:role/da-mz-emr-serverless-role".format(
        a360_account_id
    )
    athena_database = get_ssm_parameters(
        "a360_athena_database", region_name, False)
    mcd_emr_application_id = get_ssm_parameters(
        "a360_mcd_emr", region_name, False)
    v_execid, v_batchid = generate_exec_id(cfg["request_type"])
    reject_path = default_args["reject_path"].replace("{}", bucket_name)
    extracts_path = default_args["extracts_path"].replace("{}", bucket_name)
    temp_path_athena = default_args["temp_path_athena"].replace(
        "{}", bucket_name)
    v_cur_date = datetime.now().strftime("%Y%m%d%H%M%S")
    p_cur_user = "awsuser"
    ctl_file_path = cfg["ctl_file_path"].format(v_batchid)
    pg_secrets_config = get_secret("mz360_client_redshift", region_name)

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
        task_status = task_instance.state
        if task_status == "success":
            v_msg1 = "Succeeded"
            v_msg2 = "Success"
        if task_status == "skipped":
            v_msg1 = "Skipped"
            v_msg2 = "Skipped"

        # subject = f"Task {context['task_instance_key_str']} Failed"
        html_content = f"""
          <h3>Task: {context['task_instance_key_str']}</h3>
          <p>{v_msg1} on: {datetime.now()}</p>
          <h4>Log Content:</h4>
          <pre>{log_content}</pre>
          """

        # Extract the task_id from the task_instance_key_str
        task_instance_key_str = context["task_instance_key_str"]
        task_id = task_instance_key_str.split("__")[1]
        if current_env != "local":
            send_email(
                email_address=default_args["email_to"],
                email_message=html_content,
                email_subject="{} :{}{}".format(
                    v_msg2, default_args["email_subject"], task_id
                ),
                email_business_users=[],
                client="geha",
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

        if current_env != "local":
            send_email(
                email_address=default_args["email_to"],
                email_message=html_content,
                email_subject="Failure :{}{}".format(
                    default_args["email_subject"], task_id
                ),
                email_business_users=[],
                client="geha",
            )

    @task(
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email,
        trigger_rule="none_failed",
    )
    def dps_mcd_hist_bkp(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "dps_mcd_hist_bkp",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException
        ##### Daily  Backups ######
        step_set = "mcd_hist_bkup"

        procedure_call = "CALL a360_ods.prc_dps_mcd_hist_bkup('{}',{})".format(
            step_set, v_execid
        )

        print(procedure_call)
        redshift_query_execute(
            sql=procedure_call,
            cfg=cfg,
            secrets_config=secrets_config,
            query_type="",
        )

        tuple_data = redshift_query_execute(
            sql="""select param_value  from a360_core.tmdm_param_config where context = 'ARCHIVE_FILES_ON_HOLD'""",
            cfg=cfg,
            secrets_config=secrets_config,
            query_type="query",
        )
        file_prefix_list = [x for lst in tuple_data for x in lst]
        logging.info(file_prefix_list)
        for file_pattern_iter in file_prefix_list:
            complete_path_iter = "s3://{}/incoming/{}".format(
                bucket_name,
                file_pattern_iter,
            )
            file_archive_list = get_s3Filelist_using_wr(complete_path_iter)
            print(file_archive_list)
            for file in file_archive_list:
                key = "incoming/{}".format(file.split("/")[-1])
                move_and_delete_s3_file(
                    bucket_name,
                    key,
                    bucket_name,
                    "incoming_archive/{}".format(file.split("/")[-1]),
                )

    @task(
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email,
        trigger_rule="none_failed",
    )
    def business_to_internal_s3_file_transfer(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
                "business_to_internal_s3_file_transfer",
                kwargs["dag_run"].conf.get(
                    "run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException

        print("pg_secrets_config: ", str(pg_secrets_config))
        logging.info("business to internal s3 file transfer Process")
        business_to_internal_ingestion_integration(
            cfg, secrets_config, pg_secrets_config, bucket_name, destination_bucket
        )

    @task(
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email,
        trigger_rule="none_failed",
    )
    def prepare_incoming_file_from_db(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
                "prepare_incoming_file_from_db",
                kwargs["dag_run"].conf.get(
                    "run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException

        # feed_frequency = default_args["feed_frequency"] to be included in the query

        query_type = "query"
        sql = "select * from mz360.data_ingestion where data_source_type = 'Redshift' "
        pg_result = pg.pg_query_execute(
            sql, cfg, pg_secrets_config, query_type)

        # List of tuples
        print(pg_result)
        if not pg_result:
            raise Exception("No data found in the pg_result.")

        for data in pg_result:
            # src_tab = data[column_names.index[('data_source_object')]
            src_tab = "ds_aetnaprod.mzb_aet_ods.tbmzb_mtg_file"
            file_pattern = src_tab.split('.')[-1] + "_" + str(v_batchid) + "_"

            unload_redshift_datashare_data(
                bucket_name, file_pattern, src_tab, cfg, redshift_iam_role, secrets_config)

    @task(
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email,
        trigger_rule="none_failed",
    )
    def source_file_stats(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "source_file_stats",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException

        print(default_args["current_path"])
        print(redshift_iam_role)
        print(athena_database)
        # Create an empty DataFrame
        df = pd.DataFrame(
            columns=["file_name", "batch_id", "file_record_count"])
        # Create an empty List for source cd to be processed today
        source_cd_list = []
        file_present_list = []
        file_src_cd_mapping = {}
        env = "dev"
        if env == "dev":
            print(default_args["config_path"])
            with open(json_config_path, "r") as file:
                data = file.read()
            list_info = json.loads(data)
            list_info_lookup = list_info["list_config"]
            for source_cd, source_det in list_info_lookup.items():
                # print(source_cd,source_det)
                # print(
                #     " File pattern {} , header {} ,fullfile {}".format(
                #         source_det["file_pattern"],
                #         source_det["header"],
                #         source_det["fullfile"],
                #     )
                # )
                folder_prefix = source_det["key_prefix"]
                file_pattern = source_det["file_pattern"]
                if len(file_pattern.split(",")) > 1:
                    for file_pattern_iter in file_pattern.split(","):
                        complete_path_iter = "s3://{}/{}/{}".format(
                            bucket_name,
                            folder_prefix,
                            file_pattern_iter,
                        )
                        filelist = get_s3Filelist_using_wr(complete_path_iter)
                        if filelist:
                            file_present_list.extend(filelist)
                            if source_cd not in source_cd_list:
                                source_cd_list.append(source_cd)
                            # mapping each file with its source_cd
                            file_src_cd_mapping.update(
                                {file: source_cd for file in filelist}
                            )

                else:
                    complete_path_iter = "s3://{}/{}/{}".format(
                        bucket_name, folder_prefix, file_pattern
                    )
                    file_dupe_list = get_s3Filelist_using_wr(
                        complete_path_iter)
                    # Handle for individual files if full file is True then keep the latest and archive file duplicates
                    if file_dupe_list:
                        if len(file_dupe_list) > 1 and source_det["fullfile"]:
                            file_present_list.extend([file_dupe_list[0]])
                            if source_cd not in source_cd_list:
                                source_cd_list.append(source_cd)
                            # mapping each file with its source_cd
                            file_src_cd_mapping[file_dupe_list[0]] = source_cd

                            # Files to be archived into the current batchid folder
                            logging.info(
                                "Full Files to be Deleted : {}".format(
                                    file_dupe_list[1:]
                                )
                            )
                            for file in file_dupe_list[1:]:
                                bucket, key = split_s3_path(file)
                                move_and_delete_s3_file(
                                    bucket,
                                    key,
                                    bucket,
                                    "archive/{}/incoming/{}".format(
                                        v_batchid, key.split("/")[-1]
                                    ),
                                )
                        else:
                            file_present_list.extend(file_dupe_list)
                            if source_cd not in source_cd_list:
                                source_cd_list.append(source_cd)
                            # mapping each file with its source_cd
                            file_src_cd_mapping.update(
                                {file: source_cd for file in file_dupe_list}
                            )
            logging.info(
                "file name with src cd mapping : {}".format(
                    file_src_cd_mapping)
            )
            for file in file_present_list:
                bucket, key = split_s3_path(file)
                print("file: ", file)
                print("key: ", key)
                # Get source_cd from filename to read header flag from config
                file_name = key.split("/")[1]
                source_cd = file_src_cd_mapping[file]

                print("file name: ", file_name)
                print("source code: ", source_cd)

                if cfg["list_config"][source_cd]["header"]:
                    count = -1
                else:
                    count = 0
                # Avoiding S3select for full files , running glue job instead
                if cfg["list_config"][source_cd]["fullfile"]:
                    response = None
                else:
                    response = read_s3_obj_content(bucket, key)
                if response is not None:
                    try:
                        for event in response["Payload"]:
                            if "Records" in event:
                                count = count + int(
                                    event["Records"]["Payload"].decode("utf-8")
                                )
                                print(
                                    "File Name {} , File Count {}".format(key, count))
                                file_iter_df = pd.DataFrame(
                                    {
                                        "file_name": [key.split("/")[1]],
                                        "batch_id": v_batchid,
                                        "file_record_count": [count],
                                    }
                                )

                                # Concatenate the DataFrames
                                df = pd.concat(
                                    [df, file_iter_df], ignore_index=True)
                    except Exception as e:
                        print(
                            "S3 select error occurred for file {} with error {}:".format(
                                key.split("/")[1], e
                            )
                        )
                        file_iter_df = pd.DataFrame(
                            {
                                "file_name": [key.split("/")[1]],
                                "batch_id": v_batchid,
                                "file_record_count": [count],
                            }
                        )
                        # Concatenate the DataFrames
                        df = pd.concat([df, file_iter_df], ignore_index=True)
                else:
                    file_iter_df = pd.DataFrame(
                        {
                            "file_name": [key.split("/")[1]],
                            "batch_id": v_batchid,
                            "file_record_count": [count],
                        }
                    )

                    # Concatenate the DataFrames
                    df = pd.concat([df, file_iter_df], ignore_index=True)

            logging.info(df)
            # Source code Lists processed today #
            logging.info(source_cd_list)
            kwargs["ti"].xcom_push(
                key="process_list_key", value=source_cd_list)

            kwargs["ti"].xcom_push(key="batch_id", value=v_batchid)
            kwargs["ti"].xcom_push(key="batch_timestamp", value=v_cur_date)
            kwargs["ti"].xcom_push(key="exec_id", value=v_execid)
            engine = get_redshift_sqlalchemy_conn(
                "us-east-2", cfg["redshift_cluster"], cfg["redshift_db"], secrets_config
            )

            # in case of empty df it is overwriting stg_source_file_stats making file_record_count col to varchar
            if not df.empty:
                df.to_sql(
                    "stg_source_file_stats",
                    con=engine,
                    if_exists="replace",
                    index=False,
                    schema="a360_stg",
                )
            # Get file_record_count for files with S3 select failure, capture in tmp table mzb_aet_stg.stg_source_file_stats_tmp
            if file_present_list == []:
                bucket = bucket_name
            get_file_record_count(
                batch_id=v_batchid,
                incoming_path=folder_prefix,
                redshift_config_path=default_args["redshift_config_paths"].replace(
                    "{}", bucket_name
                ),
                s3_bucket=bucket,
                table_source_file_stats="stg_source_file_stats",
                script_path=default_args["script_path"].replace(
                    "{}", bucket_name),
                iam_glue_role=default_args["iam_role_glue"],
                connections_glue=default_args["connections_glue"],
            )
            ### Call Redshift procedure to handle checks for duplicate files received that were processed earlier ###
            procedure_call = "CALL a360_core.prc_check_dup_files ({},'{}')".format(
                v_batchid, v_cur_date
            )
            print(procedure_call)
            redshift_query_execute(
                sql=procedure_call,
                cfg=cfg,
                secrets_config=secrets_config,
                query_type="",
            )
            ### Archive the output data from redshift stored processed procedure from above step to archive the processed duplicate files ###
            tuple_data = redshift_query_execute(
                sql="""select file_name  from a360_core.tmdm_source_files where err_msg is not null and batch_id={}""".format(
                    v_batchid
                ),
                cfg=cfg,
                secrets_config=secrets_config,
                query_type="query",
            )
            file_archive_list = [x for lst in tuple_data for x in lst]
            logging.info(file_archive_list)
            for file in file_archive_list:
                key = "incoming/{}".format(file)
                move_and_delete_s3_file(
                    bucket_name,
                    key,
                    bucket_name,
                    "archive/{}/incoming/{}".format(v_batchid, file),
                )

    @task(
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email,
        trigger_rule="none_failed",
    )
    def stg_a360_medicare(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "stg_a360_medicare",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException
        # for src_cd, source_data in cfg["list_config"].items():
        ti = kwargs["ti"]
        v_batchid = ti.xcom_pull(task_ids="source_file_stats", key="batch_id")
        pulled_list = (
            []
            if ti.xcom_pull(task_ids="source_file_stats", key="process_list_key")
            is None
            else ti.xcom_pull(task_ids="source_file_stats", key="process_list_key")
        )
        print("Pulled List for XCOM : {}".format(pulled_list))
        if pulled_list == []:
            pulled_list = get_processing_list(cfg, secrets_config, v_batchid)
            print("Pulled List for Redshift for current Batch : {}".format(pulled_list))

        src_cd = "MEDICARE"
        run_a360_stg_job(
            cfg=cfg,
            source_cd=src_cd,
            bucket_name=bucket_name,
            secrets_config=secrets_config,
            batch_id=v_batchid,
            default_args=default_args,
            file_present_ind=file_present_ind,
            redshift_iam_role=redshift_iam_role,
        )

    @task(
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email,
        trigger_rule="none_failed",
    )
    def stg_a360_netwise(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "stg_a360_netwise",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException
        # for src_cd, source_data in cfg["list_config"].items():
        ti = kwargs["ti"]
        v_batchid = ti.xcom_pull(task_ids="source_file_stats", key="batch_id")
        pulled_list = (
            []
            if ti.xcom_pull(task_ids="source_file_stats", key="process_list_key")
            is None
            else ti.xcom_pull(task_ids="source_file_stats", key="process_list_key")
        )
        print("Pulled List for XCOM : {}".format(pulled_list))
        if pulled_list == []:
            pulled_list = get_processing_list(cfg, secrets_config, v_batchid)
            print("Pulled List for Redshift for current Batch : {}".format(pulled_list))

        src_cd = "NETWISE"
        run_a360_stg_job(
            cfg=cfg,
            source_cd=src_cd,
            bucket_name=bucket_name,
            secrets_config=secrets_config,
            batch_id=v_batchid,
            default_args=default_args,
            file_present_ind=file_present_ind,
            redshift_iam_role=redshift_iam_role,
        )

    @task(
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email,
        trigger_rule="none_failed",
    )
    def stg_a360_modelscores(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "stg_a360_modelscores",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException
        # for src_cd, source_data in cfg["list_config"].items():
        ti = kwargs["ti"]
        v_batchid = ti.xcom_pull(task_ids="source_file_stats", key="batch_id")
        pulled_list = (
            []
            if ti.xcom_pull(task_ids="source_file_stats", key="process_list_key")
            is None
            else ti.xcom_pull(task_ids="source_file_stats", key="process_list_key")
        )
        print("Pulled List for XCOM : {}".format(pulled_list))
        if pulled_list == []:
            pulled_list = get_processing_list(cfg, secrets_config, v_batchid)
            print("Pulled List for Redshift for current Batch : {}".format(pulled_list))

        src_cd = "MODELSCORES"
        run_a360_stg_job(
            cfg=cfg,
            source_cd=src_cd,
            bucket_name=bucket_name,
            secrets_config=secrets_config,
            batch_id=v_batchid,
            default_args=default_args,
            file_present_ind=file_present_ind,
            redshift_iam_role=redshift_iam_role,
        )

    @task(
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email,
        trigger_rule="none_failed",
    )
    def dps_daily_send_to_dp(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "dps_daily_send_to_dp",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException
        run_dp_job(
            task_name='send_to_dp',
            cfg=cfg,
            bucket_name=bucket_name,
            request_id='daily',
            secrets_config=secrets_config,
            batch_id=v_batchid,
            athena_database=athena_database,
            dps_bucket_name=dps_bucket_name,
            redshift_iam_role=redshift_iam_role,
            email_to=default_args["email_to"],
            client='FirstParty'
        )

    @task(
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email,
        trigger_rule="none_failed",
    )
    def dps_daily_load_dpout(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "dps_daily_load_dpout",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            # send_email()
            print("SKip")
            raise AirflowSkipException
        run_dp_job(
            task_name='load_dpout',
            cfg=cfg,
            bucket_name=bucket_name,
            request_id='daily',
            secrets_config=secrets_config,
            batch_id=v_batchid,
            athena_database=athena_database,
            dps_bucket_name=dps_bucket_name,
            redshift_iam_role=redshift_iam_role,
            email_to=default_args["email_to"],
            client='FirstParty'
        )

    @task(
        on_success_callback=send_success_email,
        on_failure_callback=send_failure_email,
        trigger_rule="none_failed",
    )
    def extracts(**kwargs):
        if kwargs["dag_run"].conf.get("run_time_params") and skip_task_check(
            "extracts",
            kwargs["dag_run"].conf.get("run_time_params").get("task_to_skip"),
        ):
            print("Skip")
            raise AirflowSkipException

        ti = kwargs["ti"]
        v_batchid = ti.xcom_pull(task_ids="source_file_stats", key="batch_id")
        v_cur_date = ti.xcom_pull(
            task_ids="source_file_stats", key="batch_timestamp")

        task_name = "extracts"
        cfg["extract_configs"][task_name]["FileNamePrefix"] = (
            cfg["extract_configs"][task_name]["FileNamePrefix"] +
            f"{str(v_cur_date)}_"
        )
        cfg["insert_query"] = cfg["insert_query"].replace(
            "<batch_id>", v_batchid)
        run_extract_task(
            cfg=cfg,
            task_name=task_name,
            default_args=default_args,
            current_dt=v_cur_date,
            redshift_iam_role=redshift_iam_role,
            bucket_name=bucket_name,
            secrets_config_redshift=secrets_config,
            mft_bucket_name=mft_bucket_name,
            batch_id=v_batchid,
        )

    (dps_mcd_hist_bkp()
     >> business_to_internal_s3_file_transfer()
     >> prepare_incoming_file_from_db()
     >> source_file_stats()
     >> stg_a360_medicare()
     >> stg_a360_netwise()
     >> stg_a360_modelscores()
     >> dps_daily_send_to_dp()
     >> dps_daily_load_dpout()
     >> extracts())


start = pipeline_function()
