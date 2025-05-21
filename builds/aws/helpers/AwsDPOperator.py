from airflow.models import BaseOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from datetime import datetime
from airflow.utils.decorators import apply_defaults
from helpers.redshift import *
from helpers.send_email import *
from airflow.operators.python import get_current_context
import helpers.s3 as s3_dax
import helpers.athena as athena_dax
import helpers.redshift as redshift_dax
import time
import awswrangler as wr


class AwsDPOperator(BaseOperator):
    """
    Custom Airflow operator to Unload data from DPInput Athena table, Send files to DP, Wait for response files, send email alerts and Optionally Load response files to Redshift Table.
    """

    @apply_defaults
    def __init__(
        self,
        cfg=None,
        iam_redshift_role=None,
        secrets_config_redshift=None,
        batch_id=None,
        request_id=None,
        athena_database=None,
        task_name=None,
        bucket_name=None,
        dps_bucket_name=None,
        email_to=None,
        client=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.cfg = cfg
        self.iam_redshift_role = iam_redshift_role
        self.secrets_config_redshift = secrets_config_redshift
        self.batch_id = batch_id
        self.request_id = request_id
        self.athena_database = athena_database
        self.task_name = task_name
        self.bucket_name = bucket_name
        self.dps_bucket_name = dps_bucket_name
        self.email_to = email_to
        self.client = client

    def execute(self, context):
        from aetna.python.mz_task import query_athena_table

        # Check task_name (send_to_dp or receive_from_dp) and run corresponding steps
        if self.task_name == "send_to_dp":
            # Step 1: Run Unload command
            self.dp_process_setup(
                request_id=self.request_id,
                bucket_name=self.bucket_name,
                step_name="unload",
                cfg=self.cfg,
                athena_database=self.athena_database,
            )

            # Step 2: Send files to dp
            send_files_count = self.dp_send_file_process(
                cfg=self.cfg,
                request_id=self.request_id,
                bucket_name=self.bucket_name,
                dps_bucket_name=self.dps_bucket_name,
            )

            athena_tab_to_dp = self.cfg["dp_config"][f"{self.request_id}"][
                "athena_tab_to_dp"
            ]
            sent_df = query_athena_table(
                cfg=self.cfg,
                table_name=athena_tab_to_dp,
                bucket_name=self.bucket_name,
                athena_database=self.athena_database,
            )
            sent_data_count = sent_df.head()["rec_count"][0]

            # Step 3: Send email alert for DP Process start
            html_content = f"""
            <h3>{self.client} {self.request_id} DP Request Started for batch {self.batch_id}</h3>
            <p>Started on: {datetime.now()}</p>
            <h4>File Count:</h4>
            <pre>{send_files_count}</pre>
            <h4>Record Count:</h4>
            <pre>{sent_data_count}</pre>
            """

            send_email(
                email_address=self.email_to,
                email_message=html_content,
                email_subject="{} {} DP Request Started for batch {}".format(
                    self.client, self.request_id, self.batch_id
                ),
                client=self.client,
            )

            # Step 4: Wait for response files
            self.dp_receive_file_process(
                cfg=self.cfg,
                request_id=self.request_id,
                bucket_name=self.bucket_name,
                dps_bucket_name=self.dps_bucket_name,
                athena_database=self.athena_database,
                sent_file_count=send_files_count,
            )

        elif self.task_name == "load_dpout":
            html_content = f"""
            <h3>{self.client} {self.request_id} DP Request Success for batch {self.batch_id}</h3>
            <p>Succeeded on: {datetime.now()}</p>
            """
            # Step 1: Load files from dp
            if (
                self.secrets_config_redshift is not None
                and self.secrets_config_redshift != ""
            ):
                self.dp_process_setup(
                    request_id=self.request_id,
                    bucket_name=self.bucket_name,
                    step_name="copy",
                    cfg=self.cfg,
                    athena_database=self.athena_database,
                    secrets_config_redshift=self.secrets_config_redshift,
                    iam_redshift_role=self.iam_redshift_role,
                )

            else:
                self.dp_process_setup(
                    request_id=self.request_id,
                    bucket_name=self.bucket_name,
                    step_name="copy",
                    cfg=self.cfg,
                    athena_database=self.athena_database,
                )

            send_email(
                email_address=self.email_to,
                email_message=html_content,
                email_subject="{} {} DP Request Success for batch {}".format(
                    self.client, self.request_id, self.batch_id
                ),
                client=self.client,
            )

    def dp_process_setup(
        self, request_id, bucket_name, step_name, cfg, athena_database, **kwargs
    ):
        s3_bucket = bucket_name
        outbound_src = cfg["dp_config"][f"{request_id}"]["outbound"]
        inbound_src = cfg["dp_config"][f"{request_id}"]["inbound"]
        dp_config_key = "{}{}".format(cfg["s3_key_dp_config"], request_id)

        if step_name == "unload":
            print(step_name)
            # del_obj = s3_dax.del_all_object_from_s3_folder(s3_bucket, outbound_src)
            # Ensure target directory is deleted
            output_path = f"s3://{s3_bucket}/{outbound_src}"
            wr.s3.delete_objects(path=output_path)
            time.sleep(30)  # Delay for S3 consistency
            unload_cmd_path = f"{dp_config_key}/unload_cmd.txt"
            # Read contents of file at s3 path unload_cmd_path in a string variable
            sql_string = s3_dax.read_s3_file(s3_bucket, unload_cmd_path)
            # sql_string = sql_string.replace("<iam>", redshift_iam_role)
            # sql_string = sql_string.replace(
            #     "<s3path>", f"s3://{s3_bucket}/{outbound_src}"
            # )
            print(sql_string)

            compression = cfg["dp_config"][f"{request_id}"]["compression"]
            session = boto3.session.Session()

            if compression == "None" or compression.strip() == "":
                compression = "NONE"

            response = athena_dax.execute_unload_query(
                boto3_session=session,
                sql=sql_string,
                database=athena_database,
                output_path=output_path,
                file_format=cfg["dp_config"][f"{request_id}"]["file_format"],
                delimiter=cfg["dp_config"][f"{request_id}"]["delimiter"],
                compression=compression,
            )
            # print("record unloaded {} ".format(response[0][0]))
            # delete manifest and metadata files
            s3path_source_file = "s3://" + s3_bucket + "/" + outbound_src
            list_files = s3_dax.get_s3Filelist_using_wr(s3path_source_file)
            list_files_manifest_metadata = list(
                filter(
                    lambda x: (
                        x.endswith("-manifest.csv")
                        or x.endswith("_metadata.json")
                        or x.endswith("-manifest.json")
                        or x.endswith(".metadata")
                    ),
                    list_files,
                )
            )
            for obj in list_files_manifest_metadata:
                bucket, s3_file = s3_dax.split_s3_path(obj)
                s3_dax.delete_bucket_object(bucket, s3_file)
        if step_name == "copy":
            print(step_name)
            ### Check whether redshift is available before running copy command ###
            if "secrets_config_redshift" in kwargs:
                secret_cfg = kwargs.get("secrets_config_redshift")
                redshift_iam_role = kwargs.get("iam_redshift_role")
                truncate_cmd_path = f"{dp_config_key}/truncate_cmd.txt"
                # Read contents of file at s3 path unload_cmd_path in a string variable
                sql_string = s3_dax.read_s3_file(s3_bucket, truncate_cmd_path)
                print(sql_string)
                query_type = ""

                response = redshift_dax.redshift_query_execute(
                    sql_string, cfg, secret_cfg, query_type
                )
                print("Truncate command successful")

                ### First Truncate the dpoutput table before running copy command ###
                secret_cfg = kwargs.get("secrets_config_redshift")
                copy_cmd_path = f"{dp_config_key}/copy_cmd.txt"
                # Read contents of file at s3 path copy_cmd_path in a string variable
                sql_string = s3_dax.read_s3_file(s3_bucket, copy_cmd_path)
                sql_string = sql_string.replace("<iam>", redshift_iam_role)
                sql_string = sql_string.replace(
                    "<s3path>", f"s3://{s3_bucket}/{inbound_src}"
                )
                print(sql_string)
                query_type = ""

                response = redshift_dax.redshift_query_execute(
                    sql_string, cfg, secret_cfg, query_type
                )
                print("Copy command successful")
                return response

    def dp_send_file_process(self, cfg, request_id, bucket_name, dps_bucket_name):
        s3_bucket = bucket_name
        s3_bucket_dp = dps_bucket_name
        print(f"DPS Bucket {s3_bucket_dp}")
        # athena_db = athena_database
        outbound_path_src = cfg["dp_config"][f"{request_id}"]["outbound"]
        inbound_path_src = cfg["dp_config"][f"{request_id}"]["inbound"]
        outbound_dp = cfg["dp_config"][f"{request_id}"]["dp_outbound"]
        inbound_dp = cfg["dp_config"][f"{request_id}"]["dp_inbound"]
        dp_reg = cfg["aws_reg_dp"]
        non_dp_reg = cfg["aws_reg_rs"]

        s3path_source_file = "s3://" + s3_bucket + "/" + outbound_path_src
        # Handle below deletion of files form incoming currently deletes entire folder when empty
        del_obj = s3_dax.del_all_object_from_s3_folder(s3_bucket_dp, inbound_dp)
        #del_obj = s3_dax.del_all_object_from_s3_folder(s3_bucket_dp, outbound_dp)
        list_zero_size = s3_dax.list_zero_byte_objects(s3_bucket, outbound_path_src)
        list_files = s3_dax.get_s3Filelist_using_wr(s3path_source_file)
        # list_file_final = list(filter(lambda x: x not in list_zero_size, list_files))
        # Filter out manifest and metadata files along with zero-size files
        list_file_final = list(
            filter(
                lambda x: (x not in list_zero_size),
                list_files,
            )
        )
        source_file_count = len(list_file_final)
        print("DP {} file count :{}".format(request_id, source_file_count))
        for obj in list_file_final:
            bucket, s3_file = s3_dax.split_s3_path(obj)
            copy_dp_files_s3 = s3_dax.copy_cross_region_s3_object(
                s3_bucket,
                s3_file,
                s3_bucket_dp,
                outbound_dp + s3_file.split("/")[-1],
                non_dp_reg,
                dp_reg,
            )
        return source_file_count

    def dp_receive_file_process(
        self,
        cfg,
        request_id,
        bucket_name,
        dps_bucket_name,
        athena_database,
        sent_file_count,
    ):
        from aetna.python.mz_task import query_athena_table

        s3_bucket = bucket_name
        s3_bucket_dp = dps_bucket_name
        athena_db = athena_database
        athena_tab_to_dp = cfg["dp_config"][f"{request_id}"]["athena_tab_to_dp"]
        athena_tab_from_dp = cfg["dp_config"][f"{request_id}"]["athena_tab_from_dp"]
        outbound_path_src = cfg["dp_config"][f"{request_id}"]["outbound"]
        inbound_path_src = cfg["dp_config"][f"{request_id}"]["inbound"]
        outbound_dp = cfg["dp_config"][f"{request_id}"]["dp_outbound"]
        inbound_dp = cfg["dp_config"][f"{request_id}"]["dp_inbound"]
        dp_reg = cfg["aws_reg_dp"]
        non_dp_reg = cfg["aws_reg_rs"]

        del_obj = s3_dax.del_all_object_from_s3_folder(s3_bucket, inbound_path_src)

        sent_df = query_athena_table(
            cfg, athena_tab_to_dp, bucket_name, athena_database
        )
        sent_data_count = sent_df.head()["rec_count"][0]

        received_data_count = 0

        while received_data_count != sent_data_count:
            received_df = query_athena_table(
                cfg, athena_tab_from_dp, bucket_name, athena_database
            )
            received_data_count = received_df.head()["rec_count"][0]
            logging.info("Waiting for {} dp files to be received ".format(request_id))
            time.sleep(300)

        s3path_files_from_dp = "s3://" + s3_bucket_dp + "/" + inbound_dp
        list_files_frm_dp = s3_dax.get_s3Filelist_using_wr(s3path_files_from_dp)
        received_file_count = len(list_files_frm_dp)

        if (
            received_data_count == sent_data_count
            and received_file_count == sent_file_count
        ):
            print("counts matched")
            copy_dp_files_s3 = (
                s3_dax.copy_cross_region_s3_datasync(  # copy_cross_region_s3_object(
                    s3_bucket_dp,
                    inbound_dp,
                    s3_bucket,
                    inbound_path_src,
                    dp_reg,
                    non_dp_reg,
                )
            )
            return True
        else:
            return False
