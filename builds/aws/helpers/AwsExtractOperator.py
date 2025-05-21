from airflow.models import BaseOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from datetime import datetime
from airflow.utils.decorators import apply_defaults
from helpers.s3 import (
    copy_s3_object,
    del_all_object_from_s3_folder,
    delete_bucket_object,
)
from helpers.common import generate_file_suffix
from helpers.redshift import *
from helpers.send_email import *
from airflow.operators.python import get_current_context
from helpers.s3 import *


class AwsExtractOperator(BaseOperator):
    """
    Custom Airflow operator to run a Extracts to unload data from Redshift to S3
    """

    @apply_defaults
    def __init__(
        self,
        extract_config=None,
        secrets_config_redshift=None,
        batch_id=None,
        current_dt=None,
        cfg=None,
        task_id=None,
        task_name=None,
        email_body=None,
        iam_redshift_role=None,
        extracts_path=None,
        iam_glue_role=None,
        script_path=None,
        glue_version=None,
        connections_glue=None,
        timeout_glue_job=None,
        extra_py_files_glue=None,
        mft_bucket_name=None,
        bucket=None,
        audit_schema=None,
        audit_table=None,
        **kwargs,
    ):
        super().__init__(task_id=task_name, **kwargs)
        self.extract_config = extract_config
        self.secrets_config_redshift = secrets_config_redshift
        self.batch_id = batch_id
        self.current_dt = current_dt
        self.cfg = cfg
        self.task_name = task_name
        self.email_body = email_body
        self.iam_redshift_role = iam_redshift_role
        self.extracts_path = extracts_path
        self.iam_glue_role = iam_glue_role
        self.script_path = script_path
        self.glue_version = glue_version
        self.connections_glue = connections_glue
        self.timeout_glue_job = timeout_glue_job
        self.extra_py_files_glue = extra_py_files_glue
        self.bucket = bucket
        self.mft_bucket_name = mft_bucket_name
        self.audit_schema = audit_schema
        self.audit_table = audit_table
        self.extract_name = None

    def execute(self, context):
        # Send email Notification for Start of Job/task
        self.send_email_notification()

        if "split_file" in self.extract_config and self.extract_config["split_file"]:
            task_id = self.extract_config["taskName"]
            S3Bucket = self.bucket
            extracts_path = self.extracts_path
            batchid = self.batch_id
            current_dt = self.current_dt
            mailboxes = self.extract_config["MailBoxes"]
            FileNameSuffix = generate_file_suffix(
                self.extract_config["FileNameSuffixPattern"]
            )
            FileNamePrefix = self.extract_config["FileNamePrefix"]

            delay = None
            if "delay" in self.extract_config:
                delay = self.extract_config["delay"]

            iam_redshift_role = self.iam_redshift_role
            if FileNameSuffix == None:
                outFile = self.extract_config["FileNamePrefix"]
            else:
                outFile = self.extract_config["FileNamePrefix"] + FileNameSuffix
            self.extract_name = outFile
            outFile = outFile + ('' if outFile.endswith('_') else '_')
            S3key = extracts_path + str(batchid) + "/" + outFile
            mft_bucket_name = self.mft_bucket_name
            s3_key_mft_outbound = self.cfg["s3_key_mft_outbound"]
            audit_schema = self.audit_schema
            audit_table = self.audit_table

            if self.extract_config["parallel"]:
                parallel_flag = "ON"
            else:
                parallel_flag = "OFF"

            unload_options = [
                f"DELIMITER '{self.extract_config['Delimiter']}' "
                f"EXTENSION '{self.extract_config['Extension']}' "
                f"PARALLEL {parallel_flag} "
                "ALLOWOVERWRITE "
                "GZIP "
                "HEADER "
                f"MAXFILESIZE {self.cfg['split_file_maxsize']} "
            ]

            if self.extract_config["quotes"]:
                unload_options.append(f" ADDQUOTES")

            selectQuery = (
                "select * FROM "
                + self.extract_config["schemaName"]
                + "."
                + self.extract_config["viewName"]
            )

            unload_query = f"""
                    UNLOAD ('{selectQuery}')
                    TO '{S3key}'
                    IAM_ROLE '{iam_redshift_role}'
                    {' '.join(unload_options)};
                """

            print(unload_query)
            print(
                "Executing Unload Command to unload data from {}.{} to {}".format(
                    self.extract_config["schemaName"],
                    self.extract_config["viewName"],
                    S3key,
                )
            )

            type = "unload"
            redshift_query_execute(
                sql=unload_query,
                cfg=self.cfg,
                secrets_config=self.secrets_config_redshift,
                query_type=type,
            )
            print("Unload Command Finished!")

            # insert data into extract_metadata_tbl
            if ("extract_metadata_tbl" in self.cfg) and ("insert_query" in self.cfg):
                type = "insert"
                extract_metadata_tbl = self.cfg["extract_metadata_tbl"]
                insert_query = self.cfg["insert_query"].format(
                    self.audit_schema, extract_metadata_tbl, S3key
                )

                print(insert_query)
                redshift_query_execute(
                    sql=insert_query,
                    cfg=self.cfg,
                    secrets_config=self.secrets_config_redshift,
                    query_type=type,
                )

                print(
                    "inserted data into {}.{} for path ilike {}".format(
                        self.audit_schema, extract_metadata_tbl, S3key
                    )
                )

            if self.extract_config["Incremental"]:
                type = "update"
                update_query = f"""UPDATE {self.audit_schema}.{self.audit_table} SET last_load_date = TO_TIMESTAMP('{self.current_dt}', 'YYYYMMDDHHMISS')  WHERE object_name = '{task_id}';"""

                print(update_query)
                print(
                    "Executing update query for updating mzb_aet_core.tmdm_object for object_name = {}".format(
                        task_id
                    )
                )

                redshift_query_execute(
                    sql=update_query,
                    cfg=self.cfg,
                    secrets_config=self.secrets_config_redshift,
                    query_type=type,
                )
                print(
                    "updated last_load_date of mzb_aet_core.tmdm_object for object_name = {}".format(
                        task_id
                    )
                )
            print("Copy to Mailbox folders Started")
            source_folder = self.cfg["s3_key_extracts_path"] + str(batchid)
            # destination_folder = json_var_val["outbound_extracts"]
            maxKeys = 5000
            delimiter = "/"
            objects = getsingleobjectlist(
                S3Bucket, source_folder + "/", maxKeys, delimiter
            )
            part_files_filter = source_folder + "/" + outFile
            print(part_files_filter)
            keys = list(filter(lambda k: part_files_filter in k, objects))
            print(keys)

            for source_path in keys:

                for elemenent in mailboxes:

                    target_file = source_path.split("/")[-1]
                    target_path = s3_key_mft_outbound + elemenent + "/"
                    print(
                        f"Copying file from {S3Bucket}/{source_path} to s3://{mft_bucket_name}/{target_path}{target_file}"
                    )

                    copy_s3_object(
                        copy_source_bucket=S3Bucket,
                        copy_source_path=source_path,
                        copy_target_bucket=mft_bucket_name,
                        copy_target_path=target_path + f"{target_file}",
                    )
                    print(
                        f"Data Copied from {S3Bucket}/{source_path} to s3://{mft_bucket_name}/{target_path}{target_file}"
                    )
                    if delay:
                        print("Sleeping for {} minutes".format(delay / 60))
                        time.sleep(delay)

        else:
            task_id = self.extract_config["taskName"]
            S3Bucket = self.bucket
            extracts_path = self.extracts_path
            batchid = self.batch_id
            current_dt = self.current_dt
            mailboxes = self.extract_config["MailBoxes"]
            FileNameSuffix = generate_file_suffix(
                self.extract_config["FileNameSuffixPattern"]
            )
            iam_redshift_role = self.iam_redshift_role
            if FileNameSuffix == None:
                outFile = self.extract_config["FileNamePrefix"]
            else:
                outFile = self.extract_config["FileNamePrefix"] + FileNameSuffix
            self.extract_name = outFile
            S3key = extracts_path + str(batchid) + "/" + outFile

            mft_bucket_name = self.mft_bucket_name
            s3_key_mft_outbound = self.cfg["s3_key_mft_outbound"]
            audit_schema = self.audit_schema
            audit_table = self.audit_table

            if self.extract_config["parallel"]:
                parallel_flag = "ON"
            else:
                parallel_flag = "OFF"

            unload_options = [
                f"DELIMITER '{self.extract_config['Delimiter']}' "
                f"EXTENSION '{self.extract_config['Extension']}' "
                f"PARALLEL {parallel_flag} "
                "ALLOWOVERWRITE"
            ]

            if self.extract_config["quotes"]:
                unload_options.append(f" ADDQUOTES")

            selectQuery = (
                "select * FROM "
                + self.extract_config["schemaName"]
                + "."
                + self.extract_config["viewName"]
            )

            unload_query = f"""
                    UNLOAD ('{selectQuery}')
                    TO '{S3key}'
                    IAM_ROLE '{iam_redshift_role}'
                    {' '.join(unload_options)};
                """

            print(unload_query)
            print(
                "Executing Unload Command to unload data from {}.{} to {}".format(
                    self.extract_config["schemaName"],
                    self.extract_config["viewName"],
                    S3key,
                )
            )

            type = "unload"
            redshift_query_execute(
                sql=unload_query,
                cfg=self.cfg,
                secrets_config=self.secrets_config_redshift,
                query_type=type,
            )
            print("Unload Command Finished!")

            # Insert data into extract_metadata_tbl
            if ("extract_metadata_tbl" in self.cfg) and ("insert_query" in self.cfg):
                type = "insert"
                extract_metadata_tbl = self.cfg["extract_metadata_tbl"]
                insert_query = self.cfg["insert_query"].format(
                    self.audit_schema, extract_metadata_tbl, S3key
                )

                print(insert_query)
                redshift_query_execute(
                    sql=insert_query,
                    cfg=self.cfg,
                    secrets_config=self.secrets_config_redshift,
                    query_type=type,
                )

                print(
                    "inserted data into {}.{} for path ilike {}".format(
                        self.audit_schema, extract_metadata_tbl, S3key
                    )
                )

            if self.extract_config["Incremental"]:
                type = "update"
                update_query = f"""UPDATE {self.audit_schema}.{self.audit_table} SET last_load_date = TO_TIMESTAMP('{self.current_dt}', 'YYYYMMDDHHMISS')  WHERE object_name = '{task_id}';"""

                print(update_query)
                print(
                    "Executing update query for updating mzb_aet_core.tmdm_object for object_name = {}".format(
                        task_id
                    )
                )

                redshift_query_execute(
                    sql=update_query,
                    cfg=self.cfg,
                    secrets_config=self.secrets_config_redshift,
                    query_type=type,
                )
                print(
                    "updated last_load_date of mzb_aet_core.tmdm_object for object_name = {}".format(
                        task_id
                    )
                )

            print("Starting Concat/Rename Process using S3Concat Glue job")

            # Header Record Generation

            def generateHeader(query, cfg, secrets_config, type, delim):
                # Execute query to get schema/column names from pg_get_cols
                headers = redshift_query_execute(
                    sql=query, cfg=cfg, secrets_config=secrets_config, query_type=type
                )
                # Concatinate Headers with delimiter
                header_string = delim.join(header[0].upper() for header in headers)
                return header_string

            if self.extract_config["header"]:
                header_query = f"select col_name from pg_get_cols('{self.extract_config['schemaName']}.{self.extract_config['viewName']}') cols(view_schema name, view_name name, col_name name, col_type varchar, col_num int);"
                print(header_query)
                header_record = generateHeader(
                    query=header_query,
                    cfg=self.cfg,
                    secrets_config=self.secrets_config_redshift,
                    type="query",
                    delim=self.extract_config["Delimiter"],
                )
                print(header_record)
            else:
                header_record = "0"

            input_files = f"{S3key}*.{self.extract_config['Extension']}"
            output_file = f"{S3key}.{self.extract_config['Extension']}"

            glue_input_files = input_files[input_files.find("/archive") :]
            glue_output_file = output_file[output_file.find("/archive") :]

            S3ConcatGlueJob = GlueJobOperator(
                task_id=task_id,
                job_name=f"{task_id}_S3Concat",
                job_desc=f"Concat/Rename files for unload of {task_id}",
                iam_role_name=self.iam_glue_role,
                script_location=f"{self.script_path}S3Concat_Rename.py",
                update_config=True,
                create_job_kwargs={
                    "GlueVersion": self.glue_version,
                    "Timeout": self.timeout_glue_job,
                    "DefaultArguments": {  # Default arguments for the job
                        "--input_files": glue_input_files,
                        "--output_file": glue_output_file,
                        "--header_record": header_record,
                        "--enable-metrics": "true",
                        "--bucket": S3Bucket,
                        "--extra-py-files": self.extra_py_files_glue,
                        "--enable-glue-datacatalog": "true",
                        # Additional arguments to pass to the job script
                    },
                    "Connections": {"Connections": [self.connections_glue]},
                },
            )
            gluecontext = get_current_context()
            S3ConcatGlueJob.execute(context=gluecontext)

            print("Concatination and renaming of the unloaded part files finished")

            print("Copy to Mailbox folders Started")

            for elemenent in mailboxes:
                source_path = (
                    glue_output_file[1:]
                    if glue_output_file.startswith("/")
                    else glue_output_file
                )
                target_file = source_path.split("/")[-1]
                target_path = s3_key_mft_outbound + elemenent + "/"
                print(
                    f"Copying file from s3://{S3Bucket}/{source_path}.gz to s3://{mft_bucket_name}/{target_path}{target_file}.gz"
                )

                copy_s3_object(
                    copy_source_bucket=S3Bucket,
                    copy_source_path=f"{source_path}.gz",
                    copy_target_bucket=mft_bucket_name,
                    copy_target_path=target_path + f"{target_file}.gz",
                )

                print(
                    f"Data Copied from {S3Bucket}/{source_path}.gz to s3://{mft_bucket_name}/{target_path}{target_file}.gz"
                )

            # Send email Notification for End of Job/task
        if (
            "generate_ctl_file" in self.extract_config
            and self.extract_config["generate_ctl_file"]
        ):
            self.generate_and_send_ctl_file()

        self.send_email_notification()

    def send_email_notification(self):
        print(self.email_body)

    def generate_and_send_ctl_file(self):
        audit_schema = self.audit_schema
        extract_metadata_tbl = self.cfg["extract_metadata_tbl"]
        v_batchid = self.batch_id
        s3_key_extracts_path = self.extracts_path + str(v_batchid)
        prefix = self.extract_name
        ctl_file_name = (
            "CTL_" + (prefix[:-1] if prefix.endswith("_") else prefix) + ".txt"
        )
        ctl_file_path = "{}/{}".format(s3_key_extracts_path, ctl_file_name)
        bucket_name, ctl_file_key = split_s3_path(ctl_file_path)
        mailboxes = self.extract_config["MailBoxes"]
        mft_bucket_name = self.mft_bucket_name

        sql = """
            with rankedFiles as ( select file_name, file_count, start_time, row_number() over (partition by replace(cast(file_name as VARCHAR),REVERSE(SPLIT_PART(REVERSE((cast(file_name as VARCHAR))),'_',2)),'') order by start_time desc) as rn from {}.{} ) 
            select REVERSE(split_part(REVERSE(file_name),'/',1)) File_Name, file_count File_Count from rankedFiles where rn = 1 and file_name ilike '%{}/{}%' order by file_name
        """.format(
            audit_schema, extract_metadata_tbl, s3_key_extracts_path, prefix
        )

        # Generate ctl file
        return_val = redshift_query_execute(
            sql=sql,
            cfg=self.cfg,
            secrets_config=self.secrets_config_redshift,
            query_type="query",
        )

        fname_list = ["File_Name|File_Count\n"]
        if "split_file" in self.extract_config and self.extract_config["split_file"]:
            for row in return_val:
                file_name = row[0]
                record_count = row[1]
                if record_count > 1:
                    file_data = file_name + "|" + str(record_count) + "\n"
                    fname_list.append(file_data)
        else:
            file_name = (
                (prefix[:-1] if prefix.endswith("_") else prefix)
                + "."
                + self.extract_config["Extension"]
                + ".gz"
            )
            total_cnt = 0
            for row in return_val:
                total_cnt += row[1]
            file_data = file_name + "|" + str(total_cnt) + "\n"
            fname_list.append(file_data)

        trigger_data = "".join(fname_list)
        print(trigger_data)
        s3_put_object(
            bucket_name=bucket_name, key=ctl_file_key, body=trigger_data.encode()
        )
        print("Control File Generated.")

        for mailbox in mailboxes:
            tgt_path = self.cfg["s3_key_mft_outbound"] + mailbox + "/" + ctl_file_name
            time.sleep(180)
            copy_s3_object(
                copy_source_bucket=bucket_name,
                copy_source_path=ctl_file_key,
                copy_target_bucket=mft_bucket_name,
                copy_target_path=tgt_path,
            )
            print("Copied {}/{} to {}/{}".format(bucket_name, ctl_file_key, mft_bucket_name, tgt_path))
