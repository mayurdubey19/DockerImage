from airflow.models import BaseOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from datetime import datetime
from airflow.utils.decorators import apply_defaults
from helpers.redshift import *
from helpers.send_email import *
from airflow.operators.python import get_current_context


class AwsStgOperator(BaseOperator):
    """
    Custom Airflow operator to run a Spark job using Glue, copy S3 to Redshift,Run Respective Redshift Procedure to add records to source canonical/dp
    and send email notifications.
    """

    @apply_defaults
    def __init__(
        self,
        source_cd,
        email_to=None,
        email_subject=None,
        email_body=None,
        cfg=None,
        output_path=None,
        file_present_ind=None,
        script_path=None,
        format_file_path_glue=None,
        iam_glue_role=None,
        iam_redshift_role=None,
        timeout_glue_job=None,
        maxretries_glue_job=None,
        common_settings_glue=None,
        additional_python_modules_glue=None,
        glue_version=None,
        extra_py_files_glue=None,
        connections_glue=None,
        secrets_config_redshift=None,
        batch_id=None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.source_cd = source_cd
        self.email_to = email_to
        self.email_subject = email_subject
        self.email_body = email_body
        self.cfg = cfg
        self.output_path = output_path
        self.file_present_ind = file_present_ind
        self.script_path = script_path
        self.format_file_path_glue = format_file_path_glue
        self.iam_glue_role = iam_glue_role
        self.iam_redshift_role = iam_redshift_role
        self.timeout_glue_job = timeout_glue_job
        self.maxretries_glue_job = maxretries_glue_job
        self.common_settings_glue = common_settings_glue
        self.additional_python_modules_glue = additional_python_modules_glue
        self.glue_version = glue_version
        self.extra_py_files_glue = extra_py_files_glue
        self.connections_glue = connections_glue
        self.secrets_config_redshift = secrets_config_redshift
        self.batch_id = batch_id

    def execute(self, context):
        # Step 1: Email notification - Job started
        if self.file_present_ind:
            self.send_email_notification()

            # Step 2: Run Spark job using Glue
            self.run_glue_job()

            # Step 3: Copy S3 to Redshift
            # if self.cfg["list_config"][self.source_cd]["stg_load_config"][
            #     "write_to_parquet"
            # ]:
            #     self.copy_s3_to_redshift()
            # else:
            #     print("Skipping copy_s3_to_redshift. Not required for this feed")
            #
            # # Step 3: Run redshift Stage Procedure for further validation/prepare record for DP
            # if (
            #     self.cfg["list_config"][self.source_cd]["stg_load_config"][
            #         "redshift_procedure"
            #     ]
            #     != ""
            # ):
            #     self.run_stg_redshift_proc()

            # Step 4: Email notification - Job completed
            self.send_email_notification()
        else:
            print("No File present. Just Truncate Staging tables")
            self.run_trunc_stg_tbl_redshift()

    def send_email_notification(self):
        print(self.email_body)

    def run_glue_job(self):
        print("starting glue stage job")
        extra_jars = self.script_path + "splittablegzip-1.3.jar"
        submit_glue_job = GlueJobOperator(
            task_id=self.cfg["list_config"][self.source_cd]["stg_load_config"][
                "task_name"
            ],
            job_name=self.cfg["list_config"][self.source_cd]["stg_load_config"][
                "glue_job"
            ],
            script_location=self.script_path
            + self.cfg["list_config"][self.source_cd]["stg_load_config"][
                "glue_job_parameters"
            ]["ScriptFilename"],
            iam_role_name=self.iam_glue_role,
            update_config=True,
            create_job_kwargs={
                "GlueVersion": self.glue_version,
                "NumberOfWorkers": self.cfg["list_config"][self.source_cd][
                    "stg_load_config"
                ]["glue_job_parameters"]["NumberOfWorkers"],
                "WorkerType": self.cfg["list_config"][self.source_cd][
                    "stg_load_config"
                ]["glue_job_parameters"]["WorkerType"],
                "Timeout": self.timeout_glue_job,
                "MaxRetries": self.maxretries_glue_job,
                "DefaultArguments": {  # Default arguments for the job
                    "--input_file": self.cfg["list_config"][self.source_cd][
                        "stg_load_config"
                    ]["glue_job_parameters"]["JobParameters"]["input_file"],
                    "--format_file": self.format_file_path_glue
                    + self.cfg["list_config"][self.source_cd]["stg_load_config"][
                        "glue_job_parameters"
                    ]["JobParameters"]["format_file"],
                    "--common_settings": self.common_settings_glue,
                    "--additional-python-modules": self.additional_python_modules_glue,
                    "--enable-metrics": "true",
                    "--extra-py-files": self.extra_py_files_glue,
                    "--enable-glue-datacatalog": "true",
                    "--extra-jars": extra_jars,
                    # Additional arguments to pass to the job script
                },
                "Connections": {"Connections": [self.connections_glue]},
            },
        )

        gluecontext = get_current_context()
        submit_glue_job.execute(context=gluecontext)

    def copy_s3_to_redshift(self):
        print("starting copy parquet to redshift")
        # print(cfg)
        table_name = self.cfg["list_config"][self.source_cd]["stg_load_config"][
            "table_name"
        ]
        schema_name = self.cfg["list_config"][self.source_cd]["stg_load_config"][
            "schema_name"
        ]

        truncate_cmd = "TRUNCATE TABLE " + schema_name + "." + table_name
        print(truncate_cmd)
        redshift_query_execute(
            sql=truncate_cmd,
            cfg=self.cfg,
            secrets_config=self.secrets_config_redshift,
            query_type="",
        )

        submit_copy_cmd = (
            "COPY "
            + schema_name
            + "."
            + table_name
            + " FROM '"
            + self.output_path
            + table_name.upper()
            + "/part' IAM_ROLE '"
            + self.iam_redshift_role
            + "' FORMAT AS PARQUET"
        )
        print(submit_copy_cmd)
        redshift_query_execute(
            sql=submit_copy_cmd,
            cfg=self.cfg,
            secrets_config=self.secrets_config_redshift,
            query_type="",
        )
        print("copy_s3_to_redshift succeeded for " + self.source_cd)

    def run_stg_redshift_proc(self):
        print("starting redshift procedure")
        procedure_call = (
            "CALL "
            + self.cfg["list_config"][self.source_cd]["stg_load_config"][
                "redshift_procedure"
            ]
            + "("
            + str(self.batch_id)
            + ")"
        )
        print(procedure_call)
        redshift_query_execute(
            sql=procedure_call,
            cfg=self.cfg,
            secrets_config=self.secrets_config_redshift,
            query_type="",
        )
        print("Finished stage job stg_" + self.source_cd)

    def run_trunc_stg_tbl_redshift(self):
        print("starting Truncate Staging tables")
        query_call = "TRUNCATE TABLE {}.{} ".format(
            self.cfg["list_config"][self.source_cd]["stg_load_config"]["schema_name"],
            self.cfg["list_config"][self.source_cd]["stg_load_config"]["table_name"],
        )
        print(query_call)
        redshift_query_execute(
            sql=query_call,
            cfg=self.cfg,
            secrets_config=self.secrets_config_redshift,
            query_type="",
        )
        if (
            "other_stg_tables"
            in self.cfg["list_config"][self.source_cd]["stg_load_config"]
            and self.cfg["list_config"][self.source_cd]["stg_load_config"][
                "other_stg_tables"
            ]
        ):
            for table in self.cfg["list_config"][self.source_cd]["stg_load_config"][
                "other_stg_tables"
            ]:
                query_call = "TRUNCATE TABLE {}".format(table)
                print(query_call)
                redshift_query_execute(
                    sql=query_call,
                    cfg=self.cfg,
                    secrets_config=self.secrets_config_redshift,
                    query_type="",
                )