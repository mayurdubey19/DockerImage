import json


def create_config(bucket_name, secret_name, list_config_inputs, frequency):
    config = {
        "bucket_name": bucket_name,
        "dps_bucket_name": "axle-a360-aetna-{}-dps",
        "aws_reg_rs": "us-east-2",
        "aws_reg_dp": "us-east-1",
        "aws_reg_mft": "us-east-1",
        "secret_name": secret_name,
        "redshift_db": "prod",
        "request_type": "1",
        "s3_key_reject_path": "archive/outbound/rejects/",
        "metric_report_path": "archive/outbound/metrics",
        "s3_key_extracts_path": "archive/outbound/extracts/",
        "mft_bucket": "axle-a360-aetna-{}-dps",
        "s3_key_mft_outbound": "outbound/",
        "process_outbound_extracts": "extracts/process_outbound_extracts",
        "outbound_extracts": "extracts/outbound_extracts",
        "split_file_maxsize": "2 GB",
        "ctl_file_path":"archive/outbound/{}/ctl_file",
        "qutr_metadata_table":"a360_extract_metadata",
        "insert_query":"insert into {}.{} select path as FILE_NAME,start_time,end_time,line_count as FILE_COUNT,transfer_size as FILE_SIZE, '<batch_id>' as batch_id from stl_unload_log where path ilike '%{}%';",
        "email_business_users_list":"DL-A360-Team@Data-Axle.com",
        "list_config": {}
    }

    for input_item in list_config_inputs:
        key = input_item['key']
        fullfile = input_item['fullfile']
        input_file = input_item['file_pattern']
        if fullfile:
            input_file += f",stg_{key.lower()}_crc_prev"
        if input_item['frequency'] == frequency:
            config["list_config"][key] = {
                "key_prefix": "incoming",
                "file_pattern": input_item['file_pattern'],
                "header": input_item['header'],
                "fullfile": fullfile,
                "stg_load_config": {
                    "task_name": f"STG_{key}",
                    "glue_job": f"STG_FCVRL_{key}",
                    "write_to_parquet": input_item.get('write_to_parquet', True),
                    "redshift_procedure": f"a360_stg.PRC_STG_{key}",
                    "redshift_procedure_params": "BATCH_ID",
                    "table_name": input_item.get('table_name', f"STG_{key}"),
                    "schema_name": "A360_STG",
                    "glue_job_parameters": {
                        "WorkerType": "G.1X",
                        "NumberOfWorkers": 5,
                        "JobBookmark": True,
                        "ScriptFilename": "FCVRL.py",
                        "JobParameters": {
                            "input_file": input_file,
                            "additional-python-modules": "h3==3.7.6,smart-open==6.3.0,openpyxl==3.1.2",
                            "format_file": f"infogroup_{key.lower()}.json"
                        }
                    },
                    "dependencies": []
                },
                "ods_load_config": {
                    "task_name": f"ODS_{key}",
                    "redshift_procedure": f"A360_ODS.PRC_ODS_{key}"
                },
                "reject_mailbox": {
                    f"INFOGROUP_{key}": input_item['reject_mailboxes']
                }
            }

    return config


def generate_json_config():
    bucket_name = "axle-a360-demo-3185"
    secret_name = "mz-aetna-redshift"
    list_config_inputs = [
        {
            "key": "NETWISE",
            "file_pattern": "DAX_NETWISE*",
            "header": True,
            "fullfile": False,
            "reject_mailboxes": [],
            "write_to_parquet": True,
            "frequency": "Weekly"
        },
        {
            "key": "MEDICARE",
            "file_pattern": "DAX_MEDICARE*",
            "header": True,
            "fullfile": True,
            "reject_mailboxes": [],
            "write_to_parquet": True,
            "frequency": "Weekly"
        },
        {
            "key": "MODEL_SCORES",
            "file_pattern": "DAX_MODELSCORES*",
            "header": True,
            "fullfile": False,
            "reject_mailboxes": ["AETN01RT"],
            "write_to_parquet": True,
            "frequency": "Daily"
        }
    ]

    config = create_config(bucket_name, secret_name, list_config_inputs, "Daily")
    print(" ------Printing Daily Config ------ \n {} ".format(json.dumps(config, indent=2)))
    config = create_config(bucket_name, secret_name, list_config_inputs, "Weekly")
    print(" -----Printing Weekly Config ------ \n {} ".format(json.dumps(config, indent=2)))
    config = create_config(bucket_name, secret_name, list_config_inputs, "Monthly")
    print("----- Printing Monthly Config ------ \n {} ".format(json.dumps(config, indent=2)))


if __name__ == "__main__":
    generate_json_config()