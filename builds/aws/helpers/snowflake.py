# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException
import snowflake.connector.errors as ProgrammingError
import snowflake.connector as sc
import logging


def get_snowflake_conn_details(snowflake_af_conn_id):
    conn_details = BaseHook.get_connection(snowflake_af_conn_id)
    if not conn_details:
        raise AirflowException(f"Connection Details for Connection {snowflake_af_conn_id} not found!")
    return conn_details


def create_snowflake_conn(snowflake_af_conn_id):
    try:
        connection_details = get_snowflake_conn_details(snowflake_af_conn_id)
        print(connection_details)
        connection_details_extra = connection_details.extra_dejson
        
        connection_snowflake = sc.connect(
            user=connection_details.login,
            password=connection_details.password,
            account=connection_details_extra["account"],
            warehouse=connection_details_extra["warehouse"],
            database=connection_details_extra["database"],
            schema=connection_details.schema,
            verify=False,
            insecure_mode=connection_details_extra["insecure_mode"]
            # role=connection_details.role
        )
        return connection_snowflake
    # except snowflake.connector.errors.SnowflakeConnectionError as e:
    #     print(f"Failed to connect to Snowflake: {e}")
    #     raise
    except Exception as e:
        print(f"An Error Occured while connecting to snowflake {e}")
        raise
    
    # conn = sc.connect(
    #     user='',
    #     password='',
    #     account='VHRMOHD-DA_AZR_US_EAST_2',
    #     warehouse='COMPUTE_AZRE2',
    #     database='GEHAPROD',
    #     schema='GEHA_DWH',
    #     automcommit=True,
    #     verify=False,
    #     insecure_mode=True,
    #     role='SYSADMIN')

    return conn


def execute_snowflake_query(sql, snowflake_af_conn_id):
    result = None
    conn = create_snowflake_conn(snowflake_af_conn_id)
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        result = cursor.fetchall()
        print(result)
        return result
    except ProgrammingError as e:
        print(f"Failure Occurred during execution : {e.errno} - {e.msg} ")
        raise
    finally:
        cursor.close()
        conn.close()


def snowflake_copy_stage_data_full(snowflake_af_conn_id, target_table, stage_area, file_format, file_pattern):
    try:

        sql = f"TRUNCATE TABLE {target_table}"
        execute_snowflake_query(sql, snowflake_af_conn_id)
        logging.info(f"Table Truncated {target_table}")
        copy_command = f"""COPY INTO {target_table}
                   FROM @{stage_area}/
                   PATTERN='.*{file_pattern}.*'
                   FILE_FORMAT = (FORMAT_NAME = '{file_format}')
                   ON_ERROR = CONTINUE"""
        result = execute_snowflake_query(copy_command, snowflake_af_conn_id)
        return result
    except Exception as e:
        print(f"Error occured while executing snowflake_copy_stage_data_full function - {e}")
        raise


def snowflake_copy_stage_data_incremental(snowflake_af_conn_id, target_table, stage_area, file_format, file_pattern, key_columns_list):
    try:
        connection_details = get_snowflake_conn_details(snowflake_af_conn_id)
        sql = """SELECT 1 FROM INFORMATION_SCHEMA.STREAMS
                    WHERE STREAM_NAME = 'STREAM_{target_table}'
                    AND TABLE_NAME = 'MY_TABLE'
                    AND SCHEMA_NAME = 'MY_SCHEMA'
                    AND DATABASE_NAME = 'MY_DATABASE' """

        copy_command = f"""COPY INTO {target_table}
                   FROM @{stage_area}/
                   PATTERN='{file_pattern}'
                   FILE_FORMAT = (FORMAT_NAME = '{file_format}')
                   ON_ERROR = CONTINUE"""
        execute_snowflake_query(copy_command, snowflake_af_conn_id)
    except Exception as e:
        print(f"Error occured - {e}")

