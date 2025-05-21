import psycopg2
from airflow.exceptions import AirflowFailException
from sqlalchemy import create_engine


def start_rds_conn(cfg, secrets_config):
    secret_name = cfg["secret_name"]
    region = cfg["aws_reg_rs"]
    env = cfg["postgres_db"]
    response = secrets_config
    connection = psycopg2.connect(
        dbname=f"{env}",
        user=response["username"],
        password=response["password"],
        host=response["host"],
        port=response["port"]
    ) 
    connection.autocommit = True
    return connection


def rds_query_execute(sql, cfg, secrets_config, query_type, **kwargs):
    conn = start_rds_conn(cfg, secrets_config)
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
    except psycopg2.Error as err:
        raise AirflowFailException(f"Error Occurred - {err.pgerror}")
    
    return result
