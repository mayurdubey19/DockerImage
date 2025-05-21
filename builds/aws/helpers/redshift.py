import redshift_connector
# from helpers.common import get_secret
from airflow.exceptions import AirflowFailException
from sqlalchemy import create_engine
import time
import traceback
from airflow.models import Variable


def start_redshift_conn(cfg, secrets_config):
    secret_name = cfg["secret_name"]
    region = cfg["aws_reg_rs"]
    env = cfg["redshift_db"]
    print(env)
    response = secrets_config
    connection = redshift_connector.connect(
        host=response["host"],
        database=f"{env}",
        user=response["username"],
        password=response["password"],
    )
    connection.autocommit = True
    return connection

def log_debug_info(conn, cursor, cfg):
    print(f"Connection object: {vars(conn)}")
    print(f"Cursor object: {vars(cursor)}")
    print(f"Catalogs: {cursor.get_catalogs()}")
    print(f"Schemas: {cursor.get_schemas()}")
    print(f"Config - {cfg}")
    curr_env = Variable.get("var-env", "local")
    print(curr_env)


def redshift_query_execute(sql, cfg, secrets_config, query_type, **kwargs):
    max_retries = 3
    attempts = 0
    while attempts < max_retries:

        conn = start_redshift_conn(cfg, secrets_config)
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
                print(f"{cursor.rowcount} rows affected!")
                result = "Success"
            cursor.close()
            conn.close()
            break 
        except redshift_connector.error.ProgrammingError as err:
            
            err_msg = str(dict(err.args[0]).get('M')).replace('"',' ') 
            if attempts + 1 < max_retries and all(word.lower() in err_msg.lower() for word in ['schema','does','not','exist']):
                print(traceback.format_exc())
                log_debug_info(conn, cursor, cfg)
                cursor.close()
                conn.close()
                attempts = attempts + 1
                print('Retrying after 5 seconds')
                time.sleep(5)
            else:
                raise AirflowFailException(f"Error Occurred - {err}")
    return result


def get_redshift_sqlalchemy_conn(region, secret_name, env, secrets_config):
    response = secrets_config
    engine = create_engine(
        "postgresql://{}:{}@{}:5439/{}".format(
            response["username"], response["password"], response["host"], env
        )
    )
    return engine