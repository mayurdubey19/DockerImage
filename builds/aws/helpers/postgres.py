from airflow.exceptions import AirflowFailException
import psycopg2


def create_pg_connection(secrets):
    #print(secrets)
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
            conn.commit()
            result = "Success"
        cursor.close()
        conn.close()
    except psycopg2.Error as e:
        print("Error connecting to PostgreSQL database:", e)
        raise AirflowFailException(f"Error connecting to PostgreSQL database: {e} ")
    return result
