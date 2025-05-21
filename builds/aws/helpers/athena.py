import time
import awswrangler as wr


def executenonquery(**kwargs):
    """
    Execute a SQL query on AWS Athena using the provided Boto3 client and monitor its execution status.
    Implements a retry mechanism

    Parameters:
    ----------
    **kwargs : dict
        A dictionary containing the following parameters:
        - boto3_client : boto3.client
            An instance of the Boto3 Athena client.
        - sql : str
            The SQL query to be executed.
        - database : str
            The name of the database in which the query will be executed.
        - output_path : str
            The S3 path where the query results will be stored.
        - default_sleep_time : int, optional (default: 10)
            The time (in seconds) to sleep between checking query execution status.

    Returns:
    -------
    bool
        True if the query execution is successful (status "SUCCEEDED"), False otherwise.

    Example:
    --------
    >>> client = boto3.client('athena', region_name='us-east-1')
    >>> query = "SELECT * FROM table_name"
    >>> database = "my_database"
    >>> output_path = "s3://my-bucket/query-results/"
    >>> success = executenonquery(boto3_client=client, sql=query, database=database, output_path=output_path)
    >>> if success:
    >>>     print("Query execution successful!")
    >>> else:
    >>>     print("Query execution failed!")
    """
    default_sleep_time = kwargs.get("default_sleep_time", 10)

    # mandatory parameters
    client = kwargs.get("boto3_client")
    sql = kwargs.get("sql")
    database = kwargs.get("database")
    output_path = kwargs.get("output_path")

    response = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": output_path},
    )
    query_execution_id = response["QueryExecutionId"]

    status = None

    while True:
        status = client.get_query_execution(QueryExecutionId=query_execution_id)[
            "QueryExecution"
        ]["Status"]["State"]
        print(f"Query execution status: {status}")
        if status in ["FAILED", "SUCCEEDED"]:
            break

        time.sleep(default_sleep_time)

    if status == "SUCCEEDED":
        print(f"Query execution completed with status {status}")
    else:
        print(f"Query execution failed with status {status}")
        status = client.get_query_execution(QueryExecutionId=query_execution_id)[
            "QueryExecution"
        ]["Status"]["State"]

    return status == "SUCCEEDED"


# returns a dataframe
def executequery(**kwargs):
    """
    Execute a SQL query on AWS Athena using the provided Boto3 session and return the query results.
    Use this when you need the sql query output to do further computation . It leverages aws data wrangler
    AWS Data Wrangler is an open-source Python library that enables you to focus on the transformation step of ETL
    by using familiar Pandas transformation commands and relying on abstracted functions to handle the extraction and load steps.

    Parameters:
    ----------
    **kwargs : dict
        A dictionary containing the following parameters:
        - boto3_session : boto3.session.Session
            An instance of the Boto3 session to use for Athena operations.
        - sql : str
            The SQL query to be executed.
        - database : str
            The name of the database in which the query will be executed.
        - output_path : str
            The S3 path where the query results will be stored.
        - entire_dag_max_retries : int, optional (default: 5)
            The maximum number of retries in case of query failures.
        - default_sleep_time : int, optional (default: 60)
            The time (in seconds) to sleep between query retries.

    Returns:
    -------
    pandas.DataFrame
        A DataFrame containing the query results.

    Raises:
    -------
    Exception
        Raises an exception if the maximum number of retries (entire_dag_max_retries) is exhausted.

    Example:
    --------
    >>> session = boto3.session.Session()
    >>> query = "SELECT * FROM table_name"
    >>> database = "my_database"
    >>> output_path = "s3://my-bucket/query-results/"
    >>> result_df = executequery(boto3_session=session, sql=query, database=database, output_path=output_path)
    >>> print(result_df.head())
    """
    failures = 0
    # Optional parameters
    entire_dag_max_retries = kwargs.get("entire_dag_max_retries", 1)
    default_sleep_time = kwargs.get("default_sleep_time", 60)

    # mandatory parameters
    session = kwargs.get("boto3_session")
    sql = kwargs.get("sql")
    database = kwargs.get("database")
    output_path = kwargs.get("output_path")

    while True:
        try:
            return wr.athena.read_sql_query(
                sql=sql,
                database=database,
                s3_output=output_path,
                boto3_session=session,
            )
        except Exception as e:
            failures += 1
            if failures == entire_dag_max_retries:
                raise Exception(
                    "Exhausted maximum number of retries. Last error stacktrace as follows",
                    e,
                )
            time.sleep(default_sleep_time)


def execute_unload_query(**kwargs):
    """
    Execute an UNLOAD query on AWS Athena using the provided Boto3 session and monitor for query completion,
    with retries in case of failures.

    Parameters:
    ----------
    **kwargs : dict
        A dictionary containing the following parameters:
        - boto3_session : boto3.session.Session
            An instance of the Boto3 session to use for Athena operations.
        - sql : str
            The UNLOAD SQL query to be executed.
        - database : str
            The name of the database in which the query will be executed.
        - output_path : str
            The S3 path where the query results will be unloaded.
        - entire_dag_max_retries : int, optional (default: 5)
            The maximum number of retries in case of query failures.
        - default_sleep_time : int, optional (default: 60)
            The time (in seconds) to sleep between query retries.
        - output_format : str, optional (default: "TEXTFILE")
            The format in which the output data should be stored (e.g., "TEXTFILE", "PARQUET").
        - delimiter : str, optional (default: "|")
            The delimiter used in the output data.
        - compression : str, optional (default: "gzip")
            The compression type to be used for the output data (e.g., "gzip", "snappy").

    Raises:
    -------
    Exception
        Raises an exception if the maximum number of retries (entire_dag_max_retries) is exhausted.

    Example:
    --------
    >>> session = boto3.session.Session()
    >>> query = "UNLOAD ('SELECT * FROM table_name') TO 's3://my-bucket/unload-results/'"
    >>> database = "my_database"
    >>> output_path = "s3://my-bucket/unload-results/"
    >>> execute_unload_query(
    >>>     boto3_session=session,
    >>>     sql=query,
    >>>     database=database,
    >>>     output_path=output_path,
    >>>     output_format="PARQUET",
    >>>     delimiter=",",
    >>>     compression="snappy"
    >>> )
    """
    failures = 0
    # Optional parameters
    entire_dag_max_retries = kwargs.get("entire_dag_max_retries", 5)
    default_sleep_time = kwargs.get("default_sleep_time", 60)
    output_format = kwargs.get("output_format", "TEXTFILE")
    delimiter = kwargs.get("delimiter", "|")
    
    # mandatory parameters
    session = kwargs.get("boto3_session")
    sql = kwargs.get("sql")
    database = kwargs.get("database")
    output_path = kwargs.get("output_path")
    compression = kwargs.get("compression")

    while True:
        try:
            wr.athena.unload(
                sql=sql,
                database=database,
                file_format=output_format,
                field_delimiter=delimiter,
                compression=compression,
                path=output_path,
                boto3_session=session,
            )
            print("Unload operation completed successfully")
            break  # Exit loop on success
        except Exception as e:
            failures += 1
            print(f"Attempt {failures} of {entire_dag_max_retries} failed")
            
            if failures == entire_dag_max_retries:
                raise Exception(
                    f"Exhausted maximum number of retries ({entire_dag_max_retries}). "
                    f"Last error stacktrace as follows: {str(e)}"
                )
            
            print(f"Waiting {default_sleep_time} seconds before retry...")
            time.sleep(default_sleep_time)
