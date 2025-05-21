from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
import sys
import json
import os
import selectors
import subprocess
import shlex
import cx_Oracle
import psycopg2
import traceback
import redshift_connector
import pymssql

class DAXRedshiftProcOperator(BaseOperator):
    template_fields = ('secret_name ', 'region', 'profile_name', 'procedure_name', 'parameters')

    @apply_defaults
    def __init__(
        self,
        secret_name ,
        procedure_name,
        parameters,
        common_settings,
        config_json_file,
        *args, **kwargs
    ):
        super(DAXRedshiftProcOperator, self).__init__(*args, **kwargs)
        self.secret_name  = secret_name 
        self.region = region 
        self.profile_name = profile_name 
        self.procedure_name = procedure_name
        self.parameters = parameters
        self.common_settings = common_settings
        self.config_json_file = config_json_file

    def execute(self, context):
        self.log.info("Starting operator execution.")
        try:
            self.log.info("Reading common settings.")
            common_settings_data = self._read_common_settings()
            self.log.info("Common settings successfully read.")

            self.log.info("Reading database configuration.")
            db_config = self._get_aws_secret()
            self.log.info("Database configuration successfully read.")

            self.log.info("Connecting to the database.")
            connection = self._connect_to_db(db_config)
            self.log.info("Successfully connected to the database.")

            self.log.info("Sending start notification.")
            self._send_notification(common_settings_data, "Procedure execution started")
            
            self.log.info("Executing stored procedure.")
            output_values = self._execute_procedure(connection)
            self.log.info("Stored procedure executed successfully.")
            
            self.log.info("Sending success notification.")
            self._send_notification(common_settings_data, "Procedure executed successfully")

            if output_values:
                self.log.info("Processing output values:")
                for param, value in output_values.items():
                    self.log.info(f" - {param}: {value}")
            else:
                self.log.info("No output values to process.")
            
        except Exception as e:
            self.log.error(f"Procedure execution failed: {str(e)}")
            if 'common_settings_data' in locals():
                self._send_notification(common_settings_data, f"Procedure execution failed: {str(e)}")
            else:
                self.log.error("Unable to send failure notification due to missing common settings data.")
            raise
        finally:
            if 'connection' in locals():
                self.log.info("Closing database connection.")
                connection.close()
                self.log.info("Database connection closed.")
            else:
                self.log.warning("Database connection could not be closed because it was never established.")

    def _get_aws_secret(self):
    
        self.log.info(f"Attempting to retrieve secret: {self.secret_name} from region: {self.region_name}")

        session = boto3.Session(profile_name=self.profile_name) if self.profile_name else boto3.Session()
        secret_client = session.client("secretsmanager", region_name=self.region_name)
        
        try:
            response = secret_client.get_secret_value(SecretId=self.secret_name)
            secret = json.loads(response['SecretString'])
            self.log.info(f"Successfully retrieved secret: {self.secret_name}")
            return secret
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == "ResourceNotFoundException":
                self.log.error(f"Secret not found: {self.secret_name}")
                self._send_notification(common_settings_data, f"Procedure Execution Failed.\nSecret not found: {self.secret_name}")
                raise AirflowException(f"Secret not found: {self.secret_name}") from e
            else:
                self.log.error(f"An error occurred: {e}")
                self._send_notification(common_settings_data, f"Procedure Execution Failed.\nAn error occurred while retrieving the secret.")
                raise AirflowException(f"An error occurred while retrieving the secret: {e}") from e

    def _read_common_settings(self):
        self.log.info("Attempting to read common settings.")
        try:
            self.log.info(f"Opening common settings JSON file: {self.common_settings}")
            with open(self.common_settings, 'r') as file:
                common_settings_data = json.load(file)
                self.log.info("Successfully read and parsed common settings JSON file.")
                return common_settings_data
        except FileNotFoundError:
            self.log.error(f"Common settings JSON file not found: {self.common_settings}")
            raise AirflowException("Common settings JSON file not found.")
        except json.JSONDecodeError as json_err:
            self.log.error(f"Error parsing common settings JSON file: {json_err.msg}")
            raise AirflowException("Error parsing common settings JSON file.")
        except Exception as e:
            self.log.error(f"Unexpected error while reading common settings: {str(e)}")
            raise AirflowException("Unexpected error reading common settings configuration.")

    
    def _connect_to_db(self, db_config):
        self.log.info("Attempting to connect to the Redshift database.")
        try:
            connection_string = f"host={db_link['host']} port={link['port']}, dbname={link['dbname']}, user={db_config['user']}"
            self.log.info(f"Connecting to Redshift database at {connection_string}")

            connection = redshift_connector.connect(
                host=db_config['host'],
                port=db_config['port'],
                database=db_config['dbname'],
                user=db_config['user'],
                password=db_config['password']
            )
            connection.autocommit = True
            self.log.info("Successfully connected to the Redshift database.")
            return connection
            
        except redshift_connector.Error as e:
            self.log.error("Redshift database connection failed.")
            error_details = f"Error: {e.msg}, SQLState: {getattr(e, 'sqlstate', 'N/A')}, Error Code: {getattr(e, 'errno', 'N/A')}"
            self.log.error(error_details)
            raise AirflowException(f"Failed to connect to Redshift database. {error_details}")

    def _create_sql_call(self):
        proc_name = self.proc_name
        params = self.params
        
        var_order = params['Variable_Order']
        placeholders = []
        
        for var in var_order:
            if var in params['IN'] or var in params['INOUT']:
                placeholders.append(f"%({var})s")
            elif var in params['OUT']:
                placeholders.append(f"@{var} OUTPUT")
        
        sql_call = f"CALL {proc_name}({', '.join(placeholders)})"
        self.log.info("Procedural Call: " + sql_call)
        return sql_call
    
    def _execute_procedure(self, connection):
        proc_name = self.proc_name
        params = self.params
        
        self.log.info("Starting procedure execution.")
        
        sql = _create_sql_call(self)

        try:
            cursor = connection.cursor()
            self.log.info(f"Executing SQL: {sql} with parameters: {params}")
            cursor.execute(sql, params['IN'])
            result = cursor.fetchall()
            cursor.close()
            self.log.info("Procedure executed successfully.")
            return result
        except redshift_connector.Error as err:
            self.log.error(f"Error occurred: {err}")
            cursor.close()
            raise AirflowException(f"Error Occurred - {err}")
            

    def _send_notification(self, common_settings_data, message):
        subject = f"{self.procedure_name} Execution Status"
        notify_email = common_settings_data.get('MZP_NOTIFY_DEFAULT', 'default_notify@example.com')
        
        self.log.info("Preparing to send email notification.")
        self.log.info(f"Notification subject: {subject}")
        self.log.info(f"Notification recipient: {notify_email}")
        self.log.info(f"Notification message: {message}")
        
        command = f"echo \"{message}\" | mail -s \"{subject}\" \"{notify_email}\""
        
        try:
            result = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            self.log.info("Email notification sent successfully.")
            
            if result.stdout:
                self.log.info(f"Notification command stdout: {result.stdout.decode().strip()}")
            if result.stderr:
                self.log.error(f"Notification command stderr: {result.stderr.decode().strip()}")
        except subprocess.CalledProcessError as e:
            self.log.error(f"Failed to send email notification. Command error: {e.stderr.decode().strip()}")
