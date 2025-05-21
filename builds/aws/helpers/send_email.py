import json
import logging
import os
import os.path
from datetime import datetime
from urllib.parse import urlparse
import boto3
from airflow.models import Variable
from airflow.operators.email import EmailOperator
from airflow.operators.python import get_current_context


def send_email(**kwargs):
    """
    Sends an email notification with optional attachments.

    Parameters:
    ----------
    **kwargs : dict
        A dictionary containing the following parameters:
        - errormessage : airflow.models.DAG
            The Airflow DAG object.
        - report_list : list, optional (default: [])
            A list of report file paths or S3 URLs to be attached to the email.
        - client : str, optional
            Market Zone client information.This is used to pick up the specific email address from Variables var-email-on-success-<client>
        - email_address : str, optional
            The notification email address.
        - email_business_users : list, optional (default: [])
            A list of email addresses for business users.
        - email_subject : str
            The subject of the email.
        - email_message : str
            The HTML content of the email.

    Raises:
    -------
    Exception
        Raises an exception if there is an error downloading report files from S3.

    Example:
    --------
    >>> send_email_without_config(
    >>>     errormessage=my_dag,
    >>>     report_list=['s3://my-bucket/report1.csv', 's3://my-bucket/report2.csv'],
    >>>     client='ClientName',
    >>>     email_address='notify@example.com',
    >>>     email_business_users=['business@example.com', 'another@example.com'],
    >>>     email_subject='Task Completion Notification',
    >>>     email_message='<p>Your task has been completed successfully.</p>'
    >>> )
    """

    current_env = Variable.get("var-env", "local")
    if current_env == "local":
        print("Email alert is not required")
    else:
        dag = kwargs.get("errormessage")

        report_list = []
        if "report_list" in kwargs:
            report_list = kwargs.get("report_list")

        if "client" in kwargs:
            mzb_client = kwargs.get("client")

        notification_email = []
        if "email_address" in kwargs:
            notification_email = kwargs.get("email_address")
        else:
            notification_email = Variable.get(
                "var-email-on-success-{}".format(mzb_client)
            )

        email_business_users = []
        if "email_business_users" in kwargs:
            email_business_users = kwargs.get("email_business_users")
        #Added by Mitesh on 9th Dec 2024 
        else:
            email_business_users.append(notification_email)

        #email_business_users.append(notification_email)

        attachment_list = []
        s3_object = boto3.client("s3", "us-east-2")

        # if custom report list is passed as arg, download it from s3 if needed
        for report in report_list:
            if report.startswith("s3"):
                try:
                    u = urlparse(report, allow_fragments=False)
                    tmp_file_name = f'/tmp/{os.path.basename(u.path.lstrip("/"))}'
                    s3_object.download_file(u.netloc, u.path.lstrip("/"), tmp_file_name)
                    logging.info(
                        f"Success downloading report file: {report} : {tmp_file_name}"
                    )
                except Exception as e:
                    logging.info(
                        f"Error downloading report file: {report} error {tmp_file_name}"
                    )
                    raise Exception(
                        f"Error downloading report file: {report} error {str(e)}"
                    )
            else:
                tmp_file_name = report

            attachment_list.append(tmp_file_name)

        current_env = Variable.get("var-env", "Unknown")
        if "email_business_users" in kwargs:
            email_subject = "PROD - " + kwargs.get("email_subject") if current_env =="Prod" else "DEV - " + kwargs.get("email_subject")

        else:
            email_subject = "Airflow {} Notification For {} : {}".format(
                current_env, mzb_client, kwargs.get("email_subject")
            )
        email_message = kwargs.get("email_message")

        email = EmailOperator(
            mime_charset="utf-8",
            task_id="email_task",
            to=email_business_users,
            cc=notification_email,
            subject=email_subject,
            html_content=email_message,
            files=attachment_list,
            dag=dag,
        )

        email.execute(context=kwargs)
        if report_list:
            os.remove(tmp_file_name)
