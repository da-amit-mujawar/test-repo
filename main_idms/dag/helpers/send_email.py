import json
import logging
import os
import os.path
from datetime import datetime
from urllib.parse import urlparse
import boto3
from airflow.models import Variable
from airflow.operators.email import EmailOperator


def send_email_without_config(**kwargs):
    dag = kwargs.get("errormessage")

    report_list = []
    if "report_list" in kwargs:
        report_list = kwargs.get("report_list")

    notification_email = []
    if "email_address" in kwargs:
        notification_email = kwargs.get("email_address")
    else:
        notification_email = Variable.get("var-email-on-success")

    email_business_users = []
    if "email_business_users" in kwargs:
        email_business_users = kwargs.get("email_business_users")

    email_business_users.append(Variable.get("var-email-on-success"))

    attachment_list = []
    s3_object = boto3.client("s3", "us-east-1")

    file_ext = ".csv"
    bucket_name = Variable.get("var-report-bucket-name")

    if "reportname" in kwargs:
        time_stamp = datetime.today().strftime("%Y%m%d")
        key = kwargs.get("reportname")[1:] + "000"
        key = key.replace("{yyyy}{mm}{dd}", time_stamp)
        file_name = os.path.basename(key)
        tmp_file_name = f"/tmp/{file_name[:-3]}{file_ext}"
        if os.path.exists(tmp_file_name):
            os.remove(tmp_file_name)
        try:
            s3_object.download_file(bucket_name, key, tmp_file_name)
        except Exception as e:
            logging.info(
                f"Error downloading report file: {tmp_file_name} error {str(e)}"
            )
            raise Exception(
                f"Error downloading report file: {tmp_file_name} error {str(e)}"
            )
        attachment_list.append(tmp_file_name)
        logging.info(tmp_file_name)

    # if custom report list is passed as arg, download it from s3 if needed
    for report in report_list:
        if report.startswith("s3"):
            try:
                u = urlparse(report, allow_fragments=False)
                tmp_file_name = f'/tmp/{os.path.basename(u.path.lstrip("/"))}'
                s3_object.download_file(u.netloc, u.path.lstrip("/"), tmp_file_name)
            except Exception as e:
                logging.info(f"Error downloading report file: {report} error {str(e)}")
                raise Exception(
                    f"Error downloading report file: {report} error {str(e)}"
                )
        else:
            tmp_file_name = report

        attachment_list.append(tmp_file_name)

    current_env = Variable.get("var-env", "Unknown")
    email_subject = (
        "Airflow " + current_env + " Notification:" + kwargs.get("email_subject")
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


def send_email(config_file, dag, report_list=[], **kwargs):
    with open(config_file) as f:
        config = json.load(f)

    notification_email = []
    if "email_address" in config:
        config_name = config["email_address"]
        if "@" in config_name:
            notification_email = [config_name]
        else:
            notification_email = [Variable.get(config_name)]

    email_business_users = []
    if "email_business_users" in config:
        config_name = config["email_business_users"]
        if "@" in config_name:
            email_business_users = [config_name]
        else:
            email_business_users = [Variable.get(config_name)]
    email_business_users.append(Variable.get("var-email-on-success"))

    attachment_list = []
    s3_object = boto3.client("s3", "us-east-1")

    if not report_list:
        bucket_name = Variable.get("var-report-bucket-name")

        file_ext = ".csv"
        for item in config:
            if "reportname" in item:
                time_stamp = datetime.today().strftime("%Y%m%d")
                key = config[item][1:] + "000"
                key = key.replace("{yyyy}{mm}{dd}", time_stamp)
                file_name = os.path.basename(key)
                logging.info(f"Report is being downloaded: {file_name}")
                tmp_file_name = f"/tmp/{file_name[:-3]}{file_ext}"
                if os.path.exists(tmp_file_name):
                    os.remove(tmp_file_name)
                try:
                    s3_object.download_file(bucket_name, key, tmp_file_name)
                except Exception as e:
                    logging.info(
                        f"Error downloading report file: {tmp_file_name} error {str(e)}"
                    )
                    raise Exception(
                        f"Error downloading report file: {tmp_file_name} error {str(e)}"
                    )
                attachment_list.append(tmp_file_name)
                logging.info(tmp_file_name)

        logging.info(report_list)
    else:
        # if custom report list is passed as arg, download it from s3 if needed
        for report in report_list:
            if report.startswith("s3"):
                try:
                    u = urlparse(report, allow_fragments=False)
                    tmp_file_name = f'/tmp/{os.path.basename(u.path.lstrip("/"))}'
                    s3_object.download_file(u.netloc, u.path.lstrip("/"), tmp_file_name)
                except Exception as e:
                    logging.info(
                        f"Error downloading report file: {report} error {str(e)}"
                    )
                    raise Exception(
                        f"Error downloading report file: {report} error {str(e)}"
                    )
            else:
                tmp_file_name = report

            attachment_list.append(tmp_file_name)

    current_date = datetime.now()
    for item in config:
        if isinstance(config[item], str):
            config[item] = config[item].replace("{dd}", current_date.strftime("%d"))
            config[item] = config[item].replace("{mm}", current_date.strftime("%m"))
            config[item] = config[item].replace("{yyyy}", current_date.strftime("%Y"))
        try:
            file_name = kwargs["dag_run"].conf["file_key"].split("/")[-1]
        except:
            file_name = ""

        config[item] = str(config[item]).replace("{file_name}", file_name)

    current_env = Variable.get("var-env", "Unknown")
    try:
        email_subject = str(config["email_subject"])
        if "{var-env}" not in email_subject:
            email_subject = "Airflow: " + current_env + ": " + email_subject
        else:
            email_subject = email_subject.replace("{var-env}", current_env)
    except:
        email_subject = "Airflow: " + current_env + ": Notification"

    email = EmailOperator(
        mime_charset="utf-8",
        task_id="email_task",
        to=email_business_users,
        cc=notification_email,
        subject=email_subject,
        html_content=config["email_message"],
        files=attachment_list,
        dag=dag,
    )
    email.execute(context=kwargs)
