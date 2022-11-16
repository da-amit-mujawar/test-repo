# *Connect SFTP with S3 and transfer files*
These DAGs are used to transfer files from SFTP to S3 and S3 to SFTP.

You need to use s3_conn_id created in Connections in Airflow.
Currently used s3 connection id is : s3_conn_id

Also, you need to create sftp connection id in Connections in Airflow.
Using the server, username and password and use this in sftp_conn_id.

These DAGs expect config file at the same directory containing required variables.

## 1. DAG: sftp_to_s3_dag
This DAG copies file from sftp server to s3 folder
Config: sftp_to_s3_config.json

## 2. DAG: sftp_dag
This DAG downloads file from S3 to local and uploads file from local to sftp server
Config: s3_to_sftp_config.json