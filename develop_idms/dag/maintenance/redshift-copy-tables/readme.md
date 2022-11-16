# Utility to copy redshift table  across aws  account or within same account across different clusters

#
Below are the mandatory parameters required to run the job through airflow:
1. copy-info >> this key shall have details about source and destination redshift connection, and full-schema flag.
2. tables-to-copy >> list of tables to copy in format schema.tablename

#example :

#for partial tables load
{"copy-info": {"src": "var-redshift-postgres-conn", "dest": "var-redshift-da-rs-01-connection", "full-schema": "false"},
"tables-to-copy": ["public.airflow_customer_bak", "public.airflow_sales"]}

# for full schema load
{"copy-info": {"src": "var-redshift-postgres-conn", "dest": "var-redshift-da-rs-01-connection", "full-schema": "true"}
}


#Pre-requites
1. IAM role should have access to both source and destination redshift connection.
2. Data will be first copied to a s3 bucket, so that bucket needs to have necessary permissions.

#Config File:
1. iam_role - iam role that will be used to unload/copy data . this can be moved to airflow variables if required.
2. s3_base_path - s3 bucket base path that will be used to unload data
3. tbl_owner - if grant of the table required. for now code is disabled for the same

#Deployment
dag name : 'redshift-copy-tables'
Run the dag via Airflow and pass necessary run time arguments as shown above.