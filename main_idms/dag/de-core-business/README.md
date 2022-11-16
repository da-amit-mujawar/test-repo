# *DATA-AXLE - BUSINESS DB PIPELINE*
Code repo to build, schedule and run pipelines for Data-Axles Business DB files.
This business DAG runs daily at 21 PM EST to ingest business/places files of 9 different types into equivalent redshift tables.
Below are the input locations for full load and changes load:
s3://axle-gold-sources/places/full/
s3://axle-gold-sources/places/changes/

Full load is received and processed on Sunday and incremental load is processed on rest of the days.

### BUSINESS PIPELINE AIRFLOW FOLDER: nua-airflow/idms/dag/de-core-business

### DAG Name: de-core-business


### CONFIGURATIONS: /config
- Default and Template config templates at service level
- config.json at the same location of business repo
- This config file takes below parameters:
1. input_bucket : Input bucket for business files
2. input_prefix_full : Input path prefix for full load files
3. input_prefix_changes : Input path prefix for changes files
4. file_extension : File extension of input files
5. full_load_day : Day on which full load will run
6. counts_table : Table name for maintaining table counts for all files everyday after successful run
7. file_types : Dictionary of file types for which tables to be populated
Below fields are retrieved from Variables:
1. output_bucket : Output bucket for manifest file
2. output_path : Output path for manifest file

### FILE TYPES:
- benefit_plans
- contacts
- happy_hours
- images
- intents
- operating_hours
- places
- tags
- ucc_filings


### OPERATORS used in this DAG: /operators
- Redshift Operator (redshift.py)
- Generic Redshift Operator (generic_redshift.py)


### DAGS step-by-step:
- Input files are read for current day and manifest file is created
- Based on the day, either full load or changes load is triggered
- Files are copied from S3 to respective redshift tables using manifest file
- Post copies are performed including ALTER and DROP table commands on temporary tables
- Counts are prepared from all 9 tables and stored in business_counts redshift tables.

### Note:
Redshift Operator and Generic Redshift Operator uses IAM roles which are coming from Airflow Variable.