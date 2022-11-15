# *DATA-AXLE - Apogee FEC Notification Report*

- Read the report file from s3, attach the file in the email and send it to the business users.
- airflow job location: /idms/dag/apogee-fec-notification/

## Setup and Dependencies

###Variables:   
    - Modify the variable  in airflow and add the following key and value
        "s3-apogee-fec-report": "<BucketName where the report file is dropped>"
    
###Scheduler:
-   The job is currently scheduled to run on 15th of every month. 
-   Airflow runs on UTC, and it is scheduled to start the job on 16th on Every Month at 2:30 AM UTC

###Configuration Parameters File:
####config.json 
    - "email_business_users": "List of Business Users email address seperated by comma(,) enclosed by quotes"
    - "report_file_name": "Report file template. Any change on the date/time part may need code revisit."    
    

