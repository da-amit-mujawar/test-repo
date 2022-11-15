<h3 align="center">Email Hygiene Step 1</h3>

---

<p align="center"> Processes and sends file to Oversight
    <br> 
</p>


### About <a name = "about"></a>

This DAG is step 1 of the Email Hygiene automation. It consists of the following tasks-
1. A Lambda function triggers the dag after receiving an input file in S3. The input file pattern is-
```
ehyg_XXX_EmailHygiene_YYYYMMDD.txt (XXX indicates the DBID)
```
This input file is synced from the folder ```\\stcsanisln01-idms\idms\IDMSFILES\ehygiene``` to S3 bucket ```idms-7933-internalfiles/neptune/ehygiene```.

2. This file is loaded into a Redshift table ```ehyg_XXX``` for some basic cleanup. 
3. To return the file to Oversight, the table is unloaded into an S3 bucket with below pattern-
```
s3://{s3-aopinput}/{emailoversight-prefix}_d045000_{dbid}_{yyyy}{mm}{dd}_
```
4. One Report is generated and emailed to the relevant stakeholders.


