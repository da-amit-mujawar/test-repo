<h3 align="center">Finance Enterprise Sales Variance DAG</h3>

---

<p align="center"> Copy file to redshift and generate report
    <br> 
</p>


### About <a name = "about"></a>

This DAG consists of the following tasks-
1. A Lambda function triggers the dag after receiving an input file in S3. The input file pattern is-
```
{file_pattern}.csv (To be updated)
```
This input file is synced from the folder ```\\stcsanisln01\DSG_Extracts\enterprise_variance``` to S3 bucket ```axle-internal-sources/raw/finance/enterprise/```.

2. This file is loaded into a Redshift table ```interna.enterprise_sales_delta``` for a column updation. 
3. A Report is generated with sample 100 records and emailed to the relevant stakeholders.