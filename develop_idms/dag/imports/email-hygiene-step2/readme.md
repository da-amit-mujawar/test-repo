<h3 align="center">Email Hygiene Step 2</h3>

---

<p align="center"> Process the returned oversight file and email URLs of unloaded files
    <br> 
</p>


### About <a name = "about"></a>

This DAG is step 2 of the Email Hygiene automation. It consists of the following tasks-
1. A Lambda function triggers the dag after receiving an input file in S3 bucket ```idms-7933-aop-output```. The input file pattern is-
```
a10_d045000_XXX_YYYYMMDD_3032559-processed (XXX indicates the DBID)
```

2. This file is loaded into a Redshift table ```ehyg_XXX_EO_Returns``` and is utilised to add validation flags in the ```ehyg_XXX``` table generated in Step 1. 
3. Two tables- ```ehyg_XXX_Suppressions``` and ```ehyg_XXX_IDMS``` are created and their data is unloaded into 2 files in S3.
4. All the tables previously created are used to build an excel report with 6 sheets.
5. Three presigned URLs for the below files are generated and emailed to Lisa Fatta. Output file patterns-
```
Report file- ehyg_XXX_report.xlsx
Suppresions- ehyg_XXX_YYYYMMDD_supressions_
IDMS- ehyg_XXX_EmailHygiene_Flags_
```
6. All the created tables are dropped and input file is moved to ```Processed``` folder
