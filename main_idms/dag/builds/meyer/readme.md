# Builds - Meyer (MZ)
- New build for Market Zone client: Meyer. 
- Load Meyer Lookup tables as source to Child views. 
- Cleanse required data 
- Generate new PIN every build for each record (unique id within school) 
- Create/calculate aggregated Columns 
- Apply AOP suppressions 
- Apply house suppressions 
- Create Email Delta table and export
- Aggregated field audit reports


## Key Data Elements
- MYRSCHOOLID required on maintable and school child table
- MCDINDIVIDUALID key to orders


## Dependencies and Data Axle Sources
- Run after ETL Build as this requires main table creation


## Processing Instructions
- **List Admin:**
  - Convert all lists up to status 120.
  - In Campaign ETL - Build Load Status screen - change status from 10 to 15 
  - Wait for completion (this will create tblmain and generate audit reports)
  - Review audit reports
  - Submit JIRA ticket for build 
  - 
- **IDMS TEAM: Run DAGs in the following order, be sure to heed notations**
  - NOTE: AOP RERUN PROCESSING: A06 – 100 records & A07 – 200 records: 
    - These run in pairs to kick off a 3rd automation process, never send separately.
    - Never RERUN before receiving previous A07 output. 
      - Contact DPServices Cyndi Durham to ensure no files are still in waiting status
    
  1. **Run meyer-step1-aop-send**
    - email hygiene (output: a04p_d045000_myr_yyyymmdd_000 to 3rd party via aop)
    - AOP Step 1: Export 100/Previous tblMain to WB (output: A06.MEYER.PROMOHISTORY.INPUT.csv000) 
    - AOP Step 2: Export 200/Current tblMain to WB  (output: A07.MEYER.CURRENT.INPUT.csv000)
    - email hygiene process runs overnight, WB AOP process runs ~4 hrs 
    - 
  2. **Wait for meyer-step1 return files to be received from aop (A07)**
  - Run meyer-step2-aop-return with return file A07.MEYER.IDMS.RETURN.FILE.gz
    - (output: a05_meyer_mcd_input.txt) 
    - MCD process runs ~2-3 hrs)
  - 
  3. **Run meyer-step3-mcd-return with return files a05_meyers_work_matched.dat, a05_meyers_xref.dat**
  - 
  4. **Run meyer-step4-aggregations (load child tables, aggregations, suppressions, model, email delta)**
  - 
- **Run these DAGs AFTER all previous steps completed dependent on meyer aggregations)**
  5. **Run meyer-step5-ehyg-return with return file a04p_d045000_myr_yyyymmdd_000_{jobid}-processed** 
     - May have invalid records, had to clean out manually in Oct/Dec run.
     - Add logic as necessary to clean email in step 1 for future.
  - **Run meyer-step6-mcd-xref-update (promo history).**
    - Wait for user approval of reports, then move return files to processed folder (a04,a05,a07)
    - Close JIRA ticket
  6. **Run meyer-step7-renamed-files** <br>
    - Adds date to files and moves to processed subfolder
    - Copies mcd file to client bucket axle-customer-meyer/mcdoriginal file name (idms-7933-aop-output)new filenamecopy to


## Testing
- Requires Meyer Implementation team approval - coordinated by Lisa Knox  
- Copy current and previous main tables from production to dev
- Refresh input files 010-016 files from s3 list-conversion folder

## UPCOMING mods
- Add lambda to auto-exec step2 (A07 file) 
- Add lambda to auto-exec step3 (a05 file)
- After completion of step04, check for (a04 file) to auto-start step-5

## Modifications ##
- **Initial Aug-Sep 2021 build** 
- IDMS-1070/1269: MZ Meyers - Build Special Requirements - PIN Generation
- IDMS-1257: MZ Meyers - Child Table School File
- IDMS-1258: MZ Meyers - Child Table Rate Chart
- IDMS-1259: MZ Meyers - Child Table First Degree
- IDMS-1260: MZ Meyers - Child Table Generic Salutation
- IDMS-1261: MZ Meyers - Child Table Lookup State NYLMet Flag
- IDMS-1262: MZ Meyers - Child Table Standardized Title
- IDMS-1360: MZ Meyers - Build Special Requirements - Calculated Fields
- IDMS-1263: MZ Meyers - House Suppression flags 
- IDMS-1270: MZ Meyers - Campaign Prioritization By School
- IDMS-1362: MZ Meyers - Email Delta File
- IDMS-1459: MZ Meyers - Mailable Flag
- IDMS-1363: MZ Meyers - Aggregate Summaries by List and total Db
- IDMS-1473: MZ Meyers - Modify MyrFinalGenders and House suppression
- 
- **Oct-2021 build** 
- IDMS-1559: MZ Meyers - Refactor myrformaljointsalutation aggregation
- IDMS-1559: MZ Meyers - Add schoolid to email delta file
- IDMS-1579: MZ Meyers - Add month to PIN generation
- IDMS-1533: MZ Meyers - AOP/MCD processing - send consolidated file, refactor updates
- IDMS-1550: MZ Meyers - Change input file formats
- IDMS-1602: MZ Meyers - Increase widths on name fields
- 
- **Nov-2021 build**
- IDMS-1663: MZ Meyers - Enhance Sqlserver Helper and Redshift Operator 
  - to capture previous Build & Other Build Info for 
  - email-delta, models and mcd promo-history processing
- 
- **Jan-2022 build**
- IDMS-1734: Remove uppercase on myrsuffix, myrspousesuffix via configuration
- 
- **Aug-2022 build**
- IDMS-2335: Provide XREF file to Meyer after each monthly update
- 
- **Nov-2022 build**
- IDMS-2464: Modify House Suppression logic
- IDMS-2465: Add Product Codes 
- IDMS-2466: Modify PIN Prefix logic



## Created with Contributions By
* **Caroline Burch** - *Initial*
* *Michael Scott* - *e-Hygiene*
* *Elina Bor* - *AOP/MCD Process setup*
* *Jayesh Patel* - *Model*
* *Caroline Burch* - *Oct 2021 build updates*
* *Saravanan Ramalingam* - *Promo history*
* *Caroline Burch* - *Nov 2021 build updates*
* *Caroline Burch* - *Jan 2022 build updates*
* *Triyanshi Gupta* - *Aug 2022 build updates*
* *Caroline Burch* - *Nov 2022 build updates*
