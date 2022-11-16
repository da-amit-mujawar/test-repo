# Data-Axle Non-Profit Database Builds
  Donor Data (Apogee-74) <br> 
  DonorBase  (1438)

## Steps To Run Job for Donor Data (Apogee-74):
1) Validate List Conversions (10 hrs for full run of 2,000+ lists) <br> 
- a. [nonprofit-validate-export-data](http://10.251.130.9:8080/tree?dag_id=nonprofit-validate-export-data) <br> 
        run all with DS exports: **{"databaseid":74,"buildid":23456,"ownerid":0,"exporttransaction":"Y"}** <br>
        reruns by owner: **{"databaseid":74,"buildid":23456,"ownerid":10005,"exporttransaction":"Y"}**  <br>
- b. [nonprofit-pivot-listconversion-audit-report](http://10.251.130.9:8080/tree?dag_id=nonprofit-pivot-listconversion-audit-report) 
       {"databaseid":74} <br>
- c. Rerun validate and pivot reports until User Approval obtained (1-2 days). <br> <br>
2) Generate Rollups (5.5 hrs)** <br>
    this steps refreshes _all builds_ with new list summary info (child13 - child16) <br>
    [builds-summary-calc-apogee-All](http://10.251.130.9:8080/tree?dag_id=builds-summary-calc-apogee-All) <br> <br>
3) Create Transaction and HH Summary table (0.5 hr) <br>
    this steps refreshes _all builds_ with new trans/summary info (child1, child3) <br>
    [builds-nonprofit-biweekly-process](http://10.251.130.9:8080/tree?dag_id=builds-nonprofit-biweekly-process) <br> <br>

FOR MONTHLY RUNS: <br>
4) Load Consumer Universe file when available (monthly) AFTER HOURS (7.5 hrs) <br>
    [builds-nonprofit-monthly-step1-load-consumer-db](http://10.251.130.9:8080/tree?dag_id=builds-nonprofit-monthly-step1-load-consumer-db) 
    **{"databaseid":74}** <br> <br>
5) Load Donors into Main Table (monthly 1.25 hrs) <br>
    [builds-nonprofit-monthly-step2-load-donors](http://10.251.130.9:8080/tree?dag_id=builds-nonprofit-monthly-step2-load-donors) <br> <br>

6) ON THE NIGHT BEFORE OR MORNING OF LIVE DATE, proceed to schedule: <br> <br>

7) Create child tables from other sources and final views (monthly 0.25 hr) <br>
    [builds-nonprofit-monthly-step3-child-tables](http://10.251.130.9:8080/tree?dag_id=builds-nonprofit-monthly-step3-child-tables) <br> <br>
8) Generate Exports (monthly 1hr) <br>
    [builds-nonprofit-monthly-step4-exports](http://10.251.130.9:8080/tree?dag_id=builds-nonprofit-monthly-step4-exports) <br> <br>
9) Activate Build (monthly) <br>
    [builds-nonprofit-monthly-step5-activate-build](http://10.251.130.9:8080/tree?dag_id=builds-nonprofit-monthly-step5-activate-build) <br> <br>
    **{"databaseid":74}** <br> <br>


### Data Sources Summary
  - 1000+ Members Apogee, 430+ Members Donorbase
  - Consumer Universe DB (active and inactive) ~600 million
  - Non-Matches to Consumer Universe are available for selection
  - Monthly Builds with bi-weekly Member refreshes available 
  - Database keyed on Individual level
  - Rollup/Aggregates on Individual (1438) or Household (74) levels
  - Source unique names *(see RETAIN-DONORS and Apply-Default-Rules)*
    - dropped for certain members
    - retained for certain members for their own use
  - Undeliverables (mail score 5) and Deceased dropped
  - Validations
    - Gifts de-duped
    - Gifts >$1,000,000 dropped
    - Emails and Phones retained & exportable, no hygiene processes

### Database Schema
- Main table: Name, Address and Consumer Attributes
- Transactions: Detail level Donation/Gift with dates and categorical information
- Individual/Household Rollup tables (3,700 attributes)


### List Conversion & AOP Processing Rules
- DDI User processes
  - NCOA is done selectively for members who signed PAF
  - New Donor Lists require DWAP approval
  - New Transaction and Suppression lists must be disapproved in DWAP 
  - Mailer and Offer Approvals not required
  - Consumer Universe (Apogee:21318 DB:19946) added to new build with status 140 (list in dw)
  - Set up **RETAIN-DONORS** rule in List Of Lists
  - **Apply-DEFAULT-Rules** is set to ON in Segment screen to exclude unique donors
    - this rule is maintained at a list level in tblMasterLoL
    - Turn off this flag ONLY when selecting unique names for member
    
- **donorbase-suppression** at individual and hh level - daily@5am
  - Applying house suppressions at individual and hh level 
  - Always Uses Same Build - 23710: Suppression Files

- APPLY **CCPA-Suppressions** 
  - donorbase-CCPA-Suppression.py - daily@5:30am
  - create donorbase_ccpa_suppression
  - load s3://{s3-donorbase-silver}/etl/build-output/DW_Final_1438_23710_20321/
  - delete matches from maintable by household (CompanyMC)
  - notification

- **DAG: Load-Donors**
  - s3 bucket: s3-donorbase-silver
  - Custom Functions: helpers.donorbase
  - create lol manifest from final tables on s3
  - create Donorbase_Donors table (maintable)
  - using lol manifest, load final tables to Donorbase_Donors
  - drop undeliverables, deceased, unique donors, consumer matches
  - refresh maintable with unique donors (do not overwrite cons db list 19946)
  - load DonorBase_Transactions (child1) from s3 transactions folder


### UPCOMING mods
  - list planned enhancements here...



### Created with Contributions By
* ***Jayesh Patel***/Ekta Sharma* - *LoadConsumerAndDonors*
* *Elina Bor* - *Database/ListConversion/AOP Process setups*
* *Saravanan Ramalingam* - *Rollups*
* *Ekta Sharma* - *Suppressions/Validations*
* *Caroline Burch* - *Readme*
* *Jayesh Patel* - Modify Consumer Universe Load for Apogee (step1,step2)
* *Caroline Burch* - Add Apogee Build Processes (bi-weekly,steps 3,4,5,readme)


