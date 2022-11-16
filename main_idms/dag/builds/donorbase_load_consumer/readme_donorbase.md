# *Data-Axle DonorBase DBid: 1438*


## Data Sources and processing rules
- 430+ Members (ListIDs) (1000+ lists for donor, trans, suppr)
- Consumer Universe DB (active and inactive)
- Non-Matches to Consumer Universe available for selection
- Monthly Builds
- Database keyed on Individual level
- Rollup/Aggregates on Individual level
- Source unique names *(see RETAIN-DONORS and Apply-Default-Rules)*
  - dropped for certain members
  - retained for certain members for their own use

## Database Schema
- Main table: Name, Address and Consumer Attributes
- Child1 Transactions: Detail level Donation/Gift with dates and categorical information
- Individual Rollup tables (3,700 attributes)
  - Child2 Cumulative Donations by Time and Category (3M/6M/9M/12M)
  - Child4 Category Donations by Date range (0-3M, 4-6M, 7-9M)
  - Child5 Average Donations by List and Date range (2Y, 3Y, PY)
  - Child6 Cumulative Donations by List and Date range (2Y, 3Y, PY)
  - Child7 Number Of Donations by List and Date range (12M/24M)


### List Conversion & AOP Processing Rules
- DDI User processes
  - NCOA is done selectively for members who signed PAF
  - New Donor Lists require DWAP approval
  - New Transaction and Suppression lists must be disapproved in DWAP 
  - Mailer and Offer Approvals not required
  - Consumer Universe DB (list 19946) added to new build with status 140 (list in dw)
  - Set up **RETAIN-DONORS** rule in List Of Lists
  - Applying house suppressions at individual and hh level 
    - s3://axle-donorbase-silver-sources/suppression (**h**{ListId}_{matchlevel}.txt)

- List Conversion Actions:
  - allowed: New, Replace, Remove 
  - not supported: NoAction, Incremental/Adds 
    - combined and processed monthly outside IDMS application (future development)
- Undeliverables dropped (mail score 5)
- Deceased dropped
- Validations
  - Gifts de-duped
  - Gifts >$1,000,000 dropped
  - Emails and Phones retained & exportable, no hygiene processes

### Campaign Processing Special SOPs
- User processes
  - **Apply-DEFAULT-Rules** is set to ON in Segment screen to exclude unique donors
    - this rule is maintained at a list level in tblMasterLoL *(Maintenance>List Of Lists)*
    - Turn off this flag ONLY when selecting unique names for member (select Transaction source-listid)
  - Applying ad-hoc suppressions for campaigns - using DP services or IDMS MatchAppend?
    - produce ind/hh level matchcode files and post to ...\DDI_Upload\DonorBaseLargeSuppressionFiles
    

## **DAILY PROCESSING ...**

### Data Science and Modeling
- Daily when for lists @ status 120
- Airflow pipeline in progress ?
  - DS team will push the model scores as needed 


## DAGs - DAILY SUPPRESSIONS 
- **donorbase-suppression** at individual and hh level - daily@5am
  - Always Uses Same Build - 23710: Suppression Files
  - s3://axle-donorbase-silver-sources/suppression (**s**{List**Owner**id}_{matchlevel}.txt)
  - Custom Functions: helpers.donorbase
  - create Donorbase_AdHoc_Suppression
  - load files from s3 posted in the last 24 hours from suppression lol
    - REPLACES (no adds) listid data in Donorbase_AdHoc_Suppression
  - unload files to s3 by owner_hh and owner_ind (remove 000 from filenamesuffix)
    - why are we doing this? will we reprocess tomorrow?
  - notification

- APPLY **donorbase-CCPA-Suppressions** 
  - donorbase-CCPA-Suppression.py - daily@5:30am
  - create donorbase_ccpa_suppression
  - load s3://{s3-donorbase-silver}/etl/build-output/DW_Final_1438_23710_20321/
  - delete matches from maintable by household (CompanyMC)
  - notification

## DAGs - Build Process

- **Validations** for Transactions - In progress...


- **Rollups/Aggregations** for Child Tables

- **Load Consumer**
  - bucket_name: axle-gold-sources/people-mmdb-universe
  - donorbase.py
  - create and load maintable with Consumer Universe db
  - add/create : IndividualMC, CompanyMC
  - notification email

- **Load-Donors**
  - s3 bucket: s3-donorbase-silver
  - Custom Functions: helpers.donorbase
  - create lol manifest from final tables on s3
  - create Donorbase_Donors table (maintable)
  - using lol manifest, load final tables to Donorbase_Donors
  - drop undeliverables, deceased, unique donors, consumer matches
  - refresh maintable with unique donors (do not overwrite cons db list 19946)
  - load DonorBase_Transactions (child1) from s3 transactions folder
    - these should be cleaned Validations prior to loading ????
    - 

- **Mail Responder** --- in progress ????
  - donorbase_mail_responder.py
  - Always Uses Same Build - 23250: Build for Mail and Response Files Only
  - get lol with codes ending in M/R
  - Load Donorbase_Mail_Responder from s3-playground ????


- **OPU** ... in progress ????
  - donorbase-opu.py
  - loads s3://axle-donorbase-raw-sources/Prior LiftEngine Orders/
  - into donorb_{orderid}_order, donorb_{orderid}_order_exp
  - unloads to s3://idms-7933-aop-input/IDMSAOPPI1/A08.{order_id}
  - get first field in temp layout

- **Previous Orders** ????
  - EMPTY FOLDER


### Custom functions
- **Helpers/donorbase.py**
  - generate_manifest, generate_manifest_json
  - get_donor_list_of_lists, get_suppression_list_of_lists


### Steps


### Key Data Elements



### Dependencies and Data Axle Sources



### Processing



### Testing



### UPCOMING mods
- Validations
- List Conversions - Adds, NoAction

### Modifications ##
- **Initial April 2022 build** 
- IDMS-????: 
- 
- **June-2022 build**


### Created with Contributions By
* ***Jayesh Patel***/Ekta Sharma* - *LoadConsumerAndDonors*
* *Elina Bor* - *Database/ListConversion/AOP Process setups*
* *Saravanan Ramalingam* - *Rollups*
* *Ekta Sharma* - *Suppressions/Validations*
* *Caroline Burch* - *Readme*

