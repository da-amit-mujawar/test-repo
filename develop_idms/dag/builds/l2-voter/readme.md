# Builds - L2 Voter DB and External45 Link table
Set up a new database in IDMS for L2 campaign processing: 
 - import build table layout from tblExternal45
 - create a view as tblMain for this new db as part of the L2 Build process
 - add via lift/shift

This project creates a view of tblExternal45 as tblMain for L2 Voter DB (1449). 

## Pre-processing


## Key Data Elements
* **static build/table name:** tblMain_23112_202112
* **order processing keys:** 
bus_abinumber and individual_mc (business contact)
* **alternate match keys:**
varchar: company_mc, individual_mc
bigint: company_id, individual_id 

## Dependencies and Data Axle Sources
Monthly VCJob: LOAD APOGEE L2 (EXTERNAL 45) 
 - which has a task to Lift and Shift tblExternal45.
 - the completion of this job generates iSupport incident to run this dag.



### Updates: