### Apogee AWS Refactor: tasks & notes
CB,JP,ES,EB,SR<br>
CB 2022.06.23

-- ====================================================================================    
#### 1. Special Validations/Tasks - discontinued
-- ====================================================================================    
- ####RESOLVED
  - Matched Count is now Validated count on audit report, which represents accountno matches<br>
  - Converted Qty is now Total count on audit report<br>
  - Company_ID files by list for Operations - use SUB-SELECT <br>
    - \\stcsanisln01-idms\idms\lrfsdecom\LargeSuppressionFiles<br>
  - 5/31/22 - CB notified DL-ApogeeUpdateTeam of above.<br>
  - 5/31/22 - CB updated build table layout to require mapping for ListCategory01.<br>
  - IDMS-2156: ListCategory01 validations add to pre-validation<br>
  - IDMS-2156: LK_Action(s) inconsistencies between don/trans lists, 
    - added NbrOfListCategory01 to Validation Report - <br>
  - MMDB Exports - Audit report, counts of actual output files<br>
    - added to export dag<br>
<br>
  
-- ====================================================================================    
#### 2. Validations to be added
-- ====================================================================================    
- ####RESOLVED
- IDMS-2156:
  - @RecencyDate - Apogee kept transactions from 1/1/1950, set default based on dbid<br>
  - Add `dropflag is null` to where clause on queries<br>
  - Add another field for `distinct ListCategory01` to show multiple categories<br>
  - AccountNo: replace '|' with space ' '<br>
  - Takes care of deleted lists that were active last build, REMOVE S3 folder, review with JP<br>
<br>


-- ====================================================================================    
#### 3. Validations handled differently
-- ====================================================================================    
- #### OPEN
- IDMS-2159: Set up like DonorBase - 
  - Set up **RETAIN-DONORS** rule in List Of Lists<br>
    - User Campaign processes
    - **Apply-DEFAULT-Rules** is set to ON in Segment screen to exclude unique donors
      - this rule is maintained at a list level in tblMasterLoL *(Maintenance>List Of Lists)*
      - Turn off this flag ONLY when selecting unique names for member (select Transaction source-listid)
    <br>
- ####RESOLVED
- 5/31/22 RESOLVED - CB notified DL-ApogeeUpdateTeam, setting up like DonorBase
  - LTD_Number_Of_List_Source_DQI on maintable, 
  - using hhsummary child1.LTD_Number_Of_List_Sources<br> 
    - THIS REQUIRES IDMSAPP CHANGES if it is a required selection<br>


-- ====================================================================================    
#### 4. Consumer Universe - Apogee Differences - Feed Enhancement Request - RESOLVED
-- ====================================================================================    
- #### OPEN
  - IDMS-2157: Match Flags 
    - CANNOT be added to ConsumerDB feed, based on idms trans/rollups... 
      - DonorMatch - using hh summary table Child1 - 31/16, used in exports/tblext39<br>
      - MgenMatch using tblMgenApogee.company_id (child2 view) - 0/0, used in exports<br>
      - Multibuyercount - using transaction tables child3 (sourcelistid, individualid) - 0/0<br>
      - IndividualDonorMatch using transaction tables child3 - 8669/4474, used in exports<br>
      - FECIndividualMatch FEC transactions child4 - 23/0<br>
      - FECMatch using FEC hh summary table child5 - 0/0<br>
      - L2VoterMatch using tblExt45 (child7 view) - 3/16
      - L2VoterIndividualMatch using tblExt45 (child7 view) - 130/40<br>
      - HaystaqIndividualMatch using tblExt44 (child8 view)<br> - 0/2
      - HH Summary - Child1<br>
          - LEMSMatchCode - required for mmdb exports<br>
          - ExternalTableFlag using tblDQI - required for ?<br>
   <br>
  - IDMS-2162: Create calendar twice a month for data/science and rollups - for donorbase and apogee<br>
<br><br>
- ####RESOLVED
  - Addr1Flag = 'N' WHERE  StreetName = ''<br>
    - confirmed addresstype=0 is no street address
  - Lems, LastName1stLetter, SCF, Zip9 - 
  - no usage: creditcard_bank
  - member_1_vendorlanguage 72/26
    - Apogee linked to External DBs: DQIHH, PUB2<br>
    - 5/31/22 - CB notified DL-ApogeeUpdateTeam of above.<br>
    - 6/2 SN requested missing fields from MMDB
    - Review Calculated Field Usage last 18 mos (shipped/counts) :<br>
  - IDMS-2158: ROLLUPS - should get static names exclude_hhsummary<br>

-- ====================================================================================    
#### 5. MMDB Exports to add to Donorbase DAG
-- ====================================================================================    
- ####OPEN:
- 5/31/2022 Per EB - MarketZone Team Qtrly Install Delivery 
  - modify to read unloaded files above
  - who is client? multi-part data exports... 
  <br><br>

- ####RESOLVED
- IDMS-2161: triyanshi - run all in parallel tasks - 
- MMDB Exports: unload to s3, each current file should have its own folder<br>
  - S3 Bucket/Folder names? <br> 
  - Must be unique on different fields depending on output, ck VC Job. 
  - Use all Ind/comp no need for isnumeric <br>
    - If numeric 12 digit, then matched to Consumer Universe, otherwise it has Matchcode in id fields <br>
OR as long as mc <> id, then it is a match to Cons Univ (per eb)
      - Political Donor File<br>
      - NonProfit MGEN MOD<br>
      - NonProfit Summary MOD<br>
      - NonProfit Summary MOD No-dedupe<br>
      - FEC Donor File HH Summary<br>
      - Combined NonProfit and FEC combined HH Summary<br>
      - L2 Voter Data<br>
- Exports - CB
  - Met with Dennis/Debra
    - Documentation of exports with join definition
    - only sending numeric company_id as varchar?
    - Send test files asap - small/large
    - Communicated each file will have its own folder of multiple files
  - 6/1/22 - CB summarizing, done.<br>
  - eft -Carla setting up with eft support
  - line count reports no longer available, table count added
 
-- ====================================================================================    
#### 6. Additional DAG Modifications
-- ====================================================================================    
 - ####OPEN
   - IDMS-2173: tblChild11 view: of Premover DAILY
     - Refresh Child11, partially run in AWS, update dag for VC steps running in IQ<br>
     <br>
   - IDMS-2181: Donorbase-CCPA-suppressions: <br>
     - add database to ccpa suppressions <br>
     - transfer data from tblsuppressions to ccpa suppressions <br>
   <br>


 - ####RESOLVED
   - Add Child tables/views to donorbase dag: <br>
   - IDMS-2170: child2 view: load exclude_tblMgenApogee <br> 
     - \\stcsanisln01-idms\idms\neptune\apogee\Mgen_apogee_Extract.txt<br>
     - change mgen to post here so it datasynchs to s3
       - \stcsanisln01-idms\idms\Neptune\IDMSFILES\ApogeeForConverts\Mgen 
       - -s3://idms-7933-apogee/transactions/mgen/Mgen_apogee_Extract.txt
   - IDMS-2174: tblChild9 view: load exclude_tblConsEmailApogee_new<br>
     - (emails for individuals existing in DA-ConsumerDB #1267)<br>
   - IDMS-2170: tblChild10 view: exclude_tblDQI_CellPhone<br> 
 
-Unloads - add to donorbase dag: <BR>
  - IDMS-2176: Apogee External Link Table tblExt39 (LEMS unique)<br>
    - Consumer Universe join HH Summary child1<br>
    - \\stcsanisln01-idms\idms\IDMSFileShare\FilesExportedFromIQ\tblExternal39_191_201206.txt<br>
  - IDMS-2171: Apogee Summaries for mGEN Update - unload hh summary child 1<br>
    - \stcsanisln01-idms\idms\IDMSFileShare\Apogee\Apogee_Linked_File.Txt<br>

Other adds to donorbase dag
- IDMS-2179: Final Audit Report (similar to Audit Report, counts on list level, total/category/hh/ind)<br>
- IDMS-2179: Activate Build - allow for use with Apogee<br>

IDMS-2179:Notification Distribution: <br>
  - ApogeeUpdateTeam@data-axle.com<br>
  - IDMSAdminConsumerPearl@data-axle.com<br>
  - DWOrders@data-axle.com<br>
  - dl-data-scienceTeamQuast@infogroup.com<br>
  - MFD-P17-Analytics-Load-Team@data-axle.com<br>
  - MFDAlterianLoadGroup@data-axle.com<br>
<br>

-- ====================================================================================    
#### 7. Apogee Jobs remaining in VC that REQUIRE MODIFICATIONS
-- ====================================================================================    
 - ####RESOLVED
   - DOWNLOAD APOGEE AI MODELS FROM AWS -- remain in VC, no changes required<br>
   - IDMS-2182: APOGEE USAGE REPORTS (cellphone) - WEEKLY (MON) -- 
     - report doesn’t use IQ. It will continue to run on VC. <br>

-- ====================================================================================    
#### 8. Changes for Implementation Time
-- ====================================================================================    
 - ####OPEN
   - Modify tblMasterLoL.LK_ListType = 'T' for Apogee For Converts, update to E <br>
   - LK_ListType definitions<br>
   - SN will update auto-suppress @implementation (addr1flag -> addresstype)
 - ####RESOLVED
   - 5/31/22 - CB notified DL-ApogeeUpdateTeam of above.<br>

-- ====================================================================================    
#### 9. L2 Items and OTHER Potential Considerations
-- ====================================================================================    
- ####RESOLVED
- Code for Consumer DB Universe - L2 Enhancement N/A CREATING NEW DB
  - Update empty Individual_ID and Company_ID in Donor tables before de-dupe<br>
          Adhoc_ListConversion.dbo.L2_Individual_ID_Ref_Table<BR>
          Adhoc_ListConversion.dbo.L2_Company_ID_Ref_Table<br>
  - Mapping to DQI required for L2 Enhancements<br>
          APOGEE-D3 TASK: ADD L2 ENHANCEMENT RECORDS TO DQI from L2 tblExt45<br>
          Updates DQI for non-existing individual_ids from L2 tblExt <br>
   - IDMS-2183:  
    - L2 Database: Build New DB and Create Child table in APOGEE (L2=External45)
    - create new db from future give them id - tblmain will be view external45
- L2 VC JOB: ETL AWS EXPORT - Apogee L2 Comp&Indiv ID Reference Table 
 - L2 VC JOB: L2 ROYALTY REPORTS
 
