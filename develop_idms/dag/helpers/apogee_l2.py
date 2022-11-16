from datetime import datetime
from airflow.models import Variable
from airflow.hooks.mssql_hook import MsSqlHook
from contextlib import closing
from helpers.redshift import *
from helpers.sqlserver import *
from airflow.operators.email_operator import EmailOperator
from airflow.utils.email import send_email_smtp
import pandas as pd
from pandas.io import sql as psql
from .s3 import save_dataframe

apogee_adhoc_l2_buildid = 19291
sample_load = '--LIMIT 1000'
#sql_output_bucket_name = 'develop_idms-2722-internalfiles'
sql_output_bucket_name = Variable.get("var-s3-bucket-names", deserialize_json=True)["s3-internal"]
#sql_output_key1 = 'neptune_apogee/L2/Apogee_L2_Export_All_test.txt'
sql_output_key1 = 'neptune_apogee/L2/Apogee_L2_Export_All_test.txt'

def combine_all_l2_tables_sql():
    #sqlhook = get_sqlserver_hook()
    sqlhook = MsSqlHook(mssql_conn_id=Variable.get('var-apogee-sqlserver-conn'))
    sql_conn = sqlhook.get_conn()
    sql_conn.autocommit(True)

    with closing(sql_conn.cursor()) as sql_cursor:
        sql_script = f"""
        /* 
        For Apogee L2 Voters' data
        Combine Individual_MC, Individual_ID, Company_MC and Company_ID fieds
            from voter's tables from all states
        Source Database: Adhoc_ListConversion database 
        Server: SQL Server 03 (PRLIDMSQLP03)
        Table names are like: DW_Final_1182_16261_16838

        SF 04.08.2020
        */

        DECLARE @lcTableName VARCHAR(MAX)
        DECLARE @lcBuildID VARCHAR(5)
        DECLARE @lcSQL VARCHAR(MAX)
        DECLARE @lcSampleLoad varchar(20)
        DECLARE @lcSampleTopREcords varchar(20)
        DECLARE @lcCounter int

        SET @lcBuildID = '{apogee_adhoc_l2_buildid}'
        SET @lcCounter =0

        /* for testing export */
        SET @lcSampleLoad = '{sample_load}'
        if @lcSampleLoad Like '--%'
        SET @lcSampleTopREcords =''
        ELSE 
            SET @lcSampleTopRecords = 'TOP ' + REPLACE(@lcSampleLoad, 'LIMIT','')
            

        USE Adhoc_ListConversion;


        /*This is for looping through each table and create a view for exporting text file */
        SET @lcSQL=''

        DROP TABLE IF EXISTS L2_All
        CREATE TABLE L2_All (Individual_Mc varchar(17), Company_MC varchar(15))

        DECLARE tableName CURSOR FOR 
            SELECT TABLE_NAME from INFORMATION_SCHEMA.TABLES where TABLE_TYPE='BASE TABLE' 
            and substring(table_name, 15, 5) = @lcBuildID order by TABLE_NAME /*and table_name in ('DW_Final_1182_16261_16824', 'DW_Final_1182_16261_16830') remember to remove the filter */
            OPEN TableName
                FETCH NEXT FROM TableName INTO @lcTableName
                WHILE @@fetch_status = 0
                    BEGIN
                    --	IF @lcSQL=''
                            --BEGIN
                                --add this for insert
                                --WITH (TABLOCK)
                                --SET @lcSQL = 'Insert into L2_All (Individual_ID, Individual_Mc, Company_ID, Company_MC) WITH (TABLOCK) '
                                --SET @lcSQL = @lcSQL + 'Select ' + @lcSampleTopRecords + ' Individual_ID, Individual_Mc, Company_ID, Company_MC from ' + @lcTableName
                                
                                SET @lcSQL = 'Insert into L2_All  WITH (TABLOCK) (Individual_MC, Company_MC) '
                                SET @lcSQL = @lcSQL + 'Select ' + @lcSampleTopRecords + ' Individual_MC, Company_MC from ' + @lcTableName 
                                SET @lcSQL = @lcSQL + ' WHERE (ISNULL(Individual_ID, ''' + ''')=''' + ''' OR ISNULL(Company_ID, ''' + ''')='''') AND'
                                SET @lcSQL = @lcSQL + ' LEFT(MailabilityScore,1) IN (''' + '1' + ''',''' + '2' + ''',''' + '3'+ ''')' + ' AND PrisonRecord = '''+ 'N' + ''' AND DMAMailPreference =''' + 'N' + ''' AND isnull(VacantIndicator,'''') <> '''+ 'Y' + ''''
                            --END
                    --	ELSE
                    --		SET @lcSQL = @lcSQL +  ' Union All ' +  ' Select ' + @lcSampleTopRecords + ' Individual_ID, Individual_Mc, Company_ID, Company_MC from ' + @lcTableName
                            --SELECT @lcSQL
                            EXEC (@lcSQL)
                            SET @lcCounter = @lcCounter + 1
                        FETCH NEXT FROM TableName INTO @lcTableName
                    END
                    
                --SELECT @lcSQL
                --EXEC (@lcSQL)
                        
        CLOSE tableName
        DEALLOCATE tableName	

        select @lcCounter

        """
        print(sql_script)
        sql_cursor.execute(sql_script)
        

def generating_individual_id_and_company_id_for_empty_ids_sql():
    #sqlhook = get_sqlserver_hook()
    sqlhook = MsSqlHook(mssql_conn_id=Variable.get('var-apogee-sqlserver-conn'))
    sql_conn = sqlhook.get_conn()
    sql_conn.autocommit(True)

    with closing(sql_conn.cursor()) as sql_cursor:
        sql_script = f"""
        /*
        Insert Individual_MC and Company_MC into Reference table
        For records with empty Individual_ID or Company_ID
        */

        --Backup Ref_tables
        Declare @lcCommandString VARCHAR(MAX)
        Declare @lcBackupTableName varchar(255)

        /**********
        if rerun, need to comment the backup off
        *********/

        --Backup L2_Individual_ID_Ref_Table table
        SET @lcBackupTableName ='##L2_Individual_ID_Ref_Table_'+ CONVERT(Varchar(8),GetDate(),112)
        SET @lcCommandString = 'SELECT  * INTO ' + @lcBackupTableName  + ' from Adhoc_ListConversion.[dbo].[L2_Individual_ID_Ref_Table]'
        --PRINT @lcCommandString
        EXECUTE (@lcCommandString);

        --Backup L2_Company_ID_Ref_Table table
        SET @lcBackupTableName ='##L2_Company_ID_Ref_Table_'+ CONVERT(Varchar(8),GetDate(),112)
        SET @lcCommandString = 'SELECT  * INTO ' + @lcBackupTableName  + ' from Adhoc_ListConversion.[dbo].[L2_Company_ID_Ref_Table]'
        --PRINT @lcCommandString
        EXECUTE (@lcCommandString);


        USE Adhoc_ListConversion

        --add WITH (TABLOCK)
        INSERT INTO dbo.L2_Individual_ID_Ref_Table WITH (TABLOCK)
            (Individual_MC, cCreatedBy)
        --SELECT top 100 Individual_MC, 'vc'       
        SELECT distinct Individual_MC, 'vc'
        FROM dbo.L2_All
        WHERE Individual_MC NOT IN (SELECT Individual_MC FROM dbo.L2_Individual_ID_Ref_Table)
        AND Individual_MC IS NOT NULL 
        AND Individual_MC <> ''
        --   AND (Individual_ID IS NULL OR LTRIM(RTRIM(Individual_ID))='')  --Individual_id here is varchar

        INSERT INTO dbo.L2_Company_ID_Ref_Table WITH (TABLOCK)
            (Company_MC, cCreatedBy)
        --SELECT top 100 Company_MC, 'vc'
        SELECT distinct Company_MC, 'vc'
        FROM dbo.L2_All
        WHERE Company_MC NOT IN (SELECT Company_MC FROM dbo.L2_Company_ID_Ref_Table)
            AND Company_MC IS NOT NULL 
            AND Company_MC <> ''
        --   AND (Company_ID IS NULL OR LTRIM(RTRIM(Company_ID)) ='') --Company_id here is varchar

        --CREATE CLUSTERED INDEX IX_Individual_MC ON dbo.L2_Individual_ID_Ref_Table(Individual_MC)
        --CREATE CLUSTERED INDEX IX_Company_MC ON dbo.L2_Company_ID_Ref_Table(Company_MC)

        --Update the varchar fields:  Need varchar to update Apogee for Convert tables
        UPDATE dbo.L2_Individual_ID_Ref_Table SET Individual_ID_VARCHAR = CAST(Individual_ID as varchar) WHERE Individual_ID_VARCHAR IS NULL
        UPDATE dbo.L2_Company_ID_Ref_Table SET Company_ID_VARCHAR = CAST(Company_ID as varchar) WHERE Company_ID_VARCHAR IS NULL


        --UPDATE dbo.L2_Individual_ID_Ref_Table SET cline = Individual_ID_VARCHAR +'|' + Individual_MC + '|' + cast(dCreatedDate as varchar)
        --UPDATE dbo.L2_Company_ID_Ref_Table SET cline =Company_ID_VARCHAR + '|' + Company_MC + '|' + cast(dCreatedDate as varchar)

        """
        print(sql_script)
        sql_cursor.execute(sql_script)


def load_field_names_to_temp_table_for_export_sql():
    #sqlhook = get_sqlserver_hook()
    sqlhook = MsSqlHook(mssql_conn_id=Variable.get('var-apogee-sqlserver-conn'))
    sql_conn = sqlhook.get_conn()
    sql_conn.autocommit(True)

    with closing(sql_conn.cursor()) as sql_cursor:
        sql_script = f"""
        DROP TABLE IF EXISTS ##Adhoc_ListConversionFields;

        Create Table ##Adhoc_ListConversionFields  (cField Varchar(80));
        Insert into ##Adhoc_ListConversionFields(cField)
        values
            ('FirstName'),
            ('MiddleName'),
            ('LastName'),
            ('FullName'),
            ('NameSuffix'),
            ('Title'),
            ('Company'),
            ('AddressLine1'),
            ('AddressLine2'),
            ('City'),
            ('State'),
            ('Zipcode'),
            ('ZipPlus4'),
            ('Residential_Addresses_AddressLine'),
            ('Residential_Addresses_City'),
            ('Residential_Addresses_ExtraAddressLine'),
            ('Residential_Addresses_State'),
            ('Residential_Addresses_Zipcode'),
            ('Residential_Addresses_Zipplus4'),
            ('ZipFull'),
            ('COUNTRYNAME'),
            ('EmailAddress'),
            ('Phone'),
            ('ID'),
            ('ListID'),
            ('SCF'),
            ('PermissionType'),
            ('LALVOTERID'),
            ('HAS_PHONE'),
            ('DropFlag'),
            ('cInclude'),
            ('ZIP'),
            ('ZIP4'),
            ('Has_Zip4'),
            ('ZipRadius'),
            ('Gender'),
            ('cSource'),
            ('PUBCODE'),
            ('PubName'),
            ('EmailAddress_MD5'),
            ('EncryptedEmail'),
            ('Individual_ID'),
            ('Individual_MC'),
            ('Company_ID'),
            ('Company_MC'),
            ('Domain'),
            ('DomainType'),
            ('AddressType'),
            ('EPD_TitleCode'),
            ('EPD_FunctionCode'),
            ('EPD_HasPostal'),
            ('EPD_HasPhone'),
            ('EPD_Location'),
            ('ListType'),
            ('MoveType'),
            ('ProductCode'),
            ('DeliveryCode'),
            ('VacantIndicator'),
            ('SeasonalIndicator'),
            ('ResBusIndicator'),
            ('MailabilityScore'),
            ('LOT'),
            ('LOTInfo'),
            ('DeliveryPoint'),
            ('DMAMailPreference'),
            ('PrisonRecord'),
            ('DMAPhoneSuppress'),
            ('DeliveryType'),
            ('AddressTypeIndicator'),
            ('CarrierRoute'),
            ('DeliveryPointDropInd'),
            ('LastClickOpen'),
            ('Voters_Active'),
            ('Residence_Addresses_CensusTract'),
            ('Residence_Addresses_CensusBlockGroup'),
            ('Residence_Addresses_CensusBlock'),
            ('VoterTelephones_LandlineConfidenceCode'),
            ('VoterTelephones_CellPhoneOnly'),
            ('VoterTelephones_CellPhoneUnformatted'),
            ('VoterTelephones_CellConfidenceCode'),
            ('Residence_Families_FamilyID'),
            ('Residence_Families_HHCount'),
            ('Residence_HHGender_Description'),
            ('Residence_HHParties_Description'),
            ('Mailing_Families_FamilyID'),
            ('Mailing_Families_HHCount'),
            ('Mailing_HHGender_Description'),
            ('Mailing_HHParties_Description'),
            ('Voters_Gender'),
            ('Voters_Age'),
            ('Voters_BirthDate'),
            ('Parties_Description'),
            ('Ethnic_Description'),
            ('CountyEthnic_Description'),
            ('Religions_Description'),
            ('Voters_CalculatedRegDate'),
            ('Voters_PlaceOfBirth'),
            ('Languages_Description'),
            ('AbsenteeTypes_Description'),
            ('MilitaryStatus_Description'),
            ('US_Congressional_District'),
            ('State_House_District'),
            ('State_Legislative_District'),
            ('State_Senate_District'),
            ('Precinct'),
            ('CommercialDataLL_Gun_Owner'),
            ('CommercialDataLL_Veteran'),
            ('CommercialDataLL_Donates_to_Animal_Welfare'),
            ('CommercialDataLL_Donates_to_Arts_and_Culture'),
            ('CommercialDataLL_Donates_to_Childrens_Causes'),
            ('CommercialDataLL_Donates_to_Healthcare'),
            ('CommercialDataLL_Donates_to_International_Aid_Caus'),
            ('CommercialDataLL_Donates_to_Veterans_Causes'),
            ('CommercialDataLL_Donates_to_Wildlife_Preservation'),
            ('CommercialDataLL_Donates_to_Conservative_Causes'),
            ('CommercialDataLL_Donates_to_Liberal_Causes'),
            ('CommercialDataLL_Donates_to_Local_Community'),
            ('CommercialDataLL_PetOwner_Horse'),
            ('CommercialDataLL_PetOwner_Cat'),
            ('CommercialDataLL_PetOwner_Dog'),
            ('CommercialDataLL_PetOwner_Other'),
            ('CommercialDataLL_Hispanic_Country_Origin'),
            ('CommercialDataLL_Household_Primary_Language'),
            ('FECDonors_NumberOfDonations'),
            ('FECDonors_TotalDonationsAmount'),
            ('FECDonors_TotalDonationsAmt_Range'),
            ('FECDonors_LastDonationDate'),
            ('FECDonors_AvgDonation'),
            ('FECDonors_AvgDonation_Range'),
            ('FECDonors_PrimaryRecipientOfContributions'),
            ('Voters_VotingPerformanceEvenYearGeneral'),
            ('Voters_VotingPerformanceEvenYearPrimary'),
            ('Voters_VotingPerformanceEvenYearGeneralPrimary'),
            ('Voters_VotingPerformanceMinorElection'),
            ('CommercialDataLL_Interest_in_History_Military_In_H'),
            ('CommercialDataLL_Interest_in_Current_Affairs_Polit'),
            ('CommercialDataLL_Interest_in_Religious_Inspiration'),
            ('CommercialDataLL_Interest_in_Theater_Performing_Ar'),
            ('CommercialDataLL_Interest_in_the_Arts_In_Household'),
            ('CommercialDataLL_Collector_Military_In_Household'),
            ('CommercialDataLL_Interest_in_Gaming_Casino_In_Hous'),
            ('CommercialDataLL_Interest_in_Exercise_Health_In_Ho'),
            ('CommercialDataLL_Interest_in_Exercise_Running_Jogg'),
            ('CommercialDataLL_Interest_in_Exercise_Walking_In_H'),
            ('CommercialDataLL_Interest_in_Exercise_Aerobic_In_H'),
            ('CommercialDataLL_Interest_in_SpectatorSports_Auto_'),
            ('CommercialDataLL_Interest_in_Nascar_In_Household'),
            ('CommercialDataLL_Interest_in_Hunting_In_Household'),
            ('CommercialDataLL_Interest_in_Fishing_In_Household'),
            ('CommercialDataLL_Interest_in_Camping_Hiking_In_Hou'),
            ('CommercialDataLL_Interest_in_Shooting_In_Household'),
            ('CommercialDataLL_Gun_Owner_Concealed_Permit'),
            ('Primary_2018'),
            ('OtherElection_2018'),
            ('AnyElection_2017'),
            ('General_2016'),
            ('Primary_2016'),
            ('OtherElection_2016'),
            ('AnyElection_2015'),
            ('General_2014'),
            ('Primary_2014'),
            ('OtherElection_2014'),
            ('AnyElection_2013'),
            ('General_2012'),
            ('Primary_2012'),
            ('OtherElection_2012'),
            ('AnyElection_2011'),
            ('General_2010'),
            ('Primary_2010'),
            ('OtherElection_2010'),
            ('AnyElection_2009'),
            ('General_2008'),
            ('Primary_2008'),
            ('OtherElection_2008'),
            ('AnyElection_2007'),
            ('General_2006')
            ('Primary_2006'),
            ('OtherElection_2006'),
            ('AnyElection_2005'),
            ('General_2004'),
            ('Primary_2004'),
            ('OtherElection_2004'),
            ('AnyElection_2003'),
            ('General_2002'),
            ('Primary_2002'),
            ('OtherElection_2002'),
            ('AnyElection_2001'),
            ('General_2000'),
            ('Primary_2000'),
            ('OtherElection_2000'),
            ('PRI_BLT_2020'),
            ('PRI_BLT_2019'),
            ('PRI_BLT_2018'),
            ('PRI_BLT_2017'),
            ('PRI_BLT_2016'),
            ('PRI_BLT_2015'),
            ('PRI_BLT_2014'),
            ('PRI_BLT_2013'),
            ('PRI_BLT_2012'),
            ('PRI_BLT_2011'),
            ('PRI_BLT_2010'),
            ('PRI_BLT_2009'),
            ('PRI_BLT_2008'),
            ('PRI_BLT_2007'),
            ('PRI_BLT_2006'),
            ('PRI_BLT_2005'),
            ('PRI_BLT_2004'),
            ('PRI_BLT_2003'),
            ('PRI_BLT_2002'),
            ('PRI_BLT_2001'),
            ('PRI_BLT_2000'),
            ('Vote_Frequency'),
            ('G18COUNTY_ALL_REGISTERED_VOTERS'),
            ('G18COUNTY_DEMOCRATS'),
            ('G18COUNTY_INDEPENDENTS_ALLOTHERS'),
            ('G18COUNTY_REPUBLICANS'),
            ('G18PRECINCT_ALL_REGISTERED_VOTERS'),
            ('G18PRECINCT_DEMOCRATS'),
            ('G18PRECINCT_INDEPENDENTS_ALLOTHERS'),
            ('G18PRECINCT_REPUBLICANS'),
            ('P18COUNTY_ALL_REGISTERED_VOTERS'),
            ('P18COUNTY_DEMOCRATS'),
            ('P18COUNTY_REPUBLICANS'),
            ('P18PRECINCT_ALL_REGISTERED_VOTERS'),
            ('P18PRECINCT_DEMOCRATS'),
            ('P18PRECINCT_REPUBLICANS'),
            ('GENERAL_2020'),
            ('PRIMARY_2020'),
            ('OTHERELECTION_2020'),
            ('ANYELECTION_2019'),
            ('PRIMARY_NUMBER'),
            ('PRE_DIRECTION'),
            ('PRIMARY_NAME'),
            ('STREET_SUFFIX'),
            ('POST_DIRECTION'),
            ('UNIT_TYPE'),
            ('UNIT_NUMBER'),
            ('Deceased_Flag'),
            ('Voters_MovedFrom_State'),
            ('Voters_MovedFrom_Date'),
            ('Voters_MovedFrom_Party_Description'),
            ('Voters_MovedFrom_VotePerformEvenYearGeneral'),
            ('Voters_MovedFrom_VotePerformEvenYearPrimary'),
            ('Voters_MovedFrom_VotePerformEvenYearGeneralPrimary'),
            ('Voters_MovedFrom_VotePerformMinorElection'),
            ('G20_Cnty_Margin_Biden_D'),
            ('G20_Cnty_Percent_Biden_D'),
            ('G20_Cnty_Vote_Biden_D'),
            ('G20_Cnty_Margin_Trump_R'),
            ('G20_Cnty_Percent_Trump_R'),
            ('G20_Cnty_Vote_Trump_R'),
            ('G20CountyTurnoutAllRegisteredVoters'),
            ('G20CountyTurnoutDemocrats'),
            ('G20CountyTurnoutIndependentsAllOthers'),
            ('G20CountyTurnoutRepublicans'),
            ('G20PrecinctTurnoutAllRegisteredVoters'),
            ('G20PrecinctTurnoutDemocrats'),
            ('G20PrecinctTurnoutIndependentsAllOthers'),
            ('G20PrecinctTurnoutRepublicans'),
            ('P20_Cnty_Pct_Biden_D'),
            ('P20_Cnty_Pct_Bloomberg_D'),
            ('P20_Cnty_Pct_Buttigieg_D'),
            ('P20_Cnty_Pct_Gabbard_D'),
            ('P20_Cnty_Pct_Klobuchar_D'),
            ('P20_Cnty_Pct_Sanders_D'),
            ('P20_Cnty_Pct_Warren_D'),
            ('P20_Cnty_Vote_Biden_D'),
            ('P20_Cnty_Vote_Bloomberg_D'),
            ('P20_Cnty_Vote_Buttigieg_D'),
            ('P20_Cnty_Vote_Gabbard_D'),
            ('P20_Cnty_Vote_Klobuchar_D'),
            ('P20_Cnty_Vote_Sanders_D'),
            ('P20_Cnty_Vote_Warren_D'),
            ('P20CountyTurnoutAllRegisteredVoters'),
            ('P20CountyTurnoutDemocrats'),
            ('P20CountyTurnoutRepublicans'),
            ('P20PrecinctTurnoutAllRegisteredVoters'),
            ('P20PrecinctTurnoutDemocrats'),
            ('P20PrecinctTurnoutRepublicans'),
            ('Voters_OfficialRegDate')
            ;
        """
        print(sql_script)
        sql_cursor.execute(sql_script)


def generate_script_for_apogee_l2_voter_table_expoert_sql():
    #sqlhook = get_sqlserver_hook()
    sqlhook = MsSqlHook(mssql_conn_id=Variable.get('var-apogee-sqlserver-conn'))
    sql_conn = sqlhook.get_conn()
    sql_conn.autocommit(True)

    with closing(sql_conn.cursor()) as sql_cursor:
        sql_script = f"""
        DECLARE @lcColumnString VARCHAR(MAX)
        DECLARE @lcSQLString VARCHAR(MAX)
        DECLARE @lcTableName VARCHAR(MAX)
        DECLARE @lcBuildID VARCHAR(5)
        DECLARE @lcDropViewSQL VARCHAR(500)
        DECLARE @lcCreateViewSQL VARCHAR(MAX)
        DECLARE @lcViewName VARCHAR(500)
        DECLARE @lcBCP VARCHAR(8000)
        DECLARE @lcSQLBCP VARCHAR (8000)
        DECLARE @lcSampleLoad varchar(20)
        DECLARE @lcSampleTopREcords varchar(20)

        SET @lcBuildID = '{apogee_adhoc_l2_buildid}'
        SET @lcSQLString =''

        /* for testing export */
        SET @lcSampleLoad = '{sample_load}'
        if @lcSampleLoad Like '--%'
        SET @lcSampleTopREcords =''
        ELSE 
            SET @lcSampleTopRecords = 'TOP ' + REPLACE(@lcSampleLoad, 'LIMIT','')
            

        USE Adhoc_ListConversion;

        /*this is for looping through column names and compose the data fields included in the export file */
        DECLARE columnName CURSOR FOR 
            SELECT cField from ##Adhoc_ListConversionFields where cField is not null  /*this table was created in previous steps */

            /*This is for looping through column names to compose the query for creating view	*/				
            Open columnName
                FETCH NEXT FROM ColumnName INTO @lcColumnString 
                WHILE @@fetch_status = 0
                    BEGIN
                    
                    
                    IF UPPER(@lcColumnString)='INDIVIDUAL_ID'
                        SET @lcColumnString = 'case when isnull(a.individual_id, ''' + ''') =''' + ''' then cast(i.individual_id as varchar) else a.individual_id end '
                    ELSE
                        BEGIN
                            IF UPPER(@lcColumnString)='COMPANY_ID'
                                SET @lcColumnString = 'case when isnull(a.Company_id, ''' + ''') ='''+ ''' then cast(c.Company_id as varchar) else a.company_id end '
                            ELSE
                                SET @lcColumnString = 'UPPER(RTRIM(REPLACE(CAST(ISNULL(' + @lcColumnString + ',' + char(39) +char(39) + ') AS varchar),' + char(39) + '|' + char(39) + ',' + char(39) + char(39) + ')))' 
                        END
                    
                    SET @lcColumnString = REPLACE(REPLACE(@lcColumnString, 'COMPANY_MC', 'A.COMPANY_MC'), 'INDIVIDUAL_MC', 'A.INDIVIDUAL_MC')	
                    PRINT @lcColumnString
                    
                    /* check if this the first column or not */
                    IF @lcSQLString = ''
                        SET @lcSQLString = @lcColumnstring
                    ELSE 
                        /*this is for adding the delimiter '|' */
                        SET @lcSQLString = @lcSQLString + ' ' + char(43) + ' ' + char(39) + '|' + char(39) + ' ' +  char(43) + ' ' + @lcColumnstring
                    
                    FETCH NEXT FROM ColumnName INTO @lcColumnString
                END

            CLOSE columnName
            DEALLOCATE columnName	
            

        /*This is for looping through each table and create a view for exporting text file */
        SET @lcCreateViewSQL=''
        SET @lcViewName = 'view_Adhoc_All'
        SET @lcDropViewSQL ='DROP VIEW IF EXISTS  '+ @lcViewName + ';'
        EXECUTE (@lcDropViewSQL)

        DECLARE tableName CURSOR FOR 
            SELECT TABLE_NAME from INFORMATION_SCHEMA.TABLES where TABLE_TYPE='BASE TABLE' 
            and substring(table_name, 15, 5) = @lcBuildID order by TABLE_NAME /*and table_name in ('DW_Final_1182_16261_16824', 'DW_Final_1182_16261_16830') remember to remove the filter */
            OPEN TableName
                FETCH NEXT FROM TableName INTO @lcTableName
                WHILE @@fetch_status = 0
                    BEGIN
                        IF @lcCreateViewSQL=''
                            BEGIN	
                            SET @lcCreateViewSQL = 'Create view view_Adhoc_All as  Select ' +@lcSampleTopREcords  + ' ' + @lcSQLString + ' as cline from ' + @lcTableName + ' a left join L2_Individual_ID_Ref_Table i on a.Individual_MC = i.Individual_MC left join L2_Company_ID_Ref_Table c on a.Company_MC = c.Company_MC'
                            SET @lcCreateViewSQL =@lcCreateViewSQL + ' WHERE LEFT(MailabilityScore,1) IN (''' + '1' + ''',''' + '2' + ''',''' + '3'+ ''')' + ' AND PrisonRecord = '''+ 'N' + ''' AND DMAMailPreference =''' + 'N' + ''' AND ISNULL(VacantIndicator,'''') <> '''+ 'Y' + ''''
                            END
                        ELSE
                            BEGIN
                            SET @lcCreateViewSQL = @lcCreateViewSQL +  ' Union All ' +  ' Select '  +@lcSampleTopREcords + ' ' + @lcSQLString + ' as cline from ' + @lcTableName + ' a left join L2_Individual_ID_Ref_Table i on a.Individual_MC = i.Individual_MC left join L2_Company_ID_Ref_Table c on a.Company_MC = c.Company_MC'
                            SET @lcCreateViewSQL =@lcCreateViewSQL + ' WHERE LEFT(MailabilityScore,1) IN (''' + '1' + ''',''' + '2' + ''',''' + '3'+ ''')' + ' AND PrisonRecord = '''+ 'N' + ''' AND DMAMailPreference =''' + 'N' + ''' AND ISNULL(VacantIndicator,'''') <> '''+ 'Y' + ''''
                            END
                        FETCH NEXT FROM TableName INTO @lcTableName
                    END
                    --for checking
                    --Select @lcCreateViewSQL
                    
                    -- uncomment when run
                    EXEC (@lcCreateViewSQL)                                               
                                
            CLOSE tableName
            DEALLOCATE tableName
        """
        print(sql_script)
        sql_cursor.execute(sql_script)

        sample_load_new = sample_load.REPLACE('LIMIT','')
        if '--' in sample_load:
            lcSampleTopREcords = ''
        else:
            lcSampleTopREcords = f"""TOP {sample_load_new}"""
        sql_script = f"""
        SELECT {lcSampleTopREcords} cLine FROM Adhoc_ListConversion.dbo.view_Adhoc_All;
        """
        print(sql_script)
        orders_df = sqlhook.get_pandas_df(sql_script)
        print(orders_df.to_markdown())
        orders_df.index = orders_df.index.str.encode('utf-8')
        save_dataframe(sql_output_bucket_name, sql_output_key1, orders_df, '|')






