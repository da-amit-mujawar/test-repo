-- reset aggregated values (in case of reruns) & set values w/no where clause


-- 13.1 DA MYRID	MYR SCHOOL NBR ‘-‘ MCD INDIVIDUAL ID.
-- 13.2 OLDMYRID	MYR SCHOOL NBR ‘-‘ MYR RECORD ID
-- 13.5  MYR FINAL ALUMNI GENDER   MYR FINAL SPOUSE GENDER
-- If MYR GENDER or MYR SPOUSE GENDER respectively is as follows, set corresponding Final Gender accordingly.
--    ‘M’, ‘MALE’  ‘M’  --    ‘F’, ‘FEMALE’  ‘F’  --    ‘UNKNOWN’  ‘U’  --    Blank or Null  ‘U’
--    ‘TRANS’  ‘U’  --    ‘X’  ‘U’

update {maintable_name}
   set  damyrid = myrschoolid + '-' + mcdindividualid,
        oldmyrid = myrschoolid + '-' + myrrecordid,
        myrstandardalumnititle = null,
        myrstandardspousetitle = null,
        myrfinalalumnigender = null,
--             case when upper(trim(myrgender)) in ('M','MALE') then 'M'
--                  when upper(trim(myrgender)) in ('F','FEMALE') then 'F'
--                  when upper(trim(nvl(myrgender,''))) in ('','UNKNOWN','TRANS','X') then 'U'
--                  else ''
--             end,
        myrfinalspousegender = null,
--             case when upper(trim(raw_field_spouse_gender)) in ('M','MALE') then 'M'
--                  when upper(trim(raw_field_spouse_gender)) in ('F','FEMALE') then 'F'
--                  when upper(trim(nvl(raw_field_spouse_gender,''))) in ('','UNKNOWN','TRANS','X') then 'U'
--                  else ''
--             end,
        myralumnicalculatedage = null,
        myralumniinferredage = null,
        myrroundedage = null,
        myrsegmentationage = null,
        myrspousematchedage = null,
        myralumniagerange = null,
        myralumnigenerationalsuffix = null,
        myrspousegenerationalsuffix = null,
      --  myralumnifullnamecreated = null,
      --  myrspousefullnamecreated = null,
        myralumnifullnamecreated = '',
        myrspousefullnamecreated = '',
        myralumnifullnamegradyear = null,
        myrspousefullnamegradyear = null,
        myrformalalumsalutation = null,
        myrformaljointsalutation = null,
        myrinformalalumnisalutation = null,
        myrinformaljointsalutation = null,
        myrfinalalumnimarried = null,
        myrgenericsalutation = null,
        myrfinalsalutation = null,
        myrprintfullname = null,
        myrspouseprintfullname = null,
        myr_product_core = null,
        myr_product_core_qd = null,
        myr_product_l4l_art_10yr = null,
        myr_product_l4l_art_20yr = null,
        myr_product_l4l_sap_10yr = null,
        myr_product_l4l_sap_20yr = null,
        myr_product_l95 = null,
        myr_product_ad_nyl = null,
        myr_product_ad_met = null,
        myr_product_pru_ci = null,
        myr_product_pru_ac = null,
        myr_product_pru_hip = null,
        myr_product_lm_bad_states = null,
        myr_product_lm_good_states = null,
        myr_product_ltc = null,
        myr_product_ltd = null
   from {maintable_name};


-- 13.3 MYR SCHOOL STATUS
-- Lookup in school file on MYR SCHOOL ID – will be either ‘FORMAL’ or ‘INFORMAL’ or ‘INACTIVE’
-- If a school has a MYR SCHOOL STATUS of ‘INACTIVE’  all of the aggregates outlined below do not need to be calculated.
-- Set the MAILABLE flag to ‘N’ and EMAILABLE flag to ‘N’ for all INACTIVE records.

    -- n/a we mark list as inactive and records are removed from future builds

-- 13.4.1 MYR STANDARD ALUMNI TITLE
-- Title Code for Alumni and will be standardized using the TITLE STANDARDIZATION table.
-- Key will be the DA ALUMNI TITLE for the alumni.  If a match is found, assign match to MYR STANDARD ALUMNI TITLE.
-- If match is not found, assign DA ALUMNI TITLE to MYR STANDARD ALUMNI TITLE.
-- Proper Case Required and punctuation...

-- matches
update {maintable_name}
   set myrstandardalumnititle = standardized_title_code
  from {maintable_name} m inner join meyer_title_standardization_new ts
    on upper(trim(replace(replace(replace(replace(m.title,'.',''),'(',''),')',''),',','')))
     = upper(trim(replace(replace(replace(replace(ts.title_code,'.',''),'(',''),')',''),',','')))
 where nvl(standardized_title_code,'') <> '' ;

-- non-matches
update {maintable_name}
   set myrstandardalumnititle = initcap(trim(title))
  from {maintable_name}
 where nvl(myrstandardalumnititle,'') = ''
   and nvl(title,'') <> '';

-- 13.4.2 MYR STANDARD SPOUSE TITLE
-- Key will be the DA SPOUSE TITLE for the spouse.  If a match is found, assign match to MYR STANDARD SPOUSE TITLE.
-- If match is not found, assign DA SPOUSE TITLE to  MYR STANDARD SPOUSE TITLE.
-- Proper Case Required and punctuation...

-- matches
update {maintable_name}
   set myrstandardspousetitle = standardized_title_code
  from {maintable_name} m inner join meyer_title_standardization_new ts
    on upper(trim(replace(replace(replace(replace(m.daspousetitle,'.',''),'(',''),')',''),',','')))
     = upper(trim(replace(replace(replace(replace(ts.title_code,'.',''),'(',''),')',''),',','')))
 where nvl(standardized_title_code,'') <> '' ;

-- non-matches
update {maintable_name}
   set myrstandardspousetitle = initcap(daspousetitle)
  from {maintable_name}
 where nvl(myrstandardspousetitle,'') = ''
   and nvl(daspousetitle,'') <> '' ;

-- 13.5a  MYR FINAL ALUMNI GENDER
-- If there isn’t a value for MYR GENDER or MYR SPOUSE GENDER,
-- use the MYR STANDARD ALUMNI TITLE or the MYR STANDARD SPOUSE TITLE
-- to assign a MYR FINAL ALUMNI GENDER or MYR FINAL SPOUSE GENDER using the table below:
-- MYR STANDARD ALUMNI TITLE/MYR STANDARD SPOUSE TITLE	Final Gender
--    Mr., Msr, Sr., Brother, Father	“M’
--    Miss, Ms., Mrs., Sister, Sis., Mlle, Mme, Sra., Srta.	‘F’
--    Any other title	‘U’

--Do you want to add any of these for male/female to the calculation below?NO per KP 6/22/2021
--Fr. Lord, Prince, King, Signor, Sir
--Lady, Madam, Madame, Princess, Queen
--Sr. could mean senior not always senor
--Msr is not in standard table, is this meant to be Msgr? I find it as a new gender neutral titles..
-- 20210726 qa test: uppercase values for match

-- 20210804 CR2020V006
--The assignment of the gender fields did not take into consideration the fact that the key pieced of spouse
-- information may be missing and therefore
-- a value of blank should be assigned to the gender field.
update {maintable_name}
   set  myrfinalalumnigender =
        case when upper(trim(myrgender)) in ('M','MALE') then 'M'
             when upper(trim(myrgender)) in ('F','FEMALE') then 'F'
        else
             case when upper(trim(myrstandardalumnititle)) in ('BROTHER','FATHER','M.','MR.','MSGR','SR.') then 'M'
                  when upper(trim(myrstandardalumnititle)) in (
                    'F','MISS','MLLE','MME','MRS.','MS.','SISTER','SIS.','SRA.','SRTA.') then 'F'
             else 'U'
             end
        end
 where nvl(myrfinalalumnigender,'')='';


-- 13.5b  MYR FINAL SPOUSE GENDER
 update {maintable_name}
    set myrfinalspousegender =
        case when nvl(myrspouselastname,'') = '' then ''
        else
            case when upper(trim(raw_field_spouse_gender)) in ('M','MALE') then 'M'
                 when upper(trim(raw_field_spouse_gender)) in ('F','FEMALE') then 'F'
            else
                case when upper(trim(myrstandardspousetitle)) in (
                          'BROTHER','FATHER','M.','MR.','MSGR','SR.','M','MR','SR') then 'M'
                     when upper(trim(myrstandardspousetitle)) in ('F','MISS','MLLE','MME','MRS.','MS.','SISTER',
                          'SIS.','SRA.','SRTA.','MRS','MS','SIS','SRA','SRTA') then 'F'
                else 'U'
                end
            end
        end
 where nvl(myrfinalspousegender,'')='';


-- 13.6 MYR ALUMNI CALCULATED AGE = Use January 1 of the SYSDATE Year + 1.
-- Example, if current SYSDATE year is 2021, date use for calculation will be 1.1.2022.
-- Take January 1st date - MYR DOB field to calculate ALUMNI age.
-- IF DOB is null or blank, set the MYR ALUMNI CALCULATED AGE to MYR AGE AS OF MM-DD-YYYY value.
-- If the MYR AGE AS OF MM-DD-YYYY field is null or blank, the MYR ALUMNI CALCULATED AGE  is null.

update {maintable_name}
   set myralumnicalculatedage =
        datediff(year,to_date(myrdob,'YYYYMMDD'), to_date(concat(date_part(year,getdate())+1,'0101'),'YYYYMMDD'))
 where nvl(myrdob,'') <> ''
   and to_date(myrdob,'YYYYMMDD') < to_date(getdate(),'YYYYMMDD');

update {maintable_name}
   set myralumnicalculatedage = myrageasofmmddyyyy
 where nvl(myrdob,'') = ''
   and udf_isnumeric(myrageasofmmddyyyy)
   and myrageasofmmddyyyy > 0;

update {maintable_name}
   set myralumnicalculatedage = null
 where nvl(myrageasofmmddyyyy,'') = ''
   and nvl(myrdob,'') = '';    --added per Cal Kirby during qa testing 20210723


-- 13.7 MYR ALUMNI INFERRED AGE
-- If MYR ALUMNI CALCULATED AGE is null AND MYR DOB and MYR AGE AS OF MM-DD-YYYY are also null,
-- then calculate MYR ALUMNI INFERRED AGE. There are 2 parts to this calculation:
-- 1. Use SYSDATE year + 1 and subtract the MYR 1DEGREE YEAR.  This will give you the number of years since graduation.
-- 2. Lookup the MYR 1DEGREE to the DEGREE table and obtain the GRADUATION AGE.
--    Add the GRADUATION AGE to the number of years since graduation, calculated in Step 1.
-- 3. If a MYR 1DEGREE is not found, the default age of ‘45’ will be assigned to the MYR ALUMNI INFERRED AGE.

-- CB: add upper() to join
update {maintable_name}
   set myralumniinferredage = to_number(f.graduationage,'999') +
                              datediff(year,to_date(m.myr1degreeyr,'YYYY'), to_date(date_part(year,getdate())+1,'YYYY'))
  from {maintable_name} m
        inner join meyer_first_degree_new f on upper(m.myr1degree) = upper(f.myr1degree)
 where nvl(m.myralumnicalculatedage,'') = '' and nvl(m.myrdob,'') = '' and nvl(m.myrageasofmmddyyyy,'') = ''
   and m.myr1degreeyr <> '' and m.myr1degreeyr > 1899 and m.myr1degreeyr < date_part(year,getdate());

update {maintable_name}
   set myralumniinferredage = 45
  from {maintable_name}
 where nvl(myralumnicalculatedage,'') = '' and nvl(myrdob,'') = '' and nvl(myrageasofmmddyyyy,'') = ''
   and nvl(myralumniinferredage,'') = '';


-- 13.8 MYR ROUNDED AGE
-- If MYR ALUMNI CALCULATED AGE is null, then populate this age.
-- Use MYR ALUMNI INFERRED AGE and round down to the nearest 5.
-- IF MYR ALUMNI INFERRED AGE is either 18 or 19, round up to 20.  This is an exception to the rules above.
-- Example:  MYR ALUMNI INFERRED AGE is 51, MYR ROUNDED AGE is 50.
-- MYR ALUMNI INFERRED AGE is 38, MYR ROUNDED AGE is 35.

update {maintable_name}
   set myrroundedage = (myralumniinferredage-mod(myralumniinferredage,5))
  from {maintable_name}
 where nvl(myralumnicalculatedage,'') = '' and myralumniinferredage not in (18,19);

update {maintable_name}
   set myrroundedage = 21
  from {maintable_name}
 where nvl(myralumnicalculatedage,'') = '' and myralumniinferredage in (18,19);


-- 13.9 MYR SEGMENTATION AGE
-- There is a hierarchy for setting MYR SEGMENTATION AGE. Use MYR ALUMNI CALCULATED AGE if not null,
-- otherwise use MYR AGE AS OF MM-DD-YYYY if not null, otherwise use MYR ALUMNI INFERRED AGE.

update {maintable_name}
   set myrsegmentationage = myralumnicalculatedage
  from {maintable_name}
 where nvl(myralumnicalculatedage,'') <> '';

update {maintable_name}
   set myrsegmentationage = myrageasofmmddyyyy
  from {maintable_name}
 where nvl(myrsegmentationage,'') = ''
   and nvl(myrageasofmmddyyyy,'') <> '';

update {maintable_name}
   set myrsegmentationage = myralumniinferredage
  from {maintable_name}
 where nvl(myrsegmentationage,'') = ''
   and nvl(myralumniinferredage,'') <> '';


-- 13.10 MYR SPOUSE MATCHED AGE	MYR SPOUSE MATCHED AGE = MYR SEGMENTATION AGE

update {maintable_name}
   set myrspousematchedage = myrsegmentationage
  from {maintable_name};


-- 13.11 MYR ALUMNI AGE RANGE	Use MYR ALUMNI CALCULATED AGE if it is not null, otherwise use MYR ALUMNI INFERRED AGE.
-- Assign the AGE RANGE based on where the age selected above falls with the range(s):
-- ’18-24’ ,’25-29’, ‘30-34’, ‘35-39’, ‘40-44’, ‘45-49’, ‘50-54’, ‘55-59’, ‘60-64’, ‘65-69’, ‘70-74’, ‘75+’
-- 20210726 CB added cast from qatest findings
update {maintable_name}
   set myralumniagerange =
        case when cast(myralumnicalculatedage as int) between 18 and 24 then '18-24'
             when cast(myralumnicalculatedage as int) between 25 and 29 then '25-29'
             when cast(myralumnicalculatedage as int) between 30 and 34 then '30-34'
             when cast(myralumnicalculatedage as int) between 35 and 39 then '35-39'
             when cast(myralumnicalculatedage as int) between 40 and 44 then '40-44'
             when cast(myralumnicalculatedage as int) between 45 and 49 then '45-49'
             when cast(myralumnicalculatedage as int) between 50 and 54 then '50-54'
             when cast(myralumnicalculatedage as int) between 55 and 59 then '55-59'
             when cast(myralumnicalculatedage as int) between 60 and 64 then '60-64'
             when cast(myralumnicalculatedage as int) between 65 and 69 then '65-69'
             when cast(myralumnicalculatedage as int) between 70 and 74 then '70-74'
             when cast(myralumnicalculatedage as int) > 74 then '75PLUS'
            else null
        end
 where nvl(myralumnicalculatedage,'') <> '';

update {maintable_name}
   set myralumniagerange =
        case when cast(myralumniinferredage as int) between 18 and 24 then '18-24'
             when cast(myralumniinferredage as int) between 25 and 29 then '25-29'
             when cast(myralumniinferredage as int) between 30 and 34 then '30-34'
             when cast(myralumniinferredage as int) between 35 and 39 then '35-39'
             when cast(myralumniinferredage as int) between 40 and 44 then '40-44'
             when cast(myralumniinferredage as int) between 45 and 49 then '45-49'
             when cast(myralumniinferredage as int) between 50 and 54 then '50-54'
             when cast(myralumniinferredage as int) between 55 and 59 then '55-59'
             when cast(myralumniinferredage as int) between 60 and 64 then '60-64'
             when cast(myralumniinferredage as int) between 65 and 69 then '65-69'
             when cast(myralumniinferredage as int) between 70 and 74 then '70-74'
             when cast(myralumniinferredage as int) > 74 then '75PLUS'
            else null
        end
 where nvl(myralumnicalculatedage,'') = ''
   and nvl(myralumniagerange,'') = ''
   and nvl(myralumniinferredage,'') <> '';


-- 13.12 MYR ALUMNI GENERATIONAL SUFFIX	Generational Suffix is considered as ONLY the following values:
--      Sr., Jr., I, II, III, IV, V, VI, VI,VII,VIII,IX,X
-- The DA ALUMNI SUFFIX is going to have to be interrogated for one of the above generational suffix values.
--      The DA ALUMNI SUFFIX for some schools contains more than one value.
--      If one of the values above can be isolated in the DA ALUMNI SUFFIX,
--      it will be assigned to the MYR ALUMNI GENERATIONAL SUFFIX.
--      Otherwise leave the MYR ALUMNI GENERATIONAL SUFFIX as null.
-- Do not modify the DA ALUMNI SUFFIX in any way during the assignment of the MYR ALUMNI GENERATIONAL SUFFIX.
-- Example:  DA ALUMNI SUFFIX is ‘Jr., ESQ, DDS’
-- Isolate ‘Jr.’
-- MYR ALUMNI GENERATIONAL SUFFIX = ‘Jr.’
-- Proper Case required and punctuation for ‘Jr.’ and ‘Sr.’.
-- Note:  the proper casing will leave the generational suffix values as required by Meyer.
-- myralumnigenerationalsuffix

-- Sr., Jr., I, II, III, IV, V, VI, VII, VIII, IX, X
update {maintable_name}
   set myralumnigenerationalsuffix =
      case --when upper(daspousesuffix) like 'SR' or upper(daspousesuffix) like 'SR.%' then 'Sr.'  --fix 20210723
           --when upper(daspousesuffix) like 'JR' or upper(daspousesuffix) like 'JR.%' then 'Jr.'  --fix 20210723
           when upper(suffix) like 'SR' or upper(suffix) like 'SR.%' then 'Sr.'  --fix 20210723
           when upper(suffix) like 'JR' or upper(suffix) like 'JR.%' then 'Jr.'  --fix 20210723
           when suffix in ('I','I.') then 'I'
           when upper(suffix) like '%II%' and upper(suffix) not like '%III%' and upper(suffix) not like '%VII%'
                and upper(suffix) not like '%VIII%' then 'II'
           when upper(suffix) like '%III%' and upper(suffix) not like '%VIII%' then 'III'
           when (upper(suffix) like '%IV%' or upper(suffix) like '%IV,%') and upper(suffix) not like '%DIV%' then 'IV'
           when upper(suffix) in ('V','V.') or upper(suffix) like 'V,%' then 'V'
           when upper(suffix) like '%VI%' and upper(suffix) not like '%VII%'
                and upper(suffix) not like '%VIII%' then 'VI'
           when upper(suffix) like '%VII%' and upper(suffix) not like '%VIII%' then 'VII'
           when upper(suffix) like '%VIII%' then 'VIII'
           when upper(suffix) like '%X%' then 'IX'
           when upper(suffix) in ('X') then 'X'
           else null
      end
 where nvl(suffix,'') <> '';


-- 13.13 MYR SPOUSE GENERATIONAL SUFFIX	Generational Suffix is considered as ONLY the following values:
--      Sr., Jr., I, II, III, IV, V, VI, VI,VII,VIII,IX,X
-- The DA SPOUSE SUFFIX is going to have to be interrogated for one of the above generational suffix values.
--      The DA SPOUSE SUFFIX for some schools contains more than one value.
--      If one of the values above can be isolated in the DA SPOUSE SUFFIX,
--      it will be assigned to the MYR SPOUSE GENERATIONAL SUFFIX.
--      Otherwise leave the MYR SPOUSE GENERATIONAL SUFFIX as null.
-- Proper Case required and punctuation for ‘Jr.’ and ‘Sr.’.
-- Note:  the proper casing will leave the generational suffix values as required by Meyer.
-- myrspousegenerationalsuffix

update {maintable_name}
   set myrspousegenerationalsuffix =
      case when upper(daspousesuffix) like 'SR' or upper(daspousesuffix) like 'SR.%' then 'Sr.'
           when upper(daspousesuffix) like 'JR' or upper(daspousesuffix) like 'JR.%' then 'Jr.'
           when daspousesuffix in ('I','I.') then 'I'
           when upper(daspousesuffix) like '%II%' and upper(daspousesuffix) not like '%III%'
                and upper(daspousesuffix) not like '%VII%' and upper(daspousesuffix) not like '%VIII%' then 'II'
           when upper(daspousesuffix) like '%III%' and upper(daspousesuffix) not like '%VIII%' then 'III'
           when (upper(daspousesuffix) like '%IV%' or upper(daspousesuffix) like '%IV,%')
                and upper(daspousesuffix) not like '%DIV%' then 'IV'
           when upper(daspousesuffix) in ('V','V.') or upper(daspousesuffix) like 'V,%' then 'V'
           when upper(daspousesuffix) like '%VI%' and upper(daspousesuffix) not like '%VII%'
                and upper(daspousesuffix) not like '%VIII%' then 'VI'
           when upper(daspousesuffix) like '%VII%' and upper(daspousesuffix) not like '%VIII%' then 'VII'
           when upper(daspousesuffix) like '%VIII%' then 'VIII'
           when upper(daspousesuffix) like '%X%' then 'IX'
           when upper(daspousesuffix) in ('X') then 'X'
           else null
      end
 where nvl(daspousesuffix,'') <> '';


-- 13.14.1 MYR ALUMNI FULLNAME CREATED	Hierarchical Rules: NEW VERSION 2021.08.11
-- Lookup MYR SCHOOL ID in School File.
--
-- /* This IF case is no title and two options for maiden name */
--
-- IF MYR SCHOOL PRO SUFFIX NO TITLE FULLNAME FLAG is ‘Y’ and  MYR SCHOOL MAIDEN NAME FULLNAME FLAG is ‘Y’ then
--  /* do not use title but use maiden name*/
--       DA ALUMNI FIRST NAME + DA ALUMNI MIDDLE NAME + MYR MAIDEN NAME + DA ALUMNI LAST NAME +
--       ‘, ‘ + DA ALUMNI SUFFIX
-- ELSE
-- IF MYR SCHOOL PRO SUFFIX NO TITLE FULLNAME FLAG is ‘Y’ and MYR SCHOOL MAIDEN NAME FULLNAME FLAG is ‘N’ then
--  /* do not use title & do not use maiden name*/
--       DA ALUMNI FIRST NAME + DA ALUMNI MIDDLE NAME + DA ALUMNI LAST NAME + ‘, ‘ + DA ALUMNI SUFFIX
-- END
--
-- Important Note:  the comma following the last name prior to a suffix needs to be followed by a blank.
-- The comma and blank need to follow the last name ONLY when the appropriate suffix is not NULL.

---Check for Spouse Title not null before we can use the dr or alumni title.
-- If MYR SCHOOL PRO SUFFIX NO TITLE FULLNAME FLAG is ‘N’
-- If MYR SPOUSE LAST NAME IS NOT NULL OR BLANK THEN
--    IF MYR STANDARD ALUMNI TITLE is not NULL AND
--        MYR STANDARD SPOUSE TITLE is not NULL then
--        IF MYR STANDARD ALUMNI TITLE is like ‘Dr%’ then
--         IF MYR SCHOOL MAIDEN NAME FULLNAME FLAG = ‘Y’
--            MYR STANDARD ALUMNI TITLE + DA ALUMNI FIRST NAME + DA ALUMNI MIDDLE NAME + MYR MAIDEN NAME +
--            DA ALUMNI LAST NAME + ‘, ‘+ MYR ALUMNI GENERATIONAL SUFFIX
--         ELSE  /* no maiden name – flag is ‘N’ */
--            MYR STANDARD ALUMNI TITLE + DA ALUMNI FIRST NAME + DA ALUMNI MIDDLE NAME + DA ALUMNI LAST NAME +
--            ‘, ‘ + MYR ALUMNI GENERATIONAL SUFFIX
--         END  /* end of the dr logic */
--       ELSE /* no ‘Dr’ */
--          IF MYR SCHOOL MAIDEN NAME FULLNAME FLAG = ‘Y’
--              MYR STANDARD ALUMNI TITLE + FA ALUMNI FIRST NAME + DA ALUMNI MIDDLE NAME + MYR MAIDEN NAME +
--              DA ALUMNI LAST NAME + ‘, ‘ + MYR ALUMNI SUFFIX
--         ELSE  /* no maiden name – flag is ‘N’ */
--              MYR STANDARD ALUMNI TITLE + DA ALUMNI FIRST NAME + DA ALUMNI MIDDLE NAME + DA ALUMNI LAST NAME +
--              ‘, ‘ + MYR ALUMNI SUFFIX
--        END  /* end of all DR logic */
--    ELSE  /* can’t use title because we don’t have a spouse title and an alumni title */
--       IF MYR SCHOOL MAIDEN NAME FULLNAME FLAG = ‘Y’
--            DA ALUMNI FIRST NAME + DA ALUMNI MIDDLE NAME + MYR MAIDEN NAME + DA ALUMNI LAST NAME +
--            ‘, ‘ + MYR ALUMNI SUFFIX
--       ELSE /* no maiden name */
--           DA ALUMNI FIRST NAME + DA ALUMNI MIDDLE NAME + DA ALUMNI LAST NAME + ‘, ‘ + MYR ALUMNI SUFFIX
--        END
--    END
-- ELSE /* Spouse last name is null or blank*/
--   IF MYR STANDARD ALUMNI TITLE is like ‘Dr%’ then
--       IF MYR SCHOOL MAIDEN NAME FULLNAME FLAG = ‘Y’
--            MYR STANDARD ALUMNI TITLE + DA ALUMNI FIRST NAME + DA ALUMNI MIDDLE NAME + MYR MAIDEN NAME +
--            DA ALUMNI LAST NAME + ‘, ‘+ MYR ALUMNI GENERATIONAL SUFFIX
--      ELSE  /* no maiden name – flag is ‘N’ */
--            MYR STANDARD ALUMNI TITLE + DA ALUMNI FIRST NAME + DA ALUMNI MIDDLE NAME + DA ALUMNI LAST NAME +
--            ‘, ‘ + MYR ALUMNI GENERATIONAL SUFFIX
--      END  /* end of the dr logic */
--   ELSE /* no ‘Dr’ */
--       IF MYR STANDARD ALUMNI TITLE is not null or blank then
--          IF MYR SCHOOL MAIDEN NAME FULLNAME FLAG = ‘Y’
--              MYR STANDARD ALUMNI TITLE + DA ALUMNI FIRST NAME + DA ALUMNI MIDDLE NAME + MYR MAIDEN NAME +
--              DA ALUMNI LAST NAME + ‘, ‘+ MYR ALUMNI SUFFIX
--          ELSE  /* no maiden name – flag is ‘N’ */
--              MYR STANDARD ALUMNI TITLE + DA ALUMNI FIRST NAME + DA ALUMNI MIDDLE NAME + DA ALUMNI LAST NAME +
--              ‘, ‘ + MYR ALUMNI SUFFIX
--          END
--      ELSE  /* standard alumni title is blank */
--          IF MYR SCHOOL MAIDEN NAME FULLNAME FLAG = ‘Y’
--              DA ALUMNI FIRST NAME + DA ALUMNI MIDDLE NAME + MYR MAIDEN NAME + DA ALUMNI LAST NAME +
--              ‘, ‘ + MYR ALUMNI SUFFIX
--          ELSE /* no maiden name */
--              DA ALUMNI FIRST NAME + DA ALUMNI MIDDLE NAME + DA ALUMNI LAST NAME + ‘, ‘ + MYR ALUMNI SUFFIX
--          END
--       END
--    END
-- END
-- END
-- Proper Case Required
-- myralumnifullnamecreated

/* 20210812 CB
Modifications to the MYR ALUMNI FULLNAME when there is a title for the alumni, but no spouse last name.
   New business rules are outlined below.
There needs to be modifications when the middle initial is presented as a single character field.
   The middle initial of a single character requires a period ‘.’ after the middle initial.
   This only applies to a middle initial, not a middle name.
There needs to be a modification when the first name is presented as a single character.
   A single character first name has to be followed by a period ‘.’ after the first name.
There are several cases when the alumni last name or the spouse last name contains only 1 character.
   These individuals are not valid from a marketing perspective.
   Any record where the spouse last name or alumni last name is only 1 character,
   set the Mailable flag to ‘N’ and the Emailable flag to ‘N’.
 */

--set the field rules
update {maintable_name}
   set da_usetitle = case when upper(s.myr_school_pro_suffix_no_title_fullname_flag) = 'N' and
                             (  (nvl(m.myrstandardalumnititle,'')<>'' and nvl(m.myrstandardspousetitle,'')<>'')
                             or (nvl(m.myrstandardalumnititle,'')<>'' and nvl(m.daspouselastname,'')='')  )
                          then 'Y' else 'N' end,
       da_usemaiden = case when upper(s.myr_school_maiden_name_fullname_flag) = 'Y' and
                                nvl(m.raw_field_maidenname,'') <> ''
                           then 'Y' else 'N' end,
       da_usegensfx = case when nvl(m.myralumnigenerationalsuffix,'')<>'' and
                                upper(s.myr_school_pro_suffix_no_title_fullname_flag) = 'N' and
                                m.myrstandardalumnititle like 'Dr%'
                           then 'Y' else 'N' end,
       da_usesuffix = case when nvl(m.suffix,'')<> '' and
                                ((upper(s.myr_school_pro_suffix_no_title_fullname_flag) = 'N' and
                                        nvl(m.myrstandardalumnititle,'') not like 'Dr%')
                                  or upper(s.myr_school_pro_suffix_no_title_fullname_flag) = 'Y')
                           then 'Y' else 'N' end
from {maintable_name} m inner join
       meyer_school_new s on m.myrschoolid = s.myrschoolid;

--Build the name...
--set to blank for concatenation
update {maintable_name} set myralumnifullnamecreated = '';

update {maintable_name}
set myralumnifullnamecreated = trim(myrstandardalumnititle)
where da_usetitle = 'Y';

-- Add period for names with initial only
update {maintable_name}
set myralumnifullnamecreated =
    case when myralumnifullnamecreated = '' then upper(trim(firstname)) || '.'
    else myralumnifullnamecreated || ' ' || upper(trim(firstname)) || '.' end
where upper(trim(firstname)) in (
        'A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z');

update {maintable_name}
set myralumnifullnamecreated =
    case when myralumnifullnamecreated = '' then trim(firstname)
    else myralumnifullnamecreated || ' ' || trim(firstname) end
where len(trim(firstname)) > 1;

-- Add period for middlenames with initial only
update {maintable_name}
set myralumnifullnamecreated =
    case when myralumnifullnamecreated = '' then upper(trim(middleinitial)) || '.'
    else myralumnifullnamecreated || ' ' || upper(trim(middleinitial)) || '.' end
where upper(trim(middleinitial)) in (
        'A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z');

update {maintable_name}
set myralumnifullnamecreated =
    case when myralumnifullnamecreated = '' then trim(middleinitial)
    else myralumnifullnamecreated || ' ' || trim(middleinitial) end
where len(trim(middleinitial)) > 1;

update {maintable_name}
set myralumnifullnamecreated =
    case when myralumnifullnamecreated = '' then trim(raw_field_maidenname)
    else myralumnifullnamecreated || ' ' || trim(raw_field_maidenname) end
where da_usemaiden = 'Y';

update {maintable_name}
set myralumnifullnamecreated =
    case when myralumnifullnamecreated = '' then trim(lastname)
    else myralumnifullnamecreated || ' ' || trim(lastname) end
where len(trim(lastname)) > 1;

update {maintable_name}
set myralumnifullnamecreated =
    case when myralumnifullnamecreated = '' then trim(myralumnigenerationalsuffix)
    else myralumnifullnamecreated || ', ' || trim(myralumnigenerationalsuffix) end
where da_usegensfx = 'Y';

update {maintable_name}
set myralumnifullnamecreated =
    case when myralumnifullnamecreated = '' then trim(suffix)
    else myralumnifullnamecreated || ', ' || trim(suffix) end
where da_usesuffix = 'Y';

-- Flag unmarketable names with lastname initial only or empty (see also aop-suppress)
update {maintable_name}
set myralumnifullnamecreated = '', mailable = 'N', emailable = 'N'
where nvl(lastname,'') = '' or len(trim(replace(lastname,'.',''))) = 1;



-- 13.14.2 MYR SPOUSE FULLNAME CREATED	Hierarchical Rules:
--
-- This field is only built if
-- MYR MARRIED TO ALUM is ‘Y’ or MYR MARITAL STATUS is ‘MARRIED’ or ‘PARTNER’ or ‘LIFE PARTNER’
-- AND DA SPOUSE LAST NAME IS NOT NULL
-- and SUPPRES SPOUSE (lookup on School id in the School table) is ‘N’.
-- Otherwise leave null.

-- Important Note:  the comma following the last name prior to a suffix needs to be followed by a blank.
-- The comma and blank need to follow the last name ONLY when the appropriate suffix is not NULL.
-- If we are going to build this string, the format must mirror the MYR ALUMNI FULLNAME CREATED.
--
-- IF MYR SCHOOL PRO SUFFIX NO TITLE SPOUSE FULLNAME FLAG is ‘Y’ and MYR SCHOOL PRO SUFFIX NO TITLE FULLNAME FLAG is ‘Y‘
--   IF  MYR SCHOOL MAIDEN NAME SPOUSE FULLNAME FLAG is ‘Y’ then
--  /* do not use title but use maiden name*/
--      DA SPOUSE FIRST NAME + DA SPOUSE MIDDLE NAME + MYR SPOUSE MAIDEN NAME + DA SPOUSE LAST NAME
--   ELSE
-- /* do not use title & do not use maiden name*/
--      DA SPOUSE FIRST NAME + DA SPOUSE MIDDLE NAME + DA SPOUSE LAST NAME
--   END
-- END /* you are done  */
-- IF  MYR STANDARD ALUMNI TITLE is not NULL AND
--      MYR STANDARD SPOUSE TITLE is not NULL then
--          /*you can use title if the flags are set to ‘N’ */
--   IF MYR SCHOOL PRO SUFFIX NO TITLE FULLNAME FLAG is ‘N’
--       AND MYR SCHOOL PRO SUFFIX NO TITLE SPOUSE FULLNAME
--       FLAG is ‘N’
--          IF MYR STANDARD SPOUSE TITLE is like ‘Dr%’ then
--             IF MYR SCHOOL MAIDEN NAME SPOUSE FULLNAME FLAG = ‘Y’
--                 MYR STANDARD SPOUSE TITLE + DA SPOUSE FIRST NAME + DA SPOUSE MIDDLE NAME + MYR SPOUSE MAIDEN NAME +
--                 DA SPOUSE LAST NAME + ‘, ‘ + MYR SPOUSE GENERATIONAL SUFFIX
--           ELSE  /* no maiden name – flag is ‘N’ */
--                MYR STANDARD SPOUSE TITLE + DA SPOUSE FIRST NAME + DA SPOUSE MIDDLE NAME + DA SPOUSE LAST NAME +
--                ‘, ‘ + MYR SPOUSE GENERATIONAL SUFFIX
--          END
--      ELSE /*no dr. */
--       IF MYR SCHOOL MAIDEN NAME SPOUSE FULLNAME FLAG = ‘Y’
--            MYR STANDARD SPOUSE TITLE + DA SPOUSE FIRST NAME + DA SPOUSE MIDDLE NAME + MYR SPOUSE MAIDEN NAME +
--            DA SPOUSE LAST NAME + ‘, ‘ + DA SPOUSE SUFFIX
--       ELSE  /* no maiden name */
--             MYR STANDARD SPOUSE TITLE + DA SPOUSE FIRST NAME + DA SPOUSE MIDDLE NAME + DA SPOUSE LAST NAME +
--             ‘, ‘ + DA SPOUSE SUFFIX
--       END
--      END
--   END
-- ELSE
--       DA SPOUSE FIRST NAME + DA SPOUSE MIDDLE NAME + DA SPOUSE LAST NAME + ‘, ‘ + DA SPOUSE SUFFIX
-- END
-- Proper Case Required

-- NEW RULES see Meyer - CR003
-- There needs to be modifications when the middle initial is presented as a single character field.
--      The middle initial of a single character requires a period ‘.’ after the middle initial.
--      This only applies to a middle initial, not a middle name.
-- There needs to be a modification when the first name is presented as a single character.
--      A single character first name has to be followed by a period ‘.’ after the first name.
-- There are several cases when the alumni last name or the spouse last name contains only 1 character.
--      These individuals are not valid from a marketing perspective.
--      Any record where the spouse last name or alumni last name is only 1 character,
--      set the Mailable flag to ‘N’ and the Emailable flag to ‘N’.

--set the field rules
update {maintable_name}
   set da_create_spousefn = case when (upper(m.myrmarriedtoalum) in ('Y','YES','MARRIED','P','PARTNER','PARTNERED')
                                        or upper(m.raw_field_marital_status) in (
                                                  'M','MARRIED','P','PARTNER','PARTNERED','LIFE PARTNER'))
                                        and nvl(m.daspouselastname,'') <> '' and s.myr_school_suppress_spouse = 'N'
                                 then 'Y' else 'N' end,
       da_usetitle_spouse = case when upper(s.myr_school_pro_suffix_no_title_fullname_flag) = 'N' and
                                      upper(s.myr_school_pro_suffix_no_title_spouse_fullname_flg) = 'N' and
                                      nvl(m.myrstandardalumnititle,'') <> '' and
                                      nvl(m.myrstandardspousetitle,'') <> ''
                                 then 'Y' else 'N' end,
       da_usemaiden_spouse = case when upper(s.myr_school_maiden_name_spouse_fullname_flag) = 'Y' and
                                      nvl(m.raw_field_spousemaidenname,'') <> ''
                                  then 'Y' else 'N' end,
       da_usegensfx_spouse = case when upper(s.myr_school_pro_suffix_no_title_spouse_fullname_flg) = 'N' and
                                      m.myrstandardspousetitle like 'Dr%' and
                                      nvl(m.myrspousegenerationalsuffix,'')<>''
                                  then 'Y' else 'N' end,
       da_usesuffix_spouse = case when nvl(m.daspousesuffix,'')<> '' and
                                       ((upper(s.myr_school_pro_suffix_no_title_fullname_flag) = 'N' and
                                         upper(s.myr_school_pro_suffix_no_title_spouse_fullname_flg) = 'N' and
                                         nvl(m.myrstandardspousetitle, '') not like 'Dr%')
                                         or  upper(s.myr_school_pro_suffix_no_title_fullname_flag) = 'Y' and
                                             upper(s.myr_school_pro_suffix_no_title_spouse_fullname_flg) = 'Y' )
                                  then 'Y' else 'N' end
from {maintable_name} m inner join
     meyer_school_new s on m.myrschoolid = s.myrschoolid;

--Build the name...
--set to blank for concatenation
update {maintable_name} set myrspousefullnamecreated = '';

update {maintable_name}
set myrspousefullnamecreated = trim(myrstandardspousetitle)
where da_create_spousefn = 'Y' and da_usetitle_spouse = 'Y';

-- Add period for names with initial only
update {maintable_name}
set myrspousefullnamecreated =
    case when myrspousefullnamecreated = '' then upper(trim(daspousefirstname)) || '.'
    else myrspousefullnamecreated || ' ' || upper(trim(daspousefirstname)) || '.' end
where upper(trim(daspousefirstname)) in (
        'A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z')
and da_create_spousefn = 'Y';

update {maintable_name}
set myrspousefullnamecreated =
    case when myrspousefullnamecreated = '' then trim(daspousefirstname)
    else myrspousefullnamecreated || ' ' || trim(daspousefirstname) end
where len(trim(daspousefirstname)) > 1
and da_create_spousefn = 'Y';


-- Add period for middlenames with initial only
update {maintable_name}
set myrspousefullnamecreated =
    case when myrspousefullnamecreated = '' then upper(trim(daspousemiddlename)) || '.'
    else myrspousefullnamecreated || ' ' || upper(trim(daspousemiddlename)) || '.' end
where upper(trim(daspousemiddlename)) in (
        'A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z')
and da_create_spousefn = 'Y';

update {maintable_name}
set myrspousefullnamecreated =
    case when myrspousefullnamecreated = '' then trim(daspousemiddlename)
    else myrspousefullnamecreated || ' ' || trim(daspousemiddlename) end
where len(trim(daspousemiddlename)) > 1
and da_create_spousefn = 'Y';


update {maintable_name}
set myrspousefullnamecreated =
    case when myrspousefullnamecreated = '' then trim(raw_field_spousemaidenname)
    else myrspousefullnamecreated || ' ' || trim(raw_field_spousemaidenname) end
where da_create_spousefn = 'Y' and da_usemaiden_spouse = 'Y';

update {maintable_name}
set myrspousefullnamecreated =
    case when myrspousefullnamecreated = '' then trim(daspouselastname)
    else myrspousefullnamecreated || ' ' || trim(daspouselastname) end
where len(trim(daspouselastname)) > 1
and da_create_spousefn = 'Y';

update {maintable_name}
set myrspousefullnamecreated =
    case when myrspousefullnamecreated = '' then trim(myrspousegenerationalsuffix)
    else myrspousefullnamecreated || ', ' || trim(myrspousegenerationalsuffix) end
where da_create_spousefn = 'Y' and da_usegensfx_spouse = 'Y';

update {maintable_name}
set myrspousefullnamecreated =
    case when myrspousefullnamecreated = '' then trim(daspousesuffix)
    else myrspousefullnamecreated || ', ' || trim(daspousesuffix) end
where da_create_spousefn = 'Y' and da_usesuffix_spouse = 'Y';

-- Do not flag unmarketable names with lastname initial only, rule only applicable to alumni

-- 13.15 MYR ALUMNI FULLNAME GRAD YEAR
-- Take the MYR ALUMNI FULLNAME CREATED and add space + an apostrophe + MYR 1DEGREE YR.
-- If the MYR 1DEGREE YR is CCYY, only use the YY portion of the MYR 1DEGREE YR.
-- Example:  Mr. John Smith ‘88
-- Proper Case Required
-- myralumnifullnamegradyear

update {maintable_name}
   set myralumnifullnamegradyear =
       case when udf_isnumeric(myr1degreeyr)
                 and myr1degreeyr <> ''
                 and myr1degreeyr > 1899
                 and myr1degreeyr < date_part(year,getdate())+10
                 and nvl(myralumnifullnamecreated,'') <> '' then
            myralumnifullnamecreated || ' \'' || right(trim(myr1degreeyr),2)
       else myralumnifullnamecreated
       end
  from {maintable_name};


-- 13.16 MYR SPOUSE FULLNAME GRAD YEAR
-- Take the MYR SPOUSE FULLNAME CREATED, when not null and add space + an apostrophe + MYR SPOUSE 1DEGREE YR.
-- If the MYR SPOUSE 1DEGREE YR is CCYY, only use the YY portion of the MYR 1DEGREE YR.
-- Example:  Mrs. Janice Smith ‘88
-- Proper Case Required
-- myrspousefullnamegradyear

update {maintable_name}
   set myrspousefullnamegradyear =
       case when udf_isnumeric(raw_field_spouse1_degreeyear)
                 and raw_field_spouse1_degreeyear > 1899
                 and raw_field_spouse1_degreeyear < date_part(year,getdate())+10
                 and nvl(myrspousefullnamecreated,'') <> '' then
            myrspousefullnamecreated || ' \'' || right(trim(raw_field_spouse1_degreeyear),2)
       else myrspousefullnamecreated
       end
from {maintable_name};


-- 13.17 MYR FORMAL ALUM SALUTATION	Title + Last Name
-- If the MYR STANDARD ALUMNI TITLE field is not null then
-- MYR STANDARD ALUMNI TITLE + DA ALUMNI LAST NAME
-- Otherwise, leave null.
-- myrformalalumsalutation

update {maintable_name}
   set myrformalalumsalutation = myrstandardalumnititle || ' ' || lastname
  from {maintable_name}
 where nvl(myrstandardalumnititle,'') <> '';

-- 13.18 MYR FORMAL JOINT SALUTATION
-- Hierarchical Rules:
-- This field is only built if MYR MARRIED TO ALUM is ‘Y’ or MYR MARITAL STATUS is ‘MARRIED’ or ‘PARTNER’ or ‘LIFE PARTNER’
-- AND MYR SPOUSE FULLNAME CREATED is not NULL.  Otherwise leave null.
--
-- Evaluate using the following hierarchy:
-- IF MYR STANDARD ALUMNI TITLE and MYR STANDARD SPOUSE TITLE are both like ‘Dr%’ and the DA ALUMNI LAST NAME = DA SPOUSE LAST NAME, then the joint salutation is
-- ‘Drs.’ + DA ALUMNI LAST NAME
-- END
--
-- IF MYR STANDARD ALUMNI TITLE IS NOT NULL AND
--    MYR STANDARD SPOUSE TITLE IS NOT NULL and
--    DA ALUMNI LAST NAME = DA SPOUSE LAST NAME
--    IF DA FINAL ALUMNI GENDER = ‘F’ AND
--        DA FINAL SPOUSE GENDER =‘M’ THEN
--          MYR STANDARD SPOUSE TITLE  + ‘and’ +
--          DA ALUMNI TITLE  +       -- MYR_STANDARD_ALUMNI_TITLE IDMS-1559
--          DA ALUMNI LAST NAME
--    ELSE
--      DA ALUMNI TITLE + ‘and’ +    -- MYR_STANDARD_ALUMNI_TITLE IDMS-1559
--      DA SPOUSE TITLE +            -- MYR_STANDARD_SPOUSE_TITLE IDMS-1559
--      DA ALUMNI LAST NAME
--   END
-- END
-- Example:  Mr. and Mrs. Jackson
--
-- IF MYR STANDARD ALUMNI TITLE and
--     MYR STANDARD SPOUSE TITLE are not null AND
--     DA ALUMNI LAST NAME != DA SPOUSE LAST NAME
--        /* male has to be first in this salutation */
--       IF DA FINAL ALUMNI GENDER = ‘F’ AND DA FINAL SPOUSE GENDER = ‘M’ THEN
--        MYR STANDARD SPOUSE TITLE  +
--        DA SPOUSE LAST NAME +  ‘and’ +
--        DA ALUMNI TITLE  +        -- MYR_STANDARD_ALUMNI_TITLE IDMS-1559
--        DA ALUMNI LAST NAME
--     ELSE
--         MYR STANDARD ALUMNI TITLE  +
--         DA ALUMNI LAST NAME+  ‘and’ +
--         DA SPOUSE TITLE +        -- MYR_STANDARD_SPOUSE_TITLE IDMS-1559
--         DA SPOUSE LAST NAME
--     END
--  END
--
-- Otherwise, leave this field null.
--
-- Proper Case Required
-- myrformaljointsalutation

update {maintable_name}
   set myrformaljointsalutation =
        case when myrstandardalumnititle like 'Dr%' and myrstandardspousetitle like 'Dr%'
                  and upper(lastname) = upper(daspouselastname) then
             'Drs.' || ' ' || lastname
        else
            case when nvl(myrstandardalumnititle,'')<>'' and nvl(myrstandardspousetitle,'')<>''
                  and upper(lastname) = upper(daspouselastname) then
                 case when myrfinalalumnigender = 'F' and myrfinalspousegender = 'M'
                      -- IDMS-1559 CB 2021.10.19
                      --then myrstandardspousetitle || ' and ' || title || ' ' || lastname
                      --else title || ' and ' || myrstandardspousetitle || ' ' || lastname
                      then myrstandardspousetitle || ' and ' || myrstandardalumnititle || ' ' || lastname
                      else myrstandardalumnititle || ' and ' || myrstandardspousetitle || ' ' || lastname
                 end
            else
                case when nvl(myrstandardalumnititle,'')<>'' and nvl(myrstandardspousetitle,'')<>''
                      and upper(lastname) <> upper(daspouselastname) then
                     case when myrfinalalumnigender = 'F' and myrfinalspousegender = 'M'
                          -- IDMS-1559 CB 2021.10.19
                          --then myrstandardspousetitle || ' ' || daspouselastname || ' and ' || title || ' ' || lastname
                          --else title || ' ' || lastname ||  ' and ' || myrstandardspousetitle || ' ' || daspouselastname
                          then myrstandardspousetitle || ' ' || daspouselastname || ' and ' || myrstandardalumnititle || ' ' || lastname
                          else myrstandardalumnititle || ' ' || lastname ||  ' and ' || myrstandardspousetitle || ' ' || daspouselastname
                     end
                end
            end
        end
  from {maintable_name}
 where (upper(myrmarriedtoalum) in ('Y','YES','MARRIED','P','PARTNER','PARTNERED')
       or upper(raw_field_marital_status) in ('M','MARRIED','P','PARTNER','PARTNERED','LIFE PARTNER'))
   and nvl(myrspousefullnamecreated,'')<>'';


-- 13.19 MYR INFORMAL ALUMNI SALUTATION	DA ALUMNI FIRST NAME
-- Proper Case Required

-- myrinformalalumnisalutation
update {maintable_name}
   set myrinformalalumnisalutation = firstname
  from {maintable_name}
 where nvl(firstname,'') <> '';

-- 13.20 MYR INFORMAL JOINT SALUTATION
-- This field is only built if MYR MARRIED TO ALUM is ‘Y’ or
-- MYR MARITAL STATUS is ‘MARRIED’ or ‘PARTNER’ or ‘LIFE PARTNER’
-- AND MYR SPOUSE FULLNAME CREATED is not NULL AND DA SPOUSE FIRST NAME is not NULL.
-- Otherwise leave null.
--
--   /* male name must come first example: Jim and Jane */
--      IF DA FINAL ALUMNI GENDER = ‘F’ AND
--          DA FINAL SPOUSE GENDER = ‘M’ THEN
--         DA SPOUSE FIRST NAME + ‘and’ +
--         DA ALUMNI FIRST NAME
--      ELSE
--       DA ALUMNI FIRST NAME + ‘and’ +
--       DA SPOUSE FIRST NAME
--     END
-- Proper Case Required

update {maintable_name}
   set myrinformaljointsalutation =
          case when upper(myrfinalalumnigender) = 'F' and upper(myrfinalspousegender) = 'M'  then
            daspousefirstname || ' and ' || firstname
          else
            firstname || ' and ' || daspousefirstname
          end
  from {maintable_name}
 where (upper(myrmarriedtoalum) in ('Y','YES','MARRIED','P','PARTNER','PARTNERED')
           or upper(raw_field_marital_status) in ('M','MARRIED','P','PARTNER','PARTNERED','LIFE PARTNER'))
   and nvl(myrspousefullnamecreated,'')<>''
   and nvl(daspousefirstname,'')<>'' ;


-- 13.21 MYR ALUMNI FINAL MARRIED  ... move to before using married rules
-- This field is built from two fields Meyer provides.
-- This field is used ONLY for the generic salutation lookup.
-- Default value is ‘S’.  This value must be upper-cased.
--
-- If MYR MARRIED TO ALUM is ‘Y’ or
-- MYR MARITAL STATUS is ‘MARRIED’ or ‘PARTNER’ or ‘LIFE PARTNER’ then set value to ‘M’.
-- This value must be uppercased.
-- This value is used in the lookup for the generic salutation.
-- myrfinalalumnimarried

update {maintable_name}
   set myrfinalalumnimarried =
          case when upper(myrmarriedtoalum) in ('Y','YES','MARRIED','P','PARTNER','PARTNERED')
               or upper(raw_field_marital_status) in ('M','MARRIED','P','PARTNER','PARTNERED','LIFE PARTNER') then 'M'
          else 'S'
          end
  from {maintable_name};


-- 13.22 MYR GENERIC SALUTATION
-- Use the following three fields to lookup in the Generic Salutation file
-- and obtain the MYR GENERIC SALUTATION field:
--      MYR ALUMNI FINAL MARRIED
--      MYR FINAL ALUMNI GENDER
--      MYR FINAL SPOUSE GENDER
-- A match should always be found.
-- myrgenericsalutation

update {maintable_name}
   set myrgenericsalutation = g.myr_generic_salutation
  from {maintable_name} m inner join
       meyer_generic_salutation_new g
              on upper(m.myrfinalalumnimarried) = upper(g.myrfinalalumnimarried)
              and upper(m.myrfinalalumnigender) = upper(g.myr_final_alumni_gender)
              and upper(m.myrfinalspousegender) = upper(g.myr_final_spouse_gender);


-- 13.23 MYR FINAL SALUTATION
-- Lookup School ID on the Client Customization table to determine
-- if we should be using the school provided salutation before moving to the ‘formal’/’informal’ logic below.
--
-- IF MYR MARRIED TO ALUM is ‘Y’ or (MYR MARITAL STATUS is ‘MARRIED’ or ‘PARTNER’ or ‘LIFE PARTNER’)
-- AND MYR SPOUSE FULLNAME CREATED is not NULL and MYR USE SCHOOL PROVIDED JOINT SALUTATION = ‘Y’ then
-- MYR JOINT SALUTATION from the school is assigned to the MYR FINAL SALUTATION
-- END /* done */
--
-- IF MYR USE SCHOOL PROVIDED JOINT SALUTATION IS ‘N’ and MYR USE SCHOOL PROVIDED SALUTATION is ‘Y’ then
-- MYR SALUTATION from the school is assigned to the MYR FINAL SALUTATION
-- END /* done */
--
-- ELSE /* assign below */
--
-- This field is dependent on whether the school is ‘FORMAL’ or ‘INFORMAL’.
-- That needs to be determined from the SCHOOL CLIENT CUSTOMIZATION table and will be in the field MYR SCHOOL STATUS.
--
-- If MYR SCHOOL STATUS is ‘FORMAL’  AND
-- (MYR MARRIED TO ALUM is ‘Y’ or MYR MARITAL STATUS is ‘MARRIED’ or ‘PARTNER’ or ‘LIFE PARTNER’)
-- AND MYR SPOUSE FULLNAME CREATED is not NULL
-- then use in the following order depending on what is available/not null:
--   MYR FORMAL JOINT SALUTATION OR
--   MYR GENERIC SALUTATION
-- ELSE
--   MYR FORMAL ALUMNI SALUTATION OR
--   MYR GENERIC SALUTATION
-- END
--
-- If MYR SCHOOL STATUS is ‘INFORMAL’ AND
-- (MYR MARRIED TO ALUM is ‘Y’ or MYR MARITAL STATUS is ‘MARRIED’ or ‘PARTNER’ or ‘LIFE PARTNER’)
-- AND MYR SPOUSE FULLNAME CREATED is not NULL
-- then use in the following order depending on what is available/not null:
--   MYR INFORMAL JOINT SALUTATION  OR
--   MYR GENERIC SALUTATION
-- ELSE
--   MYR INFORMAL ALUMNI SALUTATION OR
--   MYR GENERIC SALUTATION
-- END
-- myrfinalsalutation

-- from school data joint (no schools fit this criteria)
update {maintable_name}
   set myrfinalsalutation = m.raw_field_joint_salutation
  from {maintable_name} m inner join
       meyer_school_new s on m.myrschoolid = s.myrschoolid
 where m.myrfinalalumnimarried = 'M' and nvl(m.myrspousefullnamecreated,'')<>''
   and upper(s.myr_use_school_provided_joint_salutation) = 'Y';

-- from school data non-joint
update {maintable_name}
   set myrfinalsalutation = m.raw_field_salutation
  from {maintable_name} m inner join
       meyer_school_new s on m.myrschoolid = s.myrschoolid
 where nvl(m.myrfinalsalutation,'')=''
   and upper(s.myr_use_school_provided_joint_salutation) = 'N'
   and upper(s.myr_use_school_provided_salutation) = 'Y';

--Do Not Use School Data - FORMAL
update {maintable_name}
   set myrfinalsalutation =
            case when m.myrfinalalumnimarried = 'M'  and nvl(m.myrspousefullnamecreated,'')<>'' then
                case when nvl(m.myrformaljointsalutation,'')<>'' then m.myrformaljointsalutation
                     when nvl(m.myrgenericsalutation,'')<>'' then m.myrgenericsalutation
                end
            else -- m.myrfinalalumnimarried = 'S' then
                case when nvl(m.myrformalalumsalutation,'')<>'' then m.myrformalalumsalutation
                     when nvl(m.myrgenericsalutation,'')<>'' then m.myrgenericsalutation
                end
            end
  from {maintable_name} m inner join
       meyer_school_new s on m.myrschoolid = s.myrschoolid
 where nvl(m.myrfinalsalutation,'')=''
   and upper(s.myr_use_school_provided_joint_salutation) = 'N'
   and upper(s.myr_use_school_provided_salutation) = 'N'
   and upper(s.myr_school_salutation_type) = 'FORMAL';

--Do Not Use School Data - INFORMAL
update {maintable_name}
   set myrfinalsalutation =
            case when m.myrfinalalumnimarried = 'M' and nvl(m.myrspousefullnamecreated,'')<>'' then
                case when nvl(m.myrinformaljointsalutation,'')<>'' then m.myrinformaljointsalutation
                     when nvl(m.myrgenericsalutation,'')<>'' then m.myrgenericsalutation
                end
            else -- m.myrfinalalumnimarried = 'S' then
                case when nvl(m.myrinformalalumnisalutation,'')<>'' then m.myrinformalalumnisalutation
                     when nvl(m.myrgenericsalutation,'')<>'' then m.myrgenericsalutation
                end
            end
  from {maintable_name} m inner join
       meyer_school_new s on m.myrschoolid = s.myrschoolid
 where nvl(m.myrfinalsalutation,'')=''
   and upper(s.myr_use_school_provided_joint_salutation) = 'N'
   and upper(s.myr_use_school_provided_salutation) = 'N'
   and upper(s.myr_school_salutation_type) = 'INFORMAL';

-- 13.24 MYR PRINT FULLNAME
-- This field is dependent on whether the MYR SCHOOL FULLNAME GRAD YR FLAG is ‘Y’ or ‘N’.
-- If the MYR SCHOOL FULLNAME GRAD YR FLAG is ‘Y’ then assign the MYR ALUMNI FULLNAME GRAD YEAR to this field
-- Else /* if the MYR SCHOOL FULLNAME GRAD YR FLAG is ‘N’ */
-- Assign the MYR ALUMNI FULLNAME CREATED to this field
-- myrprintfullname

update {maintable_name}
   set myrprintfullname =
        case when ms.myr_school_grad_yr_fullname_flag = 'Y' then m.myralumnifullnamegradyear
             when ms.myr_school_grad_yr_fullname_flag = 'N' then m.myralumnifullnamecreated
        end
  from {maintable_name} m inner join
       meyer_school_new ms on m.myrschoolid = ms.myrschoolid;


-- 13.25 MYR SPOUSE PRINT FULLNAME
-- This field is dependent on whether the MYR SCHOOL FULLNAME GRAD YR FLAG is ‘Y’ or ‘N’.
-- If the MYR SCHOOL FULLNAME GRAD YR FLAG is ‘Y’ then assign the MYR SPOUSE FULLNAME GRAD YEAR to this field
-- Else /* if the MYR SCHOOL FULLNAME GRAD YR FLAG is ‘N’ */
-- Assign the MYR SPOUSE FULLNAME CREATED to this field
-- myrspouseprintfullname

update {maintable_name}
   set myrspouseprintfullname =
        case when ms.myr_school_grad_yr_fullname_flag = 'Y' then m.myrspousefullnamegradyear
             when ms.myr_school_grad_yr_fullname_flag = 'N' then m.myrspousefullnamecreated
        end
  from {maintable_name} m inner join
       meyer_school_new ms on m.myrschoolid = ms.myrschoolid;


-- 2022 November CB
-- JIRA: IDMS-2465 Meyer Product Flags
-- CHANGE REQUEST NUMBER: CR2022-007
-- Add new product fields and populate based on myralumniagerange and state
update {maintable_name}
   set myr_product_core =
          case when myralumniagerange in ('18-24','25-29','30-34','35-39','40-44','45-49','50-54','55-59','60-64','65-69','70-74')
                and nvl(state,'') not in ('AK','OR','SD','VT','WA')
               then 'C' else '' end,
       myr_product_core_qd =
          case when myralumniagerange in ('18-24','25-29','30-34','35-39','40-44','45-49')
                and nvl(state,'') not in ('AK','MT','NY','OR','SD','VT','UT','VA','WA')
               then 'QD' else '' end,
       myr_product_l4l_art_10yr =
          case when myralumniagerange in ('18-24','25-29','30-34','35-39','40-44','45-49','50-54','55-59','60-64','65-69','70-74')
                and nvl(state,'') not in ('AK','UT')
               then 'ART1' else '' end,
       myr_product_l4l_art_20yr =
          case when myralumniagerange in ('18-24','25-29','30-34','35-39','40-44','45-49','50-54','55-59','60-64')
                and nvl(state,'') not in ('AK','UT')
               then 'ART2' else '' end,
       myr_product_l4l_sap_10yr =
          case when myralumniagerange in ('18-24','25-29','30-34','35-39','40-44','45-49','50-54','55-59','60-64','65-69','70-74')
                and nvl(state,'') not in ('AK','NY','UT')
               then 'SAP1' else '' end,
       myr_product_l4l_sap_20yr =
          case when myralumniagerange in ('18-24','25-29','30-34','35-39','40-44','45-49','50-54','55-59','60-64')
                and nvl(state,'') not in ('AK','NY','UT')
               then 'SAP2' else '' end,
       myr_product_l95 =
          case when myralumniagerange in ('50-54','55-59','60-64','65-69','70-74')
                and nvl(state,'') not in ('AK','FL','LA','ME','MS','MT','NY','NC','OH','OR','SD','TX','UT','VT','WA')
               then 'L9' else '' end,
       myr_product_ad_nyl =
          case when myralumniagerange in ('18-24','25-29','30-34','35-39','40-44','45-49','50-54','55-59','60-64','65-69','70-74')
                and nvl(state,'') not in ('AK','NY','OR','SD','VT','WA')
               then 'Y' else '' end,
       myr_product_ad_met =
          case when myralumniagerange in ('18-24','25-29','30-34','35-39','40-44','45-49','50-54','55-59','60-64','65-69','70-74')
                and nvl(state,'') not in ('AK','UT')
               then 'M' else '' end,
       myr_product_pru_ci =
          case when myralumniagerange in ('18-24','25-29','30-34','35-39','40-44','45-49','50-54','55-59','60-64','65-69','70-74')
               then 'CI' else '' end,
       myr_product_pru_ac =
          case when myralumniagerange in ('18-24','25-29','30-34','35-39','40-44','45-49','50-54','55-59','60-64','65-69','70-74')
               then 'AC' else '' end,
       myr_product_pru_hip =
          case when myralumniagerange in ('18-24','25-29','30-34','35-39','40-44','45-49','50-54','55-59','60-64','65-69','70-74')
               then 'HIP' else '' end,
       myr_product_lm_bad_states =
          case when myralumniagerange in ('18-24','25-29','30-34','35-39','40-44','45-49','50-54','55-59','60-64','65-69','70-74')
                and nvl(state,'') in ('CA','AK')
               then 'Bad' else '' end,
       myr_product_lm_good_states =
          case when myralumniagerange in ('18-24','25-29','30-34','35-39','40-44','45-49','50-54','55-59','60-64','65-69','70-74')
                and nvl(state,'') not in ('CA','AK')
               then 'Good' else '' end,
       myr_product_ltc =
          case when myralumniagerange in ('45-49','50-54','55-59','60-64')
               then 'LTC' else '' end,
       myr_product_ltd =
          case when myralumniagerange in ('18-24','25-29','30-34','35-39','40-44','45-49','50-54','55-59')
               then 'LTD' else '' end
  from {maintable_name}
 where nvl(state,'') not in ('','AA','AE','AP','AS','FM','GU','MH','MP','PW','PR','VI')
;
-- end