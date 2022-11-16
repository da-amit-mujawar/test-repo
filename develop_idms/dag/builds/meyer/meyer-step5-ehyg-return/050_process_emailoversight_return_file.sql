--- Process eo file

alter table {table1-myr} add Engaged_Flag varchar(1) default 'N';
alter table {table1-myr} add Engaged_Open_Date varchar(8) default 'N';
alter table {table1-myr} add Engaged_Click_Date varchar(8) default 'N';


--alter table {maintable_name} add emailscoreflag varchar(1) default '';
--alter table {maintable_name} add eoverifiedemail varchar(2) default '';
--alter table {maintable_name} add email_Suppression_Flag varchar(1) default '';


Update {table1-myr}
	set Engaged_Flag='Y',
		Engaged_Open_Date=open_date,
		Engaged_Click_Date=click_date
		from {table-engaged-db}
		where {table1-myr}.email=	{table-engaged-db}.emailaddress;


alter table {table1-myr} add gst varchar(1) default '';

update {table1-myr}
	set gst=suppression_type
	from {table-gst-db}
		where {table1-myr}.email={table-gst-db}.emailaddress;

--set the new codes
alter table {table1-myr} add Ehygiene_Code varchar(1) default '';

update  {table1-myr}
	set Ehygiene_Code =
		case
		when ValidationStatus = 'Role'              or
		     ValidationStatus = 'Bot'               or
			 ValidationStatus = 'Malformed'         or
			 ValidationStatus = 'Disposable Email'  or
			 ValidationStatus = 'Seed Account'      or
			 ValidationStatus = 'Undeliverable'     or
			 ValidationStatus = 'SpamTrap'			   then '0'
		when Engaged_Flag = 'Y'						   then '1'
		when ValidationStatus = 'Verified'             then '2'
		when ValidationStatus = 'Catch All'            then '3'
		when ValidationStatus = 'Unknown'           or
			 ValidationStatus = 'Complainer'	       then '4'
		else '0'
	end
		where email >' ';

--Create the IDMS EO Codes
alter table {table1-myr} add eoverifiedemail varchar(2) default '';

update {table1-myr}
	set eoverifiedemail =
	Case
		when ValidationStatus='Verified'      then 'VE'
		when ValidationStatus='Catch All' then 'VD'
		when ValidationStatus='Unknown'    then 'UV'
	else ' '
	end;






-- 1 exists EMAILOVERSIGHTEMAILCD - ValidationStatusId

-- 2 exists    emailalble - Y/N
alter table {table1-myr} add emailable varchar(1) default 'Y';
update {table1-myr} set emailable = 'N' where ValidationStatusId in('2','4','5','6','7','9','11','13');


-- 3 emailscoreflag - we generate with 1-4 need to add to main
alter table {table1-myr} add emailscoreflag varchar(1) default ' ';
update {table1-myr} set emailscoreflag = Ehygiene_Code where Ehygiene_Code in('1','2','3','4');


-- 4 EOVerifiedEmail (2) - update with eoverifiedemail


-- 5 Email_Suppression_Flag set to Y for emailscoreflag=0 else N (good record)
alter table {table1-myr} add Email_Suppression_Flag varchar(1) default 'N';
update {table1-myr} set Email_Suppression_Flag = 'Y' where Ehygiene_Code = '0' or (gst is not null or gst != '');


update {maintable_name}
   set EMAILOVERSIGHTEMAILCD = b.validationstatusId,    --eo return
--       emailable = b.emailable,     --calculated (does not include gst) --20210811 CB: removed see below
       emailscoreflag = b.emailscoreflag,   --eo + engaged
       eoverifiedemail = b.eoverifiedemail, --eo verified, catchall, unknown
       email_Suppression_Flag = b.email_Suppression_Flag    --eo + gst
    from {table1-myr} b where cemail=b.email
       and nvl(cemail,'')<>'';


--20210811 CB: skip un-marketable records
/*There are several cases when the alumni last name or the spouse last name contains only 1 character.
  These individuals are not valid from a marketing perspective.
  Any record where the spouse last name or alumni last name is only 1 character,
  set the Mailable flag to ‘N’ and the Emailable flag to ‘N’. */
update {maintable_name} a
   set emailable = b.emailable     --calculated (does not include gst)
    from myr_emailoversight_return b where cemail=b.email
       and nvl(cemail,'')<>''
       and (a.emailable <> 'N' or nvl(a.emailable,'')='') ;

drop table if exists {table1-myr};







