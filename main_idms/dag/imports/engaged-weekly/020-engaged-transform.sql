-- remove the troubled records
delete from {tablename1}
    where
    (emailaddress like '%MURPHYSMITH2139@YAHOO.COM%'
     or emailaddress like '%MSMITH@CHANNELNET.COM%'
     or emailaddress like '%SPAM%'
     or emailaddress like 'ABUSE@%'
     or emailaddress like 'SPOOF@%'
     or emailaddress not like '%@%'
     or opened like '%E%')
     or (opened in('1E3','2E3','3E3','7E3'))
     or (emailaddress='' or emailaddress is null)
    or
    (SUBSTRING(emailaddress,(charindex('@',emailaddress)+1),(LEN(emailaddress)-(charindex('@',emailaddress)))) in
    ('SPAMHAUS.ORG','SPAM-RBL.COM','SPAMTRAP.US','DSBL.ORG','PROJECTHONEYPOT.ORG',
	'SPAMCOP.NET','SORBS.NET','MCAFEEASAP.COM','SPAMSOAP.COM','ABUSE.NET',
	'LAMBEAUTELE.COM','SECURENCE.COM','BARRACUDACENTRAL.ORG','ABUSIX.ORG',
	'SPAMTRAPADDRESS.COM'))
	or
     ((open_date='' or open_date is null) and (click_date='' or click_date is null));

---fix the blank open date and clickdate>opendate
update {tablename1}
	set open_date=click_date
		where (open_date='' or open_date is null or click_date>open_date);

--apply domain

alter table {tablename1}
add column domain_name varchar(80) default null;

update {tablename1}
	set domain_name=
	substring(emailaddress,(charindex('@',emailaddress)+1),
	    (LEN(emailaddress)-(charindex('@',emailaddress))));

--apply flag from global suppression file
alter table {tablename1}
add column gst varchar(1) default null;

update {tablename1}
	set gst= {gst}.suppression_type
	from {gst}
	where {gst}.emailaddress = {tablename1}.emailaddress;

--apply MD5
drop table if exists {tablename3};

create table {tablename3} as
    select *,
    md5(upper(emailaddress)) as md5_upper,
    md5(lower(emailaddress)) as md5_lower,
    ' ' as click_chg_flag
    from {tablename1};

--fix clicked, where click_date<>' ' and clicked='0'
update {tablename3}
set clicked='1' ,
    click_chg_flag='Y'
where click_date<>' ' and clicked='0';

--create 12mo file
drop table if exists exclude_engaged_12_mo;

create table exclude_engaged_12_mo as
select * from {tablename3}
where to_date(cast(open_date as date), 'YYYY-MM-DD')>= to_date(DATEADD(month, -12, getdate()),'YYYY-MM');

--create 24mo file
drop table if exists exclude_engaged_24_mo;

create table exclude_engaged_24_mo as
    select emailaddress, open_date, click_date, opened, clicked,
           optout, gst, domain_name, md5_upper, md5_lower
    from {tablename3}
	where to_date(cast(open_date as date), 'YYYY-MM-DD')>= to_date(DATEADD(month, -24, getdate()),'YYYY-MM');

--engaged_domain_db
insert into {tablename2}(domain_name)
select distinct domain_name
from {tablename3};

--apply domain_name counts
drop table if exists {engageddtemp};

create table {engageddtemp} as
    select domain_name,count(*) as count
    from {tablename3}
	group by domain_name;

update {tablename2}
	set email_count = {engageddtemp}.count
		from {engageddtemp}
		where {engageddtemp}.domain_name = {tablename2}.domain_name;

--apply last_open_date/last_clicked_date
--apply times opened/times clicked
drop table if exists {engagedtemp};

create table {engagedtemp} as
select domain_name,max(open_date) as last_open_date,
       max(click_date) as last_click_date,
       sum(cast(clicked as int)) as times_clicked,
       sum(cast(opened as int)) as times_opened
	from {tablename3}
	group by domain_name;

update {tablename2}
	set last_open_date = {engagedtemp}.last_open_date,
	    last_click_date = {engagedtemp}.last_click_date,
	    times_clicked = {engagedtemp}.times_clicked,
	    times_opened = {engagedtemp}.times_opened
		from {engagedtemp}
		where {engagedtemp}.domain_name={tablename2}.domain_name;

drop table if exists {engagedtemp};
drop table if exists {engageddtemp};