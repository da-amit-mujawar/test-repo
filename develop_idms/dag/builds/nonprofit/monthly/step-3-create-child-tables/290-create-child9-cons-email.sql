/*
 Create Apogee Child9 Table Consumer Emails extracted from 1267.
 Source is 1267 and Main Table
 This needs IDMS enhancement to allow Max-per on Email Address which is in the Child Table. IDMS currently allows only for Max-per be in the main table. TBD
 
 Update Schedule: Monthly
*/

drop table if exists ctas_exclude_tblconsemailapogee_tobedropped;
create table ctas_exclude_tblconsemailapogee_tobedropped
(
    emailaddress          varchar(80)     encode zstd,
    digitalmatch          varchar(1)      encode zstd,
    opt_out_flag          varchar(1)      encode zstd,
    vendor_code           varchar(2)      encode zstd,
    domain                varchar(80)     encode zstd,
    top_level_domain      varchar(80)     encode zstd,
    md5_email_lower       varchar(32)     encode zstd,
    md5_email_upper       varchar(32)     encode zstd,
    sha256_email          varchar(64)     encode zstd,
    sha512_email          varchar(128)    encode zstd,
    bv_flag               varchar(1)      encode zstd,
    marigold              varchar(1)      encode zstd,
    bvt_refresh_date      varchar(8)      encode zstd,
    ipst_refresh_date     varchar(8)      encode zstd,
    bestdate              varchar(8)      encode zstd,
    mgen_match_flag       varchar(1)      encode zstd,
    bridge_code           varchar(1)      encode zstd,
    best_date_range       varchar(1)      encode zstd,
    cinclude              varchar(1)      encode zstd,
    email_deliverable     varchar(1)      encode zstd,
    email_marketable      varchar(1)      encode zstd,
    email_reputation_risk varchar(1)      encode zstd,
    individual_id         varchar(25)     encode zstd,
    company_id            varchar(25)     encode zstd,
    email_match_level     varchar(1)      encode zstd -- calculated
);

--individual_id match primary
drop table if exists ctas_email_match_i_tobedropped;
select distinct a.individual_id
into ctas_email_match_i_tobedropped
from {maintable_name} a inner join
	 exclude_ConsEmail4Apogee b
on a.individual_id = b.individual_id
where nvl(b.emailaddress, '') <> '';

--no individual_id match secondary
drop table if exists ctas_email_nonmatch_i_tobedropped;
select distinct individual_id, company_id
into ctas_email_nonmatch_i_tobedropped
from {maintable_name}
where individual_id not in (select individual_id from ctas_email_match_i_tobedropped);


insert into ctas_exclude_tblconsemailapogee_tobedropped
(select distinct emailaddress,
                 digitalmatch,
                 opt_out_flag,
                 vendor_code,
                 domain,
                 top_level_domain,
                 md5_email_lower,
                 md5_email_upper,
                 sha256_email,
                 sha512_email,
                 bv_flag,
                 marigold,
                 bvt_refresh_date,
                 ipst_refresh_date,
                 bestdate,
                 mgen_match_flag,
                 bridge_code,
                 best_date_range,
                 cinclude,
                 email_deliverable,
                 email_marketable,
                 email_reputation_risk,
                 a.individual_id,
                 a.company_id,
                 'I' as email_match_level
 from exclude_ConsEmail4Apogee a
         inner join ctas_email_match_i_tobedropped b
 on a.individual_id = b.individual_id);

insert into ctas_exclude_tblconsemailapogee_tobedropped
(select distinct emailaddress,
                 digitalmatch,
                 opt_out_flag,
                 vendor_code,
                 domain,
                 top_level_domain,
                 md5_email_lower,
                 md5_email_upper,
                 sha256_email,
                 sha512_email,
                 bv_flag,
                 marigold,
                 bvt_refresh_date,
                 ipst_refresh_date,
                 bestdate,
                 mgen_match_flag,
                 bridge_code,
                 best_date_range,
                 cinclude,
                 email_deliverable,
                 email_marketable,
                 email_reputation_risk,
                 a.individual_id,
                 a.company_id,
                 'H' as email_match_level
 from exclude_ConsEmail4Apogee a
         inner join ctas_email_nonmatch_i_tobedropped b
 on a.company_id = b.company_id and nvl(a.emailaddress,'') <> ''
);

drop table if exists exclude_nonprofit_tblchild9_{dbid};
create table exclude_nonprofit_tblchild9_{dbid}
(
    emailaddress          varchar(80)      encode zstd,
    digitalmatch          varchar(1)       encode zstd,
    opt_out_flag          varchar(1)       encode zstd,
    vendor_code           varchar(2)       encode zstd,
    domain                varchar(80)      encode zstd,
    top_level_domain      varchar(80)      encode zstd,
    md5_email_lower       varchar(32)      encode zstd,
    md5_email_upper       varchar(32)      encode zstd,
    sha256_email          varchar(64)      encode zstd,
    sha512_email          varchar(128)     encode zstd,
    bv_flag               varchar(1)       encode zstd,
    marigold              varchar(1)       encode zstd,
    bvt_refresh_date      varchar(8)       encode zstd,
    ipst_refresh_date     varchar(8)       encode zstd,
    bestdate              varchar(8)       encode zstd,
    mgen_match_flag       varchar(1)       encode zstd,
    bridge_code           varchar(1)       encode zstd,
    best_date_range       varchar(1)       encode zstd,
    cinclude              varchar(1)       encode zstd,
    email_deliverable     varchar(1)       encode zstd,
    email_marketable      varchar(1)       encode zstd,
    email_reputation_risk varchar(1)       encode zstd,
    individual_id         varchar(25)      ,
    company_id            varchar(25)      encode zstd,
    email_match_level     varchar(1)       encode zstd
)
diststyle key
distkey (individual_id)
sortkey (individual_id)
;

insert into exclude_nonprofit_tblchild9_{dbid}
    (select emailaddress,
                 digitalmatch,
                 opt_out_flag,
                 vendor_code,
                 domain,
                 top_level_domain,
                 md5_email_lower,
                 md5_email_upper,
                 sha256_email,
                 sha512_email,
                 bv_flag,
                 marigold,
                 bvt_refresh_date,
                 ipst_refresh_date,
                 bestdate,
                 mgen_match_flag,
                 bridge_code,
                 best_date_range,
                 cinclude,
                 email_deliverable,
                 email_marketable,
                 email_reputation_risk,
                 individual_id,
                 company_id,
                 email_match_level
    from ctas_exclude_tblconsemailapogee_tobedropped);

drop table if exists ctas_email_match_i_tobedropped;
drop table if exists ctas_email_nonmatch_i_tobedropped;
drop table if exists ctas_exclude_tblconsemailapogee_tobedropped;
