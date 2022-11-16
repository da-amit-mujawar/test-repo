--Pre-mover export for Apogee
drop table if exists premover_74_new;
create table premover_74_new (
    record_id varchar(115),
    vendorsourceid char(2),
    mfduindicator char(1),
    property_type char(1),
    listing_status varchar(12),
    new_record_date varchar(8),
    pending_sold_date varchar(8),
    list_price_range varchar(2),
    square_foot_range varchar(2),
    cdb_indvid varchar(25),
    cdb_hhldid varchar(25)
    );

insert into premover_74_new (
    record_id,
    vendorsourceid,
    mfduindicator,
    property_type,
    listing_status,
    new_record_date,
    pending_sold_date,
    list_price_range,
    square_foot_range,
    cdb_indvid,
    cdb_hhldid)
select
    p.record_id,
    p.vendorsourceid,
    p.mfduindicator,
    p.property_type,
    p.listing_status,
    p.new_record_date,
    p.pending_sold_date,
    p.list_price_range,
    p.square_foot_range,
    p.cdb_indvid,
    p.cdb_hhldid
from {maintable_name} p inner join
     {latest_maintablename} c on p.cdb_indvid = c.individual_id
where nvl(p.cdb_indvid,'') <> '';


unload ('select
    left(record_id,18),
    vendorsourceid,
    mfduindicator,
    property_type,
    listing_status,
    new_record_date,
    pending_sold_date,
    list_price_range,
    square_foot_range,
    left(cdb_indvid,12),
    left(cdb_hhldid,12)
from premover_74_new;')
to 's3://{s3-listcoversion}/etl/apogee/premover/premover_74.txt'
iam_role '{iam}'
fixedwidth as 'record_id:18,vendorsourceid:2,mfduindicator:1,property_type:1,listing_status:12,new_record_date:8,
    pending_sold_date:8,list_price_range:2,square_foot_range:2,cdb_indvid:12,cdb_hhldid:12'
encrypted
parallel off
allowoverwrite;

drop table if exists premover_74 ;
alter table premover_74_new rename to premover_74;



