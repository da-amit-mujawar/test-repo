DROP TABLE IF EXISTS  interna.ebsprod_gaap_detail;

with ords as (
select orderid, LISTAGG(fulfillmentsystemid,',') as fulfillmentsystemid
from interna.oess_orderfulfillmentsystems
group by orderid)
select gd.*,ords.fulfillmentsystemid
into interna.ebsprod_gaap_detail
from interna.ebsprod_datalake_gaap_detail gd left outer join ords on ords.orderid=split_part(sales_order,'TNK',1);

DROP TABLE IF EXISTS reports.ebsprod_gaap_detail_attrb;

CREATE TABLE reports.ebsprod_gaap_detail_attrb 
AS
SELECT g.*,
       CASE
         WHEN m.division_tag IS NULL THEN l.division_tag
         ELSE m.division_tag
       END division_tag,
       CASE
         WHEN m.channel IS NULL THEN l.channel
         ELSE m.channel
       END channel,
       CASE
         WHEN m.division_channel IS NULL THEN l.division_channel
         ELSE m.division_channel
       END division_channel,
       CASE
         WHEN m.channel_sub_souce_key IS NULL THEN l.channel_sub_souce_key
         ELSE m.channel_sub_souce_key
       END channel_sub_souce_key,
       CASE
         WHEN m.media_code_prefix IS NULL THEN l.media_code_prefix
         ELSE m.media_code_prefix
       END media_code_prefix,
       CASE
         WHEN (FulfillmentSystemId LIKE ('%1%') AND FulfillmentSystemId NOT LIKE ('%10%') AND sales_order LIKE '%TNK%' AND p.usa_inventory_item_flg = 'Y' AND r.revenue_type = 'Not Classified') THEN ('Transactional')
         WHEN (sales_order NOT LIKE '%TNK%' AND p.usa_inventory_item_flg = 'Y' AND r.revenue_type = 'Not Classified') THEN ('Transactional')
         WHEN (r.revenue_type IS NULL) THEN ('Others')
         ELSE r.revenue_type
       END revenue_type,
       CASE
         WHEN (d.divisions IS NULL) THEN ('Others')
         ELSE d.divisions
       END divisions,
       CASE
         WHEN (d.business_units IS NULL) THEN ('Others')
         ELSE d.business_units
       END business_units,
       CASE
         WHEN (p.product_group IS NULL) THEN ('Others')
         ELSE p.product_group
       END product_group,
       CASE
         WHEN (FulfillmentSystemId LIKE ('%1%') AND FulfillmentSystemId NOT LIKE ('%10%') AND sales_order LIKE '%TNK%' AND p.usa_inventory_item_flg = 'Y') THEN ('Data Axle USA')
         WHEN (sales_order NOT LIKE '%TNK%' AND p.usa_inventory_item_flg = 'Y') THEN ('Data Axle USA')
         ELSE p.product_group
       END da_product_group,
       CASE
         WHEN (db.database_name IS NULL) THEN ('Others')
         ELSE (db.database_name)
       END database_name,
       CASE
         WHEN (sp.sub_product IS NULL) THEN ('Others')
         ELSE (sp.sub_product)
       END sub_product
FROM interna.ebsprod_gaap_detail g
  LEFT JOIN reports.media_code_lookup m ON (SUBSTRING (UPPER (g.media_code),1,7) = m.media_code_prefix)
  LEFT JOIN reports.media_code_lookup_legacy l ON (media_code = l.old_media_code_prefix)
  LEFT JOIN reports.revenue_type_lookup r ON g.inventory_item_id = r.inventory_item_id
  LEFT JOIN reports.product_group_lookup p ON g.inventory_item_id = p.inventory_item_id
  LEFT JOIN reports.division_group_lookup d ON g.segment2 = d.segment2
  LEFT JOIN reports.database_lookup db ON g.inventory_item_id = db.inventory_item_id
  LEFT JOIN reports.subproduct_lookup sp ON sp.inventory_item_id = db.inventory_item_id;

UPDATE reports.ebsprod_gaap_detail_attrb
   SET division_tag = 'Others',
       division_channel = 'Others',
       channel = 'Others',
       channel_sub_souce_key = 'Others',
       media_code_prefix='Others'
WHERE division_tag IS NULL;

DROP TABLE IF EXISTS reports.ebsprod_gaap_detail_attrb_table;

CREATE TABLE reports.ebsprod_gaap_detail_attrb_table 
AS
SELECT g.*
FROM reports.ebsprod_gaap_detail_attrb g
WHERE g.invoice_date >= '2015-01-01'
AND   g.gldist_amount != 0
AND   g.revenue_pct_split IN (100,-100)
AND   g.segment3 = '410000'
AND   g.gl_posted_date IS NOT NULL;
