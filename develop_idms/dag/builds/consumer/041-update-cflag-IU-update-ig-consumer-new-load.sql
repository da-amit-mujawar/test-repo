DROP TABLE IF EXISTS NoSuchTable;


UPDATE ig_consumer_email_new_temp SET cflag ='IU'
FROM ig_consumer_email_new_temp A 
INNER JOIN
(
  SELECT individual_id, MIN(emailaddress) AS min_email FROM ig_consumer_email_new_temp 
  WHERE individual_id<>'000000000000' AND individual_id<>'' AND individual_id IS NOT NULL AND opt_out_flag='0'
  GROUP BY individual_id) B
  ON A.individual_id = B.individual_id AND 
     A.emailaddress=B.min_email AND 
     A.individual_id IN(SELECT individual_id FROM {new-load-table}
                        WHERE individual_id<>'000000000000' AND 
                        individual_id<>'' AND 
                        individual_id IS NOT NULL);

--UPDATE recordefor IU flag
UPDATE {new-load-table}
SET opt_out_flag=B.opt_out_flag,
    emailaddress=B.emailaddress,
    vendor_code=B.vendor_code,
    categories=B.categories,
    domain=B.epd_domain, 
    top_level_domain=B.top_level_domain,
    opt_out_date=B.opt_out_date,
    car_make=B.car_make,
    car_model=B.car_model,
    car_year=B.car_year,
    date_entered_yyyymm=B.date_entered_yyyymm,
    email_domain=B.email_domain,
    open_flag=B.open_flag,
    open_date_yyyymm=B.open_date_yyyymm,
    click_flag=B.click_flag,
    click_date_yyyymm=B.click_date_yyyymm,
    md5_email_lower=B.md5_email_lower,
    md5_email_upper=B.md5_email_upper,
    ipaddress=B.ipaddress,
    url=B.url ,
    sha256_email=B.sha256_email ,
    sha512_email=B.sha512_email ,
    bv_flag=B.bv_flag,
    marigold=B.marigold,
    bvt_refresh_date=B.bvt_refresh_date,
    ipst_status_code=B.ipst_status_code,
    ipst_refresh_date=B.ipst_refresh_date,
    bestdate=B.best_date_yyyymmdd,
    reactivation_flag=B.reactivation_flag,
    mgen_match_flag=B.mgen_match_flag,
    bridge_code=B.bridge_code,
    best_date_range=B.best_date_range,
    matchcode_lrfs=B.matchcode_lrfs,
    haspostal='U',
    email_deliverable = B.email_deliverable,
    email_marketable=B.email_marketable,
    email_reputation_risk=B.email_reputation_risk,
    email_deployable = B.email_deployable
FROM {new-load-table} A 
INNER JOIN ig_consumer_email_new_temp B 
ON A.individual_id = B.individual_id AND 
   B.cflag ='IU' AND 
   A.haspostal ='Y';
   

