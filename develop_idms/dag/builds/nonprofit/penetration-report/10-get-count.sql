--generate total universe in last 24 month
-- This SQL Runs for One List at a time in a loop
select count(distinct individual_id) as total_donors 
  from {child_table_name}_{buildid}_{build}  -- {tblanme}_{BuildId}_{Build}
  where to_date(detail_donationdate,'YYYYMMDD')>=add_months(getdate(),-{NOM})
    and sourcelistid = {Source_List_ID};
    