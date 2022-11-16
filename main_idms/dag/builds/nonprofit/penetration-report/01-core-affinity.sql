

--generate matching count in last 24 month
select {total_donors} as total_donors, sourcelistid,count(distinct individual_id) as total_matched_donors,
       (nvl(count(distinct individual_id),0)*1.0/{total_donors})  overlap 
  from {child_table_name}_{buildid}_{build}  
 where to_date(detail_donationdate,'YYYYMMDD')>=add_months(getdate(),-{NOM})
   and sourcelistid NOT IN ({consumer_listid}) -- Add optional runtime list of listids to be excluded
   and individual_id in ( 
                          select individual_id as individual_id 
                            from {child_table_name}_{buildid}_{build}
                           where sourcelistid = {Source_List_ID}
                           group by individual_id 
                        )
  group by sourcelistid 
  order by sourcelistid;
