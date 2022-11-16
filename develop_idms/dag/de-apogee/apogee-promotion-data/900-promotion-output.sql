--this is for final output
--we may want to export the non-matches too by removing the filter for checking for household_id
--Note that key fields (keycode, client_id, and project_id) are not unique in splitrecaps table
--so there will be duplicates
UNLOAD ('SELECT CE_Household_ID, CE_Selected_Individual_ID, a.KEYCODE,
                PERSONAL_NAME,PROFESSIONAL_TITLE,BUSINESS_NAME,AUXILLIARY_ADDRESS,
                SECONDARY_ADDRESS,PRIMARY_ADDRESS,CITY,STATE,ZIP,ZIP_4,PACKAGE_CODE,
                a.Client_id,a.Project_id,Package_ID,List_ID,List_Name,List_Name_2,
                Merge_Key,Quantity,Mailing_type
           FROM {mailfile_table} a
      LEFT JOIN (SELECT *
                  FROM (SELECT spl.*, ROW_NUMBER()
				                OVER (PARTITION BY project_id, client_id, keycode
                               ORDER BY CASE WHEN quantity ~ ''^[0-9]+$'' THEN quantity::NUMERIC
                                             WHEN quantity IS NULL      THEN 0
                                             ELSE -1
                                         END DESC) rn
                         FROM {splitrecap_table} spl)
                  WHERE rn = 1) b
             ON a.client_id = b.client_id AND a.project_id = b.project_id AND
                a.keycode = b.keycode
          WHERE NVL(CE_Household_ID, 0) <> 0')
    TO '{s3_report_location}'
       iam_role '{iam}'
       kms_key_id '{kmskey}'
       ENCRYPTED
       PARQUET
       CLEANPATH;


