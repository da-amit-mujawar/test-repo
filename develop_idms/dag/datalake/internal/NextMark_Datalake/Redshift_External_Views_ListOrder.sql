DROP TABLE IF EXISTS spectrumdb.List_Order;

CREATE EXTERNAL TABLE spectrumdb.List_Order 
(
base_actual_rate_units VARCHAR(1000) 
	,base_ap_rate NUMERIC(18,0) 
	,base_ar_rate NUMERIC(18,0)
	,base_broker_est_comm_rate NUMERIC(18,0)
	,base_commission_rate NUMERIC(18,0)  
	,base_commission_units VARCHAR(300) 
	,base_discount_rate NUMERIC(18,0)  
	,base_discount_units VARCHAR(300) 
	,base_manager_est_comm_rate NUMERIC(18,0)
	,base_name VARCHAR(300)   
	,base_select_name VARCHAR(300)
	,base_select_names_wrate VARCHAR(300)
	,campaign_name VARCHAR(300)   
	,cancellation_notes VARCHAR(300)
	,cancellation_status VARCHAR(300)
	,clearance_ref_no VARCHAR(300)  
	,clearance_status VARCHAR(300) 
	,cleared_by VARCHAR(300) 
	,client_account_name VARCHAR(300)
	,client_contact_name VARCHAR(300)
	,client_credit_approval_notes VARCHAR(300)
	,client_credit_approved_by VARCHAR(300)  
	,client_credit_required VARCHAR(300)  
	,client_credit_status VARCHAR(300)  
	,client_po_number VARCHAR(300)  
	,contact_name VARCHAR(300) 
	,date_cancelled TIMESTAMP WITHOUT TIME ZONE   
	,date_cleared TIMESTAMP WITHOUT TIME ZONE  
	,date_client_credit_approved TIMESTAMP WITHOUT TIME ZONE 
	,date_created TIMESTAMP WITHOUT TIME ZONE  
	,date_data_receipt_cutoff TIMESTAMP WITHOUT TIME ZONE
	,date_data_received TIMESTAMP WITHOUT TIME ZONE 
	,date_first_usage TIMESTAMP WITHOUT TIME ZONE
	,date_last_updated TIMESTAMP WITHOUT TIME ZONE 
	,date_last_usage TIMESTAMP WITHOUT TIME ZONE  
	,date_lo_object_last_updated TIMESTAMP WITHOUT TIME ZONE 
	,date_mail_end TIMESTAMP WITHOUT TIME ZONE 
	,date_mail_start TIMESTAMP WITHOUT TIME ZONE 
	,date_needed_by TIMESTAMP WITHOUT TIME ZONE 
	,date_ordered TIMESTAMP WITHOUT TIME ZONE  
	,date_revised TIMESTAMP WITHOUT TIME ZONE 
	,date_shipped TIMESTAMP WITHOUT TIME ZONE
	,exch_broker_run_chg_max_amt NUMERIC(18,0) 
	,exch_broker_run_chg_min_amt NUMERIC(18,0)
	,exch_manage_run_chg_max_amt NUMERIC(18,0)
	,exch_manage_run_chg_min_amt NUMERIC(18,0)
	,exchange_broker_run_charge_rate NUMERIC(18,0)
	,exchange_broker_run_charge_units VARCHAR(3) 
	,exchange_manager_run_charge_rate NUMERIC(18,0)
	,exchange_manager_run_charge_units VARCHAR(3) 
	,for_review_flag VARCHAR(3) 
	,key_code VARCHAR(300)
	,list_name VARCHAR(300)  
	,list_order_pc_paid NUMERIC(18,0) 
	,list_order_pc_received VARCHAR(300) 
	,list_order_financial_status VARCHAR(100)
	,list_order_name VARCHAR(100)  
	,list_order_number VARCHAR(300)
	,list_order_rentexch VARCHAR(100) 
	,list_order_status VARCHAR(100)  
	,list_order_testcont VARCHAR(100) 
	,list_order_type VARCHAR(100) 
	,mailer_account_name VARCHAR(100)
	,mailer_sb_account_name VARCHAR(100)
	,manager_account_name VARCHAR(100) 
	,mgr_contact_profit_center_code VARCHAR(300) 
	,mgr_contact_profit_center_name VARCHAR(300)
	,net_name_ap_pc VARCHAR(300) 
	,net_name_ap_run_charge_rate NUMERIC(18,0)
	,net_name_ar_pc VARCHAR(300)
	,net_name_ar_run_charge_rate NUMERIC(18,0) 
	,net_name_run_charge_units VARCHAR(3) 
	,net_name_run_chg_comm_rate NUMERIC(18,0) 
	,net_name_run_chg_comm_units VARCHAR(300)
	,net_name_run_chg_disc_rate NUMERIC(18,0)
	,net_name_run_chg_disc_units VARCHAR(10)
	,number_clickthroughs NUMERIC(18,0)
	,number_delivered NUMERIC(18,0) 
	,number_opens NUMERIC(18,0)
	,number_orders NUMERIC(18,0) 
	,number_responses NUMERIC(18,0) 
	,number_who_clicked NUMERIC(18,0) 
	,offer_description VARCHAR(200)  
	,offer_type VARCHAR(200)   
	,omit_names VARCHAR(200)   
	,output_ap_rate_per_f NUMERIC(18,0)   
	,output_ap_rate_per_m NUMERIC(18,0)   
	,output_ar_rate_per_f NUMERIC(18,0)   
	,output_ar_rate_per_m NUMERIC(18,0)   
	,output_commission_rate_per_f NUMERIC(18,0)   
	,output_commission_rate_per_m NUMERIC(18,0)   
	,output_discount_rate_per_f NUMERIC(18,0)   
	,output_discount_rate_per_m NUMERIC(18,0)   
	,owner_account_name VARCHAR(100)   
	,owner_exch_ship_bal VARCHAR(100)   
	,owner_parent_account VARCHAR(100)   
	,owner_sb_account_name VARCHAR(100)   
	,preclearance_yn VARCHAR(3)   
	,profit_center_code VARCHAR(1000)   
	,profit_center_division VARCHAR(300)   
	,profit_center_name VARCHAR(300)   
	,qty_at_full_rate NUMERIC(18,0)   
	,qty_at_run_rate NUMERIC(18,0)   
	,qty_available NUMERIC(18,0)   
	,qty_order_exchange NUMERIC(18,0)   
	,qty_order_rental NUMERIC(18,0)   
	,qty_order_total NUMERIC(18,0)   
	,qty_rental_useable_names VARCHAR(300)   
	,qty_ship_exchange NUMERIC(18,0)   
	,qty_ship_rental NUMERIC(18,0)   
	,qty_ship_total NUMERIC(18,0)   
	,qty_total_useable_names NUMERIC(18,0)   
	,qty_usage_to_date NUMERIC(18,0)   
	,quantity_rule VARCHAR(300)   
	,reuse_type VARCHAR(300)   
	,salesperson_alias VARCHAR(300)   
	,salesperson_full_name VARCHAR(300)   
	,select_ap_rate_per_f NUMERIC(18,0)   
	,select_ap_rate_per_m NUMERIC(18,0)   
	,select_ar_rate_per_m NUMERIC(18,0)   
	,select_ar_rate_per_f NUMERIC(18,0)   
	,select_commission_rate_per_f NUMERIC(18,0)   
	,select_commission_rate_per_m NUMERIC(18,0)   
	,select_discount_rate_per_f NUMERIC(18,0)   
	,select_discount_rate_per_m NUMERIC(18,0)   
	,select_names VARCHAR(300)   
	,ship_label VARCHAR(300)   
	,ship_to_contact_name VARCHAR(300)   
	,shipping_amount NUMERIC(18,0)   
	,shipping_method VARCHAR(300)   
	,tax_amount NUMERIC(18,0)   
	,vendor_credit_status VARCHAR(300)   
	,nm_campaign_id NUMERIC(18,0)   
	,nm_client_account_id NUMERIC(18,0)   
	,nm_list_id NUMERIC(18,0)   
	,nm_list_order_id NUMERIC(18,0)   
	,nm_mailer_account_id NUMERIC(18,0)   
	,nm_mailer_serv_bur_acct_id NUMERIC(18,0)   
	,nm_manager_account_id NUMERIC(18,0)   
	,nm_owner_acct_id NUMERIC(18,0)   
	,nm_owner_sb_account_id NUMERIC(18,0)   
	,nm_salesperson_member_id NUMERIC(18,0)   
	,nm_owner_parent_acct_id VARCHAR(300)   
	,xcreated_date TIMESTAMP WITHOUT TIME ZONE   
	,nm_client_contact_id VARCHAR(300)   
	,cv_dma_drop_number NUMERIC(18,0)   
	,cv_hygiene_drop_number NUMERIC(18,0)   
	,cv_intrafile_dups_number NUMERIC(18,0)   
	,cv_mailable_names VARCHAR(300)   
	,cv_nixie_drop_number NUMERIC(18,0)   
	,cv_other_drop_description VARCHAR(1000)   
	,cv_other_drop_number NUMERIC(18,0)   
	,cv_total_drop_number NUMERIC(18,0)   
	,exchange_reconciled_yn VARCHAR(3)   
	,output_names VARCHAR(300)   
	,clearance_instructions VARCHAR(300)   
	,shipping_method_ref VARCHAR(300)   
	,nm_ship_to_contact_id VARCHAR(300)   
	,ship_to_city VARCHAR(300)   
	,ship_to_postal_code VARCHAR(300)   
	,ship_to_state VARCHAR(300)   
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   'separatorChar' = '|',
   'acceptinvchars'='',
   'quoteChar' = '"',
   'escapeChar' = '\\'
   )
STORED AS textfile
LOCATION 's3://axle-internal-sources/raw/nextmark/listorder'
TABLE PROPERTIES ('skip.header.line.count'='1');

 
-------------------------------------------
-- Create a view in interna schema
-------------------------------------------
DROP VIEW IF EXISTS interna.List_Order;

CREATE VIEW interna.List_Order 
AS 
SELECT * 
  FROM spectrumdb.List_Order
  WITH NO SCHEMA BINDING;
 

-- Verify Counts
SELECT COUNT(*) 
  FROM interna.List_Order;

SELECT TOP 100 * 
  FROM interna.List_Order;

