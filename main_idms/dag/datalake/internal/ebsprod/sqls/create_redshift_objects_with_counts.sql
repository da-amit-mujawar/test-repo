DROP TABLE IF EXISTS spectrumdb.ebsprod_ar_ar_collectors;

CREATE EXTERNAL TABLE spectrumdb.ebsprod_ar_ar_collectors
(
	collector_id BIGINT
	,last_updated_by INTEGER
	,last_update_date TIMESTAMP
	,last_update_login BIGINT
	,creation_date TIMESTAMP
	,created_by INTEGER
	,name VARCHAR(1000)
	,employee_id INTEGER
	,description VARCHAR(1000)
	,status VARCHAR(1000)
	,inactive_date VARCHAR(1000)
	,alias VARCHAR(1000)
	,telephone_number VARCHAR(1000)
	,attribute_category VARCHAR(1000)
	,attribute1 VARCHAR(1000)
	,attribute2 VARCHAR(1000)
	,attribute3 VARCHAR(1000)
	,attribute4 VARCHAR(1000)
	,attribute5 VARCHAR(1000)
	,attribute6 VARCHAR(1000)
	,attribute7 VARCHAR(1000)
	,attribute8 VARCHAR(1000)
	,attribute9 VARCHAR(1000)
	,attribute10 VARCHAR(1000)
	,attribute11 VARCHAR(1000)
	,attribute12 VARCHAR(1000)
	,attribute13 VARCHAR(1000)
	,attribute14 VARCHAR(1000)
	,attribute15 VARCHAR(1000)
	,resource_id INTEGER
	,resource_type VARCHAR(1000)
	,zd_edition_name VARCHAR(1000)
	,zd_sync VARCHAR(1000)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   'separatorChar' = '|',
   'quoteChar' = '"',
   'escapeChar' = '\\'
   )
STORED AS textfile
LOCATION 's3://axle-internal-sources/raw/ebsprod/ar_ar_collectors/'
TABLE PROPERTIES ('compression_type'='gzip');

-------------------------------------------
-- Create a view in interna schema
-------------------------------------------
DROP VIEW IF EXISTS interna.ebsprod_ar_ar_collectors;

CREATE VIEW interna.ebsprod_ar_ar_collectors 
AS 
SELECT * 
  FROM spectrumdb.ebsprod_ar_ar_collectors
  WITH NO SCHEMA BINDING;
 

-- Verify Counts
SELECT COUNT(*) 
  FROM interna.ebsprod_ar_ar_collectors;

SELECT TOP 10 * 
  FROM interna.ebsprod_ar_ar_collectors;


DROP TABLE IF EXISTS spectrumdb.ebsprod_gl_gl_accts_desc_table;

CREATE EXTERNAL TABLE spectrumdb.ebsprod_gl_gl_accts_desc_table
(
	code_combination_id INTEGER
	,segment1 VARCHAR(1000)
	,segment2 VARCHAR(1000)
	,segment3 VARCHAR(1000)
	,segment4 VARCHAR(1000)
	,segment5 VARCHAR(1000)
	,segment6 VARCHAR(1000)
	,concatenated_segments VARCHAR(1000)
	,segment1_desc VARCHAR(1000)
	,segment2_desc VARCHAR(1000)
	,segment3_desc VARCHAR(1000)
	,segment4_desc VARCHAR(1000)
	,segment5_desc VARCHAR(1000)
	,segment6_desc VARCHAR(1000)
	,concat_segment_desc VARCHAR(1000)
	,last_update_date TIMESTAMP
	,run_date TIMESTAMP
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   'separatorChar' = '|',
   'quoteChar' = '"',
   'escapeChar' = '\\'
   )
STORED AS textfile
LOCATION 's3://axle-internal-sources/raw/ebsprod/gl_gl_accts_desc_table/'
TABLE PROPERTIES ('compression_type'='gzip');


-------------------------------------------
-- Create a view in interna schema
-------------------------------------------
DROP VIEW IF EXISTS interna.ebsprod_gl_gl_accts_desc_table;

CREATE VIEW interna.ebsprod_gl_gl_accts_desc_table 
AS 
SELECT * 
  FROM spectrumdb.ebsprod_gl_gl_accts_desc_table
  WITH NO SCHEMA BINDING;
 

-- Verify Counts
SELECT COUNT(*) 
  FROM interna.ebsprod_gl_gl_accts_desc_table;

SELECT TOP 10 * 
  FROM interna.ebsprod_gl_gl_accts_desc_table;

DROP TABLE IF EXISTS spectrumdb.ebsprod_gl_gl_code_combinations;
 
CREATE EXTERNAL TABLE spectrumdb.ebsprod_gl_gl_code_combinations (
	code_combination_id BIGINT
	,last_update_date TIMESTAMP
	,last_updated_by INTEGER
	,chart_of_accounts_id BIGINT
	,detail_posting_allowed_flag VARCHAR(1000)
	,detail_budgeting_allowed_flag VARCHAR(1000)
	,account_type VARCHAR(1000)
	,enabled_flag VARCHAR(1000)
	,summary_flag VARCHAR(1000)
	,segment1 VARCHAR(1000)
	,segment2 VARCHAR(1000)
	,segment3 VARCHAR(1000)
	,segment4 VARCHAR(1000)
	,segment5 VARCHAR(1000)
	,segment6 VARCHAR(1000)
	,segment7 VARCHAR(1000)
	,segment8 VARCHAR(1000)
	,segment9 VARCHAR(1000)
	,segment10 VARCHAR(1000)
	,segment11 VARCHAR(1000)
	,segment12 VARCHAR(1000)
	,segment13 VARCHAR(1000)
	,segment14 VARCHAR(1000)
	,segment15 VARCHAR(1000)
	,segment16 VARCHAR(1000)
	,segment17 VARCHAR(1000)
	,segment18 VARCHAR(1000)
	,segment19 VARCHAR(1000)
	,segment20 VARCHAR(1000)
	,segment21 VARCHAR(1000)
	,segment22 VARCHAR(1000)
	,segment23 VARCHAR(1000)
	,segment24 VARCHAR(1000)
	,segment25 VARCHAR(1000)
	,segment26 VARCHAR(1000)
	,segment27 VARCHAR(1000)
	,segment28 VARCHAR(1000)
	,segment29 VARCHAR(1000)
	,segment30 VARCHAR(1000)
	,description VARCHAR(1000)
	,template_id BIGINT
	,allocation_create_flag VARCHAR(1000)
	,start_date_active TIMESTAMP
	,end_date_active TIMESTAMP
	,attribute1 VARCHAR(1000)
	,attribute2 VARCHAR(1000)
	,attribute3 VARCHAR(1000)
	,attribute4 VARCHAR(1000)
	,attribute5 VARCHAR(1000)
	,attribute6 VARCHAR(1000)
	,attribute7 VARCHAR(1000)
	,attribute8 VARCHAR(1000)
	,attribute9 VARCHAR(1000)
	,attribute10 VARCHAR(1000)
	,context VARCHAR(1000)
	,segment_attribute1 VARCHAR(1000)
	,segment_attribute2 VARCHAR(1000)
	,segment_attribute3 VARCHAR(1000)
	,segment_attribute4 VARCHAR(1000)
	,segment_attribute5 VARCHAR(1000)
	,segment_attribute6 VARCHAR(1000)
	,segment_attribute7 VARCHAR(1000)
	,segment_attribute8 VARCHAR(1000)
	,segment_attribute9 VARCHAR(1000)
	,segment_attribute10 VARCHAR(1000)
	,segment_attribute11 VARCHAR(1000)
	,segment_attribute12 VARCHAR(1000)
	,segment_attribute13 VARCHAR(1000)
	,segment_attribute14 VARCHAR(1000)
	,segment_attribute15 VARCHAR(1000)
	,segment_attribute16 VARCHAR(1000)
	,segment_attribute17 VARCHAR(1000)
	,segment_attribute18 VARCHAR(1000)
	,segment_attribute19 VARCHAR(1000)
	,segment_attribute20 VARCHAR(1000)
	,segment_attribute21 VARCHAR(1000)
	,segment_attribute22 VARCHAR(1000)
	,segment_attribute23 VARCHAR(1000)
	,segment_attribute24 VARCHAR(1000)
	,segment_attribute25 VARCHAR(1000)
	,segment_attribute26 VARCHAR(1000)
	,segment_attribute27 VARCHAR(1000)
	,segment_attribute28 VARCHAR(1000)
	,segment_attribute29 VARCHAR(1000)
	,segment_attribute30 VARCHAR(1000)
	,segment_attribute31 VARCHAR(1000)
	,segment_attribute32 VARCHAR(1000)
	,segment_attribute33 VARCHAR(1000)
	,segment_attribute34 VARCHAR(1000)
	,segment_attribute35 VARCHAR(1000)
	,segment_attribute36 VARCHAR(1000)
	,segment_attribute37 VARCHAR(1000)
	,segment_attribute38 VARCHAR(1000)
	,segment_attribute39 VARCHAR(1000)
	,segment_attribute40 VARCHAR(1000)
	,segment_attribute41 VARCHAR(1000)
	,segment_attribute42 VARCHAR(1000)
	,reference1 VARCHAR(1000)
	,reference2 VARCHAR(1000)
	,reference3 VARCHAR(1000)
	,reference4 VARCHAR(1000)
	,reference5 VARCHAR(1000)
	,jgzz_recon_flag VARCHAR(1000)
	,jgzz_recon_context VARCHAR(1000)
	,preserve_flag VARCHAR(1000)
	,refresh_flag VARCHAR(1000)
	,igi_balanced_budget_flag VARCHAR(1000)
	,company_cost_center_org_id BIGINT
	,revaluation_id BIGINT
	,ledger_segment VARCHAR(1000)
	,ledger_type_code VARCHAR(1000)
	,alternate_code_combination_id BIGINT    
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   'separatorChar' = '|',
   'quoteChar' = '"',
   'escapeChar' = '\\'
   )
STORED AS textfile
LOCATION 's3://axle-internal-sources/raw/ebsprod/gl_gl_code_combinations/'
TABLE PROPERTIES ('compression_type'='gzip');

-------------------------------------------
-- Create a view in interna schema
-------------------------------------------
DROP VIEW IF EXISTS interna.ebsprod_gl_gl_code_combinations;

CREATE VIEW interna.ebsprod_gl_gl_code_combinations 
AS 
SELECT * 
  FROM spectrumdb.ebsprod_gl_gl_code_combinations
  WITH NO SCHEMA BINDING;
 

-- Verify Counts
SELECT COUNT(*) 
  FROM interna.ebsprod_gl_gl_code_combinations;

SELECT TOP 10 * 
  FROM interna.ebsprod_gl_gl_code_combinations;

DROP TABLE IF EXISTS spectrumdb.ebsprod_inv_mtl_system_items_b;
 
CREATE EXTERNAL TABLE spectrumdb.ebsprod_inv_mtl_system_items_b (
	inventory_item_id INTEGER
	,organization_id INTEGER
	,last_update_date TIMESTAMP
	,last_updated_by INTEGER
	,creation_date VARCHAR(1000)
	,created_by INTEGER
	,last_update_login INTEGER
	,summary_flag VARCHAR(1000)
	,enabled_flag VARCHAR(1000)
	,start_date_active VARCHAR(1000)
	,end_date_active VARCHAR(1000)
	,description VARCHAR(1000)
	,buyer_id INTEGER
	,accounting_rule_id INTEGER
	,invoicing_rule_id INTEGER
	,segment1 VARCHAR(1000)
	,segment2 VARCHAR(1000)
	,segment3 VARCHAR(1000)
	,segment4 VARCHAR(1000)
	,segment5 VARCHAR(1000)
	,segment6 VARCHAR(1000)
	,segment7 VARCHAR(1000)
	,segment8 VARCHAR(1000)
	,segment9 VARCHAR(1000)
	,segment10 VARCHAR(1000)
	,segment11 VARCHAR(1000)
	,segment12 VARCHAR(1000)
	,segment13 VARCHAR(1000)
	,segment14 VARCHAR(1000)
	,segment15 VARCHAR(1000)
	,segment16 VARCHAR(1000)
	,segment17 VARCHAR(1000)
	,segment18 VARCHAR(1000)
	,segment19 VARCHAR(1000)
	,segment20 VARCHAR(1000)
	,attribute_category VARCHAR(1000)
	,attribute1 VARCHAR(1000)
	,attribute2 VARCHAR(1000)
	,attribute3 VARCHAR(1000)
	,attribute4 VARCHAR(1000)
	,attribute5 VARCHAR(1000)
	,attribute6 VARCHAR(1000)
	,attribute7 VARCHAR(1000)
	,attribute8 VARCHAR(1000)
	,attribute9 VARCHAR(1000)
	,attribute10 VARCHAR(1000)
	,attribute11 VARCHAR(1000)
	,attribute12 VARCHAR(1000)
	,attribute13 VARCHAR(1000)
	,attribute14 VARCHAR(1000)
	,attribute15 VARCHAR(1000)
	,purchasing_item_flag VARCHAR(1000)
	,shippable_item_flag VARCHAR(1000)
	,customer_order_flag VARCHAR(1000)
	,internal_order_flag VARCHAR(1000)
	,service_item_flag VARCHAR(1000)
	,inventory_item_flag VARCHAR(1000)
	,eng_item_flag VARCHAR(1000)
	,inventory_asset_flag VARCHAR(1000)
	,purchasing_enabled_flag VARCHAR(1000)
	,customer_order_enabled_flag VARCHAR(1000)
	,internal_order_enabled_flag VARCHAR(1000)
	,so_transactions_flag VARCHAR(1000)
	,mtl_transactions_enabled_flag VARCHAR(1000)
	,stock_enabled_flag VARCHAR(1000)
	,bom_enabled_flag VARCHAR(1000)
	,build_in_wip_flag VARCHAR(1000)
	,revision_qty_control_code INTEGER
	,item_catalog_group_id INTEGER
	,catalog_status_flag VARCHAR(1000)
	,returnable_flag VARCHAR(1000)
	,default_shipping_org INTEGER
	,collateral_flag VARCHAR(1000)
	,taxable_flag VARCHAR(1000)
	,qty_rcv_exception_code VARCHAR(1000)
	,allow_item_desc_update_flag VARCHAR(1000)
	,inspection_required_flag VARCHAR(1000)
	,receipt_required_flag VARCHAR(1000)
	,market_price DOUBLE PRECISION   
	,hazard_class_id INTEGER
	,rfq_required_flag VARCHAR(1000)
	,qty_rcv_tolerance DOUBLE PRECISION   
	,list_price_per_unit DOUBLE PRECISION   
	,un_number_id INTEGER
	,price_tolerance_percent DOUBLE PRECISION   
	,asset_category_id INTEGER
	,rounding_factor INTEGER
	,unit_of_issue VARCHAR(1000)
	,enforce_ship_to_location_code VARCHAR(1000)
	,allow_substitute_receipts_flag VARCHAR(1000)
	,allow_unordered_receipts_flag VARCHAR(1000)
	,allow_express_delivery_flag VARCHAR(1000)
	,days_early_receipt_allowed DOUBLE PRECISION   
	,days_late_receipt_allowed DOUBLE PRECISION   
	,receipt_days_exception_code VARCHAR(1000)
	,receiving_routing_id INTEGER
	,invoice_close_tolerance DOUBLE PRECISION   
	,receive_close_tolerance DOUBLE PRECISION   
	,auto_lot_alpha_prefix VARCHAR(1000)
	,start_auto_lot_number VARCHAR(1000)
	,lot_control_code INTEGER
	,shelf_life_code INTEGER
	,shelf_life_days DOUBLE PRECISION   
	,serial_number_control_code INTEGER
	,start_auto_serial_number VARCHAR(1000)
	,auto_serial_alpha_prefix VARCHAR(1000)
	,source_type INTEGER
	,source_organization_id INTEGER
	,source_subinventory VARCHAR(1000)
	,expense_account INTEGER
	,encumbrance_account INTEGER
	,restrict_subinventories_code INTEGER
	,unit_weight DOUBLE PRECISION   
	,weight_uom_code VARCHAR(1000)
	,volume_uom_code VARCHAR(1000)
	,unit_volume DOUBLE PRECISION   
	,restrict_locators_code INTEGER
	,location_control_code INTEGER
	,shrinkage_rate DOUBLE PRECISION   
	,acceptable_early_days DOUBLE PRECISION   
	,planning_time_fence_code INTEGER
	,demand_time_fence_code INTEGER
	,lead_time_lot_size DOUBLE PRECISION   
	,std_lot_size INTEGER
	,cum_manufacturing_lead_time DOUBLE PRECISION   
	,overrun_percentage DOUBLE PRECISION   
	,mrp_calculate_atp_flag VARCHAR(1000)
	,acceptable_rate_increase DOUBLE PRECISION   
	,acceptable_rate_decrease DOUBLE PRECISION   
	,cumulative_total_lead_time DOUBLE PRECISION   
	,planning_time_fence_days DOUBLE PRECISION   
	,demand_time_fence_days DOUBLE PRECISION   
	,end_assembly_pegging_flag VARCHAR(1000)
	,repetitive_planning_flag VARCHAR(1000)
	,planning_exception_set VARCHAR(1000)
	,bom_item_type INTEGER
	,pick_components_flag VARCHAR(1000)
	,replenish_to_order_flag VARCHAR(1000)
	,base_item_id INTEGER
	,atp_components_flag VARCHAR(1000)
	,atp_flag VARCHAR(1000)
	,fixed_lead_time DOUBLE PRECISION   
	,variable_lead_time DOUBLE PRECISION   
	,wip_supply_locator_id INTEGER
	,wip_supply_type INTEGER
	,wip_supply_subinventory VARCHAR(1000)
	,primary_uom_code VARCHAR(1000)
	,primary_unit_of_measure VARCHAR(1000)
	,allowed_units_lookup_code INTEGER
	,cost_of_sales_account INTEGER
	,sales_account INTEGER
	,default_include_in_rollup_flag VARCHAR(1000)
	,inventory_item_status_code VARCHAR(1000)
	,inventory_planning_code INTEGER
	,planner_code VARCHAR(1000)
	,planning_make_buy_code INTEGER
	,fixed_lot_multiplier DOUBLE PRECISION   
	,rounding_control_type INTEGER
	,carrying_cost DOUBLE PRECISION   
	,postprocessing_lead_time DOUBLE PRECISION   
	,preprocessing_lead_time DOUBLE PRECISION   
	,full_lead_time DOUBLE PRECISION   
	,order_cost DOUBLE PRECISION   
	,mrp_safety_stock_percent DOUBLE PRECISION   
	,mrp_safety_stock_code INTEGER
	,min_minmax_quantity DOUBLE PRECISION   
	,max_minmax_quantity DOUBLE PRECISION   
	,minimum_order_quantity DOUBLE PRECISION   
	,fixed_order_quantity DOUBLE PRECISION   
	,fixed_days_supply DOUBLE PRECISION   
	,maximum_order_quantity DOUBLE PRECISION   
	,atp_rule_id INTEGER
	,picking_rule_id INTEGER
	,reservable_type INTEGER
	,positive_measurement_error DOUBLE PRECISION   
	,negative_measurement_error DOUBLE PRECISION   
	,engineering_ecn_code VARCHAR(1000)
	,engineering_item_id INTEGER
	,engineering_date VARCHAR(1000)
	,service_starting_delay DOUBLE PRECISION   
	,vendor_warranty_flag VARCHAR(1000)
	,serviceable_component_flag VARCHAR(1000)
	,serviceable_product_flag VARCHAR(1000)
	,base_warranty_service_id INTEGER
	,payment_terms_id INTEGER
	,preventive_maintenance_flag VARCHAR(1000)
	,primary_specialist_id INTEGER
	,secondary_specialist_id INTEGER
	,serviceable_item_class_id INTEGER
	,time_billable_flag VARCHAR(1000)
	,material_billable_flag VARCHAR(1000)
	,expense_billable_flag VARCHAR(1000)
	,prorate_service_flag VARCHAR(1000)
	,coverage_schedule_id INTEGER
	,service_duration_period_code VARCHAR(1000)
	,service_duration DOUBLE PRECISION   
	,warranty_vendor_id INTEGER
	,max_warranty_amount DOUBLE PRECISION   
	,response_time_period_code VARCHAR(1000)
	,response_time_value DOUBLE PRECISION   
	,new_revision_code VARCHAR(1000)
	,invoiceable_item_flag VARCHAR(1000)
	,tax_code VARCHAR(1000)
	,invoice_enabled_flag VARCHAR(1000)
	,must_use_approved_vendor_flag VARCHAR(1000)
	,request_id INTEGER
	,program_application_id INTEGER
	,program_id INTEGER
	,program_update_date VARCHAR(1000)
	,outside_operation_flag VARCHAR(1000)
	,outside_operation_uom_type VARCHAR(1000)
	,safety_stock_bucket_days DOUBLE PRECISION   
	,auto_reduce_mps INTEGER
	,costing_enabled_flag VARCHAR(1000)
	,auto_created_config_flag VARCHAR(1000)
	,cycle_count_enabled_flag VARCHAR(1000)
	,item_type VARCHAR(1000)
	,model_config_clause_name VARCHAR(1000)
	,ship_model_complete_flag VARCHAR(1000)
	,mrp_planning_code INTEGER
	,return_inspection_requirement INTEGER
	,ato_forecast_control INTEGER
	,release_time_fence_code INTEGER
	,release_time_fence_days DOUBLE PRECISION   
	,container_item_flag VARCHAR(1000)
	,vehicle_item_flag VARCHAR(1000)
	,maximum_load_weight DOUBLE PRECISION   
	,minimum_fill_percent DOUBLE PRECISION   
	,container_type_code VARCHAR(1000)
	,internal_volume DOUBLE PRECISION   
	,wh_update_date VARCHAR(1000)
	,product_family_item_id INTEGER
	,global_attribute_category VARCHAR(1000)
	,global_attribute1 VARCHAR(1000)
	,global_attribute2 VARCHAR(1000)
	,global_attribute3 VARCHAR(1000)
	,global_attribute4 VARCHAR(1000)
	,global_attribute5 VARCHAR(1000)
	,global_attribute6 VARCHAR(1000)
	,global_attribute7 VARCHAR(1000)
	,global_attribute8 VARCHAR(1000)
	,global_attribute9 VARCHAR(1000)
	,global_attribute10 VARCHAR(1000)
	,purchasing_tax_code VARCHAR(1000)
	,overcompletion_tolerance_type INTEGER
	,overcompletion_tolerance_value DOUBLE PRECISION   
	,effectivity_control INTEGER
	,check_shortages_flag VARCHAR(1000)
	,over_shipment_tolerance DOUBLE PRECISION   
	,under_shipment_tolerance DOUBLE PRECISION   
	,over_return_tolerance DOUBLE PRECISION   
	,under_return_tolerance DOUBLE PRECISION   
	,equipment_type INTEGER
	,recovered_part_disp_code VARCHAR(1000)
	,defect_tracking_on_flag VARCHAR(1000)
	,usage_item_flag VARCHAR(1000)
	,event_flag VARCHAR(1000)
	,electronic_flag VARCHAR(1000)
	,downloadable_flag VARCHAR(1000)
	,vol_discount_exempt_flag VARCHAR(1000)
	,coupon_exempt_flag VARCHAR(1000)
	,comms_nl_trackable_flag VARCHAR(1000)
	,asset_creation_code VARCHAR(1000)
	,comms_activation_reqd_flag VARCHAR(1000)
	,orderable_on_web_flag VARCHAR(1000)
	,back_orderable_flag VARCHAR(1000)
	,web_status VARCHAR(1000)
	,indivisible_flag VARCHAR(1000)
	,dimension_uom_code VARCHAR(1000)
	,unit_length DOUBLE PRECISION   
	,unit_width DOUBLE PRECISION   
	,unit_height DOUBLE PRECISION   
	,bulk_picked_flag VARCHAR(1000)
	,lot_status_enabled VARCHAR(1000)
	,default_lot_status_id INTEGER
	,serial_status_enabled VARCHAR(1000)
	,default_serial_status_id INTEGER
	,lot_split_enabled VARCHAR(1000)
	,lot_merge_enabled VARCHAR(1000)
	,inventory_carry_penalty DOUBLE PRECISION   
	,operation_slack_penalty DOUBLE PRECISION   
	,financing_allowed_flag VARCHAR(1000)
	,eam_item_type INTEGER
	,eam_activity_type_code VARCHAR(1000)
	,eam_activity_cause_code VARCHAR(1000)
	,eam_act_notification_flag VARCHAR(1000)
	,eam_act_shutdown_status VARCHAR(1000)
	,dual_uom_control INTEGER
	,secondary_uom_code VARCHAR(1000)
	,dual_uom_deviation_high DOUBLE PRECISION   
	,dual_uom_deviation_low DOUBLE PRECISION   
	,contract_item_type_code VARCHAR(1000)
	,subscription_depend_flag VARCHAR(1000)
	,serv_req_enabled_code VARCHAR(1000)
	,serv_billing_enabled_flag VARCHAR(1000)
	,serv_importance_level INTEGER
	,planned_inv_point_flag VARCHAR(1000)
	,lot_translate_enabled VARCHAR(1000)
	,default_so_source_type VARCHAR(1000)
	,create_supply_flag VARCHAR(1000)
	,substitution_window_code INTEGER
	,substitution_window_days DOUBLE PRECISION   
	,ib_item_instance_class VARCHAR(1000)
	,config_model_type VARCHAR(1000)
	,lot_substitution_enabled VARCHAR(1000)
	,minimum_license_quantity DOUBLE PRECISION   
	,eam_activity_source_code VARCHAR(1000)
	,lifecycle_id INTEGER
	,current_phase_id INTEGER
	,object_version_number INTEGER
	,tracking_quantity_ind VARCHAR(1000)
	,ont_pricing_qty_source VARCHAR(1000)
	,secondary_default_ind VARCHAR(1000)
	,option_specific_sourced INTEGER
	,approval_status VARCHAR(1000)
	,vmi_minimum_units DOUBLE PRECISION   
	,vmi_minimum_days DOUBLE PRECISION   
	,vmi_maximum_units DOUBLE PRECISION   
	,vmi_maximum_days DOUBLE PRECISION   
	,vmi_fixed_order_quantity DOUBLE PRECISION   
	,so_authorization_flag INTEGER
	,consigned_flag INTEGER
	,asn_autoexpire_flag INTEGER
	,vmi_forecast_type INTEGER
	,forecast_horizon INTEGER
	,exclude_from_budget_flag INTEGER
	,days_tgt_inv_supply DOUBLE PRECISION   
	,days_tgt_inv_window DOUBLE PRECISION   
	,days_max_inv_supply DOUBLE PRECISION   
	,days_max_inv_window DOUBLE PRECISION   
	,drp_planned_flag INTEGER
	,critical_component_flag INTEGER
	,continous_transfer INTEGER
	,convergence DOUBLE PRECISION   
	,divergence DOUBLE PRECISION   
	,config_orgs VARCHAR(1000)
	,config_match VARCHAR(1000)
	,global_attribute11 VARCHAR(1000)
	,global_attribute12 VARCHAR(1000)
	,global_attribute13 VARCHAR(1000)
	,global_attribute14 VARCHAR(1000)
	,global_attribute15 VARCHAR(1000)
	,global_attribute16 VARCHAR(1000)
	,global_attribute17 VARCHAR(1000)
	,global_attribute18 VARCHAR(1000)
	,global_attribute19 VARCHAR(1000)
	,global_attribute20 VARCHAR(1000)
	,attribute16 VARCHAR(1000)
	,attribute17 VARCHAR(1000)
	,attribute18 VARCHAR(1000)
	,attribute19 VARCHAR(1000)
	,attribute20 VARCHAR(1000)
	,attribute21 VARCHAR(1000)
	,attribute22 VARCHAR(1000)
	,attribute23 VARCHAR(1000)
	,attribute24 VARCHAR(1000)
	,attribute25 VARCHAR(1000)
	,attribute26 VARCHAR(1000)
	,attribute27 VARCHAR(1000)
	,attribute28 VARCHAR(1000)
	,attribute29 VARCHAR(1000)
	,attribute30 VARCHAR(1000)
	,cas_number VARCHAR(1000)
	,child_lot_flag VARCHAR(1000)
	,child_lot_prefix VARCHAR(1000)
	,child_lot_starting_number INTEGER
	,child_lot_validation_flag VARCHAR(1000)
	,copy_lot_attribute_flag VARCHAR(1000)
	,default_grade VARCHAR(1000)
	,expiration_action_code VARCHAR(1000)
	,expiration_action_interval INTEGER
	,grade_control_flag VARCHAR(1000)
	,hazardous_material_flag VARCHAR(1000)
	,hold_days DOUBLE PRECISION   
	,lot_divisible_flag VARCHAR(1000)
	,maturity_days DOUBLE PRECISION   
	,parent_child_generation_flag VARCHAR(1000)
	,process_costing_enabled_flag VARCHAR(1000)
	,process_execution_enabled_flag VARCHAR(1000)
	,process_quality_enabled_flag VARCHAR(1000)
	,process_supply_locator_id INTEGER
	,process_supply_subinventory VARCHAR(1000)
	,process_yield_locator_id INTEGER
	,process_yield_subinventory VARCHAR(1000)
	,recipe_enabled_flag VARCHAR(1000)
	,retest_interval DOUBLE PRECISION   
	,charge_periodicity_code VARCHAR(1000)
	,repair_leadtime DOUBLE PRECISION   
	,repair_yield INTEGER
	,preposition_point VARCHAR(1000)
	,repair_program INTEGER
	,subcontracting_component INTEGER
	,outsourced_assembly INTEGER
	,ego_master_items_dff_ctx VARCHAR(1000)
	,gdsn_outbound_enabled_flag VARCHAR(1000)
	,trade_item_descriptor VARCHAR(1000)
	,style_item_id INTEGER
	,style_item_flag VARCHAR(1000)
	,last_submitted_nir_id INTEGER
	,default_material_status_id INTEGER
	,serial_tagging_flag VARCHAR(1000)   
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   'separatorChar' = '|',
   'quoteChar' = '"',
   'escapeChar' = '\\'
   )
STORED AS textfile
LOCATION 's3://axle-internal-sources/raw/ebsprod/inv_mtl_system_items_b/'
TABLE PROPERTIES ('compression_type'='gzip');

CREATE VIEW interna.ebsprod_inv_mtl_system_items_b 
AS 
SELECT * 
  FROM spectrumdb.ebsprod_inv_mtl_system_items_b
  WITH NO SCHEMA BINDING;
  
-- Verify Counts
SELECT COUNT(*) 
  FROM interna.ebsprod_inv_mtl_system_items_b;

SELECT TOP 10 * 
  FROM interna.ebsprod_inv_mtl_system_items_b;

DROP TABLE IF EXISTS spectrumdb.ebsprod_ar_ra_cust_trx_line_gl_dist_all;
 
CREATE EXTERNAL TABLE spectrumdb.ebsprod_ar_ra_cust_trx_line_gl_dist_all (
	cust_trx_line_gl_dist_id BIGINT
	,customer_trx_line_id BIGINT
	,code_combination_id BIGINT
	,set_of_books_id INTEGER
	,last_update_date TIMESTAMP
	,last_updated_by INTEGER
	,creation_date TIMESTAMP
	,created_by INTEGER
	,last_update_login INTEGER
	,"percent" DOUBLE PRECISION   
	,amount DOUBLE PRECISION   
	,gl_date TIMESTAMP
	,gl_posted_date TIMESTAMP
	,cust_trx_line_salesrep_id BIGINT
	,comments VARCHAR(1000)
	,attribute_category VARCHAR(1000)
	,attribute1 VARCHAR(1000)
	,attribute2 VARCHAR(1000)
	,attribute3 VARCHAR(1000)
	,attribute4 VARCHAR(1000)
	,attribute5 VARCHAR(1000)
	,attribute6 VARCHAR(1000)
	,attribute7 VARCHAR(1000)
	,attribute8 VARCHAR(1000)
	,attribute9 VARCHAR(1000)
	,attribute10 VARCHAR(1000)
	,request_id BIGINT
	,program_application_id INTEGER
	,program_id INTEGER
	,program_update_date TIMESTAMP
	,concatenated_segments VARCHAR(1000)
	,original_gl_date TIMESTAMP
	,post_request_id INTEGER
	,posting_control_id INTEGER
	,account_class VARCHAR(1000)
	,ra_post_loop_number INTEGER
	,customer_trx_id BIGINT
	,account_set_flag VARCHAR(1000)
	,acctd_amount DOUBLE PRECISION   
	,ussgl_transaction_code VARCHAR(1000)
	,ussgl_transaction_code_context VARCHAR(1000)
	,attribute11 VARCHAR(1000)
	,attribute12 VARCHAR(1000)
	,attribute13 VARCHAR(1000)
	,attribute14 VARCHAR(1000)
	,attribute15 VARCHAR(1000)
	,latest_rec_flag VARCHAR(1000)
	,org_id INTEGER
	,mrc_gl_posted_date VARCHAR(1000)
	,mrc_posting_control_id VARCHAR(1000)
	,mrc_acctd_amount VARCHAR(1000)
	,mrc_account_class VARCHAR(1000)
	,mrc_customer_trx_id VARCHAR(1000)
	,mrc_amount VARCHAR(1000)
	,collected_tax_ccid INTEGER
	,collected_tax_concat_seg VARCHAR(1000)
	,revenue_adjustment_id INTEGER
	,rev_adj_class_temp VARCHAR(1000)
	,rec_offset_flag VARCHAR(1000)
	,event_id BIGINT
	,user_generated_flag VARCHAR(1000)
	,rounding_correction_flag VARCHAR(1000)
	,cogs_request_id BIGINT
	,ccid_change_flag VARCHAR(1000)
	,global_attribute1 VARCHAR(1000)
	,global_attribute2 VARCHAR(1000)
	,global_attribute3 VARCHAR(1000)
	,global_attribute4 VARCHAR(1000)
	,global_attribute5 VARCHAR(1000)
	,global_attribute6 VARCHAR(1000)
	,global_attribute7 VARCHAR(1000)
	,global_attribute8 VARCHAR(1000)
	,global_attribute9 VARCHAR(1000)
	,global_attribute10 VARCHAR(1000)
	,global_attribute11 VARCHAR(1000)
	,global_attribute12 VARCHAR(1000)
	,global_attribute13 VARCHAR(1000)
	,global_attribute14 VARCHAR(1000)
	,global_attribute15 VARCHAR(1000)
	,global_attribute16 VARCHAR(1000)
	,global_attribute17 VARCHAR(1000)
	,global_attribute18 VARCHAR(1000)
	,global_attribute19 VARCHAR(1000)
	,global_attribute20 VARCHAR(1000)
	,global_attribute21 VARCHAR(1000)
	,global_attribute22 VARCHAR(1000)
	,global_attribute23 VARCHAR(1000)
	,global_attribute24 VARCHAR(1000)
	,global_attribute25 VARCHAR(1000)
	,global_attribute26 VARCHAR(1000)
	,global_attribute27 VARCHAR(1000)
	,global_attribute28 VARCHAR(1000)
	,global_attribute29 VARCHAR(1000)
	,global_attribute30 VARCHAR(1000)
	,global_attribute_category VARCHAR(1000)    
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   'separatorChar' = '|',
   'quoteChar' = '"',
   'escapeChar' = '\\'
   )
STORED AS textfile
LOCATION 's3://axle-internal-sources/raw/ebsprod/ar_ra_cust_trx_line_gl_dist_all/'
TABLE PROPERTIES ('compression_type'='gzip');    

-------------------------------------------
-- Create a view in interna schema
-------------------------------------------
DROP VIEW IF EXISTS interna.ebsprod_ar_ra_cust_trx_line_gl_dist_all;

CREATE VIEW interna.ebsprod_ar_ra_cust_trx_line_gl_dist_all 
AS 
SELECT * 
  FROM spectrumdb.ebsprod_ar_ra_cust_trx_line_gl_dist_all
  WITH NO SCHEMA BINDING;
 

-- Verify Counts
SELECT COUNT(*) 
  FROM interna.ebsprod_ar_ra_cust_trx_line_gl_dist_all;

SELECT TOP 10 * 
  FROM interna.ebsprod_ar_ra_cust_trx_line_gl_dist_all;

DROP TABLE IF EXISTS spectrumdb.ebsprod_ar_ra_cust_trx_line_salesreps_all;
 
CREATE EXTERNAL TABLE spectrumdb.ebsprod_ar_ra_cust_trx_line_salesreps_all (
	cust_trx_line_salesrep_id BIGINT
	,last_update_date TIMESTAMP
	,last_updated_by INTEGER
	,creation_date TIMESTAMP
	,created_by INTEGER
	,last_update_login INTEGER
	,customer_trx_id BIGINT
	,salesrep_id BIGINT
	,customer_trx_line_id BIGINT
	,revenue_amount_split DOUBLE PRECISION   
	,non_revenue_amount_split DOUBLE PRECISION   
	,attribute_category VARCHAR(1000)
	,attribute1 VARCHAR(1000)
	,attribute2 VARCHAR(1000)
	,attribute3 VARCHAR(1000)
	,attribute4 VARCHAR(1000)
	,attribute5 VARCHAR(1000)
	,attribute6 VARCHAR(1000)
	,attribute7 VARCHAR(1000)
	,attribute8 VARCHAR(1000)
	,attribute9 VARCHAR(1000)
	,attribute10 VARCHAR(1000)
	,request_id BIGINT
	,program_application_id INTEGER
	,program_id INTEGER
	,program_update_date VARCHAR(1000)
	,non_revenue_percent_split DOUBLE PRECISION   
	,revenue_percent_split DOUBLE PRECISION   
	,original_line_salesrep_id BIGINT
	,prev_cust_trx_line_salesrep_id BIGINT
	,attribute11 VARCHAR(1000)
	,attribute12 VARCHAR(1000)
	,attribute13 VARCHAR(1000)
	,attribute14 VARCHAR(1000)
	,attribute15 VARCHAR(1000)
	,org_id INTEGER
	,wh_update_date TIMESTAMP
	,revenue_adjustment_id INTEGER
	,revenue_salesgroup_id INTEGER
	,non_revenue_salesgroup_id INTEGER    
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   'separatorChar' = '|',
   'quoteChar' = '"',
   'escapeChar' = '\\'
   )
STORED AS textfile
LOCATION 's3://axle-internal-sources/raw/ebsprod/ar_ra_cust_trx_line_salesreps_all/'
TABLE PROPERTIES ('compression_type'='gzip');    

-------------------------------------------
-- Create a view in interna schema
-------------------------------------------
DROP VIEW IF EXISTS interna.ebsprod_ar_ra_cust_trx_line_salesreps_all;

CREATE VIEW interna.ebsprod_ar_ra_cust_trx_line_salesreps_all 
AS 
SELECT * 
  FROM spectrumdb.ebsprod_ar_ra_cust_trx_line_salesreps_all
  WITH NO SCHEMA BINDING;
 

-- Verify Counts
SELECT COUNT(*) 
  FROM interna.ebsprod_ar_ra_cust_trx_line_salesreps_all;

SELECT TOP 10 * 
  FROM interna.ebsprod_ar_ra_cust_trx_line_salesreps_all;

DROP TABLE IF EXISTS spectrumdb.ebsprod_ar_ra_cust_trx_types_all;
 
CREATE EXTERNAL TABLE spectrumdb.ebsprod_ar_ra_cust_trx_types_all (
	cust_trx_type_id INTEGER
	,last_update_date TIMESTAMP
	,last_updated_by VARCHAR(1000)
	,creation_date TIMESTAMP
	,created_by VARCHAR(1000)
	,last_update_login VARCHAR(1000)
	,post_to_gl VARCHAR(1000)
	,accounting_affect_flag VARCHAR(1000)
	,credit_memo_type_id VARCHAR(1000)
	,status VARCHAR(1000)
	,name VARCHAR(1000)
	,description VARCHAR(1000)
	,"type" VARCHAR(1000)
	,default_term VARCHAR(1000)
	,default_printing_option VARCHAR(1000)
	,default_status VARCHAR(1000)
	,gl_id_rev VARCHAR(1000)
	,gl_id_freight VARCHAR(1000)
	,gl_id_rec VARCHAR(1000)
	,subsequent_trx_type_id VARCHAR(1000)
	,set_of_books_id VARCHAR(1000)
	,attribute_category VARCHAR(1000)
	,attribute1 VARCHAR(1000)
	,attribute2 VARCHAR(1000)
	,attribute3 VARCHAR(1000)
	,attribute4 VARCHAR(1000)
	,attribute5 VARCHAR(1000)
	,attribute6 VARCHAR(1000)
	,attribute7 VARCHAR(1000)
	,attribute8 VARCHAR(1000)
	,attribute9 VARCHAR(1000)
	,attribute10 VARCHAR(1000)
	,allow_freight_flag VARCHAR(1000)
	,allow_overapplication_flag VARCHAR(1000)
	,creation_sign VARCHAR(1000)
	,end_date TIMESTAMP
	,gl_id_clearing VARCHAR(1000)
	,gl_id_tax VARCHAR(1000)
	,gl_id_unbilled VARCHAR(1000)
	,gl_id_unearned VARCHAR(1000)
	,start_date TIMESTAMP
	,tax_calculation_flag VARCHAR(1000)
	,attribute11 VARCHAR(1000)
	,attribute12 VARCHAR(1000)
	,attribute13 VARCHAR(1000)
	,attribute14 VARCHAR(1000)
	,attribute15 VARCHAR(1000)
	,natural_application_only_flag VARCHAR(1000)
	,org_id VARCHAR(1000)
	,global_attribute1 VARCHAR(1000)
	,global_attribute2 VARCHAR(1000)
	,global_attribute3 VARCHAR(1000)
	,global_attribute4 VARCHAR(1000)
	,global_attribute5 VARCHAR(1000)
	,global_attribute6 VARCHAR(1000)
	,global_attribute7 VARCHAR(1000)
	,global_attribute8 VARCHAR(1000)
	,global_attribute9 VARCHAR(1000)
	,global_attribute10 VARCHAR(1000)
	,global_attribute11 VARCHAR(1000)
	,global_attribute12 VARCHAR(1000)
	,global_attribute13 VARCHAR(1000)
	,global_attribute14 VARCHAR(1000)
	,global_attribute15 VARCHAR(1000)
	,global_attribute16 VARCHAR(1000)
	,global_attribute17 VARCHAR(1000)
	,global_attribute18 VARCHAR(1000)
	,global_attribute19 VARCHAR(1000)
	,global_attribute20 VARCHAR(1000)
	,global_attribute_category VARCHAR(1000)
	,rule_set_id VARCHAR(1000)
	,signed_flag VARCHAR(1000)
	,drawee_issued_flag VARCHAR(1000)
	,magnetic_format_code VARCHAR(1000)
	,format_program_id VARCHAR(1000)
	,gl_id_unpaid_rec VARCHAR(1000)
	,gl_id_remittance VARCHAR(1000)
	,gl_id_factor VARCHAR(1000)
	,allocate_tax_freight VARCHAR(1000)
	,legal_entity_id VARCHAR(1000)
	,exclude_from_late_charges VARCHAR(1000)
	,adj_post_to_gl VARCHAR(1000)
	,zd_edition_name VARCHAR(1000)
	,zd_sync VARCHAR(1000)
	,warehouse_query_date TIMESTAMP    
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   'separatorChar' = '|',
   'quoteChar' = '"',
   'escapeChar' = '\\'
   )
STORED AS textfile
LOCATION 's3://axle-internal-sources/raw/ebsprod/ar_ra_cust_trx_types_all/'
TABLE PROPERTIES ('compression_type'='gzip');    

-------------------------------------------
-- Create a view in interna schema
-------------------------------------------
DROP VIEW IF EXISTS interna.ebsprod_ar_ra_cust_trx_types_all;

CREATE VIEW interna.ebsprod_ar_ra_cust_trx_types_all 
AS 
SELECT * 
  FROM spectrumdb.ebsprod_ar_ra_cust_trx_types_all
  WITH NO SCHEMA BINDING;
 

-- Verify Counts
SELECT COUNT(*) 
  FROM interna.ebsprod_ar_ra_cust_trx_types_all;

SELECT TOP 10 * 
  FROM interna.ebsprod_ar_ra_cust_trx_types_all;

DROP TABLE IF EXISTS spectrumdb.ebsprod_ar_ra_customer_trx_all;
 
CREATE EXTERNAL TABLE spectrumdb.ebsprod_ar_ra_customer_trx_all (
	customer_trx_id BIGINT
	,last_update_date TIMESTAMP
	,last_updated_by INTEGER
	,creation_date TIMESTAMP
	,created_by INTEGER
	,last_update_login INTEGER
	,trx_number VARCHAR(1000)
	,cust_trx_type_id INTEGER
	,trx_date TIMESTAMP
	,set_of_books_id INTEGER
	,bill_to_contact_id BIGINT
	,batch_id BIGINT
	,batch_source_id INTEGER
	,reason_code VARCHAR(1000)
	,sold_to_customer_id BIGINT
	,sold_to_contact_id BIGINT
	,sold_to_site_use_id BIGINT
	,bill_to_customer_id BIGINT
	,bill_to_site_use_id BIGINT
	,ship_to_customer_id BIGINT
	,ship_to_contact_id BIGINT
	,ship_to_site_use_id BIGINT
	,shipment_id INTEGER
	,remit_to_address_id INTEGER
	,term_id INTEGER
	,term_due_date TIMESTAMP
	,previous_customer_trx_id BIGINT
	,primary_salesrep_id BIGINT
	,printing_original_date TIMESTAMP
	,printing_last_printed TIMESTAMP
	,printing_option VARCHAR(1000)
	,printing_count BIGINT
	,printing_pending VARCHAR(1000)
	,purchase_order VARCHAR(1000)
	,purchase_order_revision VARCHAR(1000)
	,purchase_order_date TIMESTAMP
	,customer_reference VARCHAR(1000)
	,customer_reference_date TIMESTAMP
	,comments VARCHAR(1000)
	,internal_notes VARCHAR(1000)
	,exchange_rate_type VARCHAR(1000)
	,exchange_date TIMESTAMP
	,exchange_rate DOUBLE PRECISION   
	,territory_id INTEGER
	,invoice_currency_code VARCHAR(1000)
	,initial_customer_trx_id INTEGER
	,agreement_id INTEGER
	,end_date_commitment TIMESTAMP
	,start_date_commitment TIMESTAMP
	,last_printed_sequence_num INTEGER
	,attribute_category VARCHAR(1000)
	,attribute1 VARCHAR(1000)
	,attribute2 VARCHAR(1000)
	,attribute3 VARCHAR(1000)
	,attribute4 VARCHAR(1000)
	,attribute5 VARCHAR(1000)
	,attribute6 VARCHAR(1000)
	,attribute7 VARCHAR(1000)
	,attribute8 VARCHAR(1000)
	,attribute9 VARCHAR(1000)
	,attribute10 VARCHAR(1000)
	,orig_system_batch_name VARCHAR(1000)
	,post_request_id INTEGER
	,request_id BIGINT
	,program_application_id BIGINT
	,program_id INTEGER
	,program_update_date TIMESTAMP
	,finance_charges VARCHAR(1000)
	,complete_flag VARCHAR(1000)
	,posting_control_id INTEGER
	,bill_to_address_id BIGINT
	,ra_post_loop_number INTEGER
	,ship_to_address_id BIGINT
	,credit_method_for_rules VARCHAR(1000)
	,credit_method_for_installments VARCHAR(1000)
	,receipt_method_id INTEGER
	,attribute11 VARCHAR(1000)
	,attribute12 VARCHAR(1000)
	,attribute13 VARCHAR(1000)
	,attribute14 VARCHAR(1000)
	,attribute15 VARCHAR(1000)
	,related_customer_trx_id BIGINT
	,invoicing_rule_id INTEGER
	,ship_via VARCHAR(1000)
	,ship_date_actual TIMESTAMP
	,waybill_number VARCHAR(1000)
	,fob_point VARCHAR(1000)
	,customer_bank_account_id INTEGER
	,interface_header_attribute1 VARCHAR(1000)
	,interface_header_attribute2 VARCHAR(1000)
	,interface_header_attribute3 VARCHAR(1000)
	,interface_header_attribute4 VARCHAR(1000)
	,interface_header_attribute5 VARCHAR(1000)
	,interface_header_attribute6 VARCHAR(1000)
	,interface_header_attribute7 VARCHAR(1000)
	,interface_header_attribute8 VARCHAR(1000)
	,interface_header_context VARCHAR(1000)
	,default_ussgl_trx_code_context VARCHAR(1000)
	,interface_header_attribute10 VARCHAR(1000)
	,interface_header_attribute11 VARCHAR(1000)
	,interface_header_attribute12 VARCHAR(1000)
	,interface_header_attribute13 VARCHAR(1000)
	,interface_header_attribute14 VARCHAR(1000)
	,interface_header_attribute15 VARCHAR(1000)
	,interface_header_attribute9 VARCHAR(1000)
	,default_ussgl_transaction_code VARCHAR(1000)
	,recurred_from_trx_number VARCHAR(1000)
	,status_trx VARCHAR(1000)
	,doc_sequence_id INTEGER
	,doc_sequence_value INTEGER
	,paying_customer_id BIGINT
	,paying_site_use_id BIGINT
	,related_batch_source_id INTEGER
	,default_tax_exempt_flag VARCHAR(1000)
	,created_from VARCHAR(1000)
	,org_id INTEGER
	,wh_update_date TIMESTAMP
	,global_attribute1 VARCHAR(1000)
	,global_attribute2 VARCHAR(1000)
	,global_attribute3 VARCHAR(1000)
	,global_attribute4 VARCHAR(1000)
	,global_attribute5 VARCHAR(1000)
	,global_attribute6 VARCHAR(1000)
	,global_attribute7 VARCHAR(1000)
	,global_attribute8 VARCHAR(1000)
	,global_attribute9 VARCHAR(1000)
	,global_attribute10 VARCHAR(1000)
	,global_attribute11 VARCHAR(1000)
	,global_attribute12 VARCHAR(1000)
	,global_attribute13 VARCHAR(1000)
	,global_attribute14 VARCHAR(1000)
	,global_attribute15 VARCHAR(1000)
	,global_attribute16 VARCHAR(1000)
	,global_attribute17 VARCHAR(1000)
	,global_attribute18 VARCHAR(1000)
	,global_attribute19 VARCHAR(1000)
	,global_attribute20 VARCHAR(1000)
	,global_attribute_category VARCHAR(1000)
	,edi_processed_flag VARCHAR(1000)
	,edi_processed_status VARCHAR(1000)
	,global_attribute21 VARCHAR(1000)
	,global_attribute22 VARCHAR(1000)
	,global_attribute23 VARCHAR(1000)
	,global_attribute24 VARCHAR(1000)
	,global_attribute25 VARCHAR(1000)
	,global_attribute26 VARCHAR(1000)
	,global_attribute27 VARCHAR(1000)
	,global_attribute28 VARCHAR(1000)
	,global_attribute29 VARCHAR(1000)
	,global_attribute30 VARCHAR(1000)
	,mrc_exchange_rate_type VARCHAR(1000)
	,mrc_exchange_date VARCHAR(1000)
	,mrc_exchange_rate VARCHAR(1000)
	,payment_server_order_num VARCHAR(1000)
	,approval_code VARCHAR(1000)
	,address_verification_code VARCHAR(1000)
	,old_trx_number VARCHAR(1000)
	,br_amount DOUBLE PRECISION   
	,br_unpaid_flag VARCHAR(1000)
	,br_on_hold_flag VARCHAR(1000)
	,drawee_id BIGINT
	,drawee_contact_id BIGINT
	,drawee_site_use_id BIGINT
	,remittance_bank_account_id BIGINT
	,override_remit_account_flag VARCHAR(1000)
	,drawee_bank_account_id BIGINT
	,special_instructions VARCHAR(1000)
	,remittance_batch_id BIGINT
	,prepayment_flag VARCHAR(1000)
	,ct_reference VARCHAR(1000)
	,contract_id BIGINT
	,bill_template_id BIGINT
	,reversed_cash_receipt_id BIGINT
	,cc_error_code VARCHAR(1000)
	,cc_error_text VARCHAR(1000)
	,cc_error_flag VARCHAR(1000)
	,upgrade_method VARCHAR(1000)
	,legal_entity_id INTEGER
	,remit_bank_acct_use_id INTEGER
	,payment_trxn_extension_id INTEGER
	,ax_accounted_flag VARCHAR(1000)
	,application_id INTEGER
	,payment_attributes VARCHAR(1000)
	,billing_date TIMESTAMP
	,interest_header_id INTEGER
	,late_charges_assessed VARCHAR(1000)    
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   'separatorChar' = '|',
   'quoteChar' = '"',
   'escapeChar' = '\\'
   )
STORED AS textfile
LOCATION 's3://axle-internal-sources/raw/ebsprod/ar_ra_customer_trx_all/'
TABLE PROPERTIES ('compression_type'='gzip');    

-------------------------------------------
-- Create a view in interna schema
-------------------------------------------
DROP VIEW IF EXISTS interna.ebsprod_ar_ra_customer_trx_all;

CREATE VIEW interna.ebsprod_ar_ra_customer_trx_all 
AS 
SELECT * 
  FROM spectrumdb.ebsprod_ar_ra_customer_trx_all
  WITH NO SCHEMA BINDING;
 

-- Verify Counts
SELECT COUNT(*) 
  FROM interna.ebsprod_ar_ra_customer_trx_all;

SELECT TOP 10 * 
  FROM interna.ebsprod_ar_ra_customer_trx_all;

DROP TABLE IF EXISTS spectrumdb.ebsprod_ar_ra_customer_trx_lines_all;
 
CREATE EXTERNAL TABLE spectrumdb.ebsprod_ar_ra_customer_trx_lines_all (
	customer_trx_line_id BIGINT
	,last_update_date TIMESTAMP
	,last_updated_by INTEGER
	,creation_date TIMESTAMP
	,created_by INTEGER
	,last_update_login INTEGER
	,customer_trx_id BIGINT
	,line_number INTEGER
	,set_of_books_id INTEGER
	,reason_code VARCHAR(1000)
	,inventory_item_id INTEGER
	,description VARCHAR(1000)
	,previous_customer_trx_id INTEGER
	,previous_customer_trx_line_id INTEGER
	,quantity_ordered DOUBLE PRECISION   
	,quantity_credited DOUBLE PRECISION   
	,quantity_invoiced DOUBLE PRECISION   
	,unit_standard_price DOUBLE PRECISION   
	,unit_selling_price DOUBLE PRECISION   
	,sales_order VARCHAR(1000)
	,sales_order_revision INTEGER
	,sales_order_line VARCHAR(1000)
	,sales_order_date TIMESTAMP
	,accounting_rule_id INTEGER
	,accounting_rule_duration INTEGER
	,line_type VARCHAR(1000)
	,attribute_category VARCHAR(1000)
	,attribute1 VARCHAR(1000)
	,attribute2 VARCHAR(1000)
	,attribute3 VARCHAR(1000)
	,attribute4 VARCHAR(1000)
	,attribute5 VARCHAR(1000)
	,attribute6 VARCHAR(1000)
	,attribute7 VARCHAR(1000)
	,attribute8 VARCHAR(1000)
	,attribute9 VARCHAR(1000)
	,attribute10 VARCHAR(1000)
	,request_id BIGINT
	,program_application_id INTEGER
	,program_id INTEGER
	,program_update_date TIMESTAMP
	,rule_start_date TIMESTAMP
	,initial_customer_trx_line_id BIGINT
	,interface_line_context VARCHAR(1000)
	,interface_line_attribute1 VARCHAR(1000)
	,interface_line_attribute2 VARCHAR(1000)
	,interface_line_attribute3 VARCHAR(1000)
	,interface_line_attribute4 VARCHAR(1000)
	,interface_line_attribute5 VARCHAR(1000)
	,interface_line_attribute6 VARCHAR(1000)
	,interface_line_attribute7 VARCHAR(1000)
	,interface_line_attribute8 VARCHAR(1000)
	,sales_order_source VARCHAR(1000)
	,taxable_flag VARCHAR(1000)
	,extended_amount DOUBLE PRECISION   
	,revenue_amount DOUBLE PRECISION   
	,autorule_complete_flag VARCHAR(1000)
	,link_to_cust_trx_line_id BIGINT
	,attribute11 VARCHAR(1000)
	,attribute12 VARCHAR(1000)
	,attribute13 VARCHAR(1000)
	,attribute14 VARCHAR(1000)
	,attribute15 VARCHAR(1000)
	,tax_precedence INTEGER
	,tax_rate DOUBLE PRECISION   
	,item_exception_rate_id INTEGER
	,tax_exemption_id INTEGER
	,memo_line_id INTEGER
	,autorule_duration_processed INTEGER
	,uom_code VARCHAR(1000)
	,default_ussgl_transaction_code VARCHAR(1000)
	,default_ussgl_trx_code_context VARCHAR(1000)
	,interface_line_attribute10 VARCHAR(1000)
	,interface_line_attribute11 VARCHAR(1000)
	,interface_line_attribute12 VARCHAR(1000)
	,interface_line_attribute13 VARCHAR(1000)
	,interface_line_attribute14 VARCHAR(1000)
	,interface_line_attribute15 VARCHAR(1000)
	,interface_line_attribute9 VARCHAR(1000)
	,vat_tax_id INTEGER
	,autotax VARCHAR(1000)
	,last_period_to_credit INTEGER
	,item_context VARCHAR(1000)
	,tax_exempt_flag VARCHAR(1000)
	,tax_exempt_number VARCHAR(1000)
	,tax_exempt_reason_code VARCHAR(1000)
	,tax_vendor_return_code VARCHAR(1000)
	,sales_tax_id INTEGER
	,location_segment_id INTEGER
	,movement_id INTEGER
	,org_id INTEGER
	,wh_update_date TIMESTAMP
	,global_attribute1 VARCHAR(1000)
	,global_attribute2 VARCHAR(1000)
	,global_attribute3 VARCHAR(1000)
	,global_attribute4 VARCHAR(1000)
	,global_attribute5 VARCHAR(1000)
	,global_attribute6 VARCHAR(1000)
	,global_attribute7 VARCHAR(1000)
	,global_attribute8 VARCHAR(1000)
	,global_attribute9 VARCHAR(1000)
	,global_attribute10 VARCHAR(1000)
	,global_attribute11 VARCHAR(1000)
	,global_attribute12 VARCHAR(1000)
	,global_attribute13 VARCHAR(1000)
	,global_attribute14 VARCHAR(1000)
	,global_attribute15 VARCHAR(1000)
	,global_attribute16 VARCHAR(1000)
	,global_attribute17 VARCHAR(1000)
	,global_attribute18 VARCHAR(1000)
	,global_attribute19 VARCHAR(1000)
	,global_attribute20 VARCHAR(1000)
	,global_attribute_category VARCHAR(1000)
	,gross_unit_selling_price DOUBLE PRECISION   
	,gross_extended_amount DOUBLE PRECISION   
	,amount_includes_tax_flag VARCHAR(1000)
	,taxable_amount DOUBLE PRECISION   
	,warehouse_id INTEGER
	,translated_description VARCHAR(1000)
	,extended_acctd_amount DOUBLE PRECISION   
	,br_ref_customer_trx_id INTEGER
	,br_ref_payment_schedule_id INTEGER
	,br_adjustment_id INTEGER
	,mrc_extended_acctd_amount DOUBLE PRECISION   
	,payment_set_id INTEGER
	,contract_line_id INTEGER
	,source_data_key1 VARCHAR(1000)
	,source_data_key2 VARCHAR(1000)
	,source_data_key3 VARCHAR(1000)
	,source_data_key4 VARCHAR(1000)
	,source_data_key5 VARCHAR(1000)
	,invoiced_line_acctg_level VARCHAR(1000)
	,override_auto_accounting_flag VARCHAR(1000)
	,rule_end_date TIMESTAMP
	,ship_to_customer_id BIGINT
	,ship_to_address_id BIGINT
	,ship_to_site_use_id BIGINT
	,ship_to_contact_id BIGINT
	,historical_flag VARCHAR(1000)
	,tax_line_id BIGINT
	,line_recoverable DOUBLE PRECISION   
	,tax_recoverable DOUBLE PRECISION   
	,tax_classification_code VARCHAR(1000)
	,amount_due_remaining DOUBLE PRECISION   
	,acctd_amount_due_remaining DOUBLE PRECISION   
	,amount_due_original DOUBLE PRECISION   
	,acctd_amount_due_original DOUBLE PRECISION   
	,chrg_amount_remaining DOUBLE PRECISION   
	,chrg_acctd_amount_remaining DOUBLE PRECISION   
	,frt_adj_remaining DOUBLE PRECISION   
	,frt_adj_acctd_remaining DOUBLE PRECISION   
	,frt_ed_amount DOUBLE PRECISION   
	,frt_ed_acctd_amount DOUBLE PRECISION   
	,frt_uned_amount DOUBLE PRECISION   
	,frt_uned_acctd_amount DOUBLE PRECISION   
	,deferral_exclusion_flag VARCHAR(1000)
	,payment_trxn_extension_id INTEGER
	,interest_line_id INTEGER    
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   'separatorChar' = '|',
   'quoteChar' = '"',
   'escapeChar' = '\\'
   )
STORED AS textfile
LOCATION 's3://axle-internal-sources/raw/ebsprod/ar_ra_customer_trx_lines_all/'
TABLE PROPERTIES ('compression_type'='gzip');    

-------------------------------------------
-- Create a view in interna schema
-------------------------------------------
DROP VIEW IF EXISTS interna.ebsprod_ar_ra_customer_trx_lines_all;

CREATE VIEW interna.ebsprod_ar_ra_customer_trx_lines_all 
AS 
SELECT * 
  FROM spectrumdb.ebsprod_ar_ra_customer_trx_lines_all
  WITH NO SCHEMA BINDING;
 

-- Verify Counts
SELECT COUNT(*) 
  FROM interna.ebsprod_ar_ra_customer_trx_lines_all;

SELECT TOP 10 * 
  FROM interna.ebsprod_ar_ra_customer_trx_lines_all;

DROP TABLE IF EXISTS spectrumdb.ebsprod_apps_ra_customers;
 
CREATE EXTERNAL TABLE spectrumdb.ebsprod_apps_ra_customers (
	row_id VARCHAR(1000)
	,customer_id BIGINT
	,party_id INTEGER
	,party_number VARCHAR(1000)
	,party_type VARCHAR(1000)
	,last_update_date TIMESTAMP
	,last_updated_by INTEGER
	,creation_date TIMESTAMP
	,created_by INTEGER
	,customer_name VARCHAR(1000)
	,customer_number VARCHAR(1000)
	,orig_system_reference VARCHAR(1000)
	,status VARCHAR(1000)
	,last_update_login INTEGER
	,customer_type VARCHAR(1000)
	,customer_prospect_code VARCHAR(1000)
	,customer_class_code VARCHAR(1000)
	,primary_salesrep_id BIGINT
	,sales_channel_code VARCHAR(1000)
	,sic_code VARCHAR(1000)
	,order_type_id INTEGER
	,price_list_id INTEGER
	,attribute_category VARCHAR(1000)
	,attribute1 VARCHAR(1000)
	,attribute2 VARCHAR(1000)
	,attribute3 VARCHAR(1000)
	,attribute4 VARCHAR(1000)
	,attribute5 VARCHAR(1000)
	,attribute6 VARCHAR(1000)
	,attribute7 VARCHAR(1000)
	,attribute8 VARCHAR(1000)
	,attribute9 VARCHAR(1000)
	,attribute10 VARCHAR(1000)
	,request_id BIGINT
	,program_application_id INTEGER
	,program_id INTEGER
	,program_update_date TIMESTAMP
	,analysis_fy VARCHAR(1000)
	,customer_category_code VARCHAR(1000)
	,customer_group_code VARCHAR(1000)
	,customer_key VARCHAR(1000)
	,customer_subgroup_code VARCHAR(1000)
	,fiscal_yearend_month VARCHAR(1000)
	,net_worth DOUBLE PRECISION   
	,num_of_employees INTEGER
	,potential_revenue_curr_fy DOUBLE PRECISION   
	,potential_revenue_next_fy DOUBLE PRECISION   
	,rank VARCHAR(1000)
	,reference_use_flag VARCHAR(1000)
	,tax_code VARCHAR(1000)
	,tax_reference VARCHAR(1000)
	,attribute11 VARCHAR(1000)
	,attribute12 VARCHAR(1000)
	,attribute13 VARCHAR(1000)
	,attribute14 VARCHAR(1000)
	,attribute15 VARCHAR(1000)
	,third_party_flag VARCHAR(1000)
	,access_template_entity_code VARCHAR(1000)
	,primary_specialist_id INTEGER
	,secondary_specialist_id INTEGER
	,competitor_flag VARCHAR(1000)
	,dunning_site_use_id INTEGER
	,statement_site_use_id INTEGER
	,orig_system VARCHAR(1000)
	,year_established INTEGER
	,coterminate_day_month VARCHAR(1000)
	,fob_point VARCHAR(1000)
	,freight_term VARCHAR(1000)
	,gsa_indicator VARCHAR(1000)
	,ship_partial VARCHAR(1000)
	,ship_via VARCHAR(1000)
	,warehouse_id INTEGER
	,payment_term_id INTEGER
	,tax_exempt VARCHAR(1000)
	,tax_exempt_num VARCHAR(1000)
	,tax_exempt_reason_code VARCHAR(1000)
	,jgzz_fiscal_code VARCHAR(1000)
	,do_not_mail_flag VARCHAR(1000)
	,mission_statement VARCHAR(1000)
	,customer_name_phonetic VARCHAR(1000)
	,tax_header_level_flag VARCHAR(1000)
	,tax_rounding_rule VARCHAR(1000)
	,wh_update_date VARCHAR(1000)
	,global_attribute1 VARCHAR(1000)
	,global_attribute2 VARCHAR(1000)
	,global_attribute3 VARCHAR(1000)
	,global_attribute4 VARCHAR(1000)
	,global_attribute5 VARCHAR(1000)
	,global_attribute6 VARCHAR(1000)
	,global_attribute7 VARCHAR(1000)
	,global_attribute8 VARCHAR(1000)
	,global_attribute9 VARCHAR(1000)
	,global_attribute10 VARCHAR(1000)
	,global_attribute11 VARCHAR(1000)
	,global_attribute12 VARCHAR(1000)
	,global_attribute13 VARCHAR(1000)
	,global_attribute14 VARCHAR(1000)
	,global_attribute15 VARCHAR(1000)
	,global_attribute16 VARCHAR(1000)
	,global_attribute17 VARCHAR(1000)
	,global_attribute18 VARCHAR(1000)
	,global_attribute19 VARCHAR(1000)
	,global_attribute20 VARCHAR(1000)
	,global_attribute_category VARCHAR(1000)
	,url VARCHAR(1000)
	,"language" VARCHAR(1000)
	,translated_customer_name VARCHAR(1000)
	,person_pre_name_adjunct VARCHAR(1000)
	,person_first_name VARCHAR(1000)
	,person_middle_name VARCHAR(1000)
	,person_last_name VARCHAR(1000)
	,person_name_suffix VARCHAR(1000)
	,person_first_name_phonetic VARCHAR(1000)
	,person_last_name_phonetic VARCHAR(1000)
	,ship_sets_include_lines_flag VARCHAR(1000)
	,arrivalsets_include_lines_flag VARCHAR(1000)
	,sched_date_push_flag VARCHAR(1000)
	,over_shipment_tolerance DOUBLE PRECISION   
	,under_shipment_tolerance DOUBLE PRECISION   
	,over_return_tolerance DOUBLE PRECISION   
	,under_return_tolerance DOUBLE PRECISION   
	,item_cross_ref_pref VARCHAR(1000)
	,date_type_preference VARCHAR(1000)
	,dates_negative_tolerance DOUBLE PRECISION   
	,dates_positive_tolerance DOUBLE PRECISION   
	,invoice_quantity_rule VARCHAR(1000)
	,attribute16 VARCHAR(1000)
	,attribute17 VARCHAR(1000)
	,attribute18 VARCHAR(1000)
	,attribute19 VARCHAR(1000)
	,attribute20 VARCHAR(1000)
	,duns_number INTEGER
	,duns_number_c VARCHAR(1000)
	,party_last_update_date TIMESTAMP
	,sic_code_type VARCHAR(1000)    
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   'separatorChar' = '|',
   'quoteChar' = '"',
   'escapeChar' = '\\'
   )
STORED AS textfile
LOCATION 's3://axle-internal-sources/raw/ebsprod/apps_ra_customers/'
TABLE PROPERTIES ('compression_type'='gzip');    

-------------------------------------------
-- Create a view in interna schema
-------------------------------------------
DROP VIEW IF EXISTS interna.ebsprod_apps_ra_customers;

CREATE VIEW interna.ebsprod_apps_ra_customers 
AS 
SELECT * 
  FROM spectrumdb.ebsprod_apps_ra_customers
  WITH NO SCHEMA BINDING;
 

-- Verify Counts
SELECT COUNT(*) 
  FROM interna.ebsprod_apps_ra_customers;

SELECT TOP 10 * 
  FROM interna.ebsprod_apps_ra_customers;

DROP TABLE IF EXISTS spectrumdb.ebsprod_apps_ra_salesreps_all;
 
CREATE EXTERNAL TABLE spectrumdb.ebsprod_apps_ra_salesreps_all (
	salesrep_id INTEGER
	,resource_id INTEGER
	,last_update_date TIMESTAMP
	,last_updated_by VARCHAR(1000)
	,creation_date TIMESTAMP
	,created_by INTEGER
	,last_update_login INTEGER
	,sales_credit_type_id INTEGER
	,name VARCHAR(1000)
	,status VARCHAR(1000)
	,start_date_active TIMESTAMP
	,end_date_active TIMESTAMP
	,gl_id_rev INTEGER
	,gl_id_freight INTEGER
	,gl_id_rec INTEGER
	,set_of_books_id INTEGER
	,salesrep_number VARCHAR(1000)
	,org_id INTEGER
	,email_address VARCHAR(1000)
	,wh_update_date TIMESTAMP
	,person_id INTEGER
	,sales_tax_geocode VARCHAR(1000)
	,sales_tax_inside_city_limits VARCHAR(1000)
	,object_version_number INTEGER
	,attribute_category VARCHAR(1000)
	,attribute1 VARCHAR(1000)
	,attribute2 VARCHAR(1000)
	,attribute3 VARCHAR(1000)
	,attribute4 VARCHAR(1000)
	,attribute5 VARCHAR(1000)
	,attribute6 VARCHAR(1000)
	,attribute7 VARCHAR(1000)
	,attribute8 VARCHAR(1000)
	,attribute9 VARCHAR(1000)
	,attribute10 VARCHAR(1000)
	,attribute11 VARCHAR(1000)
	,attribute12 VARCHAR(1000)
	,attribute13 VARCHAR(1000)
	,attribute14 VARCHAR(1000)
	,attribute15 VARCHAR(1000)
	,security_group_id INTEGER    
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   'separatorChar' = '|',
   'quoteChar' = '"',
   'escapeChar' = '\\'
   )
STORED AS textfile
LOCATION 's3://axle-internal-sources/raw/ebsprod/apps_ra_salesreps_all/'
TABLE PROPERTIES ('compression_type'='gzip');    

-------------------------------------------
-- Create a view in interna schema
-------------------------------------------
DROP VIEW IF EXISTS interna.ebsprod_apps_ra_salesreps_all;

CREATE VIEW interna.ebsprod_apps_ra_salesreps_all 
AS 
SELECT * 
  FROM spectrumdb.ebsprod_apps_ra_salesreps_all
  WITH NO SCHEMA BINDING;
 

-- Verify Counts
SELECT COUNT(*) 
  FROM interna.ebsprod_apps_ra_salesreps_all;

SELECT TOP 10 * 
  FROM interna.ebsprod_apps_ra_salesreps_all;

DROP TABLE IF EXISTS spectrumdb.ebsprod_ra_addresses_all;

CREATE EXTERNAL TABLE spectrumdb.ebsprod_apps_ra_addresses_all (PARTY_SITE_ID INTEGER,
PARTY_ID INTEGER,
PARTY_LOCATION_ID INTEGER,
KEY_ACCOUNT_FLAG VARCHAR(4),
TERRITORY_ID INTEGER,
ADDRESS_KEY VARCHAR(2000),
BILL_TO_FLAG VARCHAR(4),
MARKET_FLAG VARCHAR(4),
SHIP_TO_FLAG VARCHAR(4),
LOCATION_ID INTEGER,
SERVICE_TERRITORY_ID INTEGER,
ADDRESS_ID INTEGER,
CUSTOMER_ID INTEGER,
STATUS VARCHAR(4),
ORIG_SYSTEM_REFERENCE VARCHAR(960),
ORG_ID INTEGER,
COUNTRY VARCHAR(240),
ADDRESS1 VARCHAR(960),
ADDRESS2 VARCHAR(960),
ADDRESS3 VARCHAR(960),
ADDRESS4 VARCHAR(960),
CITY VARCHAR(240),
POSTAL_CODE VARCHAR(240),
STATE VARCHAR(240),
PROVINCE VARCHAR(240),
COUNTY VARCHAR(240),
ATTRIBUTE_CATEGORY VARCHAR(120),
ATTRIBUTE1 VARCHAR(600),
ATTRIBUTE2 VARCHAR(600),
ATTRIBUTE3 VARCHAR(600),
ATTRIBUTE4 VARCHAR(600),
ATTRIBUTE5 VARCHAR(600),
ATTRIBUTE6 VARCHAR(600),
ATTRIBUTE7 VARCHAR(600),
ATTRIBUTE8 VARCHAR(600),
ATTRIBUTE9 VARCHAR(600),
ATTRIBUTE10 VARCHAR(600),
ATTRIBUTE11 VARCHAR(600),
ATTRIBUTE12 VARCHAR(600),
ATTRIBUTE13 VARCHAR(600),
ATTRIBUTE14 VARCHAR(600),
ATTRIBUTE15 VARCHAR(600),
ATTRIBUTE16 VARCHAR(600),
ATTRIBUTE17 VARCHAR(600),
ATTRIBUTE18 VARCHAR(600),
ATTRIBUTE19 VARCHAR(600),
ATTRIBUTE20 VARCHAR(600),
GLOBAL_ATTRIBUTE1 VARCHAR(600),
GLOBAL_ATTRIBUTE2 VARCHAR(600),
GLOBAL_ATTRIBUTE3 VARCHAR(600),
GLOBAL_ATTRIBUTE4 VARCHAR(600),
GLOBAL_ATTRIBUTE5 VARCHAR(600),
GLOBAL_ATTRIBUTE6 VARCHAR(600),
GLOBAL_ATTRIBUTE7 VARCHAR(600),
GLOBAL_ATTRIBUTE8 VARCHAR(600),
GLOBAL_ATTRIBUTE9 VARCHAR(600),
GLOBAL_ATTRIBUTE10 VARCHAR(600),
GLOBAL_ATTRIBUTE11 VARCHAR(600),
GLOBAL_ATTRIBUTE12 VARCHAR(600),
GLOBAL_ATTRIBUTE13 VARCHAR(600),
GLOBAL_ATTRIBUTE14 VARCHAR(600),
GLOBAL_ATTRIBUTE15 VARCHAR(600),
GLOBAL_ATTRIBUTE16 VARCHAR(600),
GLOBAL_ATTRIBUTE17 VARCHAR(600),
GLOBAL_ATTRIBUTE18 VARCHAR(600),
GLOBAL_ATTRIBUTE19 VARCHAR(600),
GLOBAL_ATTRIBUTE20 VARCHAR(600),
IDENTIFYING_ADDRESS_FLAG VARCHAR(4),
CREATION_DATE DATE,
CREATED_BY INTEGER,
LAST_UPDATE_DATE DATE,
LAST_UPDATED_BY INTEGER) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES (
'separatorChar' = '|',
'quoteChar' = '"',
'escapeChar' = '\\'
) 
STORED AS textfile 
LOCATION 's3://axle-internal-sources/raw/ebsprod/apps_ra_addresses_all/' 
TABLE PROPERTIES ('compression_type'='gzip','numRows'='3433975');

-------------------------------------------
-- Create a view in interna schema
-------------------------------------------

DROP VIEW IF EXISTS interna.ebsprod_apps_ra_addresses_all;

CREATE VIEW interna.ebsprod_apps_ra_addresses_all
AS
SELECT *
  FROM spectrumdb.ebsprod_apps_ra_addresses_all
  WITH NO SCHEMA BINDING;

-- Verify Counts
SELECT COUNT(*)
  FROM interna.ebsprod_apps_ra_addresses_all;

SELECT TOP 10 *
  FROM interna.ebsprod_apps_ra_addresses_all;


DROP TABLE IF EXISTS spectrumdb.ebsprod_apps_ra_site_uses_all

CREATE EXTERNAL TABLE spectrumdb.ebsprod_apps_ra_site_uses_all (SITE_USE_ID INTEGER,
LAST_UPDATE_DATE DATE,
LAST_UPDATED_BY INTEGER,
CREATION_DATE DATE,
CREATED_BY INTEGER,
SITE_USE_CODE VARCHAR(120),
ADDRESS_ID INTEGER,
PRIMARY_FLAG VARCHAR(4),
STATUS VARCHAR(4),
LOCATION VARCHAR(160),
LAST_UPDATE_LOGIN INTEGER,
CONTACT_ID INTEGER,
BILL_TO_SITE_USE_ID INTEGER,
ORIG_SYSTEM_REFERENCE VARCHAR(960),
SIC_CODE VARCHAR(120),
PAYMENT_TERM_ID INTEGER,
GSA_INDICATOR VARCHAR(4),
SHIP_PARTIAL VARCHAR(4),
SHIP_VIA VARCHAR(120),
FOB_POINT VARCHAR(120),
ORDER_TYPE_ID INTEGER,
PRICE_LIST_ID INTEGER,
FREIGHT_TERM VARCHAR(120),
WAREHOUSE_ID INTEGER,
TERRITORY_ID INTEGER,
ATTRIBUTE_CATEGORY VARCHAR(120),
ATTRIBUTE1 VARCHAR(600),
ATTRIBUTE2 VARCHAR(600),
ATTRIBUTE3 VARCHAR(600),
ATTRIBUTE4 VARCHAR(600),
ATTRIBUTE5 VARCHAR(600),
ATTRIBUTE6 VARCHAR(600),
ATTRIBUTE7 VARCHAR(600),
ATTRIBUTE8 VARCHAR(600),
ATTRIBUTE9 VARCHAR(600),
ATTRIBUTE10 VARCHAR(600),
REQUEST_ID INTEGER,
PROGRAM_APPLICATION_ID INTEGER,
PROGRAM_ID INTEGER,
PROGRAM_UPDATE_DATE DATE,
TAX_REFERENCE VARCHAR(200),
SORT_PRIORITY INTEGER,
TAX_CODE VARCHAR(200),
ATTRIBUTE11 VARCHAR(600),
ATTRIBUTE12 VARCHAR(600),
ATTRIBUTE13 VARCHAR(600),
ATTRIBUTE14 VARCHAR(600),
ATTRIBUTE15 VARCHAR(600),
ATTRIBUTE16 VARCHAR(600),
ATTRIBUTE17 VARCHAR(600),
ATTRIBUTE18 VARCHAR(600),
ATTRIBUTE19 VARCHAR(600),
ATTRIBUTE20 VARCHAR(600),
ATTRIBUTE21 VARCHAR(600),
ATTRIBUTE22 VARCHAR(600),
ATTRIBUTE23 VARCHAR(600),
ATTRIBUTE24 VARCHAR(600),
ATTRIBUTE25 VARCHAR(600),
LAST_ACCRUE_CHARGE_DATE DATE,
SECOND_LAST_ACCRUE_CHARGE_DATE DATE,
LAST_UNACCRUE_CHARGE_DATE DATE,
SECOND_LAST_UNACCRUE_CHRG_DATE DATE,
DEMAND_CLASS_CODE VARCHAR(120),
TAX_EXEMPT VARCHAR(4),
TAX_EXEMPT_NUM VARCHAR(4),
TAX_EXEMPT_REASON_CODE VARCHAR(4),
ORG_ID INTEGER,
TAX_CLASSIFICATION VARCHAR(120),
TAX_HEADER_LEVEL_FLAG VARCHAR(4),
TAX_ROUNDING_RULE VARCHAR(120),
WH_UPDATE_DATE DATE,
GLOBAL_ATTRIBUTE1 VARCHAR(600),
GLOBAL_ATTRIBUTE2 VARCHAR(600),
GLOBAL_ATTRIBUTE3 VARCHAR(600),
GLOBAL_ATTRIBUTE4 VARCHAR(600),
GLOBAL_ATTRIBUTE5 VARCHAR(600),
GLOBAL_ATTRIBUTE6 VARCHAR(600),
GLOBAL_ATTRIBUTE7 VARCHAR(600),
GLOBAL_ATTRIBUTE8 VARCHAR(600),
GLOBAL_ATTRIBUTE9 VARCHAR(600),
GLOBAL_ATTRIBUTE10 VARCHAR(600),
GLOBAL_ATTRIBUTE11 VARCHAR(600),
GLOBAL_ATTRIBUTE12 VARCHAR(600),
GLOBAL_ATTRIBUTE13 VARCHAR(600),
GLOBAL_ATTRIBUTE14 VARCHAR(600),
GLOBAL_ATTRIBUTE15 VARCHAR(600),
GLOBAL_ATTRIBUTE16 VARCHAR(600),
GLOBAL_ATTRIBUTE17 VARCHAR(600),
GLOBAL_ATTRIBUTE18 VARCHAR(600),
GLOBAL_ATTRIBUTE19 VARCHAR(600),
GLOBAL_ATTRIBUTE20 VARCHAR(600),
GLOBAL_ATTRIBUTE_CATEGORY VARCHAR(120),
PRIMARY_SALESREP_ID INTEGER,
FINCHRG_RECEIVABLES_TRX_ID INTEGER,
GL_ID_REC INTEGER,
GL_ID_REV INTEGER,
GL_ID_TAX INTEGER,
GL_ID_FREIGHT INTEGER,
GL_ID_CLEARING INTEGER,
GL_ID_UNBILLED INTEGER,
GL_ID_UNEARNED INTEGER,
GL_ID_UNPAID_REC INTEGER,
GL_ID_REMITTANCE INTEGER,
GL_ID_FACTOR INTEGER,
DATES_NEGATIVE_TOLERANCE INTEGER,
DATES_POSITIVE_TOLERANCE INTEGER,
DATE_TYPE_PREFERENCE VARCHAR(80),
OVER_SHIPMENT_TOLERANCE INTEGER,
UNDER_SHIPMENT_TOLERANCE INTEGER,
ITEM_CROSS_REF_PREF VARCHAR(120),
OVER_RETURN_TOLERANCE INTEGER,
UNDER_RETURN_TOLERANCE INTEGER,
SHIP_SETS_INCLUDE_LINES_FLAG VARCHAR(4),
ARRIVALSETS_INCLUDE_LINES_FLAG VARCHAR(4),
SCHED_DATE_PUSH_FLAG VARCHAR(4),
INVOICE_QUANTITY_RULE VARCHAR(120),
PRICING_EVENT VARCHAR(120)) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES (
'separatorChar' = '|',
'quoteChar' = '"',
'escapeChar' = '\\'
)
STORED AS textfile
LOCATION 's3://axle-internal-sources/raw/ebsprod/apps_ra_site_uses_all/'
TABLE PROPERTIES ('compression_type'='gzip','numRows'='6556667');
-------------------------------------------
-- Create a view in interna schema
-------------------------------------------

DROP VIEW IF EXISTS interna.ebsprod_apps_ra_site_uses_all;

CREATE VIEW interna.ebsprod_apps_ra_site_uses_all;
AS
SELECT *
  FROM spectrumdb.ebsprod_apps_ra_site_uses_all
  WITH NO SCHEMA BINDING;

-- Verify Counts
SELECT COUNT(*)
  FROM interna.ebsprod_apps_ra_site_uses_all;

SELECT TOP 10 *
  FROM interna.ebsprod_apps_ra_site_uses_all;

