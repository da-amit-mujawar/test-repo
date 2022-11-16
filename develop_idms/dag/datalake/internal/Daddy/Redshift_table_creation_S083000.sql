
--DROP TABLE IF EXISTS spectrumdb.S083000_t001_client;
--
--CREATE EXTERNAL TABLE spectrumdb.S083000_t001_client(
--	client_id smallint ,
--	client_name nvarchar(50) ,
--	administration_server_id smallint ,
--	client_administration_database_name varchar(50) ,
--	autosense_html_url varchar(1000) ,
--	global_administration_server_id smallint ,
--	open_rate_tracking_url varchar(500) ,
--	click_through_tracking_url varchar(500) ,
--	max_record_size int ,
--	deployment_set_batch_size int ,
--	return_path varchar(255) ,
--	soft_bounce_threshold smallint ,
--	hard_bounce_threshold smallint ,
--	reset_bounce_after_days smallint ,
--	reset_preference_after_days smallint ,
--	report_period int ,
--	deployment_enabled char(10) ,
--	application_user_name varchar(50) ,
--	application_password varchar(50) ,
--	triggered_list_id int ,
--	adapte_account_id int ,
--	max_rcd_num_t050 int ,
--	pre_arch_period_t050 int ,
--	max_dpl_set_rcd_num int ,
--	adapte_enabled SMALLINT ,
--	adv_suppression_list_server_id smallint ,
--	adv_suppression_list_db_name varchar(50) ,
--	adv_suppression_list_intake_server_id smallint ,
--	adv_suppression_list_intake_db_name varchar(50) ,
--	subscriber_list_snapshot_db varchar(50) ,
--	alert_email_address varchar(1000) ,
--	nanny_enabled nchar(10) ,
--	min_throughput int ,
--	max_throughput int ,
--	deliverability_seed_list_id int ,
--	num_resends_allowed int ,
--	report_server_id smallint ,
--	report_database_name varchar(50) ,
--	frequency_monitoring_control smallint ,
--	bounce_counter_updated_by_harvester SMALLINT ,
--	invalidation_type smallint ,
--	kill_type smallint ,
--	undelivered_threshold int)
--ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--WITH SERDEPROPERTIES (
--   'separatorChar' = '|',
--   'quoteChar' = '"',
--   'escapeChar' = '\\'
--   )
--STORED AS textfile
--LOCATION 's3://axle-internal-sources/raw/daddy/S083000_t001_client/';
------TABLE PROPERTIES ('skip.header.line.count'='1');
----TABLE PROPERTIES ('compression_type'='gzip');
-- 
---------------------------------------------
---- Create a view in interna schema
---------------------------------------------
--DROP VIEW IF EXISTS interna.S083000_t001_client;
--
--CREATE VIEW interna.S083000_t001_client AS 
--SELECT * 
--  FROM spectrumdb.S083000_t001_client
--  WITH NO SCHEMA BINDING;
-- 
--
---- Verify Counts
--SELECT COUNT(*) 
--  FROM interna.S083000_t001_client;
--
--SELECT TOP 100 * 
--  FROM interna.S083000_t001_client;
--
--
---------
--
--DROP TABLE IF EXISTS spectrumdb.S083000_t002_list;
--
--CREATE EXTERNAL TABLE spectrumdb.S083000_t002_list(
--	list_id int ,
--	list_update_frequency_in_hours smallint ,
--	list_name nvarchar(50) ,
--	list_short_name nvarchar(50) ,
--	description nvarchar(255) ,
--	subscriber_list_server_id smallint ,
--	subscriber_list_database_name varchar(50) ,
--	subscriber_list_table_name varchar(50) ,
--	subscriber_list_update_table_name varchar(50) ,
--	frequency_monitoring_type_cde smallint ,
--	list_status_cde smallint ,
--	createdby_user_id smallint ,
--	createdby_datetime timestamp ,
--	updatedby_user_id smallint ,
--	updatedby_datetime timestamp ,
--	records int ,
--	records_valid int ,
--	records_invalid int ,
--	records_valid_optin_in int ,
--	records_valid_optin_3rd int ,
--	records_valid_optin_re int ,
--	records_valid_optin_ne int ,
--	records_optin_in int ,
--	records_optin_3rd int ,
--	records_optin_re int ,
--	records_optin_ne int ,
--	records_optout_in int ,
--	records_optout_3rd int ,
--	records_optout_re int ,
--	records_optout_ne int ,
--	records_number_update_date timestamp ,
--	records_valid_mfp_text int ,
--	records_valid_mfp_text_optin_in int ,
--	records_valid_mfp_text_optin_3rd int ,
--	records_valid_mfp_text_optin_re int ,
--	records_valid_mfp_text_optin_ne int ,
--	records_valid_mfp_html int ,
--	records_valid_mfp_html_optin_in int ,
--	records_valid_mfp_html_optin_3rd int ,
--	records_valid_mfp_html_optin_re int ,
--	records_valid_mfp_html_optin_ne int ,
--	records_valid_mfp_aol int ,
--	records_valid_mfp_aol_optin_in int ,
--	records_valid_mfp_aol_optin_3rd int ,
--	records_valid_mfp_aol_optin_re int ,
--	records_valid_mfp_aol_optin_ne int ,
--	records_valid_mfp_u int ,
--	records_valid_mfp_u_optin_in int ,
--	records_valid_mfp_u_optin_3rd int ,
--	records_valid_mfp_u_optin_re int ,
--	records_valid_mfp_u_optin_ne int ,
--	records_valid_hc int ,
--	records_valid_hc_optin_in int ,
--	records_valid_hc_optin_3rd int ,
--	records_valid_hc_optin_re int ,
--	records_valid_hc_optin_ne int ,
--	from_list_name nvarchar(50) ,
--	subscriber_list_table_name_snapshot varchar(50) ,
--	group_member SMALLINT ,
--	records_optin_po int ,
--	records_optin_tm int ,
--	records_optin_fax int ,
--	records_valid_optin_po int ,
--	records_valid_optin_tm int ,
--	records_valid_optin_fax int ,
--	division_id smallint ,
--	public_ind char(1) ,
--	usage_type_cde SMALLINT ,
--	omnidex_enabled char(1) ,
--	cost_per_thousand numeric(5, 2) ,
--	suppression_list_id int ,
--	associated_client_id smallint ,
--	associated_list_id int)
--ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--WITH SERDEPROPERTIES (
--   'separatorChar' = '|',
--   'quoteChar' = '"',
--   'escapeChar' = '\\'
--   )
--STORED AS textfile
--LOCATION 's3://axle-internal-sources/raw/daddy/S083000_t002_list/';
------TABLE PROPERTIES ('skip.header.line.count'='1');
----TABLE PROPERTIES ('compression_type'='gzip');
-- 
---------------------------------------------
---- Create a view in interna schema
---------------------------------------------
--DROP VIEW IF EXISTS interna.S083000_t002_list;
--
--CREATE VIEW interna.S083000_t002_list AS 
--SELECT * 
--  FROM spectrumdb.S083000_t002_list
--  WITH NO SCHEMA BINDING;
-- 
--
---- Verify Counts
--SELECT COUNT(*) 
--  FROM interna.S083000_t002_list;
--
--SELECT TOP 100 * 
--  FROM interna.S083000_t002_list;
--
--
---------
--
DROP TABLE IF EXISTS spectrumdb.S083000_t006_message;

CREATE EXTERNAL TABLE spectrumdb.S083000_t006_message(
	message_id int ,
	message_name nvarchar(100) ,
	description nvarchar(255) ,
	message_status_cde numeric(5, 2) ,
	limit_number_of_records int ,
	mail_datetime timestamp ,
	message_test_round_number smallint ,
	message_approval_round_number SMALLINT ,
	createdby_user_id smallint ,
	createdby_datetime datetime ,
	updatedby_user_id smallint ,
	updatedby_datetime datetime ,
	message_type_cde SMALLINT ,
	autosense_html_ind char(1) ,
	html_openrate_ind char(1) ,
	html_open_triggered_message_id int ,
	use_header_footer_ind char(1) ,
	bounce_reset_done_ind SMALLINT ,
	marketer_report_alert_ind SMALLINT ,
	preference_reset_done_ind SMALLINT ,
	html_to_aol_ind SMALLINT ,
	export_ctr int ,
	submitted_for_deployment_date timestamp ,
	init_segment_prj int ,
	sup_segment_prj int ,
	sup_fm_segment_prj int ,
	sup_fm_dedup_segment_prj int ,
	prj_date timestamp ,
	num_valid_ind SMALLINT ,
	marked_for_prj_ind SMALLINT ,
	num_invalidate_date timestamp ,
	t050_pre_arch_ind SMALLINT ,
	t050_pre_arch_date timestamp ,
	active_ind SMALLINT ,
	book_dpl_optin_drop int ,
	dpl_priority int ,
	booked_date timestamp ,
	advertiser_id int ,
	sup_adv_segment_prj int ,
	aux_seed_group int ,
	deliverability_report_ind int ,
	email_message_id int ,
	division_id smallint ,
	public_ind char(1) ,
	clickseal_username nvarchar(50) ,
	clickseal_password nvarchar(50) ,
	clickseal_suppression_list_ids char(50) ,
	unsubcentral_url varchar(200) ,
	unsubcentral_file_type varchar(20) ,
	delivery_tracking_ind char(1) ,
	notification_launches char(1) ,
	notification_deployed char(1) ,
	notification_projection_done char(1) ,
	notification_before_launched char(1) ,
	hours_before_launched numeric(8, 1) ,
	notification_error_config char(1) ,
	notification_triggered_expiration char(1) ,
	notification_fm_exemption char(1) ,
	throttling_rate int ,
	schedule_id int ,
	parent_message_id int ,
	delivery_type_cde SMALLINT ,
	marked_for_export_ind SMALLINT ,
	export_date timestamp ,
	follow_up_ct_message_ind SMALLINT ,
	use_pre_compiled_audience_ind char(1) ,
	encoding_type_id smallint ,
	time_zone_id int ,
	spam_check_score numeric(5, 1) ,
	email_count int ,
	header_content_id int ,
	footer_content_id int ,
	use_inline_editor char(1) ,
	admin_email_alert_sent SMALLINT ,
	frequency_monitoring_inuse char(1) ,
	complaint_alert_mail_datetime datetime ,
	approval_alert_mail_sent_datetime datetime ,
	approval_alert_mail_send_datetime_to_user datetime ,
	override_domain_suppression SMALLINT ,
	audience_migration_complete SMALLINT ,
	ftaf_enabled_ind SMALLINT ,
	throttle_rate int ,
	throttle_interval smallint ,
	primary_category_id smallint ,
	secondary_category_id smallint ,
	rate float ,
	rate_type_id SMALLINT ,
	purchase_order nvarchar(100) ,
	business_unit_id SMALLINT ,
	suspended_message_id int ,
	suspended_datetime datetime ,
	tapclicks_userid nvarchar(100))
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   'separatorChar' = '|',
   'quoteChar' = '"',
   'escapeChar' = '\\'
   )
STORED AS textfile
LOCATION 's3://axle-internal-sources/raw/daddy/S083000_t006_message/';
----TABLE PROPERTIES ('skip.header.line.count'='1');
--TABLE PROPERTIES ('compression_type'='gzip');
 
-------------------------------------------
-- Create a view in interna schema
-------------------------------------------
DROP VIEW IF EXISTS interna.S083000_t006_message;

CREATE VIEW interna.S083000_t006_message AS 
SELECT * 
  FROM spectrumdb.S083000_t006_message
  WITH NO SCHEMA BINDING;
 

-- Verify Counts
SELECT COUNT(*) 
  FROM interna.S083000_t006_message;

SELECT TOP 100 * 
  FROM interna.S083000_t006_message;
--
-------------
--
--DROP TABLE IF EXISTS spectrumdb.S083000_t015_segment;
--
--CREATE EXTERNAL TABLE spectrumdb.S083000_t015_segment(
--	segment_id int ,
--	segment_name nvarchar(100) ,
--	description varchar(255) ,
--	segment_status_cde smallint ,
--	segment_value_no_cap int ,
--	segment_value_distinct int ,
--	last_time_segment_execution_result int ,
--	last_sql_query_text_to_execute_the_segment varchar(6700) ,
--	good_for_message_type_cde smallint ,
--	createdby_user_id smallint ,
--	createdby_datetime timestamp ,
--	updatedby_user_id smallint ,
--	updatedby_datetime timestamp ,
--	last_refresh_date timestamp ,
--	parent_segment_id int ,
--	copy_times int ,
--	split_times int ,
--	start_boundary int ,
--	end_boundary int ,
--	segment_cap int ,
--	system_segment_ind smallint ,
--	message_type_id smallint ,
--	segment_type_id smallint ,
--	dedup_ind smallint ,
--	split_set_id int ,
--	marked_for_refresh_ind smallint ,
--	marked_for_refresh_date timestamp ,
--	start_refresh_date timestamp ,
--	split_filter varchar(500) ,
--	split_filter_update_date timestamp ,
--	num_valid_ind smallint ,
--	num_invalidate_date timestamp ,
--	default_segment smallint ,
--	marked_for_prj_ind smallint ,
--	marked_for_prj_date timestamp ,
--	start_prj_date timestamp ,
--	finish_prj_date timestamp ,
--	marked_for_cache_ind smallint ,
--	marked_for_cache_date timestamp ,
--	start_cache_date timestamp ,
--	finish_cache_date timestamp ,
--	last_cache_date timestamp ,
--	init_segment_prj int ,
--	fm_segment_prj int ,
--	fm_dedup_segment_prj int ,
--	prj_num_valid_ind smallint ,
--	division_id smallint ,
--	public_ind char(1) ,
--	fact_id numeric(5, 0))
--ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--WITH SERDEPROPERTIES (
--   'separatorChar' = '|',
--   'quoteChar' = '"',
--   'escapeChar' = '\\'
--   )
--STORED AS textfile
--LOCATION 's3://axle-internal-sources/raw/daddy/S083000_t015_segment/';
------TABLE PROPERTIES ('skip.header.line.count'='1');
----TABLE PROPERTIES ('compression_type'='gzip');
-- 
---------------------------------------------
---- Create a view in interna schema
---------------------------------------------
--DROP VIEW IF EXISTS interna.S083000_t015_segment;
--
--CREATE VIEW interna.S083000_t015_segment AS 
--SELECT * 
--  FROM spectrumdb.S083000_t015_segment
--  WITH NO SCHEMA BINDING;
-- 
--
---- Verify Counts
--SELECT COUNT(*) 
--  FROM interna.S083000_t015_segment;
--
--SELECT TOP 100 * 
--  FROM interna.S083000_t015_segment;
--
--------------
--
--
--DROP TABLE IF EXISTS spectrumdb.S083000_t032_email_message;
--
--CREATE EXTERNAL TABLE spectrumdb.S083000_t032_email_message(
--	email_message_id int,
--	subject_line nvarchar(200) ,
--	from_name nvarchar(80) ,
--	from_email_address nvarchar(80) ,
--	reply_to_email_address nvarchar(80) ,
--	email_message_status_cde smallint ,
--	createdby_user_id smallint ,
--	createdby_datetime timestamp ,
--	updatedby_user_id smallint ,
--	updatedby_datetime timestamp ,
--	brand_id int)
--ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--WITH SERDEPROPERTIES (
--   'separatorChar' = '|',
--   'quoteChar' = '"',
--   'escapeChar' = '\\'
--   )
--STORED AS textfile
--LOCATION 's3://axle-internal-sources/raw/daddy/S083000_t032_email_message/';
----TABLE PROPERTIES ('skip.header.line.count'='1');
----TABLE PROPERTIES ('compression_type'='gzip');
-- 
---------------------------------------------
---- Create a view in interna schema
---------------------------------------------
--DROP VIEW IF EXISTS interna.S083000_t032_email_message;
--
--CREATE VIEW interna.S083000_t032_email_message AS 
--SELECT * 
--  FROM spectrumdb.S083000_t032_email_message
--  WITH NO SCHEMA BINDING;
-- 
--
---- Verify Counts
--SELECT COUNT(*) 
--  FROM interna.S083000_t032_email_message;
--
--SELECT TOP 100 * 
--  FROM interna.S083000_t032_email_message;
--
-------------
--
--DROP TABLE IF EXISTS spectrumdb.S083000_t035_hard_bounce;
--
--CREATE EXTERNAL TABLE spectrumdb.S083000_t035_hard_bounce(
----	id int ,
--	message_id int ,
--	list_id int ,
--	subscriber_id_internal int ,
--	segment_id int ,
--	bounce_date timestamp ,
--	division_id smallint ,
----	is_moved int ,
--	email_address varchar(80))
--ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--WITH SERDEPROPERTIES (
--   'separatorChar' = '|',
--   'quoteChar' = '"',
--   'escapeChar' = '\\'
--   )
--STORED AS textfile
--LOCATION 's3://axle-internal-sources/raw/daddy/S083000_t035_hard_bounce/';
----TABLE PROPERTIES ('skip.header.line.count'='1');
----TABLE PROPERTIES ('compression_type'='gzip');
-- 
---------------------------------------------
---- Create a view in interna schema
---------------------------------------------
--DROP VIEW IF EXISTS interna.S083000_t035_hard_bounce;
--
--CREATE VIEW interna.S083000_t035_hard_bounce AS 
--SELECT * 
--  FROM spectrumdb.S083000_t035_hard_bounce
--  WITH NO SCHEMA BINDING;
-- 
--
---- Verify Counts
--SELECT COUNT(*) 
--  FROM interna.S083000_t035_hard_bounce;
--
--SELECT TOP 100 * 
--  FROM interna.S083000_t035_hard_bounce;
--
-------------
--
--
--DROP TABLE IF EXISTS spectrumdb.S083000_t036_soft_bounce;
--
--CREATE EXTERNAL TABLE spectrumdb.S083000_t036_soft_bounce(
--	message_id int ,
--	list_id int ,
--	subscriber_id_internal int ,
--	segment_id int ,
--	bounce_date timestamp ,
--	division_id smallint ,
--	email_address varchar(80))
--ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--WITH SERDEPROPERTIES (
--   'separatorChar' = '|',
--   'quoteChar' = '"',
--   'escapeChar' = '\\'
--   )
--STORED AS textfile
--LOCATION 's3://axle-internal-sources/raw/daddy/S083000_t036_soft_bounce/';
----TABLE PROPERTIES ('skip.header.line.count'='1');
----TABLE PROPERTIES ('compression_type'='gzip');
-- 
---------------------------------------------
---- Create a view in interna schema
---------------------------------------------
--DROP VIEW IF EXISTS interna.S083000_t036_soft_bounce;
--
--CREATE VIEW interna.S083000_t036_soft_bounce AS 
--SELECT * 
--  FROM spectrumdb.S083000_t036_soft_bounce
--  WITH NO SCHEMA BINDING;
-- 
--
---- Verify Counts
--SELECT COUNT(*) 
--  FROM interna.S083000_t036_soft_bounce;
--
--SELECT TOP 100 * 
--  FROM interna.S083000_t036_soft_bounce;
--
-----------
--
--
--DROP TABLE IF EXISTS spectrumdb.S083000_t037_undeliverable;
--
--CREATE EXTERNAL TABLE spectrumdb.S083000_t037_undeliverable(
--	message_id int ,
--	list_id int ,
--	brand_id int ,
--	subscriber_id_internal int ,
--	segment_id int ,
--	type nvarchar(6) ,
--	undelivered_date timestamp)
--ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--WITH SERDEPROPERTIES (
--   'separatorChar' = '|',
--   'quoteChar' = '"',
--   'escapeChar' = '\\'
--   )
--STORED AS textfile
--LOCATION 's3://axle-internal-sources/raw/daddy/S083000_t037_undeliverable/';
----TABLE PROPERTIES ('skip.header.line.count'='1');
----TABLE PROPERTIES ('compression_type'='gzip');
-- 
---------------------------------------------
---- Create a view in interna schema
---------------------------------------------
--DROP VIEW IF EXISTS interna.S083000_t037_undeliverable;
--
--CREATE VIEW interna.S083000_t037_undeliverable AS 
--SELECT * 
--  FROM spectrumdb.S083000_t037_undeliverable
--  WITH NO SCHEMA BINDING;
-- 
--
---- Verify Counts
--SELECT COUNT(*) 
--  FROM interna.S083000_t037_undeliverable;
--
--SELECT TOP 100 * 
--  FROM interna.S083000_t037_undeliverable;
--
--
----------
--
--DROP TABLE IF EXISTS spectrumdb.S083000_t044_trackable_url;
--
--CREATE EXTERNAL TABLE spectrumdb.S083000_t044_trackable_url(
--	url_id int ,
--	url_actual_hyperlink nvarchar(800) ,
--	mnemonic_text nvarchar(200) ,
--	triggered_message_id int ,
--	tracked_ind char(1))
--ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--WITH SERDEPROPERTIES (
--   'separatorChar' = '|',
--   'quoteChar' = '"',
--   'escapeChar' = '\\'
--   )
--STORED AS textfile
--LOCATION 's3://axle-internal-sources/raw/daddy/S083000_t044_trackable_url/';
----TABLE PROPERTIES ('skip.header.line.count'='1');
----TABLE PROPERTIES ('compression_type'='gzip');
-- 
---------------------------------------------
---- Create a view in interna schema
---------------------------------------------
--DROP VIEW IF EXISTS interna.S083000_t044_trackable_url;
--
--CREATE VIEW interna.S083000_t044_trackable_url AS 
--SELECT * 
--  FROM spectrumdb.S083000_t044_trackable_url
--  WITH NO SCHEMA BINDING;
-- 
--
---- Verify Counts
--SELECT COUNT(*) 
--  FROM interna.S083000_t044_trackable_url;
--
--SELECT TOP 100 * 
--  FROM interna.S083000_t044_trackable_url;
--
-----------
--
--DROP TABLE IF EXISTS spectrumdb.S083000_t050_message_candidate;
--
--CREATE EXTERNAL TABLE spectrumdb.S083000_t050_message_candidate(
--	id int ,
--	message_id int ,
--	segment_id int ,
--	list_id int ,
--	subscriber_id_internal int ,
--	domain_id int ,
--	email_address varchar(100) ,
--	number_of_opens int ,
--	first_tracked_datetime timestamp ,
--	last_tracked_datetime timestamp ,
--	update_list_ind smallint ,
--	autosense_ind smallint ,
--	requested smallint ,
--	message_format_preference char(1) ,
--	html_capable int ,
--	pushed_mail_content_type_id smallint ,
--	html_sniff int ,
--	opened_mail_content_type_id smallint ,
--	followed_up_ind int ,
--	Record_Insert_Datetime timestamp)
--ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--WITH SERDEPROPERTIES (
--   'separatorChar' = '|',
--   'quoteChar' = '"',
--   'escapeChar' = '\\'
--   )
--STORED AS textfile
--LOCATION 's3://axle-internal-sources/raw/daddy/S083000_t050_message_candidate/';
----TABLE PROPERTIES ('skip.header.line.count'='1');
----TABLE PROPERTIES ('compression_type'='gzip');
-- 
---------------------------------------------
---- Create a view in interna schema
---------------------------------------------
--DROP VIEW IF EXISTS interna.S083000_t050_message_candidate;
--
--CREATE VIEW interna.S083000_t050_message_candidate AS 
--SELECT * 
--  FROM spectrumdb.S083000_t050_message_candidate
--  WITH NO SCHEMA BINDING;
-- 
--
---- Verify Counts
--SELECT COUNT(*) 
--  FROM interna.S083000_t050_message_candidate;
--
--SELECT TOP 100 * 
--  FROM interna.S083000_t050_message_candidate;
--
----------
--
--DROP TABLE IF EXISTS spectrumdb.S083000_t082_message_list_trackable_url_assoc;
--
--CREATE EXTERNAL TABLE spectrumdb.S083000_t082_message_list_trackable_url_assoc(
--	message_id int ,
--	segment_id int ,
--	list_id int ,
--	subscriber_id_internal int ,
--	url_id int ,
--	id int ,
--	number_of_click_throughs int ,
--	number_of_pass_alongs int ,
--	first_tracked_datetime timestamp ,
--	last_tracked_datetime timestamp ,
--	content_type_id smallint ,
--	followed_up_ind int)
--ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--WITH SERDEPROPERTIES (
--   'separatorChar' = '|',
--   'quoteChar' = '"',
--   'escapeChar' = '\\'
--   )
--STORED AS textfile
--LOCATION 's3://axle-internal-sources/raw/daddy/S083000_t082_message_list_trackable_url_assoc/';
----TABLE PROPERTIES ('skip.header.line.count'='1');
----TABLE PROPERTIES ('compression_type'='gzip');
-- 
---------------------------------------------
---- Create a view in interna schema
---------------------------------------------
--DROP VIEW IF EXISTS interna.S083000_t082_message_list_trackable_url_assoc;
--
--CREATE VIEW interna.S083000_t082_message_list_trackable_url_assoc AS 
--SELECT * 
--  FROM spectrumdb.S083000_t082_message_list_trackable_url_assoc
--  WITH NO SCHEMA BINDING;
-- 
--
---- Verify Counts
--SELECT COUNT(*) 
--  FROM interna.S083000_t082_message_list_trackable_url_assoc;
--
--SELECT TOP 100 * 
--  FROM interna.S083000_t082_message_list_trackable_url_assoc;
--
--
-----------
--
DROP TABLE IF EXISTS spectrumdb.S083000_t151_rpt_delivery_response_message_segment;

CREATE EXTERNAL TABLE spectrumdb.S083000_t151_rpt_delivery_response_message_segment(
	message_id int ,
	segment_id int ,
	domain_id int ,
	pushed int ,
	pushed_text int ,
	pushed_html int ,
	pushed_aol int ,
	pushed_multi_part int ,
	deployed int ,
	deployed_text int ,
	deployed_html int ,
	deployed_aol int ,
	deployed_multi_part int ,
	hard_bounce int ,
	hard_bounce_text int ,
	hard_bounce_html int ,
	hard_bounce_aol int ,
	hard_bounce_multi_part int ,
	soft_bounce int ,
	soft_bounce_text int ,
	soft_bounce_html int ,
	soft_bounce_aol int ,
	soft_bounce_multi_part int ,
	delivered int ,
	delivered_text int ,
	delivered_html int ,
	delivered_aol int ,
	delivered_multi_part int ,
	click_through_total int ,
	click_through_total_text int ,
	click_through_total_html int ,
	click_through_total_aol int ,
	click_through_total_multi_part int ,
	click_through_distinct int ,
	click_through_distinct_text int ,
	click_through_distinct_html int ,
	click_through_distinct_aol int ,
	click_through_distinct_multi_part int ,
	click_through_distinct_msg int ,
	click_through_distinct_msg_text int ,
	click_through_distinct_msg_html int ,
	click_through_distinct_msg_aol int ,
	click_through_distinct_msg_multi_part int ,
	open_rate_total int ,
	open_rate_total_text int ,
	open_rate_total_html int ,
	open_rate_total_multi_part int ,
	open_rate_total_aol int ,
	open_rate_distinct int ,
	open_rate_distinct_text int ,
	open_rate_distinct_html int ,
	open_rate_distinct_multi_part int ,
	open_rate_distinct_aol int ,
	pass_along_total int ,
	pass_along_distinct int ,
	pass_along_distinct_msg int ,
	unsubscribe int ,
	ecom_tran int ,
	ecom_amount Double precision ,
	complaint int ,
	block int ,
	undeliverable int ,
	undeliverable_text int ,
	undeliverable_html int ,
	undeliverable_aol int ,
	undeliverable_multi_part int ,
	update_date timestamp ,
	list_id int ,
	ftaf_distinct int ,
	ftaf_total int ,
	brand_id int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   'separatorChar' = '|',
   'quoteChar' = '"',
   'escapeChar' = '\\'
   )
STORED AS textfile
LOCATION 's3://axle-internal-sources/raw/daddy/S083000_t151_rpt_delivery_response_message_segment/';
--TABLE PROPERTIES ('skip.header.line.count'='1');
--TABLE PROPERTIES ('compression_type'='gzip');
 
-------------------------------------------
-- Create a view in interna schema
-------------------------------------------
DROP VIEW IF EXISTS interna.S083000_t151_rpt_delivery_response_message_segment;

CREATE VIEW interna.S083000_t151_rpt_delivery_response_message_segment AS 
SELECT * 
  FROM spectrumdb.S083000_t151_rpt_delivery_response_message_segment
  WITH NO SCHEMA BINDING;
 

-- Verify Counts
SELECT COUNT(*) 
  FROM interna.S083000_t151_rpt_delivery_response_message_segment;

SELECT TOP 100 * 
  FROM interna.S083000_t151_rpt_delivery_response_message_segment;
--
-----------
--
--DROP TABLE IF EXISTS spectrumdb.S083000_t203_division;
--
--CREATE EXTERNAL TABLE spectrumdb.S083000_t203_division(
--	division_id smallint,
--	division_cde nchar(5) ,
--	division_name nvarchar(100) ,
--	division_status smallint ,
--	description nvarchar(255) ,
--	division_type smallint ,
--	canspam_template_id smallint)
--ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--WITH SERDEPROPERTIES (
--   'separatorChar' = '|',
--   'quoteChar' = '"',
--   'escapeChar' = '\\'
--   )
--STORED AS textfile
--LOCATION 's3://axle-internal-sources/raw/daddy/S083000_t203_division/';
----TABLE PROPERTIES ('skip.header.line.count'='1');
----TABLE PROPERTIES ('compression_type'='gzip');
-- 
---------------------------------------------
---- Create a view in interna schema
---------------------------------------------
--DROP VIEW IF EXISTS interna.S083000_t203_division;
--
--CREATE VIEW interna.S083000_t203_division AS 
--SELECT * 
--  FROM spectrumdb.S083000_t203_division
--  WITH NO SCHEMA BINDING;
-- 
--
---- Verify Counts
--SELECT COUNT(*) 
--  FROM interna.S083000_t203_division;
--
--SELECT TOP 100 * 
--  FROM interna.S083000_t203_division;
--
-----------
--
--DROP TABLE IF EXISTS spectrumdb.S083000_t216_message_category;
--
--CREATE EXTERNAL TABLE spectrumdb.S083000_t216_message_category(
--	category_id smallint,
--	category_name nvarchar(128) ,
--	category_description nvarchar(255) ,
--	primary_ind int ,
--	active_ind int ,
--	createdby_datetime timestamp)
--ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--WITH SERDEPROPERTIES (
--   'separatorChar' = '|',
--   'quoteChar' = '"',
--   'escapeChar' = '\\'
--   )
--STORED AS textfile
--LOCATION 's3://axle-internal-sources/raw/daddy/S083000_t216_message_category/';
----TABLE PROPERTIES ('skip.header.line.count'='1');
----TABLE PROPERTIES ('compression_type'='gzip');
-- 
---------------------------------------------
---- Create a view in interna schema
---------------------------------------------
--DROP VIEW IF EXISTS interna.S083000_t216_message_category;
--
--CREATE VIEW interna.S083000_t216_message_category AS 
--SELECT * 
--  FROM spectrumdb.S083000_t216_message_category
--  WITH NO SCHEMA BINDING;
-- 
--
---- Verify Counts
--SELECT COUNT(*) 
--  FROM interna.S083000_t216_message_category;
--
--SELECT TOP 100 * 
--  FROM interna.S083000_t216_message_category;
--  
--------------
--  
--  
--  
-- DROP TABLE IF EXISTS spectrumdb.S083000_t217_message_rate_type;
--
--CREATE EXTERNAL TABLE spectrumdb.S083000_t217_message_rate_type(
--	rate_type_id smallint ,
--	rate_type_name nvarchar(50) ,
--	active_ind int )
--ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--WITH SERDEPROPERTIES (
--   'separatorChar' = '|',
--   'quoteChar' = '"',
--   'escapeChar' = '\\'
--   )
--STORED AS textfile
--LOCATION 's3://axle-internal-sources/raw/daddy/S083000_t217_message_rate_type/';
----TABLE PROPERTIES ('skip.header.line.count'='1');
----TABLE PROPERTIES ('compression_type'='gzip');
-- 
---------------------------------------------
---- Create a view in interna schema
---------------------------------------------
--DROP VIEW IF EXISTS interna.S083000_t217_message_rate_type;
--
--CREATE VIEW interna.S083000_t217_message_rate_type AS 
--SELECT * 
--  FROM spectrumdb.S083000_t217_message_rate_type
--  WITH NO SCHEMA BINDING;
-- 
--
---- Verify Counts
--SELECT COUNT(*) 
--  FROM interna.S083000_t217_message_rate_type;
--
--SELECT TOP 100 * 
--  FROM interna.S083000_t217_message_rate_type;
--
-----------
--
--DROP TABLE IF EXISTS spectrumdb.S083000_t218_business_unit;
--
--CREATE EXTERNAL TABLE spectrumdb.S083000_t218_business_unit(
--	business_unit_id smallint ,
--	business_unit_name nvarchar(50) ,
--	active_ind int)
--ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--WITH SERDEPROPERTIES (
--   'separatorChar' = '|',
--   'quoteChar' = '"',
--   'escapeChar' = '\\'
--   )
--STORED AS textfile
--LOCATION 's3://axle-internal-sources/raw/daddy/S083000_t218_business_unit/';
----TABLE PROPERTIES ('skip.header.line.count'='1');
----TABLE PROPERTIES ('compression_type'='gzip');
-- 
---------------------------------------------
---- Create a view in interna schema
---------------------------------------------
--DROP VIEW IF EXISTS interna.S083000_t218_business_unit;
--
--CREATE VIEW interna.S083000_t218_business_unit AS 
--SELECT * 
--  FROM spectrumdb.S083000_t218_business_unit
--  WITH NO SCHEMA BINDING;
-- 
--
---- Verify Counts
--SELECT COUNT(*) 
--  FROM interna.S083000_t218_business_unit;
--
--SELECT TOP 100 * 
--  FROM interna.S083000_t218_business_unit;


