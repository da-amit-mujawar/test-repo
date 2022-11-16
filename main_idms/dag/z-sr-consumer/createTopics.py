"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("CreateTopic").getOrCreate()

s3_file = "c:/Saravanan/Projects/IGRP/GitHub/nua-airflow/develop_idms/dag/z-sr-consumer/data/json/NE01_ODB43_202109.jsonl"
bootstrap_servers = "10.10.1.116:9092"
topic_name = "NE01_ODB43_202109"
file_path = s3_file
df = spark.read.format("json").load(file_path)

lst_stru = struct('FamilyID', 'arrival_date', 'arrival_attrib_source', 'add_date',
                  'add_attrib_source', 'upgrade_date', 'upgrade_attrib_source', 'recency_date',
                  'recency_attrib_source', 'tele_acquisition_date', 'address_segment_count',
                  'property_segment_count', 'telephone_segment_count', 'hhld_member_count',
                  'vehicle_section_count', 'testdata_segment_count', 'hhld_source_inds_part1',
                  'hhld_source_inds_part2', 'hhld_source_inds_part3', 'source_ever_inds_part1',
                  'source_ever_inds_part2', 'source_ever_inds_part3', 'cross_section',
                  'move_status_code', 'move_status_date', 'household_internal_inds',
                  'ncoa_move_type', 'departure_date', 'household_maintenance_inds',
                  'hhld_internal_use_area', 'ContributorCreditIndicators', 'political_contrib_cnt',
                  'political_contrib_last_date', 'political_contrib_last_party',
                  'political_contrib_last_amt', 'political_contrib_total_amt',
                  'travel_card_refresh_year', 'bank_card_holder_refresh_year',
                  'active_bank_card_refresh_date', 'language_code', 'language_score',
                  'occupation_code_female', 'occupation_code_male', 'marital_status_date',
                  'reported_marital_status', 'LifeStageIndicators', 'gp_in_hh_year',
                  'child_presence_refresh_year', 'LifeStyleIndicators', 'LifeStyleIndicators2',
                  'AilmentsCategory', 'vacation_home_year', 'reported_income_div_1000',
                  'reported_income_date', 'second_property_total_value', 'MailResponsiveIndicators',
                  'mail_resp_refresh_year', 'mob_refresh_year', 'boat_propulsion_code',
                  'boat_hull_type_code', 'boat_length', 'aircraft_type_code', 'aircraft_mfr_year',
                  'pilot_license_code', 'pimy_count', 'pimy_most_recent_year',
                  'MotorcycleTrailerIndicators', 'survey_home_value_code', 'survey_home_value_date',
                  'last_line_name', 'state_abbr', 'postal_finance_num', 'zip_code', 'zip_add_on',
                  'zip4_match_level', 'footnotes_usps', 'census_state', 'census_county',
                  'census_tract', 'census_bg', 'coordinates_match_level', '___RuntimeStatus',
                  '___GUID', 'ODB41_ADDRESS', 'ODB41_PROPERTY', 'ODB41_TELEPHONE',
                  'ODB41_INDIVIDUAL', 'ODB41_VEHICLE', 'ODB41_TEST')

tdf = df.select(to_json(lst_stru).alias("value"), to_json(struct('FamilyID')).alias("key"))
tdf.write.format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("topic", topic_name) \
    .save()
"""
