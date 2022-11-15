DROP TABLE IF EXISTS nosuchtable;
COPY {new-load-table}
(
	firstname,
	middleinitial,
	lastname,
	last_name_suffix,
	title_code,
	filler_1,
	gender,
	filler_2,
	house_number,
	street_pre_directional,
	street_name,
	street_suffix,
	street_post_directional,
	unit_type,
	unit_number,
	city,
	state,
	census_state_code_2010,
	county_code,
	filler_3,
	filler_4,
	filler_5,
	zip,
	zip_four,
	carrier_route_code,
	delivery_point_bar_code,
	area_code,
	phonenumber,
	nielsen_county_rank,
	filler_6,
	time_zone,
	age_code,
	filler_7,
	birth_date,
	income,
	filler_8,
	last_contribution_date,
	length_of_residence,
	residence_type,
	filler_9,
	delivery_unit_size_raw,
	filler_10,
	own_rent_indicator,
	filler_11,
	location_type,
	filler_12,
	pool_type,
	home_value_code,
	filler_13,
	home_sale_date,
	home_sale_price_code,
	filler_14,
	home_sale_price,
	home_equity_estimate,
	filler_15,
	finance_type,
	filler_16,
	mortgage_loan_type,
	filler_17,
	boat_propulsion_code,
	filler_18,
	filler_19,
	mortgage_amount,
	filler_20,
	congressional_district,
	credit_card_code,
	filler_21,
	filler_22,
	filler_23,
	filler_24,
	filler_25,
	filler_26,
	filler_27,
	number_trade_lines,
	marital_status,
	filler_28,
	filler_29,
	spousal_indicator_raw,
	filler_30,
	filler_31,
	filler_32,
	filler_33,
	cat_owner,
	dog_owner,
	grandparent_in_house,
	veteran_flag,
	stocks_or_bonds,
	dm_high_tech_household,
	female_occupation,
	male_occupation,
	census_average_years_attended_school_code,
	filler_34,
	census_phhblack_code,
	census_of_population_graduated_college,
	census_phhspnsh_code,
	census_percent_of_population_married,
	census_precent_of_population_single_parent,
	filler_35,
	filler_36,
	filler_37,
	filler_38,
	filler_39,
	filler_40,
	filler_41,
	expendable_income_rank,
	filler_42,
	loan_to_value_ratio_ltv,
	filler_43,
	filler_44,
	filler_45,
	donor_code,
	filler_46,
	filler_47,
	filler_48,
	mail_responsive_current,
	mail_responsive_recent,
	mail_responsive_ever,
	filler_49,
	filler_50,
	filler_51,
	filler_52,
	filler_53,
	filler_54,
	filler_55,
	census_tract_2010,
	census_block_group_2010,
	filler_56,
	filler_57,
	filler_58,
	filler_59,
	address_type,
	filler_60,
	hoh_head_of_household,
	first_name_hoh,
	middle_initial_hoh,
	last_name_hoh,
	last_name_suffix_hoh,
	title_code_hoh,
	filler_61,
	gender_hoh,
	filler_62,
	birth_date_hoh,
	individual_id_hoh,
	hoh_age_code,
	filler_63,
	number_trade_lines_hoh,
	credit_card_code_hoh,
	filler_64,
	filler_65,
	filler_66,
	filler_67,
	filler_68,
	filler_69,
	filler_70,
	filler_71,
	marital_status_hoh,
	filler_72,
	filler_73,
	filler_74,
	filler_75,
	filler_76,
	company_id,
	individual_id,
	location_id,
	filler_77,
	filler_78,
	filler_79,
	filler_80,
	filler_81,
	filler_82,
	filler_83,
	usps_delivery_service_type_code,
	nielsen_county_region,
	filler_84,
	lot_size,
	home_size,
	home_age,
	actual_age_flag,
	political_contribution_amount,
	filler_85,
	filler_86,
	pilot_license_code,
	mortgage_interest_rates_old,
	last_party_contributed_to,
	filler_87,
	aircraft_type_code,
	filler_88,
	boat_owner_indicator,
	registered_voter,
	political_party_affiliation,
	filler_89,
	number_of_adults_in_household,
	household_member_count,
	filler_90,
	medag18p,
	census_percent_of_population_with_3_households,
	census_phhasian_code,
	medhhd1,
	wealth_finder,
	filler_91,
	numberofchildren,
	alternatehhind,
	countryoforign,
	donotmailflag,
	emailpresenceflag,
	routetype,
	householdarrivaldate_year,
	householdarrivaldate,
	homeagesource,
	homebuiltyear,
	mortgagemonth,
	homevaluesource,
	actualincome,
	infopersona_cluster,
	filler_92,
	infopersona_supercluster,
	filler_93,
	lifestyle,
	listtype,
	marriedscore,
	medhhd2,
	mortgageloantype,
	mortgageinterestsource,
	occupancycount,
	agedatasource,
	presenceofchildren,
	recencydate,
	refreshdate,
	scf,
	vendorethnicity,
	vendorethnicgroup,
	vendorlanguage,
	vendorreligion,
	censuscountycode2010,
	fulfillmentflag,
	childrenbygender0,
	childrenbygender1,
	childrenbygender2,
	childrenbygender3,
	childrenbygender4,
	childrenbygender5,
	childrenbygender6,
	childrenbygender7,
	childrenbygender8,
	childrenbygender9,
	childrenbygender10,
	childrenbygender11,
	childrenbygender12,
	childrenbygender13,
	childrenbygender14,
	childrenbygender15,
	childrenbygender16,
	childrenbygender17,
	markettargetage0_5,
	markettargetage6_11,
	markettargetage12_17,
	markettargetage12_17female,
	markettargetage12_17male,
	children0_5,
	children6_11,
	children0_11,
	childrenbybirthmonth0,
	childrenbybirthmonth1,
	childrenbybirthmonth2,
	childrenbybirthmonth3,
	childrenbybirthmonth4,
	childrenbybirthmonth5,
	childrenbybirthmonth6,
	childrenbybirthmonth7,
	childrenbybirthmonth8,
	childrenbybirthmonth9,
	childrenbybirthmonth10,
	childrenbybirthmonth11,
	childrenbybirthmonth12,
	childrenbybirthmonth13,
	childrenbybirthmonth14,
	childrenbybirthmonth15,
	childrenbybirthmonth16,
	childrenbybirthmonth17,
	filler_94,
	purchasing_power_income,
	filler_95,
	potential_investor_consumer,
	filler_96,
	sub_family_indicator,
	filler_97,
	filler_98,
	number_of_contributions,
	aircraft_mfg_year,
	cbsa_code,
	filler_99,
	filler_100,
	filler_101,
	heavy_internet_user,
	early_technology_adopter,
	csa_code,
	csa_code_description,
	cbsa_level,
	mail_confidence,
	boat_hull_type,
	boat_length,
	flatitude,
	filler_102,
	flongitude,
	vendor_matchcode,
	do_not_call_flag,
	has_primary_telephone,
	addressline1,
	digitalmatch,
	age,
	mortgagedate,
	loanamount,
	lhi_african_american_ethnic_products,
	lhi_american_history,
	lhi_general_apparel,
	lhi_automotive,
	lhi_baseball,
	lhi_basketball,
	lhi_bible,
	lhi_birds,
	lhi_publish_books,
	lhi_business_items,
	lhi_cats,
	lhi_collectibles,
	lhi_college,
	lhi_computers,
	lhi_cooking,
	lhi_general_crafts,
	lhi_current_events,
	lhi_do_it_yourselfer,
	lhi_charitable_donor,
	lhi_personalized_products,
	lhi_horses,
	lhi_culture_arts,
	lhi_dogs,
	lhi_children_family,
	lhi_fishing,
	lhi_fitness_exercise,
	lhi_football,
	lhi_gardening,
	lhi_gambling,
	lhi_gift_giver,
	lhi_golf,
	lhi_general_health,
	lhi_hispanic_ethnic_products,
	lhi_hobbies,
	lhi_hockey,
	lhi_home_decorating,
	lhi_humor,
	lhi_hunting,
	lhi_inspirational,
	lhi_apparel_kids,
	lhi_apparel_mens,
	lhi_money_making,
	lhi_motorcycles,
	lhi_music,
	lhi_ocean,
	lhi_outdoors,
	lhi_pets,
	lhi_personal_finance,
	lhi_rural_farming,
	lhi_general_sports,
	lhi_stationery,
	lhi_general_travel,
	lhi_movies,
	lhi_wildlife,
	lhi_apparel_womens,
	lhi_unused_01,
	lhi_photography,
	lhi_seniors,
	lhi_aviation,
	lhi_electionics,
	lhi_internet,
	lhi_credit_card_user,
	lhi_beauty_cosmetic,
	lhi_asian_ethnic_products,
	lhi_dieting,
	lhi_science,
	lhi_sweepstakes,
	lhi_tobacco,
	lhi_bargain_seekers,
	lhi_publications,
	lhi_catalogs,
	lhi_hightech,
	lhi_apparel_accessories,
	lhi_apparel_mens_fashions,
	lhi_apparel_womens_fashions,
	lhi_auto_racing,
	lhi_trucks,
	lhi_home_office_products,
	lhi_politically_conservative,
	lhi_crocheting,
	lhi_knitting,
	lhi_needlepoint,
	lhi_quilting,
	lhi_sewing,
	lhi_fiction,
	lhi_games,
	lhi_unused_03,
	lhi_gourmet,
	lhi_history,
	lhi_internet_access,
	lhi_internet_buying,
	lhi_liberal,
	lhi_books_nonfiction,
	lhi_boating_sailing,
	lhi_camping_hiking,
	lhi_hunting_fishing,
	lhi_photo_processing,
	lhi_magazine_subscriber,
	lhi_books_science_fiction,
	lhi_skiing,
	lhi_soccer,
	lhi_tennis,
	lhi_travel_cruises,
	lhi_travel_rv,
	lhi_travel_us,
	tw_greenmodel,
	tw_highvaluesecurityinvestor,
	tw_highereducation,
	tw_highvaluestockinvestor,
	tw_heavyinvestmenttraders,
	tw_physicalfitnessclubs,
	tw_highriskinvestor,
	tw_lowriskinvestor,
	tw_frequentbusinesstraveler,
	tw_frequentflyer,
	tw_annuities,
	tw_lifeinsurance,
	tw_luxurycarbuyer,
	tw_cruise,
	tw_timeshareowner,
	tw_impulsebuyer,
	tw_luxuryhotel,
	tw_pbsdonor,
	tw_safetysecurityconscious,
	tw_shopaholics,
	tw_avidthemeparkvisitor,
	tw_foreigntravelvacation,
	tw_conservative,
	tw_leaningconservative,
	tw_leaningliberal,
	tw_religiousdonor,
	tw_gardenmaintenance,
	tw_heavyfamilyrestaurantvisitor,
	tw_heavypayperviewsports,
	tw_nascar,
	tw_liberal,
	tw_dogproducts,
	tw_heavypayperviewmovies,
	tw_dietproducts,
	tw_organicfoods,
	tw_heavyvitaminanddietarysupplement,
	tw_babyproducts,
	tw_heavycouponuser,
	tw_opinionleaders,
	tw_catproducts,
	tw_creditcardrewards,
	tw_highendsportingequipment,
	tw_homeimprovement,
	tw_highendelectronicsbuyer,
	tw_avidgamer,
	tw_rentalcar,
	tw_avidcellphoneuser,
	tw_countryclubmember,
	tw_professionaltaxpreparation,
	tw_onsitetaxpreparationservice,
	tw_classicalmusicconcerts,
	tw_rockmusicconcerts,
	tw_countrymusicconcerts,
	tw_livetheater,
	tw_heavybookbuyer,
	tw_freshwaterfishing,
	tw_saltwaterfishing,
	tw_hunting,
	tw_allterrainvehicle,
	tw_powerboating,
	tw_outdooractivities,
	tw_adventureseekers,
	tw_lowendsportingequipment,
	tw_professionalbaseballsportsfans,
	tw_professionalbasketballsportsfans,
	tw_professionalfootballsportsfans,
	tw_soccersportsfans,
	tw_collegebasketballsportsfans,
	tw_collegefootballsportsfans,
	tw_golfsportsfans,
	tw_tennissportsfans,
	tw_professionalwrestlingsportsfans,
	tw_internationallongdistance,
	tw_heavyfrozendinner,
	tw_cookfromscratch,
	tw_cookforfun,
	tw_winelover,
	tw_onlinepurchasepersonal,
	tw_onlinepurchasebusiness,
	tw_onlinetravelplan,
	tw_blogwriting,
	tw_voiceoverinternet,
	tw_wifioutsideofhome,
	tw_wifiinhome,
	tw_wholesaleclub,
	tw_autoclub,
	tw_diyautomaintenance,
	tw_onlinegamingactivity,
	tw_onlineinvestmenttrading,
	tw_onlinebillpayment,
	tw_onlinetvdownload,
	tw_mobileinternetaccess,
	tw_socialmedianetwork,
	tw_specialtyorganicfoodstore,
	tw_homeoffice,
	tw_businessbanking,
	tw_moderate_economyhotel,
	tw_e_reader,
	tw_cellphoneonlymodel,
	tw_alternativemedicine,
	tw_non_religiousdonor,
	tw_majorhomeremodeling,
	tw_avidsmartphoneusers,
	tw_healthinsurance,
	tw_satellitetv,
	tw_onlinemusicdownload,
	tw_adulteducation,
	tw_pilates_yoga,
	tw_fastfood,
	tw_hybridcars,
	tw_heavy_online_buyer,
	tw_high_end_appparel,
	tw_heavy_catalog_buyer,
	tw_gift_buyers,
	tw_camping,
	tw_sports_fanatics,
	tw_financial_planner,
	tw_work_health_insurance,
	tw_comprehensive_auto_insurance,
	tw_small_business_insurance,
	tw_heavy_snack_eaters,
	tw_heavy_domestic_travelers,
	tw_new_vehicle_buyer,
	tw_suv_buyer,
	tw_minivan_buyer,
	tw_auto_loan,
	tw_education_loan,
	tw_real_estate_investment,
	tw_fantasy_sports,
	tw_hockey_buyer,
	tw_auto_lease,
	tw_digital_payment,
	tw_high_mileage_usage,
	tw_motorcycle_owner,
	tw_union_member,
	tw_national_park_visitor,
	tw_online_streaming_and_devices,
	tw_road_trip,
	tw_social_networking_ad_click,
	tw_satellite_radio,
	heatingtype_code,
	income_producing_assets,
	marketarea,
	phonenumbertype,
	revolver_to_transactor,
	second_property_ind,
	consumerstabilityrawscore,
	consumerstabilitypercentile,
	ailments,
	mailscore,
	populationdensity,
	filler_103,
	number_of_surnames,
	homeowner,
	education_model,
	education_model_ext,
	income_producing_assets_ext,
	titlecode_ext,
	boatpropulsioncode_ext,
	vendorethnicity_ext,
	vendorreligion_ext,
	vendorlanguage_ext,
	vendorethnicgroup_ext,
	countryoforign_ext,
	contributionamount,
	lemsmatchcode,
	mortgagepresent,
	discretionary_income_score,
	membercount,
	sesi,
	zipfourmatchlevel,
	no_stats_indicator,
	vacant_indicator,
	timezone,
	areaprefix,
	correctivelensespresence,
	actualvacation_expense,
	vacation_expense,
	vacation_expense_dic,
	nid,
	nid_description,
	markettargetage18_44female,
	pprofsnl,
	pwhitcol,
	pbluecol,
	pcolgrad,
	routenumber,
	boxtype,
	boxnumber,
	nameaddresssource,
	mailorderbuyerflag,
	creditcard_bank,
	markettargetage0_11,
	markettargetage0_17,
	markettargetage18_24female,
	markettargetage18_24male,
	markettargetage18_34female,
	markettargetage18_34male,
	markettargetage25_34female,
	markettargetage25_34male,
	markettargetage35_44female,
	markettargetage35_44male,
	markettargetage35_54female,
	markettargetage35_54male,
	markettargetage45_54female,
	markettargetage45_54male,
	markettargetage50plus,
	markettargetage55_64female,
	markettargetage55_64male,
	markettargetage65plus,
	householdsize1_2,
	householdsize3plus,
	phonesourcecat,
	secondaryareacode,
	secondaryphone,
	numberoffireplaces,
	pool,
	bedroomcount,
	bathroomcount,
	garagetype,
	homeownersource,
	salepricesource,
	saledatesource,
	roomcount,
	multipleunit,
	purchasing_power_indicator,
	censusstatecode2020,
	censuscountycode2020,
	censustract2020,
	censusblockgroup2020,
	secondmortgageamount,
	secondmortgagetypecode,
	lot_size_sqfeet,
	universal_sqfeet,
	universal_sqfeet_ind,
	living_sqfeet,
	groundfloor_sqfeet,
	gross_sqfeet,
	adjustedgross_sqfeet,
	basement_sqfeet,
	garage_sqfeet,
	fullbath,
	halfbath,
	fireplace_ind,
	fireplacetype,
	pooltype,
	roofcover,
	roofshape,
	conditiontype,
	exteriorwalltype,
	foundationtype,
	constructionquality,
	stories,
	styletype,
	airconditioningtype,
	electricwiringtype,
	fueltype,
	sewertype,
	watertype,
	construction_type_detail,
	heating_type_detail,
	filler_105
)
FROM 's3://{s3-internal}{s3-key-new}'
IAM_ROLE '{iam}'
GZIP
FIXEDWIDTH
'firstname:15,
middleinitial:1,
lastname:20,
last_name_suffix:3,
title_code:1,
filler_1:4,
gender:1,
filler_2:7,
house_number:10,
street_pre_directional:2,
street_name:28,
street_suffix:4,
street_post_directional:2,
unit_type:4,
unit_number:8,
city:16,
state:2,
census_state_code_2010:2, 
county_code:3,
filler_3:14,
filler_4:4,
filler_5:30,
zip:5,
zip_four:4,
carrier_route_code:4,
delivery_point_bar_code:3,
area_code:3,
phonenumber:7,
nielsen_county_rank:1,
filler_6:25,
time_zone:1,
age_code:1,
filler_7:7,
birth_date:8,
income:1,
filler_8:19,
last_contribution_date:8,
length_of_residence:2,
residence_type:1,
filler_9:5,
delivery_unit_size_raw:1,
filler_10:13, 
own_rent_indicator:1,
filler_11:15, 
location_type:1,
filler_12:6, 
pool_type:3,
home_value_code:1,
filler_13:19,
home_sale_date:8,
home_sale_price_code:1,
filler_14:19,
home_sale_price:12,
home_equity_estimate:1,
filler_15:18,
finance_type:1,
filler_16:28,
mortgage_loan_type:1, 
filler_17:30,
boat_propulsion_code:1,
filler_18:19, 
filler_19:6,
mortgage_amount:1,
filler_20:19,
congressional_district:2,
credit_card_code:10,
filler_21:30, 
filler_22:30, 
filler_23:30, 
filler_24:30, 
filler_25:30, 
filler_26:30, 
filler_27:30, 
number_trade_lines:1,
marital_status:1,
filler_28:7, 
filler_29:6,
spousal_indicator_raw:3,
filler_30:1,
filler_31:10,
filler_32:1,
filler_33:10,
cat_owner:3,
dog_owner:3,
grandparent_in_house:3,
veteran_flag:3,
stocks_or_bonds:3,
dm_high_tech_household:3,
female_occupation:2,
male_occupation:2,
census_average_years_attended_school_code:1,
filler_34:32,
census_phhblack_code:1,
census_of_population_graduated_college:1,
census_phhspnsh_code:1,
census_percent_of_population_married:1,
census_precent_of_population_single_parent:1,
filler_35:2,
filler_36:1,
filler_37:4,
filler_38:1,
filler_39:3,
filler_40:3,
filler_41:3,
expendable_income_rank:1,
filler_42:15,
loan_to_value_ratio_ltv:1,
filler_43:11, 
filler_44:1,
filler_45:15,
donor_code:5, 
filler_46:2,
filler_47:2,
filler_48:36,
mail_responsive_current:1,
mail_responsive_recent:1,
mail_responsive_ever:1,
filler_49:20,
filler_50:45,
filler_51:30,
filler_52:6,
filler_53:13,
filler_54:1,
filler_55:40,
census_tract_2010:6,
census_block_group_2010:1,
filler_56:10,
filler_57:11,
filler_58:1,
filler_59:3,
address_type:1,
filler_60:4,
hoh_head_of_household:3,
first_name_hoh:15,
middle_initial_hoh:1,
last_name_hoh:20,
last_name_suffix_hoh:3,
title_code_hoh:1,
filler_61:4,
gender_hoh:1,
filler_62:7,
birth_date_hoh:8,
individual_id_hoh:12, 
hoh_age_code:1,
filler_63:7,
number_trade_lines_hoh:1,
credit_card_code_hoh:10,
filler_64:30,
filler_65:30,
filler_66:30,
filler_67:30,
filler_68:30,
filler_69:30,
filler_70:30,
filler_71:3,
marital_status_hoh:1,
filler_72:7, 
filler_73:6,
filler_74:2,
filler_75:1,
filler_76:10,
company_id:12,
individual_id:12,
location_id:12,
filler_77:9,
filler_78:9,
filler_79:9,
filler_80:9,
filler_81:9,
filler_82:7,
filler_83:10,
usps_delivery_service_type_code:1,
nielsen_county_region:1,
filler_84:20,
lot_size:5,
home_size:5,
home_age:3,
actual_age_flag:1,
political_contribution_amount:24,
filler_85:1,
filler_86:24,
pilot_license_code:1,
mortgage_interest_rates_old:4,
last_party_contributed_to:1,
filler_87:24,
aircraft_type_code:1,
filler_88:24,
boat_owner_indicator:3,
registered_voter:3,
political_party_affiliation:1,
filler_89:19, 
number_of_adults_in_household:1,
household_member_count:1,
filler_90:1,
medag18p:4,
census_percent_of_population_with_3_households:1,
census_phhasian_code:1,
medhhd1:9,
wealth_finder:1,
filler_91:24,
numberofchildren:1,
alternatehhind:1,
countryoforign:2,
donotmailflag:1,
emailpresenceflag:1,
routetype:1,
householdarrivaldate_year:4,
householdarrivaldate:6,
homeagesource:1,
homebuiltyear:4,
mortgagemonth:2,
homevaluesource:1,
actualincome:7,
infopersona_cluster:2,
filler_92:36, 
infopersona_supercluster:1,
filler_93:21,
lifestyle:35,
listtype:2,
marriedscore:1,
medhhd2:6,
mortgageloantype:1,
mortgageinterestsource:1,
occupancycount:2,
agedatasource:1,
presenceofchildren:1,
recencydate:6,
refreshdate:6,
scf:3,
vendorethnicity:2,
vendorethnicgroup:1,
vendorlanguage:2,
vendorreligion:1,
censuscountycode2010:3,
fulfillmentflag:1,
childrenbygender0:1,
childrenbygender1:1,
childrenbygender2:1,
childrenbygender3:1,
childrenbygender4:1,
childrenbygender5:1,
childrenbygender6:1,
childrenbygender7:1,
childrenbygender8:1,
childrenbygender9:1,
childrenbygender10:1,
childrenbygender11:1,
childrenbygender12:1,
childrenbygender13:1,
childrenbygender14:1,
childrenbygender15:1,
childrenbygender16:1,
childrenbygender17:1,
markettargetage0_5:1,
markettargetage6_11:1,
markettargetage12_17:1,
markettargetage12_17female:1,
markettargetage12_17male:1,
children0_5:1,
children6_11:1,
children0_11:1,
childrenbybirthmonth0:1,
childrenbybirthmonth1:1,
childrenbybirthmonth2:1,
childrenbybirthmonth3:1,
childrenbybirthmonth4:1,
childrenbybirthmonth5:1,
childrenbybirthmonth6:1,
childrenbybirthmonth7:1,
childrenbybirthmonth8:1,
childrenbybirthmonth9:1,
childrenbybirthmonth10:1,
childrenbybirthmonth11:1,
childrenbybirthmonth12:1,
childrenbybirthmonth13:1,
childrenbybirthmonth14:1,
childrenbybirthmonth15:1,
childrenbybirthmonth16:1,
childrenbybirthmonth17:1,
filler_94:3,
purchasing_power_income:1,
filler_95:19,
potential_investor_consumer:2,
filler_96:24, 
sub_family_indicator:3, 
filler_97:40,
filler_98:40,
number_of_contributions:3,
aircraft_mfg_year:4,
cbsa_code:5,
filler_99:30,
filler_100:2,
filler_101:22,
heavy_internet_user:2,
early_technology_adopter:2,
csa_code:3,
csa_code_description:50,
cbsa_level:1,
mail_confidence:2,
boat_hull_type:1,
boat_length:2,
flatitude:10,
filler_102:1,
flongitude:10,
vendor_matchcode:1,
do_not_call_flag:1,
has_primary_telephone:1,
addressline1:30,
digitalmatch:1,
age:2,
mortgagedate:8,
loanamount:4,
lhi_african_american_ethnic_products:1,
lhi_american_history:1,
lhi_general_apparel:1,
lhi_automotive:1,
lhi_baseball:1,
lhi_basketball:1,
lhi_bible:1,
lhi_birds:1,
lhi_publish_books:1,
lhi_business_items:1,
lhi_cats:1,
lhi_collectibles:1,
lhi_college:1,
lhi_computers:1,
lhi_cooking:1,
lhi_general_crafts:1,
lhi_current_events:1,
lhi_do_it_yourselfer:1,
lhi_charitable_donor:1,
lhi_personalized_products:1,
lhi_horses:1,
lhi_culture_arts:1,
lhi_dogs:1,
lhi_children_family:1,
lhi_fishing:1,
lhi_fitness_exercise:1,
lhi_football:1,
lhi_gardening:1,
lhi_gambling:1,
lhi_gift_giver:1,
lhi_golf:1,
lhi_general_health:1,
lhi_hispanic_ethnic_products:1,
lhi_hobbies:1,
lhi_hockey:1,
lhi_home_decorating:1,
lhi_humor:1,
lhi_hunting:1,
lhi_inspirational:1,
lhi_apparel_kids:1,
lhi_apparel_mens:1,
lhi_money_making:1,
lhi_motorcycles:1,
lhi_music:1,
lhi_ocean:1,
lhi_outdoors:1,
lhi_pets:1,
lhi_personal_finance:1,
lhi_rural_farming:1,
lhi_general_sports:1,
lhi_stationery:1,
lhi_general_travel:1,
lhi_movies:1,
lhi_wildlife:1,
lhi_apparel_womens:1,
lhi_unused_01:1,
lhi_photography:1,
lhi_seniors:1,
lhi_aviation:1,
lhi_electionics:1,
lhi_internet:1,
lhi_credit_card_user:1,
lhi_beauty_cosmetic:1,
lhi_asian_ethnic_products:1,
lhi_dieting:1,
lhi_science:1,
lhi_sweepstakes:1,
lhi_tobacco:1,
lhi_bargain_seekers:1,
lhi_publications:1,
lhi_catalogs:1,
lhi_hightech:1,
lhi_apparel_accessories:1,
lhi_apparel_mens_fashions:1,
lhi_apparel_womens_fashions:1,
lhi_auto_racing:1,
lhi_trucks:1,
lhi_home_office_products:1,
lhi_politically_conservative:1,
lhi_crocheting:1,
lhi_knitting:1,
lhi_needlepoint:1,
lhi_quilting:1,
lhi_sewing:1,
lhi_fiction:1,
lhi_games:1,
lhi_unused_03:1,
lhi_gourmet:1,
lhi_history:1,
lhi_internet_access:1,
lhi_internet_buying:1,
lhi_liberal:1,
lhi_books_nonfiction:1,
lhi_boating_sailing:1,
lhi_camping_hiking:1,
lhi_hunting_fishing:1,
lhi_photo_processing:1,
lhi_magazine_subscriber:1,
lhi_books_science_fiction:1,
lhi_skiing:1,
lhi_soccer:1,
lhi_tennis:1,
lhi_travel_cruises:1,
lhi_travel_rv:1,
lhi_travel_us:1,
tw_greenmodel:1,
tw_highvaluesecurityinvestor:1,
tw_highereducation:1,
tw_highvaluestockinvestor:1,
tw_heavyinvestmenttraders:1,
tw_physicalfitnessclubs:1,
tw_highriskinvestor:1,
tw_lowriskinvestor:1,
tw_frequentbusinesstraveler:1,
tw_frequentflyer:1,
tw_annuities:1,
tw_lifeinsurance:1,
tw_luxurycarbuyer:1,
tw_cruise:1,
tw_timeshareowner:1,
tw_impulsebuyer:1,
tw_luxuryhotel:1,
tw_pbsdonor:1,
tw_safetysecurityconscious:1,
tw_shopaholics:1,
tw_avidthemeparkvisitor:1,
tw_foreigntravelvacation:1,
tw_conservative:1,
tw_leaningconservative:1,
tw_leaningliberal:1,
tw_religiousdonor:1,
tw_gardenmaintenance:1,
tw_heavyfamilyrestaurantvisitor:1,
tw_heavypayperviewsports:1,
tw_nascar:1,
tw_liberal:1,
tw_dogproducts:1,
tw_heavypayperviewmovies:1,
tw_dietproducts:1,
tw_organicfoods:1,
tw_heavyvitaminanddietarysupplement:1,
tw_babyproducts:1,
tw_heavycouponuser:1,
tw_opinionleaders:1,
tw_catproducts:1,
tw_creditcardrewards:1,
tw_highendsportingequipment:1,
tw_homeimprovement:1,
tw_highendelectronicsbuyer:1,
tw_avidgamer:1,
tw_rentalcar:1,
tw_avidcellphoneuser:1,
tw_countryclubmember:1,
tw_professionaltaxpreparation:1,
tw_onsitetaxpreparationservice:1,
tw_classicalmusicconcerts:1,
tw_rockmusicconcerts:1,
tw_countrymusicconcerts:1,
tw_livetheater:1,
tw_heavybookbuyer:1,
tw_freshwaterfishing:1,
tw_saltwaterfishing:1,
tw_hunting:1,
tw_allterrainvehicle:1,
tw_powerboating:1,
tw_outdooractivities:1,
tw_adventureseekers:1,
tw_lowendsportingequipment:1,
tw_professionalbaseballsportsfans:1,
tw_professionalbasketballsportsfans:1,
tw_professionalfootballsportsfans:1,
tw_soccersportsfans:1,
tw_collegebasketballsportsfans:1,
tw_collegefootballsportsfans:1,
tw_golfsportsfans:1,
tw_tennissportsfans:1,
tw_professionalwrestlingsportsfans:1,
tw_internationallongdistance:1,
tw_heavyfrozendinner:1,
tw_cookfromscratch:1,
tw_cookforfun:1,
tw_winelover:1,
tw_onlinepurchasepersonal:1,
tw_onlinepurchasebusiness:1,
tw_onlinetravelplan:1,
tw_blogwriting:1,
tw_voiceoverinternet:1,
tw_wifioutsideofhome:1,
tw_wifiinhome:1,
tw_wholesaleclub:1,
tw_autoclub:1,
tw_diyautomaintenance:1,
tw_onlinegamingactivity:1,
tw_onlineinvestmenttrading:1,
tw_onlinebillpayment:1,
tw_onlinetvdownload:1,
tw_mobileinternetaccess:1,
tw_socialmedianetwork:1,
tw_specialtyorganicfoodstore:1,
tw_homeoffice:1,
tw_businessbanking:1,
tw_moderate_economyhotel:1,
tw_e_reader:1,
tw_cellphoneonlymodel:1,
tw_alternativemedicine:1,
tw_non_religiousdonor:1,
tw_majorhomeremodeling:1,
tw_avidsmartphoneusers:1,
tw_healthinsurance:1,
tw_satellitetv:1,
tw_onlinemusicdownload:1,
tw_adulteducation:1,
tw_pilates_yoga:1,
tw_fastfood:1,
tw_hybridcars:1,
tw_heavy_online_buyer:1,
tw_high_end_appparel:1,
tw_heavy_catalog_buyer:1,
tw_gift_buyers:1,
tw_camping:1,
tw_sports_fanatics:1,
tw_financial_planner:1,
tw_work_health_insurance:1,
tw_comprehensive_auto_insurance:1,
tw_small_business_insurance:1,
tw_heavy_snack_eaters:1,
tw_heavy_domestic_travelers:1,
tw_new_vehicle_buyer:1,
tw_suv_buyer:1,
tw_minivan_buyer:1,
tw_auto_loan:1,
tw_education_loan:1,
tw_real_estate_investment:1,
tw_fantasy_sports:1,
tw_hockey_buyer:1,
tw_auto_lease:1,  
tw_digital_payment:1,
tw_high_mileage_usage:1,
tw_motorcycle_owner:1,
tw_union_member:1,
tw_national_park_visitor:1,
tw_online_streaming_and_devices:1,
tw_road_trip:1,
tw_social_networking_ad_click:1,
tw_satellite_radio:1,
heatingtype_code:1,
income_producing_assets:1,
marketarea:3,
phonenumbertype:1,
revolver_to_transactor:2,
second_property_ind:1,
consumerstabilityrawscore:3,
consumerstabilitypercentile:2,
ailments:15,
mailscore:1,
populationdensity:9, 
filler_103:1,
number_of_surnames:1, 
homeowner:1,
education_model:1,	
education_model_ext:31,
income_producing_assets_ext:23,
titlecode_ext:4,	
boatpropulsioncode_ext:19,
vendorethnicity_ext:39,
vendorreligion_ext:16,
vendorlanguage_ext:45,
vendorethnicgroup_ext:16,
countryoforign_ext:20,
contributionamount:7,
lemsmatchcode:18,
mortgagepresent:3, 
discretionary_income_score:2, 
membercount:1,
sesi:2,
zipfourmatchlevel:1, 
no_stats_indicator:1,
vacant_indicator:1,
timezone:1,
areaprefix:6, 
correctivelensespresence:1,
actualvacation_expense:4,
vacation_expense:1,
vacation_expense_dic:15,
nid:6,
nid_description:50,
markettargetage18_44female:1,
pprofsnl:6,
pwhitcol:6,
pbluecol:6,
pcolgrad:6,
routenumber:3,
boxtype:1,
boxnumber:6,
nameaddresssource:1,
mailorderbuyerflag:1,
creditcard_bank:1,
markettargetage0_11:1,
markettargetage0_17:1,
markettargetage18_24female:1,
markettargetage18_24male:1,
markettargetage18_34female:1,
markettargetage18_34male:1,
markettargetage25_34female:1,
markettargetage25_34male:1,
markettargetage35_44female:1,
markettargetage35_44male:1,
markettargetage35_54female:1,
markettargetage35_54male:1,
markettargetage45_54female:1,
markettargetage45_54male:1,
markettargetage50plus:1,
markettargetage55_64female:1,
markettargetage55_64male:1,
markettargetage65plus:1,
householdsize1_2:1,
householdsize3plus:1,
phonesourcecat:1,
secondaryareacode:3,
secondaryphone:7,
numberoffireplaces:1,
pool:1, 
bedroomcount:2,
bathroomcount:3,
garagetype:1,
homeownersource:1,
salepricesource:1,
saledatesource:1,
roomcount:2,
multipleunit:1,
purchasing_power_indicator:7,
censusstatecode2020:2,
censuscountycode2020:3,
censustract2020:6,
censusblockgroup2020:1,
secondmortgageamount:4,
secondmortgagetypecode:1,
lot_size_sqfeet:9,
universal_sqfeet:5,
universal_sqfeet_ind:1,
living_sqfeet:5,
groundfloor_sqfeet:5,
gross_sqfeet:5,
adjustedgross_sqfeet:5,
basement_sqfeet:5,
garage_sqfeet:5,
fullbath:1,
halfbath:1,
fireplace_ind:1,
fireplacetype:3,
pooltype:3,
roofcover:3,
roofshape:1,
conditiontype:1,
exteriorwalltype:3,
foundationtype:3,
constructionquality:1,
stories:3,
styletype:3,
airconditioningtype:3,
electricwiringtype:3,
fueltype:3,
sewertype:1,
watertype:1,
construction_type_detail:3,
heating_type_detail:3,
filler_105:1';


