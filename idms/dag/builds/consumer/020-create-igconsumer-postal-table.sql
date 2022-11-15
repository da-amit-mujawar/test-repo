DROP TABLE IF EXISTS nosuchtable;
DROP TABLE IF EXISTS {new-load-table};

CREATE TABLE {new-load-table}
(
	id BIGINT IDENTITY PRIMARY KEY DISTKEY SORTKEY ENCODE raw,
	listid  INT DEFAULT 15794,
	permissiontype VARCHAR(1)  DEFAULT 'R',
	zipradius VARCHAR(1) DEFAULT '',
	georadius VARCHAR(1) DEFAULT '',
	firstname VARCHAR(15),
	middleinitial VARCHAR(1),
	lastname VARCHAR(20),
	last_name_suffix VARCHAR(3),
	title_code VARCHAR(1),
	filler_1 VARCHAR(4),
	gender VARCHAR(1),
	filler_2 VARCHAR(7),
	house_number VARCHAR(10),
	street_pre_directional VARCHAR(2),
	street_name VARCHAR(28),
	street_suffix VARCHAR(4),
	street_post_directional VARCHAR(2),
	unit_type VARCHAR(4),
	unit_number VARCHAR(8),
	city VARCHAR(28), 
	state VARCHAR(2),
	census_state_code_2010 VARCHAR(2), 
	county_code VARCHAR(3),
	filler_3 VARCHAR(14),
	filler_4 VARCHAR (4),
	filler_5 VARCHAR (30),
	zip VARCHAR(5),
	zip_four VARCHAR(4),
	carrier_route_code VARCHAR(4),
	delivery_point_bar_code VARCHAR(3),
	area_code VARCHAR(3),
	phonenumber VARCHAR(7),
	nielsen_county_rank VARCHAR(1),
	filler_6 VARCHAR(25),
	time_zone VARCHAR(1),
	age_code VARCHAR(1),
	filler_7 VARCHAR(7),
	birth_date VARCHAR(8),
	income VARCHAR(1),
	filler_8 VARCHAR(19),
	last_contribution_date VARCHAR(8),
	length_of_residence VARCHAR(2),
	residence_type VARCHAR(1),
	filler_9 VARCHAR(5),
	delivery_unit_size_raw VARCHAR(1),
	filler_10 VARCHAR(13), 
	own_rent_indicator VARCHAR(1),
	filler_11 VARCHAR(15), 
	location_type VARCHAR(1),
	filler_12 VARCHAR(6), 
	pool_type VARCHAR(3),
	home_value_code VARCHAR(1),
	filler_13 VARCHAR(19),
	home_sale_date VARCHAR(8),
	home_sale_price_code VARCHAR(1),
	filler_14 VARCHAR(19),
	home_sale_price VARCHAR(12),
	home_equity_estimate VARCHAR(1),
	filler_15 VARCHAR(18),
	finance_type VARCHAR(1),
	filler_16 VARCHAR(28),
	mortgage_loan_type VARCHAR(1), 
	filler_17 VARCHAR(30),
	boat_propulsion_code VARCHAR(1),
	filler_18 VARCHAR(19), 
	filler_19 VARCHAR(6),
	mortgage_amount VARCHAR(1),
	filler_20 VARCHAR(19),
	congressional_district VARCHAR(2),
	credit_card_code VARCHAR(22),
	filler_21 VARCHAR(30), 
	filler_22 VARCHAR(30), 
	filler_23 VARCHAR(30), 
	filler_24 VARCHAR(30), 
	filler_25 VARCHAR(30), 
	filler_26 VARCHAR(30), 
	filler_27 VARCHAR(30), 
	number_trade_lines VARCHAR(1),
	marital_status VARCHAR(1),
	filler_28 VARCHAR(7), 
	filler_29 VARCHAR(6),
	spousal_indicator_raw VARCHAR(3),
	filler_30 VARCHAR(1),
	filler_31 VARCHAR(10),
	filler_32 VARCHAR(1),
	filler_33 VARCHAR(10),
	cat_owner VARCHAR(3),
	dog_owner VARCHAR(3),
	grandparent_in_house VARCHAR(3),
	veteran_flag VARCHAR(3),
	stocks_or_bonds VARCHAR(3),
	dm_high_tech_household VARCHAR(3),
	female_occupation VARCHAR(2),
	male_occupation VARCHAR(2),
	census_average_years_attended_school_code VARCHAR(1),
	filler_34 VARCHAR(32),
	census_phhblack_code VARCHAR(1),
	census_of_population_graduated_college VARCHAR(1),
	census_phhspnsh_code VARCHAR(1),
	census_percent_of_population_married VARCHAR(1),
	census_precent_of_population_single_parent VARCHAR(1),
	filler_35 VARCHAR(2),
	filler_36 VARCHAR(1),
	filler_37 VARCHAR(4),
	filler_38 VARCHAR(1),
	filler_39 VARCHAR(3),
	filler_40 VARCHAR(3),
	filler_41 VARCHAR(3),
	expendable_income_rank VARCHAR(1),
	filler_42 VARCHAR(15),
	loan_to_value_ratio_ltv VARCHAR(1),
	filler_43 VARCHAR(11), 
	filler_44 VARCHAR(1),
	filler_45 VARCHAR(15),
	donor_code VARCHAR(5), 
	filler_46 VARCHAR(2),
	filler_47 VARCHAR(2),
	filler_48 VARCHAR(36),
	mail_responsive_current VARCHAR(1),
	mail_responsive_recent VARCHAR(1),
	mail_responsive_ever VARCHAR(1),
	filler_49 VARCHAR(20),
	filler_50 VARCHAR(45),
	filler_51 VARCHAR(30),
	filler_52 VARCHAR(6),
	filler_53 VARCHAR(13),
	filler_54 VARCHAR(1),
	filler_55 VARCHAR(40),
	census_tract_2010 VARCHAR(6),
	census_block_group_2010 VARCHAR(1),
	filler_56 VARCHAR(10),
	filler_57 VARCHAR(11),
	filler_58 VARCHAR(1),
	filler_59 VARCHAR(3),
	address_type VARCHAR(1),
	filler_60 VARCHAR(4),
	hoh_head_of_household VARCHAR(3),
	first_name_hoh VARCHAR(15),
	middle_initial_hoh VARCHAR(1),
	last_name_hoh VARCHAR(20),
	last_name_suffix_hoh VARCHAR(3),
	title_code_hoh VARCHAR(1),
	filler_61 VARCHAR(4),
	gender_hoh VARCHAR(1),
	filler_62 VARCHAR(7),
	birth_date_hoh VARCHAR(8),
	individual_id_hoh BIGINT, 
	hoh_age_code VARCHAR(1),
	filler_63 VARCHAR(7),
	number_trade_lines_hoh VARCHAR(1),
	credit_card_code_hoh VARCHAR(10),
	filler_64 VARCHAR(30),
	filler_65 VARCHAR(30),
	filler_66 VARCHAR(30),
	filler_67 VARCHAR(30),
	filler_68 VARCHAR(30),
	filler_69 VARCHAR(30),
	filler_70 VARCHAR(30),
	filler_71 VARCHAR(3),
	marital_status_hoh VARCHAR(1),
	filler_72 VARCHAR(7), 
	filler_73 VARCHAR(6),
	filler_74 VARCHAR(2),
	filler_75 VARCHAR(1),
	filler_76 VARCHAR(10),
	company_id VARCHAR(20),
	individual_id VARCHAR(21),
	location_id VARCHAR(12),
	filler_77 VARCHAR(9),
	filler_78 VARCHAR(9),
	filler_79 VARCHAR(9),
	filler_80 VARCHAR(9),
	filler_81 VARCHAR(9),
	filler_82 VARCHAR(7),
	filler_83 VARCHAR(10),
	usps_delivery_service_type_code VARCHAR(1),
	nielsen_county_region VARCHAR(1),
	filler_84 VARCHAR(20),
	lot_size VARCHAR(5),
	home_size VARCHAR(5),
	home_age VARCHAR(3),
	actual_age_flag VARCHAR(1),
	political_contribution_amount VARCHAR(24),
	filler_85 VARCHAR(1),
	filler_86 VARCHAR(24),
	pilot_license_code VARCHAR(1),
	mortgage_interest_rates_old VARCHAR(4),
	last_party_contributed_to VARCHAR(1),
	filler_87 VARCHAR(24),
	aircraft_type_code VARCHAR(1),
	filler_88 VARCHAR(24),
	boat_owner_indicator VARCHAR(3),
	registered_voter VARCHAR(3),
	political_party_affiliation VARCHAR(1),
	filler_89 VARCHAR(19), 
	number_of_adults_in_household VARCHAR(1),
	household_member_count VARCHAR(1),
	filler_90 VARCHAR(1),
	medag18p VARCHAR(4),
	census_percent_of_population_with_3_households VARCHAR(1),
	census_phhasian_code VARCHAR(1),
	medhhd1 VARCHAR(9),
	wealth_finder VARCHAR(1),
	filler_91 VARCHAR(24),
	numberofchildren VARCHAR(1),
	alternatehhind VARCHAR(1),
	countryoforign VARCHAR(2),
	donotmailflag VARCHAR(1),
	emailpresenceflag VARCHAR(1),
	routetype VARCHAR(1),
	householdarrivaldate_year VARCHAR(4),
	householdarrivaldate VARCHAR(6),
	homeagesource VARCHAR(1),
	homebuiltyear VARCHAR(4),
	mortgagemonth VARCHAR(2),
	homevaluesource VARCHAR(1),
	actualincome VARCHAR(7),
	infopersona_cluster VARCHAR(2),
	filler_92 VARCHAR(36), 
	infopersona_supercluster VARCHAR(1),
	filler_93 VARCHAR(21),
	lifestyle VARCHAR(35),
	listtype VARCHAR(2),
	marriedscore VARCHAR(1),
	medhhd2 VARCHAR(6),
	mortgageloantype VARCHAR(1),
	mortgageinterestsource VARCHAR(1),
	occupancycount VARCHAR(2),
	agedatasource VARCHAR(1),
	presenceofchildren VARCHAR(1),
	recencydate VARCHAR(6),
	refreshdate VARCHAR(6),
	scf VARCHAR(3),
	vendorethnicity VARCHAR(2),
	vendorethnicgroup VARCHAR(1),
	vendorlanguage VARCHAR(2),
	vendorreligion VARCHAR(1),
	censuscountycode2010 VARCHAR(3),
	fulfillmentflag VARCHAR(1),
	childrenbygender0 VARCHAR(1),
	childrenbygender1 VARCHAR(1),
	childrenbygender2 VARCHAR(1),
	childrenbygender3 VARCHAR(1),
	childrenbygender4 VARCHAR(1),
	childrenbygender5 VARCHAR(1),
	childrenbygender6 VARCHAR(1),
	childrenbygender7 VARCHAR(1),
	childrenbygender8 VARCHAR(1),
	childrenbygender9 VARCHAR(1),
	childrenbygender10 VARCHAR(1),
	childrenbygender11 VARCHAR(1),
	childrenbygender12 VARCHAR(1),
	childrenbygender13 VARCHAR(1),
	childrenbygender14 VARCHAR(1),
	childrenbygender15 VARCHAR(1),
	childrenbygender16 VARCHAR(1),
	childrenbygender17 VARCHAR(1),
	markettargetage0_5 VARCHAR(1),
	markettargetage6_11 VARCHAR(1),
	markettargetage12_17 VARCHAR(1),
	markettargetage12_17female VARCHAR(1),
	markettargetage12_17male VARCHAR(1),
	children0_5 VARCHAR(1),
	children6_11 VARCHAR(1),
	children0_11 VARCHAR(1),
	childrenbybirthmonth0 VARCHAR(1),
	childrenbybirthmonth1 VARCHAR(1),
	childrenbybirthmonth2 VARCHAR(1),
	childrenbybirthmonth3 VARCHAR(1),
	childrenbybirthmonth4 VARCHAR(1),
	childrenbybirthmonth5 VARCHAR(1),
	childrenbybirthmonth6 VARCHAR(1),
	childrenbybirthmonth7 VARCHAR(1),
	childrenbybirthmonth8 VARCHAR(1),
	childrenbybirthmonth9 VARCHAR(1),
	childrenbybirthmonth10 VARCHAR(1),
	childrenbybirthmonth11 VARCHAR(1),
	childrenbybirthmonth12 VARCHAR(1),
	childrenbybirthmonth13 VARCHAR(1),
	childrenbybirthmonth14 VARCHAR(1),
	childrenbybirthmonth15 VARCHAR(1),
	childrenbybirthmonth16 VARCHAR(1),
	childrenbybirthmonth17 VARCHAR(1),
	filler_94 VARCHAR(3),
	purchasing_power_income VARCHAR(1),
	filler_95 VARCHAR(19),
	potential_investor_consumer VARCHAR(2),
	filler_96 VARCHAR(24), 
	sub_family_indicator VARCHAR(3), 
	filler_97 VARCHAR(40),
	filler_98 VARCHAR(40),
	number_of_contributions VARCHAR(3),
	aircraft_mfg_year VARCHAR(4),
	cbsa_code VARCHAR(5),
	filler_99 VARCHAR(30),
	filler_100 VARCHAR(2),
	filler_101 VARCHAR(22),
	heavy_internet_user VARCHAR(2),
	early_technology_adopter VARCHAR(2),
	csa_code VARCHAR(3),
	csa_code_description VARCHAR(50),
	cbsa_level VARCHAR(1),
	mail_confidence VARCHAR(2),
	boat_hull_type VARCHAR(1),
	boat_length VARCHAR(2),
	flatitude FLOAT,
	filler_102 VARCHAR(1),
	flongitude FLOAT,
	vendor_matchcode VARCHAR(1),
	do_not_call_flag VARCHAR(1),
	has_primary_telephone VARCHAR(1),
	addressline1 VARCHAR(40),
	digitalmatch VARCHAR(1),
	age VARCHAR(2),
	mortgagedate VARCHAR(8),
	loanamount VARCHAR(4),
	lhi_african_american_ethnic_products VARCHAR(1),
	lhi_american_history VARCHAR(1),
	lhi_general_apparel VARCHAR(1),
	lhi_automotive VARCHAR(1),
	lhi_baseball VARCHAR(1),
	lhi_basketball VARCHAR(1),
	lhi_bible VARCHAR(1),
	lhi_birds VARCHAR(1),
	lhi_publish_books VARCHAR(1),
	lhi_business_items VARCHAR(1),
	lhi_cats VARCHAR(1),
	lhi_collectibles VARCHAR(1),
	lhi_college VARCHAR(1),
	lhi_computers VARCHAR(1),
	lhi_cooking VARCHAR(1),
	lhi_general_crafts VARCHAR(1),
	lhi_current_events VARCHAR(1),
	lhi_do_it_yourselfer VARCHAR(1),
	lhi_charitable_donor VARCHAR(1),
	lhi_personalized_products VARCHAR(1),
	lhi_horses VARCHAR(1),
	lhi_culture_arts VARCHAR(1),
	lhi_dogs VARCHAR(1),
	lhi_children_family VARCHAR(1),
	lhi_fishing VARCHAR(1),
	lhi_fitness_exercise VARCHAR(1),
	lhi_football VARCHAR(1),
	lhi_gardening VARCHAR(1),
	lhi_gambling VARCHAR(1),
	lhi_gift_giver VARCHAR(1),
	lhi_golf VARCHAR(1),
	lhi_general_health VARCHAR(1),
	lhi_hispanic_ethnic_products VARCHAR(1),
	lhi_hobbies VARCHAR(1),
	lhi_hockey VARCHAR(1),
	lhi_home_decorating VARCHAR(1),
	lhi_humor VARCHAR(1),
	lhi_hunting VARCHAR(1),
	lhi_inspirational VARCHAR(1),
	lhi_apparel_kids VARCHAR(1),
	lhi_apparel_mens VARCHAR(1),
	lhi_money_making VARCHAR(1),
	lhi_motorcycles VARCHAR(1),
	lhi_music VARCHAR(1),
	lhi_ocean VARCHAR(1),
	lhi_outdoors VARCHAR(1),
	lhi_pets VARCHAR(1),
	lhi_personal_finance VARCHAR(1),
	lhi_rural_farming VARCHAR(1),
	lhi_general_sports VARCHAR(1),
	lhi_stationery VARCHAR(1),
	lhi_general_travel VARCHAR(1),
	lhi_movies VARCHAR(1),
	lhi_wildlife VARCHAR(1),
	lhi_apparel_womens VARCHAR(1),
	lhi_unused_01 VARCHAR(1),
	lhi_photography VARCHAR(1),
	lhi_seniors VARCHAR(1),
	lhi_aviation VARCHAR(1),
	lhi_electionics VARCHAR(1),
	lhi_internet VARCHAR(1),
	lhi_credit_card_user VARCHAR(1),
	lhi_beauty_cosmetic VARCHAR(1),
	lhi_asian_ethnic_products VARCHAR(1),
	lhi_dieting VARCHAR(1),
	lhi_science VARCHAR(1),
	lhi_sweepstakes VARCHAR(1),
	lhi_tobacco VARCHAR(1),
	lhi_bargain_seekers VARCHAR(1),
	lhi_publications VARCHAR(1),
	lhi_catalogs VARCHAR(1),
	lhi_hightech VARCHAR(1),
	lhi_apparel_accessories VARCHAR(1),
	lhi_apparel_mens_fashions VARCHAR(1),
	lhi_apparel_womens_fashions VARCHAR(1),
	lhi_auto_racing VARCHAR(1),
	lhi_trucks VARCHAR(1),
	lhi_home_office_products VARCHAR(1),
	lhi_politically_conservative VARCHAR(1),
	lhi_crocheting VARCHAR(1),
	lhi_knitting VARCHAR(1),
	lhi_needlepoint VARCHAR(1),
	lhi_quilting VARCHAR(1),
	lhi_sewing VARCHAR(1),
	lhi_fiction VARCHAR(1),
	lhi_games VARCHAR(1),
	lhi_unused_03 VARCHAR(1),
	lhi_gourmet VARCHAR(1),
	lhi_history VARCHAR(1),
	lhi_internet_access VARCHAR(1),
	lhi_internet_buying VARCHAR(1),
	lhi_liberal VARCHAR(1),
	lhi_books_nonfiction VARCHAR(1),
	lhi_boating_sailing VARCHAR(1),
	lhi_camping_hiking VARCHAR(1),
	lhi_hunting_fishing VARCHAR(1),
	lhi_photo_processing VARCHAR(1),
	lhi_magazine_subscriber VARCHAR(1),
	lhi_books_science_fiction VARCHAR(1),
	lhi_skiing VARCHAR(1),
	lhi_soccer VARCHAR(1),
	lhi_tennis VARCHAR(1),
	lhi_travel_cruises VARCHAR(1),
	lhi_travel_rv VARCHAR(1),
	lhi_travel_us VARCHAR(1),
	tw_greenmodel VARCHAR(1),
	tw_highvaluesecurityinvestor VARCHAR(1),
	tw_highereducation VARCHAR(1),
	tw_highvaluestockinvestor VARCHAR(1),
	tw_heavyinvestmenttraders VARCHAR(1),
	tw_physicalfitnessclubs VARCHAR(1),
	tw_highriskinvestor VARCHAR(1),
	tw_lowriskinvestor VARCHAR(1),
	tw_frequentbusinesstraveler VARCHAR(1),
	tw_frequentflyer VARCHAR(1),
	tw_annuities VARCHAR(1),
	tw_lifeinsurance VARCHAR(1),
	tw_luxurycarbuyer VARCHAR(1),
	tw_cruise VARCHAR(1),
	tw_timeshareowner VARCHAR(1),
	tw_impulsebuyer VARCHAR(1),
	tw_luxuryhotel VARCHAR(1),
	tw_pbsdonor VARCHAR(1),
	tw_safetysecurityconscious VARCHAR(1),
	tw_shopaholics VARCHAR(1),
	tw_avidthemeparkvisitor VARCHAR(1),
	tw_foreigntravelvacation VARCHAR(1),
	tw_conservative VARCHAR(1),
	tw_leaningconservative VARCHAR(1),
	tw_leaningliberal VARCHAR(1),
	tw_religiousdonor VARCHAR(1),
	tw_gardenmaintenance VARCHAR(1),
	tw_heavyfamilyrestaurantvisitor VARCHAR(1),
	tw_heavypayperviewsports VARCHAR(1),
	tw_nascar VARCHAR(1),
	tw_liberal VARCHAR(1),
	tw_dogproducts VARCHAR(1),
	tw_heavypayperviewmovies VARCHAR(1),
	tw_dietproducts VARCHAR(1),
	tw_organicfoods VARCHAR(1),
	tw_heavyvitaminanddietarysupplement VARCHAR(1),
	tw_babyproducts VARCHAR(1),
	tw_heavycouponuser VARCHAR(1),
	tw_opinionleaders VARCHAR(1),
	tw_catproducts VARCHAR(1),
	tw_creditcardrewards VARCHAR(1),
	tw_highendsportingequipment VARCHAR(1),
	tw_homeimprovement VARCHAR(1),
	tw_highendelectronicsbuyer VARCHAR(1),
	tw_avidgamer VARCHAR(1),
	tw_rentalcar VARCHAR(1),
	tw_avidcellphoneuser VARCHAR(1),
	tw_countryclubmember VARCHAR(1),
	tw_professionaltaxpreparation VARCHAR(1),
	tw_onsitetaxpreparationservice VARCHAR(1),
	tw_classicalmusicconcerts VARCHAR(1),
	tw_rockmusicconcerts VARCHAR(1),
	tw_countrymusicconcerts VARCHAR(1),
	tw_livetheater VARCHAR(1),
	tw_heavybookbuyer VARCHAR(1),
	tw_freshwaterfishing VARCHAR(1),
	tw_saltwaterfishing VARCHAR(1),
	tw_hunting VARCHAR(1),
	tw_allterrainvehicle VARCHAR(1),
	tw_powerboating VARCHAR(1),
	tw_outdooractivities VARCHAR(1),
	tw_adventureseekers VARCHAR(1),
	tw_lowendsportingequipment VARCHAR(1),
	tw_professionalbaseballsportsfans VARCHAR(1),
	tw_professionalbasketballsportsfans VARCHAR(1),
	tw_professionalfootballsportsfans VARCHAR(1),
	tw_soccersportsfans VARCHAR(1),
	tw_collegebasketballsportsfans VARCHAR(1),
	tw_collegefootballsportsfans VARCHAR(1),
	tw_golfsportsfans VARCHAR(1),
	tw_tennissportsfans VARCHAR(1),
	tw_professionalwrestlingsportsfans VARCHAR(1),
	tw_internationallongdistance VARCHAR(1),
	tw_heavyfrozendinner VARCHAR(1),
	tw_cookfromscratch VARCHAR(1),
	tw_cookforfun VARCHAR(1),
	tw_winelover VARCHAR(1),
	tw_onlinepurchasepersonal VARCHAR(1),
	tw_onlinepurchasebusiness VARCHAR(1),
	tw_onlinetravelplan VARCHAR(1),
	tw_blogwriting VARCHAR(1),
	tw_voiceoverinternet VARCHAR(1),
	tw_wifioutsideofhome VARCHAR(1),
	tw_wifiinhome VARCHAR(1),
	tw_wholesaleclub VARCHAR(1),
	tw_autoclub VARCHAR(1),
	tw_diyautomaintenance VARCHAR(1),
	tw_onlinegamingactivity VARCHAR(1),
	tw_onlineinvestmenttrading VARCHAR(1),
	tw_onlinebillpayment VARCHAR(1),
	tw_onlinetvdownload VARCHAR(1),
	tw_mobileinternetaccess VARCHAR(1),
	tw_socialmedianetwork VARCHAR(1),
	tw_specialtyorganicfoodstore VARCHAR(1),
	tw_homeoffice VARCHAR(1),
	tw_businessbanking VARCHAR(1),
	tw_moderate_economyhotel VARCHAR(1),
	tw_e_reader VARCHAR(1),
	tw_cellphoneonlymodel VARCHAR(1),
	tw_alternativemedicine VARCHAR(1),
	tw_non_religiousdonor VARCHAR(1),
	tw_majorhomeremodeling VARCHAR(1),
	tw_avidsmartphoneusers VARCHAR(1),
	tw_healthinsurance VARCHAR(1),
	tw_satellitetv VARCHAR(1),
	tw_onlinemusicdownload VARCHAR(1),
	tw_adulteducation VARCHAR(1),
	tw_pilates_yoga VARCHAR(1),
	tw_fastfood VARCHAR(1),
	tw_hybridcars VARCHAR(1),
	tw_heavy_online_buyer VARCHAR(1),
	tw_high_end_appparel VARCHAR(1),
	tw_heavy_catalog_buyer VARCHAR(1),
	tw_gift_buyers VARCHAR(1),
	tw_camping VARCHAR(1),
	tw_sports_fanatics VARCHAR(1),
	tw_financial_planner VARCHAR(1),
	tw_work_health_insurance VARCHAR(1),
	tw_comprehensive_auto_insurance VARCHAR(1),
	tw_small_business_insurance VARCHAR(1),
	tw_heavy_snack_eaters VARCHAR(1),
	tw_heavy_domestic_travelers VARCHAR(1),
	tw_new_vehicle_buyer VARCHAR(1),
	tw_suv_buyer VARCHAR(1),
	tw_minivan_buyer VARCHAR(1),
	tw_auto_loan VARCHAR(1),
	tw_education_loan VARCHAR(1),
	tw_real_estate_investment VARCHAR(1),
	tw_fantasy_sports VARCHAR(1),
	tw_hockey_buyer VARCHAR(1),
	tw_auto_lease  VARCHAR(1), 
	tw_digital_payment  VARCHAR(1),
	tw_high_mileage_usage  VARCHAR(1),
	tw_motorcycle_owner  VARCHAR(1),
	tw_union_member  VARCHAR(1),
	tw_national_park_visitor  VARCHAR(1),
	tw_online_streaming_and_devices  VARCHAR(1),
	tw_road_trip  VARCHAR(1),
	tw_social_networking_ad_click  VARCHAR(1),
	tw_satellite_radio  VARCHAR(1),
	heatingtype_code VARCHAR(1),
	income_producing_assets VARCHAR(1),
	marketarea VARCHAR(3),
	phonenumbertype VARCHAR(1),
	revolver_to_transactor VARCHAR(2),
	second_property_ind VARCHAR(1),
	consumerstabilityrawscore VARCHAR(3),
	consumerstabilitypercentile VARCHAR(2),
	ailments VARCHAR(15),
	mailscore VARCHAR(1), 
	populationdensity VARCHAR(9), 
	filler_103 VARCHAR(1),
	number_of_surnames VARCHAR(1), 
	homeowner VARCHAR(1), 
	education_model VARCHAR(1),	
	education_model_ext VARCHAR(31),
	income_producing_assets_ext VARCHAR(23),
	titlecode_ext VARCHAR(4),	
	boatpropulsioncode_ext VARCHAR(19),
	vendorethnicity_ext VARCHAR(39),
	vendorreligion_ext VARCHAR(16),
	vendorlanguage_ext VARCHAR(45),
	vendorethnicgroup_ext VARCHAR(16),
	countryoforign_ext VARCHAR(20),
	contributionamount VARCHAR(7),
	lemsmatchcode  VARCHAR(18),
	mortgagepresent  VARCHAR(3), 
	discretionary_income_score VARCHAR(2), 
	membercount VARCHAR(1),
	sesi VARCHAR(2),
	zipfourmatchlevel VARCHAR(1), 
	no_stats_indicator VARCHAR(1),
	vacant_indicator VARCHAR(1),
	timezone VARCHAR(1),
	areaprefix VARCHAR(6), 
	correctivelensespresence VARCHAR(1),
	actualvacation_expense VARCHAR(4),
	vacation_expense VARCHAR(1),
	vacation_expense_dic VARCHAR(15),
	nid VARCHAR(6),
	nid_description VARCHAR(50) DEFAULT '', 
	gui_probable VARCHAR(1) DEFAULT 'N',
	gui_probable_mightbe VARCHAR(1) DEFAULT 'N',
	gui_accurate VARCHAR(1) DEFAULT 'N',
	telemarketingflag VARCHAR(1) DEFAULT 'N', 
	phoneincludeallflag VARCHAR(1) DEFAULT 'N', 
	segmentcode_a VARCHAR(1) DEFAULT '',
	segmentcode_e VARCHAR(1) DEFAULT '',
	segmentcode_f VARCHAR(1) DEFAULT '',
	segmentcode_g VARCHAR(1) DEFAULT '',
	segmentcode_h VARCHAR(1) DEFAULT '',
	segmentcode_i VARCHAR(1) DEFAULT '',
	segmentcode_j VARCHAR(1) DEFAULT '',
	opt_out_flag VARCHAR(1)  DEFAULT '',
	emailaddress VARCHAR (80) DEFAULT '',
	vendor_code	VARCHAR (2)  DEFAULT '',
	categories VARCHAR(100)  DEFAULT '',
	domain VARCHAR (80)  DEFAULT '',
	top_level_domain VARCHAR (6)  DEFAULT '',
	opt_out_date VARCHAR (8)  DEFAULT '',
	car_make VARCHAR (15)  DEFAULT '',
	car_model VARCHAR (30)  DEFAULT '',
	car_year VARCHAR (4)  DEFAULT '',
	date_entered_yyyymm VARCHAR (6)  DEFAULT '',
	email_domain VARCHAR (20)  DEFAULT '',
	open_flag VARCHAR(1)  DEFAULT '',
	open_date_yyyymm VARCHAR(6)  DEFAULT '',
	click_flag VARCHAR(1),
	click_date_yyyymm VARCHAR(6)  DEFAULT '',
	md5_email_lower VARCHAR (32)  DEFAULT '',
	md5_email_upper VARCHAR (32)  DEFAULT '',
	ipaddress VARCHAR(35) DEFAULT '',
	url VARCHAR(60) DEFAULT '',
	sha256_email VARCHAR(64) DEFAULT '',
	sha512_email VARCHAR(128) DEFAULT '',
	bv_flag VARCHAR(1)  DEFAULT '',
	marigold VARCHAR(1)  DEFAULT '',
	bvt_refresh_date VARCHAR (8)  DEFAULT '',
	ipst_status_code VARCHAR(1)  DEFAULT '',
	ipst_refresh_date VARCHAR (8)  DEFAULT '',
	bestdate VARCHAR (8),
	reactivation_flag VARCHAR(1)  DEFAULT '',
	mgen_match_flag VARCHAR(1)  DEFAULT '',
	bridge_code VARCHAR(1)  DEFAULT '',
	best_date_range VARCHAR(1)  DEFAULT '',
	haspostal VARCHAR(1) DEFAULT 'Y',
	infogroupcontact VARCHAR(1) DEFAULT 'Y',
	matchcode_lrfs VARCHAR(21) DEFAULT '', 
	cinclude VARCHAR(1) Default 'N',
	matchlevel VARCHAR (1) Default 'I',
	individual_id_bak BigInt DEFAULT 0, 
	company_id_bak BigInt DEFAULT 0, 
	email_deliverable VARCHAR (1),
	email_marketable VARCHAR (1),
	email_reputation_risk VARCHAR (1),
	markettargetage18_44female  VARCHAR(1),
	pprofsnl VARCHAR(6),
	pwhitcol VARCHAR(6),
	pbluecol VARCHAR(6),
	pcolgrad  VARCHAR(6),
	routenumber  VARCHAR(3),
	boxtype VARCHAR(1),
	boxnumber  VARCHAR(6),
	nameaddresssource  VARCHAR(1),
	mailorderbuyerflag  VARCHAR(1),
	creditcard_bank  VARCHAR(1),
	markettargetage0_11  VARCHAR(1),
	markettargetage0_17  VARCHAR(1),
	markettargetage18_24female  VARCHAR(1),
	markettargetage18_24male  VARCHAR(1),
	markettargetage18_34female  VARCHAR(1),
	markettargetage18_34male  VARCHAR(1),
	markettargetage25_34female  VARCHAR(1),
	markettargetage25_34male  VARCHAR(1),
	markettargetage35_44female VARCHAR(1),
	markettargetage35_44male  VARCHAR(1),
	markettargetage35_54female VARCHAR(1),
	markettargetage35_54male  VARCHAR(1),
	markettargetage45_54female VARCHAR(1),
	markettargetage45_54male  VARCHAR(1),
	markettargetage50plus VARCHAR(1),
	markettargetage55_64female VARCHAR(1),
	markettargetage55_64male VARCHAR(1),
	markettargetage65plus VARCHAR(1),
	householdsize1_2 VARCHAR(1),
	householdsize3plus VARCHAR(1),
	phonesourcecat VARCHAR(1),
	secondaryareacode VARCHAR(3),
	secondaryphone VARCHAR(7),
	numberoffireplaces  VARCHAR(1),
	pool  VARCHAR(1), 
	bedroomcount  VARCHAR(2),
	bathroomcount  VARCHAR(3),
	garagetype VARCHAR(1),
	homeownersource  VARCHAR(1),
	salepricesource  VARCHAR(1),
	saledatesource  VARCHAR(1),
	roomcount  VARCHAR(2),
	multipleunit  VARCHAR(1),
	purchasing_power_indicator  VARCHAR(7),
	filler_104 VARCHAR(1),
	phone_number VARCHAR(10) DEFAULT '',
	fips_code VARCHAR(5) DEFAULT '',
	lifestyle01 VARCHAR(1)  DEFAULT '',
	lifestyle02 VARCHAR(1)  DEFAULT '',
	lifestyle03 VARCHAR(1)  DEFAULT '',
	lifestyle04 VARCHAR(1)  DEFAULT '',
	lifestyle05 VARCHAR(1)  DEFAULT '',
	lifestyle06 VARCHAR(1)  DEFAULT '',
	lifestyle07 VARCHAR(1)  DEFAULT '',
	lifestyle08 VARCHAR(1)  DEFAULT '',
	lifestyle09 VARCHAR(1)  DEFAULT '',
	lifestyle10 VARCHAR(1)  DEFAULT '',
	lifestyle11 VARCHAR(1)  DEFAULT '',
	lifestyle12 VARCHAR(1)  DEFAULT '',
	lifestyle13 VARCHAR(1)  DEFAULT '',
	lifestyle14 VARCHAR(1)  DEFAULT '',
	lifestyle15 VARCHAR(1)  DEFAULT '',
	lifestyle16 VARCHAR(1)  DEFAULT '',
	lifestyle17 VARCHAR(1)  DEFAULT '',
	lifestyle18 VARCHAR(1)  DEFAULT '',
	lifestyle19 VARCHAR(1)  DEFAULT '',
	lifestyle20 VARCHAR(1)  DEFAULT '',
	lifestyle21 VARCHAR(1)  DEFAULT '',
	lifestyle22 VARCHAR(1)  DEFAULT '',
	lifestyle23 VARCHAR(1)  DEFAULT '',
	lifestyle24 VARCHAR(1)  DEFAULT '',
	lifestyle25 VARCHAR(1)  DEFAULT '',
	lifestyle26 VARCHAR(1)  DEFAULT '',
	lifestyle27 VARCHAR(1)  DEFAULT '',
	lifestyle28 VARCHAR(1)  DEFAULT '',
	lifestyle29 VARCHAR(1)  DEFAULT '',
	lifestyle30 VARCHAR(1)  DEFAULT '',
	lifestyle31 VARCHAR(1)  DEFAULT '',
	lifestyle32 VARCHAR(1)  DEFAULT '',
	lifestyle33 VARCHAR(1)  DEFAULT '',
	lifestyle34 VARCHAR(1)  DEFAULT '',
	lifestyle35 VARCHAR(1)  DEFAULT '',
	ailments_1 VARCHAR(1) DEFAULT '',
	ailments_2 VARCHAR(1) DEFAULT '',
	ailments_3 VARCHAR(1) DEFAULT '',
	ailments_4 VARCHAR(1) DEFAULT '',
	ailments_5 VARCHAR(1) DEFAULT '',
	ailments_6 VARCHAR(1) DEFAULT '',
	ailments_7 VARCHAR(1) DEFAULT '',
	ailments_8 VARCHAR(1) DEFAULT '',
	ailments_9 VARCHAR(1) DEFAULT '',
	ailments_10 VARCHAR(1) DEFAULT '',
	ailments_11 VARCHAR(1) DEFAULT '',
	ailments_12 VARCHAR(1) DEFAULT '',
	ailments_13 VARCHAR(1) DEFAULT '',
	ailments_14 VARCHAR(1) DEFAULT '',
	ailments_15 VARCHAR(1) DEFAULT '',
	census_combined_code VARCHAR(20)  DEFAULT '',
	census_state_country_tract  VARCHAR(11)  DEFAULT '',
	contact_name VARCHAR(40) DEFAULT '',
	donor_code1 VARCHAR(1)  DEFAULT '',
	donor_code2 VARCHAR(1)  DEFAULT '',
	donor_code3 VARCHAR(1)  DEFAULT '',
	donor_code4 VARCHAR(1)  DEFAULT '',
	donor_code5 VARCHAR(1)  DEFAULT '',
	zip9 VARCHAR(9)  DEFAULT '',
	mortgage_interest_rates VARCHAR(5)  DEFAULT '',
	hardkey VARCHAR(75)  DEFAULT '',
	countycodebystatecode VARCHAR(5) DEFAULT '',
	carrierroutecodebyzipcode VARCHAR(9) DEFAULT '',
	birthmonth VARCHAR(2) DEFAULT '',
	birthdayofmonth VARCHAR(2)  DEFAULT '',
	birthyearmonth VARCHAR(6) DEFAULT '',
	statecity VARCHAR(50)  DEFAULT '',
	fullname VARCHAR(60)  DEFAULT '',
	title VARCHAR(50)  DEFAULT '',
	gui_commonsuppression VARCHAR(1) DEFAULT '',
	statecountyname VARCHAR(60)  DEFAULT '',
	statecityzip VARCHAR(50)  DEFAULT '',
	zipend VARCHAR(1) DEFAULT '',
	statecityscf VARCHAR(50)  DEFAULT '',
	statecountycode VARCHAR(5),
	unit_number_flag VARCHAR(1) DEFAULT 'N',
	addresszip VARCHAR(45) DEFAULT '',
	stateneighborhood VARCHAR(60) DEFAULT '',
	statecityneighborhood VARCHAR(90) DEFAULT '',
	mailorderbuyer_01 VARCHAR(2)  DEFAULT '',
	mailorderbuyer_02 VARCHAR(2)  DEFAULT '',
	mailorderbuyer_03 VARCHAR(2)  DEFAULT '',
	mailorderbuyer_04 VARCHAR(2)  DEFAULT '',
	mailorderbuyer_05 VARCHAR(2)  DEFAULT '',
	mailorderbuyer_06 VARCHAR(2)  DEFAULT '',
	mailorderbuyer_07 VARCHAR(2)  DEFAULT '',
	mailorderbuyer_08 VARCHAR(2)  DEFAULT '',
	mailorderbuyer_09 VARCHAR(2)  DEFAULT '',
	mailorderbuyer_10 VARCHAR(2)  DEFAULT '',
	mailorderbuyer_11 VARCHAR(2)  DEFAULT '',
	mailorderbuyer_12 VARCHAR(2)  DEFAULT '',
	mailorderbuyer_13 VARCHAR(2)  DEFAULT '',
	mailorderbuyer_14 VARCHAR(2)  DEFAULT '',
	mailorderbuyer_15 VARCHAR(2)  DEFAULT '',
	mailorderbuyer_16 VARCHAR(2)  DEFAULT '',
	mailorderbuyer_18 VARCHAR(2)  DEFAULT '',
	individual_mc VARCHAR(17),
	company_mc VARCHAR(15),
	address_mc VARCHAR(17),
	preferredcity VARCHAR(50),
	cellphone VARCHAR(10),
	cellphone_areacode VARCHAR(3),
	cellphone_number VARCHAR(7),
	cellphone_cordcutter VARCHAR(1),
	cellphone_prepaid_indicator VARCHAR(1),
	cellphone_donotcallflag VARCHAR(1),
	cellphone_verified_code VARCHAR(1),
	cellphone_activitystatus VARCHAR(2),
	cellphone_filterflag VARCHAR(1),
	cellphone_filterreason VARCHAR(2),
	cellphone_isdqi VARCHAR(1),
	cellphone_matchlevel VARCHAR(7),
	cellphone_matchscore VARCHAR(3),
	cellphone_modifiedscore VARCHAR(3),
	cellphone_individualmatch VARCHAR(1),
	nielsen_county_rank_description  VARCHAR(100) DEFAULT '',
	income_description  VARCHAR(100) DEFAULT '',
	age_code_description  VARCHAR(100) DEFAULT '',
	home_equity_estimate_description  VARCHAR(100) DEFAULT '',
	home_sale_price_code_description  VARCHAR(100) DEFAULT '',
	home_value_code_description  VARCHAR(100) DEFAULT '',
	own_rent_indicator_description  VARCHAR(100) DEFAULT '',
	expendable_income_rank_description  VARCHAR(100) DEFAULT '',
	mortgage_amount_description  VARCHAR(100) DEFAULT '',
	mortgage_loan_type_description  VARCHAR(100) DEFAULT '',
	loan_to_value_ratio_ltv_description  VARCHAR(100) DEFAULT '',
	censusstatecode2020 VARCHAR(2),
	censuscountycode2020 VARCHAR(3),
	censustract2020 VARCHAR(6),
	censusblockgroup2020 VARCHAR(1),
	secondmortgageamount VARCHAR(4),
	secondmortgagetypecode VARCHAR(1),
	lot_size_sqfeet VARCHAR(9),
	universal_sqfeet VARCHAR(5),
	universal_sqfeet_ind VARCHAR(1),
	living_sqfeet VARCHAR(5),
	groundfloor_sqfeet VARCHAR(5),
	gross_sqfeet VARCHAR(5),
	adjustedgross_sqfeet VARCHAR(5),
	basement_sqfeet VARCHAR(5),
	garage_sqfeet VARCHAR(5),
	fullbath VARCHAR(1),
	halfbath VARCHAR(1),
	fireplace_ind VARCHAR(1),
	fireplacetype VARCHAR(3),
	pooltype VARCHAR(3),
	roofcover VARCHAR(3),
	roofshape VARCHAR(1),
	conditiontype VARCHAR(1),
	exteriorwalltype VARCHAR(3),
	foundationtype VARCHAR(3),
	constructionquality VARCHAR(1),
	stories VARCHAR(3),
	styletype VARCHAR(3),
	airconditioningtype VARCHAR(3),
	electricwiringtype VARCHAR(3),
	fueltype VARCHAR(3),
	sewertype VARCHAR(1),
	watertype VARCHAR(1),
	construction_type_detail VARCHAR(3),
	heating_type_detail VARCHAR(3),
	filler_105 VARCHAR(1),
	email_deployable VARCHAR(1)
);
