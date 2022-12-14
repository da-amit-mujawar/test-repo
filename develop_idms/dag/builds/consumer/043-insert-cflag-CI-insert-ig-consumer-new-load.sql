DROP TABLE IF EXISTS NoSuchTable;


UPDATE ig_consumer_email_new_temp
SET cFlag='CI'
FROM ig_consumer_email_new_temp 
WHERE cflag='' AND 
opt_out_flag='0' AND 
Company_ID IN 
(
    SELECT Company_ID FROM {new-load-table} GROUP BY Company_ID 
) AND 
Company_ID<>'000000000000' AND 
Company_ID<>'' AND Company_ID IS NOT NULL
;

--Insert records for CI flag
INSERT INTO {new-load-table}
(
    company_id,
    individual_id,
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
    zip,
    zip_four,
    carrier_route_code,
    delivery_point_bar_code,
    area_code,
    phonenumber,
    nielsen_county_rank,
    time_zone,
    income,
    last_contribution_date,
    length_of_residence,
    residence_type,
    delivery_unit_size_raw,
    own_rent_indicator,
    location_type,
    pool_type,
    home_value_code,
    home_sale_date,
    home_sale_price_code,
    home_sale_price,
    home_equity_estimate,
    finance_type,
    mortgage_loan_type,
    boat_propulsion_code,
    mortgage_amount,
    congressional_district,
    cat_owner,
    dog_owner,
    grandparent_in_house,
    veteran_flag,
    stocks_or_bonds,
    dm_high_tech_household,
    female_occupation,
    male_occupation,
    census_average_years_attended_school_code,
    census_phhblack_code,
    census_of_population_graduated_college,
    census_phhspnsh_code,
    census_percent_of_population_married,
    census_precent_of_population_single_parent,
    expendable_income_rank,
    loan_to_value_ratio_ltv,
    donor_code,
    mail_responsive_current,
    mail_responsive_recent,
    mail_responsive_ever,
    census_tract_2010,
    census_block_group_2010,
    address_type,
    hoh_head_of_household,
    first_name_hoh,
    middle_initial_hoh,
    last_name_hoh,
    last_name_suffix_hoh,
    title_code_hoh,
    gender_hoh,
    birth_date_hoh,
    individual_id_hoh,
    hoh_age_code,
    number_trade_lines_hoh,
    credit_card_code_hoh,
    marital_status_hoh,
    location_id,
    usps_delivery_service_type_code,
    nielsen_county_region,
    lot_size,
    home_size,
    home_age,
    political_contribution_amount,
    pilot_license_code,
    mortgage_interest_rates_old,
    aircraft_type_code,
    boat_owner_indicator,
    number_of_adults_in_household,
    household_member_count,
    medag18p,
    census_percent_of_population_with_3_households,
    census_phhasian_code,
    medhhd1,
    wealth_finder,
    numberofchildren,
    alternatehhind,
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
    infopersona_supercluster,
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
    censuscountycode2010,
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
    purchasing_power_income,
    potential_investor_consumer,
    sub_family_indicator,
    number_of_contributions,
    aircraft_mfg_year,
    cbsa_code,
    heavy_internet_user,
    early_technology_adopter,
    csa_code,
    csa_code_description,
    cbsa_level,
    mail_confidence,
    boat_hull_type,
    boat_length,
    flatitude,
    flongitude,
    vendor_matchcode,
    do_not_call_flag,
    has_primary_telephone,
    addressline1,
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
    number_of_surnames,
    homeowner,
    education_model,
    education_model_ext,
    income_producing_assets_ext,
    boatpropulsioncode_ext,
    contributionamount,
    lemsmatchcode,
    discretionary_income_score, 
    sesi,
    zipfourmatchlevel,
    areaprefix, 
    correctivelensespresence,
    actualvacation_expense,
    vacation_expense,
    vacation_expense_dic,
    nid,
    nid_description, 
    title_code,
    firstname,
    middleinitial,
    lastname,
    gender,
    opt_out_flag, 
    emailaddress, 
    vendor_code, 
    categories, 
    domain, 
    top_level_domain, 
    opt_out_date, 
    car_make, 
    car_model, 
    car_year, 
    date_entered_yyyymm, 
    email_domain, 
    open_flag, 
    open_date_yyyymm, 
    click_flag, 
    click_date_yyyymm, 
    md5_email_lower, 
    md5_email_upper,
    ipaddress,
    url,
    sha256_email,
    sha512_email, 
    bv_flag, 
    marigold, 
    bvt_refresh_date, 
    ipst_status_code, 
    ipst_refresh_date, 
    bestdate, 
    reactivation_flag, 
    mgen_match_flag, 
    bridge_code, 
    best_date_range, 
    digitalmatch,
    matchcode_lrfs,
    haspostal,
    matchlevel,
    email_deliverable,
    email_marketable,
    email_reputation_risk,
    email_deployable
)
SELECT 
    A.company_id,
    B.matchcode_lrfs,
    A.house_number,
    A.street_pre_directional,
    A.street_name,
    A.street_suffix,
    A.street_post_directional,
    A.unit_type,
    A.unit_number,
    A.city,
    A.state,
    A.census_state_code_2010,
    A.county_code,
    A.zip,
    A.zip_four,
    A.carrier_route_code,
    A.delivery_point_bar_code,
    A.area_code,
    A.phonenumber,
    A.nielsen_county_rank,
    A.time_zone,
    A.income,
    A.last_contribution_date,
    A.length_of_residence,
    A.residence_type,
    A.delivery_unit_size_raw,
    A.own_rent_indicator,
    A.location_type,
    A.pool_type,
    A.home_value_code,
    A.home_sale_date,
    A.home_sale_price_code,
    A.home_sale_price,
    A.home_equity_estimate,
    A.finance_type,
    A.mortgage_loan_type,
    A.boat_propulsion_code,
    A.mortgage_amount,
    A.congressional_district,
    A.cat_owner,
    A.dog_owner,
    A.grandparent_in_house,
    A.veteran_flag,
    A.stocks_or_bonds,
    A.dm_high_tech_household,
    A.female_occupation,
    A.male_occupation,
    A.census_average_years_attended_school_code,
    A.census_phhblack_code,
    A.census_of_population_graduated_college,
    A.census_phhspnsh_code,
    A.census_percent_of_population_married,
    A.census_precent_of_population_single_parent,
    A.expendable_income_rank,
    A.loan_to_value_ratio_ltv,
    A.donor_code,
    A.mail_responsive_current,
    A.mail_responsive_recent,
    A.mail_responsive_ever,
    A.census_tract_2010,
    A.census_block_group_2010,
    A.address_type,
    A.hoh_head_of_household,
    A.first_name_hoh,
    A.middle_initial_hoh,
    A.last_name_hoh,
    A.last_name_suffix_hoh,
    A.title_code_hoh,
    A.gender_hoh,
    A.birth_date_hoh,
    A.individual_id_hoh,
    A.hoh_age_code,
    A.number_trade_lines_hoh,
    A.credit_card_code_hoh,
    A.marital_status_hoh,
    A.location_id,
    A.usps_delivery_service_type_code,
    A.nielsen_county_region,
    A.lot_size,
    A.home_size,
    A.home_age,
    A.political_contribution_amount,
    A.pilot_license_code,
    A.mortgage_interest_rates_old,
    A.aircraft_type_code,
    A.boat_owner_indicator,
    A.number_of_adults_in_household,
    A.household_member_count,
    A.medag18p,
    A.census_percent_of_population_with_3_households,
    A.census_phhasian_code,
    A.medhhd1,
    A.wealth_finder,
    A.numberofchildren,
    A.alternatehhind,
    A.donotmailflag,
    A.emailpresenceflag,
    A.routetype,
    A.householdarrivaldate_year,
    A.householdarrivaldate,
    A.homeagesource,
    A.homebuiltyear,
    A.mortgagemonth,
    A.homevaluesource,
    A.actualincome,
    A.infopersona_cluster,
    A.infopersona_supercluster,
    A.lifestyle,
    A.listtype,
    A.marriedscore,
    A.medhhd2,
    A.mortgageloantype,
    A.mortgageinterestsource,
    A.occupancycount,
    A.agedatasource,
    A.presenceofchildren,
    A.recencydate,
    A.refreshdate,
    A.scf,
    A.censuscountycode2010,
    A.childrenbygender0,
    A.childrenbygender1,
    A.childrenbygender2,
    A.childrenbygender3,
    A.childrenbygender4,
    A.childrenbygender5,
    A.childrenbygender6,
    A.childrenbygender7,
    A.childrenbygender8,
    A.childrenbygender9,
    A.childrenbygender10,
    A.childrenbygender11,
    A.childrenbygender12,
    A.childrenbygender13,
    A.childrenbygender14,
    A.childrenbygender15,
    A.childrenbygender16,
    A.childrenbygender17,
    A.markettargetage0_5,
    A.markettargetage6_11,
    A.markettargetage12_17,
    A.markettargetage12_17female,
    A.markettargetage12_17male,
    A.children0_5,
    A.children6_11,
    A.children0_11,
    A.childrenbybirthmonth0,
    A.childrenbybirthmonth1,
    A.childrenbybirthmonth2,
    A.childrenbybirthmonth3,
    A.childrenbybirthmonth4,
    A.childrenbybirthmonth5,
    A.childrenbybirthmonth6,
    A.childrenbybirthmonth7,
    A.childrenbybirthmonth8,
    A.childrenbybirthmonth9,
    A.childrenbybirthmonth10,
    A.childrenbybirthmonth11,
    A.childrenbybirthmonth12,
    A.childrenbybirthmonth13,
    A.childrenbybirthmonth14,
    A.childrenbybirthmonth15,
    A.childrenbybirthmonth16,
    A.childrenbybirthmonth17,
    A.purchasing_power_income,
    A.potential_investor_consumer,
    A.sub_family_indicator,
    A.number_of_contributions,
    A.aircraft_mfg_year,
    A.cbsa_code,
    A.heavy_internet_user,
    A.early_technology_adopter,
    A.csa_code,
    A.csa_code_description,
    A.cbsa_level,
    A.mail_confidence,
    A.boat_hull_type,
    A.boat_length,
    A.flatitude,
    A.flongitude,
    A.vendor_matchcode,
    A.do_not_call_flag,
    A.has_primary_telephone,
    A.addressline1,
    A.mortgagedate,
    A.loanamount,
    A.lhi_african_american_ethnic_products,
    A.lhi_american_history,
    A.lhi_general_apparel,
    A.lhi_automotive,
    A.lhi_baseball,
    A.lhi_basketball,
    A.lhi_bible,
    A.lhi_birds,
    A.lhi_publish_books,
    A.lhi_business_items,
    A.lhi_cats,
    A.lhi_collectibles,
    A.lhi_college,
    A.lhi_computers,
    A.lhi_cooking,
    A.lhi_general_crafts,
    A.lhi_current_events,
    A.lhi_do_it_yourselfer,
    A.lhi_charitable_donor,
    A.lhi_personalized_products,
    A.lhi_horses,
    A.lhi_culture_arts,
    A.lhi_dogs,
    A.lhi_children_family,
    A.lhi_fishing,
    A.lhi_fitness_exercise,
    A.lhi_football,
    A.lhi_gardening,
    A.lhi_gambling,
    A.lhi_gift_giver,
    A.lhi_golf,
    A.lhi_general_health,
    A.lhi_hispanic_ethnic_products,
    A.lhi_hobbies,
    A.lhi_hockey,
    A.lhi_home_decorating,
    A.lhi_humor,
    A.lhi_hunting,
    A.lhi_inspirational,
    A.lhi_apparel_kids,
    A.lhi_apparel_mens,
    A.lhi_money_making,
    A.lhi_motorcycles,
    A.lhi_music,
    A.lhi_ocean,
    A.lhi_outdoors,
    A.lhi_pets,
    A.lhi_personal_finance,
    A.lhi_rural_farming,
    A.lhi_general_sports,
    A.lhi_stationery,
    A.lhi_general_travel,
    A.lhi_movies,
    A.lhi_wildlife,
    A.lhi_apparel_womens,
    A.lhi_unused_01,
    A.lhi_photography,
    A.lhi_seniors,
    A.lhi_aviation,
    A.lhi_electionics,
    A.lhi_internet,
    A.lhi_credit_card_user,
    A.lhi_beauty_cosmetic,
    A.lhi_asian_ethnic_products,
    A.lhi_dieting,
    A.lhi_science,
    A.lhi_sweepstakes,
    A.lhi_tobacco,
    A.lhi_bargain_seekers,
    A.lhi_publications,
    A.lhi_catalogs,
    A.lhi_hightech,
    A.lhi_apparel_accessories,
    A.lhi_apparel_mens_fashions,
    A.lhi_apparel_womens_fashions,
    A.lhi_auto_racing,
    A.lhi_trucks,
    A.lhi_home_office_products,
    A.lhi_politically_conservative,
    A.lhi_crocheting,
    A.lhi_knitting,
    A.lhi_needlepoint,
    A.lhi_quilting,
    A.lhi_sewing,
    A.lhi_fiction,
    A.lhi_games,
    A.lhi_unused_03,
    A.lhi_gourmet,
    A.lhi_history,
    A.lhi_internet_access,
    A.lhi_internet_buying,
    A.lhi_liberal,
    A.lhi_books_nonfiction,
    A.lhi_boating_sailing,
    A.lhi_camping_hiking,
    A.lhi_hunting_fishing,
    A.lhi_photo_processing,
    A.lhi_magazine_subscriber,
    A.lhi_books_science_fiction,
    A.lhi_skiing,
    A.lhi_soccer,
    A.lhi_tennis,
    A.lhi_travel_cruises,
    A.lhi_travel_rv,
    A.lhi_travel_us,
    A.tw_greenmodel,
    A.tw_highvaluesecurityinvestor,
    A.tw_highereducation,
    A.tw_highvaluestockinvestor,
    A.tw_heavyinvestmenttraders,
    A.tw_physicalfitnessclubs,
    A.tw_highriskinvestor,
    A.tw_lowriskinvestor,
    A.tw_frequentbusinesstraveler,
    A.tw_frequentflyer,
    A.tw_annuities,
    A.tw_lifeinsurance,
    A.tw_luxurycarbuyer,
    A.tw_cruise,
    A.tw_timeshareowner,
    A.tw_impulsebuyer,
    A.tw_luxuryhotel,
    A.tw_pbsdonor,
    A.tw_safetysecurityconscious,
    A.tw_shopaholics,
    A.tw_avidthemeparkvisitor,
    A.tw_foreigntravelvacation,
    A.tw_conservative,
    A.tw_leaningconservative,
    A.tw_leaningliberal,
    A.tw_religiousdonor,
    A.tw_gardenmaintenance,
    A.tw_heavyfamilyrestaurantvisitor,
    A.tw_heavypayperviewsports,
    A.tw_nascar,
    A.tw_liberal,
    A.tw_dogproducts,
    A.tw_heavypayperviewmovies,
    A.tw_dietproducts,
    A.tw_organicfoods,
    A.tw_heavyvitaminanddietarysupplement,
    A.tw_babyproducts,
    A.tw_heavycouponuser,
    A.tw_opinionleaders,
    A.tw_catproducts,
    A.tw_creditcardrewards,
    A.tw_highendsportingequipment,
    A.tw_homeimprovement,
    A.tw_highendelectronicsbuyer,
    A.tw_avidgamer,
    A.tw_rentalcar,
    A.tw_avidcellphoneuser,
    A.tw_countryclubmember,
    A.tw_professionaltaxpreparation,
    A.tw_onsitetaxpreparationservice,
    A.tw_classicalmusicconcerts,
    A.tw_rockmusicconcerts,
    A.tw_countrymusicconcerts,
    A.tw_livetheater,
    A.tw_heavybookbuyer,
    A.tw_freshwaterfishing,
    A.tw_saltwaterfishing,
    A.tw_hunting,
    A.tw_allterrainvehicle,
    A.tw_powerboating,
    A.tw_outdooractivities,
    A.tw_adventureseekers,
    A.tw_lowendsportingequipment,
    A.tw_professionalbaseballsportsfans,
    A.tw_professionalbasketballsportsfans,
    A.tw_professionalfootballsportsfans,
    A.tw_soccersportsfans,
    A.tw_collegebasketballsportsfans,
    A.tw_collegefootballsportsfans,
    A.tw_golfsportsfans,
    A.tw_tennissportsfans,
    A.tw_professionalwrestlingsportsfans,
    A.tw_internationallongdistance,
    A.tw_heavyfrozendinner,
    A.tw_cookfromscratch,
    A.tw_cookforfun,
    A.tw_winelover,
    A.tw_onlinepurchasepersonal,
    A.tw_onlinepurchasebusiness,
    A.tw_onlinetravelplan,
    A.tw_blogwriting,
    A.tw_voiceoverinternet,
    A.tw_wifioutsideofhome,
    A.tw_wifiinhome,
    A.tw_wholesaleclub,
    A.tw_autoclub,
    A.tw_diyautomaintenance,
    A.tw_onlinegamingactivity,
    A.tw_onlineinvestmenttrading,
    A.tw_onlinebillpayment,
    A.tw_onlinetvdownload,
    A.tw_mobileinternetaccess,
    A.tw_socialmedianetwork,
    A.tw_specialtyorganicfoodstore,
    A.tw_homeoffice,
    A.tw_businessbanking,
    A.tw_moderate_economyhotel,
    A.tw_e_reader,
    A.tw_cellphoneonlymodel,
    A.tw_alternativemedicine,
    A.tw_non_religiousdonor,
    A.tw_majorhomeremodeling,
    A.tw_avidsmartphoneusers,
    A.tw_healthinsurance,
    A.tw_satellitetv,
    A.tw_onlinemusicdownload,
    A.tw_adulteducation,
    A.tw_pilates_yoga,
    A.tw_fastfood,
    A.tw_hybridcars,
    A.tw_heavy_online_buyer,
    A.tw_high_end_appparel,
    A.tw_heavy_catalog_buyer,
    A.tw_gift_buyers,
    A.tw_camping,
    A.tw_sports_fanatics,
    A.tw_financial_planner,
    A.tw_work_health_insurance,
    A.tw_comprehensive_auto_insurance,
    A.tw_small_business_insurance,
    A.tw_heavy_snack_eaters,
    A.tw_heavy_domestic_travelers,
    A.tw_new_vehicle_buyer,
    A.tw_suv_buyer,
    A.tw_minivan_buyer,
    A.tw_auto_loan,
    A.tw_education_loan,
    A.tw_real_estate_investment,
    A.tw_fantasy_sports,
    A.tw_hockey_buyer,
    A.tw_auto_lease, 
    A.tw_digital_payment,
    A.tw_high_mileage_usage,
    A.tw_motorcycle_owner,
    A.tw_union_member,
    A.tw_national_park_visitor,
    A.tw_online_streaming_and_devices,
    A.tw_road_trip,
    A.tw_social_networking_ad_click,
    A.tw_satellite_radio,
    A.heatingtype_code,
    A.income_producing_assets,
    A.marketarea,
    A.phonenumbertype,
    A.revolver_to_transactor,
    A.second_property_ind,
    A.consumerstabilityrawscore,
    A.consumerstabilitypercentile,
    A.ailments,
    A.mailscore,
    A.populationdensity,
    A.number_of_surnames,
    A.homeowner,
    A.education_model,
    A.education_model_ext,
    A.income_producing_assets_ext,
    A.boatpropulsioncode_ext,
    A.contributionamount,
    A.lemsmatchcode,
    A.discretionary_income_score, 
    A.sesi,
    A.zipfourmatchlevel,
    A.areaprefix, 
    A.correctivelensespresence,
    A.actualvacation_expense,
    A.vacation_expense,
    A.vacation_expense_dic,
    A.nid,
    A.nid_description,
    B.titlecode,
    B.cfirstname,
    B.middle_initial,
    B.clastname,
    B.gender,
    B.opt_out_flag, 
    B.emailaddress, 
    B.vendor_code, 
    B.categories, 
    B.epd_domain, 
    B.top_level_domain, 
    B.opt_out_date, 
    B.car_make, 
    B.car_model, 
    B.car_year, 
    B.date_entered_yyyymm, 
    B.email_domain, 
    B.open_flag, 
    B.open_date_yyyymm, 
    B.click_flag, 
    B.click_date_yyyymm, 
    B.md5_email_lower, 
    B.md5_email_upper, 
    B.ipaddress,
    B.url ,
    B.sha256_email ,
    B.sha512_email ,
    B.bv_flag, 
    B.marigold, 
    B.bvt_refresh_date, 
    B.ipst_status_code, 
    B.ipst_refresh_date, 
    B.best_date_yyyymmdd,
    B.reactivation_flag, 
    B.mgen_match_flag, 
    B.bridge_code, 
    B.best_date_range, 
    B.consumer_digital_match, 
    B.matchcode_lrfs,
    'C', 
    'H',
    B.email_deliverable,
    B.email_marketable,
    B.email_reputation_risk,
    B.email_deployable
FROM {new-load-table} A 
INNER JOIN 
(
    SELECT company_id, MIN(id) AS min_id FROM {new-load-table} 
    WHERE company_id <>'000000000000' GROUP BY company_id
) C
ON A.company_id = C.company_id AND A.id=C.min_id 
INNER JOIN ig_consumer_email_new_temp B 
ON B.company_id =C.company_id
WHERE b.cflag='CI'; 


