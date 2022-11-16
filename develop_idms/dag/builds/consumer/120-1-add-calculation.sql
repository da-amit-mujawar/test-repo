DROP TABLE IF EXISTS nosuchtable;
DROP TABLE IF EXISTS IG_CONSUMER_NEW_TEMP CASCADE;

CREATE TABLE IG_CONSUMER_NEW_TEMP
DISTSTYLE EVEN
AS
SELECT
  a.id AS id,
  NVL(a.listid, 15794) AS listid,
  NVL(a.permissiontype, 'R') AS permissiontype,
  NVL(a.zipradius, '') AS zipradius,
  NVL(a.georadius, '') AS georadius,
  a.firstname AS firstname,
  a.middleinitial AS middleinitial,
  a.lastname AS lastname,
  a.last_name_suffix AS last_name_suffix,
  a.title_code AS title_code,
  a.gender AS gender,
  a.house_number AS house_number,
  a.street_pre_directional AS street_pre_directional,
  a.street_name AS street_name,
  a.street_suffix AS street_suffix,
  a.street_post_directional AS street_post_directional,
  a.unit_type AS unit_type,
  a.unit_number AS unit_number,
  a.city AS city,
  a.state AS state,
  a.census_state_code_2010 AS census_state_code_2010,
  a.county_code AS county_code,
  a.zip AS zip,
  a.zip_four AS zip_four,
  a.carrier_route_code AS carrier_route_code,
  a.delivery_point_bar_code AS delivery_point_bar_code,
  CASE WHEN LTRIM(RTRIM(a.AREA_CODE)) LIKE '0%' THEN '' ELSE a.area_code END AS area_code,
  a.nielsen_county_rank AS nielsen_county_rank,
  a.time_zone AS time_zone,
  a.age_code AS age_code,
  a.birth_date AS birth_date,
  a.income AS income,
  a.last_contribution_date AS last_contribution_date,
  a.length_of_residence AS length_of_residence,
  a.residence_type AS residence_type,
  a.delivery_unit_size_raw AS delivery_unit_size_raw,
  a.own_rent_indicator AS own_rent_indicator,
  a.location_type AS location_type,
  a.pool_type AS pool_type,
  a.home_value_code AS home_value_code,
  a.home_sale_date AS home_sale_date,
  a.home_sale_price_code AS home_sale_price_code,
  CASE WHEN (a.home_sale_price_code BETWEEN 'A' AND 'M' ) THEN RIGHT(concat('00000000',REPLACE(REPLACE(ltrim(rtrim(a.home_sale_price)), '$', ''), ',', '')), 8)
       ELSE REPLACE(REPLACE(ltrim(rtrim(a.home_sale_price)), '$', ''), ',', '') END AS home_sale_price,
  a.home_equity_estimate AS home_equity_estimate,
  a.finance_type AS finance_type,
  a.mortgage_loan_type AS mortgage_loan_type,
  a.boat_propulsion_code AS boat_propulsion_code,
  a.mortgage_amount AS mortgage_amount,
  a.congressional_district AS congressional_district,
  CASE WHEN ',' + TRIM(
      CASE
        WHEN SUBSTRING(CREDIT_CARD_CODE, 1, 1) = '1' THEN '1,'
        ELSE ''
      END + CASE
        WHEN SUBSTRING(CREDIT_CARD_CODE, 2, 1) = '1' THEN '2,'
        ELSE ''
      END + CASE
        WHEN SUBSTRING(CREDIT_CARD_CODE, 3, 1) = '1' THEN '3,'
        ELSE ''
      END + CASE
        WHEN SUBSTRING(CREDIT_CARD_CODE, 4, 1) = '1' THEN '4,'
        ELSE ''
      END + CASE
        WHEN SUBSTRING(CREDIT_CARD_CODE, 5, 1) = '1' THEN '5,'
        ELSE ''
      END + CASE
        WHEN SUBSTRING(CREDIT_CARD_CODE, 6, 1) = '1' THEN '6,'
        ELSE ''
      END + CASE
        WHEN SUBSTRING(CREDIT_CARD_CODE, 7, 1) = '1' THEN '7,'
        ELSE ''
      END + CASE
        WHEN SUBSTRING(CREDIT_CARD_CODE, 8, 1) = '1' THEN '8,'
        ELSE ''
      END + CASE
        WHEN SUBSTRING(CREDIT_CARD_CODE, 9, 1) = '1' THEN '9,'
        ELSE ''
      END + CASE
        WHEN SUBSTRING(CREDIT_CARD_CODE, 10, 1) = '1' THEN '10,'
        ELSE ''
      END
    ) = ',' THEN '' 
  ELSE ',' + TRIM(
      CASE
        WHEN SUBSTRING(CREDIT_CARD_CODE, 1, 1) = '1' THEN '1,'
        ELSE ''
      END + CASE
        WHEN SUBSTRING(CREDIT_CARD_CODE, 2, 1) = '1' THEN '2,'
        ELSE ''
      END + CASE
        WHEN SUBSTRING(CREDIT_CARD_CODE, 3, 1) = '1' THEN '3,'
        ELSE ''
      END + CASE
        WHEN SUBSTRING(CREDIT_CARD_CODE, 4, 1) = '1' THEN '4,'
        ELSE ''
      END + CASE
        WHEN SUBSTRING(CREDIT_CARD_CODE, 5, 1) = '1' THEN '5,'
        ELSE ''
      END + CASE
        WHEN SUBSTRING(CREDIT_CARD_CODE, 6, 1) = '1' THEN '6,'
        ELSE ''
      END + CASE
        WHEN SUBSTRING(CREDIT_CARD_CODE, 7, 1) = '1' THEN '7,'
        ELSE ''
      END + CASE
        WHEN SUBSTRING(CREDIT_CARD_CODE, 8, 1) = '1' THEN '8,'
        ELSE ''
      END + CASE
        WHEN SUBSTRING(CREDIT_CARD_CODE, 9, 1) = '1' THEN '9,'
        ELSE ''
      END + CASE
        WHEN SUBSTRING(CREDIT_CARD_CODE, 10, 1) = '1' THEN '10,'
        ELSE ''
      END
    ) END  AS credit_card_code,
  a.number_trade_lines AS number_trade_lines,
  CASE WHEN LTRIM(RTRIM(a.marital_status)) = '' OR a.marital_status is null THEN 'U' ELSE a.marital_status END AS marital_status,
  a.spousal_indicator_raw AS spousal_indicator_raw,
  a.cat_owner AS cat_owner,
  a.dog_owner AS dog_owner,
  a.grandparent_in_house AS grandparent_in_house,
  a.veteran_flag AS veteran_flag,
  a.stocks_or_bonds AS stocks_or_bonds,
  a.dm_high_tech_household AS dm_high_tech_household,
  a.female_occupation AS female_occupation,
  a.male_occupation AS male_occupation,
  a.census_average_years_attended_school_code AS census_average_years_attended_school_code,
  a.census_phhblack_code AS census_phhblack_code,
  a.census_of_population_graduated_college AS census_of_population_graduated_college,
  a.census_phhspnsh_code AS census_phhspnsh_code,
  a.census_percent_of_population_married AS census_percent_of_population_married,
  a.census_precent_of_population_single_parent AS census_precent_of_population_single_parent,
  a.expendable_income_rank AS expendable_income_rank,
  a.loan_to_value_ratio_ltv AS loan_to_value_ratio_ltv,
  a.donor_code AS donor_code,
  a.mail_responsive_current AS mail_responsive_current,
  a.mail_responsive_recent AS mail_responsive_recent,
  a.mail_responsive_ever AS mail_responsive_ever,
  a.census_tract_2010 AS census_tract_2010,
  a.census_block_group_2010 AS census_block_group_2010,
  a.address_type AS address_type,
  a.hoh_head_of_household AS hoh_head_of_household,
  a.first_name_hoh AS first_name_hoh,
  a.middle_initial_hoh AS middle_initial_hoh,
  a.last_name_hoh AS last_name_hoh,
  a.last_name_suffix_hoh AS last_name_suffix_hoh,
  a.title_code_hoh AS title_code_hoh,
  a.gender_hoh AS gender_hoh,
  a.birth_date_hoh AS birth_date_hoh,
  a.individual_id_hoh AS individual_id_hoh,
  a.hoh_age_code AS hoh_age_code,
  a.number_trade_lines_hoh AS number_trade_lines_hoh,
  a.credit_card_code_hoh AS credit_card_code_hoh,
  a.marital_status_hoh AS marital_status_hoh,
  LTRIM(RTRIM(a.company_id)) AS company_id,
  a.individual_id AS individual_id,
  a.location_id AS location_id,
  a.usps_delivery_service_type_code AS usps_delivery_service_type_code,
  a.nielsen_county_region AS nielsen_county_region,
  a.lot_size AS lot_size,
  a.home_size AS home_size,
  a.home_age AS home_age,
  a.actual_age_flag AS actual_age_flag,
  a.political_contribution_amount AS political_contribution_amount,
  a.pilot_license_code AS pilot_license_code,
  a.mortgage_interest_rates_old AS mortgage_interest_rates_old,
  a.last_party_contributed_to AS last_party_contributed_to,
  a.aircraft_type_code AS aircraft_type_code,
  a.boat_owner_indicator AS boat_owner_indicator,
  a.registered_voter AS registered_voter,
  a.political_party_affiliation AS political_party_affiliation,
  a.number_of_adults_in_household AS number_of_adults_in_household,
  a.household_member_count AS household_member_count,
  a.medag18p AS medag18p,
  a.census_percent_of_population_with_3_households AS census_percent_of_population_with_3_households,
  a.census_phhasian_code AS census_phhasian_code,
  a.medhhd1 AS medhhd1,
  a.wealth_finder AS wealth_finder,
  a.numberofchildren AS numberofchildren,
  a.alternatehhind AS alternatehhind,
  a.countryoforign AS countryoforign,
  a.donotmailflag AS donotmailflag,
  a.emailpresenceflag AS emailpresenceflag,
  a.routetype AS routetype,
  a.householdarrivaldate_year AS householdarrivaldate_year,
  a.householdarrivaldate AS householdarrivaldate,
  a.homeagesource AS homeagesource,
  a.homebuiltyear AS homebuiltyear,
  a.mortgagemonth AS mortgagemonth,
  a.homevaluesource AS homevaluesource,
  a.actualincome AS actualincome,
  a.infopersona_cluster AS infopersona_cluster,
  a.infopersona_supercluster AS infopersona_supercluster,
  a.lifestyle AS lifestyle,
  a.listtype AS listtype,
  a.marriedscore AS marriedscore,
  a.medhhd2 AS medhhd2,
  a.mortgageloantype AS mortgageloantype,
  a.mortgageinterestsource AS mortgageinterestsource,
  a.occupancycount AS occupancycount,
  CASE
    WHEN a.agedatasource = '' THEN '0'
    ELSE a.agedatasource
  END AS agedatasource,
  a.presenceofchildren AS presenceofchildren,
  a.recencydate AS recencydate,
  a.refreshdate AS refreshdate,
  a.scf as scf,
  a.vendorethnicity AS vendorethnicity,
  a.vendorethnicgroup AS vendorethnicgroup,
  a.vendorlanguage AS vendorlanguage,
  a.vendorreligion AS vendorreligion,
  a.censuscountycode2010 AS censuscountycode2010,
  a.fulfillmentflag AS fulfillmentflag,
  a.childrenbygender0 AS childrenbygender0,
  a.childrenbygender1 AS childrenbygender1,
  a.childrenbygender2 AS childrenbygender2,
  a.childrenbygender3 AS childrenbygender3,
  a.childrenbygender4 AS childrenbygender4,
  a.childrenbygender5 AS childrenbygender5,
  a.childrenbygender6 AS childrenbygender6,
  a.childrenbygender7 AS childrenbygender7,
  a.childrenbygender8 AS childrenbygender8,
  a.childrenbygender9 AS childrenbygender9,
  a.childrenbygender10 AS childrenbygender10,
  a.childrenbygender11 AS childrenbygender11,
  a.childrenbygender12 AS childrenbygender12,
  a.childrenbygender13 AS childrenbygender13,
  a.childrenbygender14 AS childrenbygender14,
  a.childrenbygender15 AS childrenbygender15,
  a.childrenbygender16 AS childrenbygender16,
  a.childrenbygender17 AS childrenbygender17,
  a.markettargetage0_5 AS markettargetage0_5,
  a.markettargetage6_11 AS markettargetage6_11,
  a.markettargetage12_17 AS markettargetage12_17,
  a.markettargetage12_17female AS markettargetage12_17female,
  a.markettargetage12_17male AS markettargetage12_17male,
  a.children0_5 AS children0_5,
  a.children6_11 AS children6_11,
  a.children0_11 AS children0_11,
  a.childrenbybirthmonth0 AS childrenbybirthmonth0,
  a.childrenbybirthmonth1 AS childrenbybirthmonth1,
  a.childrenbybirthmonth2 AS childrenbybirthmonth2,
  a.childrenbybirthmonth3 AS childrenbybirthmonth3,
  a.childrenbybirthmonth4 AS childrenbybirthmonth4,
  a.childrenbybirthmonth5 AS childrenbybirthmonth5,
  a.childrenbybirthmonth6 AS childrenbybirthmonth6,
  a.childrenbybirthmonth7 AS childrenbybirthmonth7,
  a.childrenbybirthmonth8 AS childrenbybirthmonth8,
  a.childrenbybirthmonth9 AS childrenbybirthmonth9,
  a.childrenbybirthmonth10 AS childrenbybirthmonth10,
  a.childrenbybirthmonth11 AS childrenbybirthmonth11,
  a.childrenbybirthmonth12 AS childrenbybirthmonth12,
  a.childrenbybirthmonth13 AS childrenbybirthmonth13,
  a.childrenbybirthmonth14 AS childrenbybirthmonth14,
  a.childrenbybirthmonth15 AS childrenbybirthmonth15,
  a.childrenbybirthmonth16 AS childrenbybirthmonth16,
  a.childrenbybirthmonth17 AS childrenbybirthmonth17,
  a.purchasing_power_income AS purchasing_power_income,
  a.potential_investor_consumer AS potential_investor_consumer,
  a.sub_family_indicator AS sub_family_indicator,
  a.number_of_contributions AS number_of_contributions,
  a.aircraft_mfg_year AS aircraft_mfg_year,
  a.cbsa_code AS cbsa_code,
  a.heavy_internet_user AS heavy_internet_user,
  a.early_technology_adopter AS early_technology_adopter,
  a.csa_code AS csa_code,
  a.csa_code_description AS csa_code_description,
  a.cbsa_level AS cbsa_level,
  a.mail_confidence AS mail_confidence,
  a.boat_hull_type AS boat_hull_type,
  a.boat_length AS boat_length,
  a.flatitude AS flatitude,
  a.flongitude AS flongitude,
  a.vendor_matchcode AS vendor_matchcode,
  CASE WHEN b.cPhone IS NOT NULL AND a.do_not_call_flag <> '8' THEN '8'
       ELSE a.do_not_call_flag
       END AS do_not_call_flag,
  a.has_primary_telephone AS has_primary_telephone,
  a.addressline1 AS addressline1,
  a.digitalmatch AS digitalmatch,
  a.age AS age,
  a.mortgagedate AS mortgagedate,
  a.loanamount AS loanamount,
  a.lhi_african_american_ethnic_products AS lhi_african_american_ethnic_products,
  a.lhi_american_history AS lhi_american_history,
  a.lhi_general_apparel AS lhi_general_apparel,
  a.lhi_automotive AS lhi_automotive,
  a.lhi_baseball AS lhi_baseball,
  a.lhi_basketball AS lhi_basketball,
  a.lhi_bible AS lhi_bible,
  a.lhi_birds AS lhi_birds,
  a.lhi_publish_books AS lhi_publish_books,
  a.lhi_business_items AS lhi_business_items,
  a.lhi_cats AS lhi_cats,
  a.lhi_collectibles AS lhi_collectibles,
  a.lhi_college AS lhi_college,
  a.lhi_computers AS lhi_computers,
  a.lhi_cooking AS lhi_cooking,
  a.lhi_general_crafts AS lhi_general_crafts,
  a.lhi_current_events AS lhi_current_events,
  a.lhi_do_it_yourselfer AS lhi_do_it_yourselfer,
  a.lhi_charitable_donor AS lhi_charitable_donor,
  a.lhi_personalized_products AS lhi_personalized_products,
  a.lhi_horses AS lhi_horses,
  a.lhi_culture_arts AS lhi_culture_arts,
  a.lhi_dogs AS lhi_dogs,
  a.lhi_children_family AS lhi_children_family,
  a.lhi_fishing AS lhi_fishing,
  a.lhi_fitness_exercise AS lhi_fitness_exercise,
  a.lhi_football AS lhi_football,
  a.lhi_gardening AS lhi_gardening,
  a.lhi_gambling AS lhi_gambling,
  a.lhi_gift_giver AS lhi_gift_giver,
  a.lhi_golf AS lhi_golf,
  a.lhi_general_health AS lhi_general_health,
  a.lhi_hispanic_ethnic_products AS lhi_hispanic_ethnic_products,
  a.lhi_hobbies AS lhi_hobbies,
  a.lhi_hockey AS lhi_hockey,
  a.lhi_home_decorating AS lhi_home_decorating,
  a.lhi_humor AS lhi_humor,
  a.lhi_hunting AS lhi_hunting,
  a.lhi_inspirational AS lhi_inspirational,
  a.lhi_apparel_kids AS lhi_apparel_kids,
  a.lhi_apparel_mens AS lhi_apparel_mens,
  a.lhi_money_making AS lhi_money_making,
  a.lhi_motorcycles AS lhi_motorcycles,
  a.lhi_music AS lhi_music,
  a.lhi_ocean AS lhi_ocean,
  a.lhi_outdoors AS lhi_outdoors,
  a.lhi_pets AS lhi_pets,
  a.lhi_personal_finance AS lhi_personal_finance,
  a.lhi_rural_farming AS lhi_rural_farming,
  a.lhi_general_sports AS lhi_general_sports,
  a.lhi_stationery AS lhi_stationery,
  a.lhi_general_travel AS lhi_general_travel,
  a.lhi_movies AS lhi_movies,
  a.lhi_wildlife AS lhi_wildlife,
  a.lhi_apparel_womens AS lhi_apparel_womens,
  a.lhi_unused_01 AS lhi_unused_01,
  a.lhi_photography AS lhi_photography,
  a.lhi_seniors AS lhi_seniors,
  a.lhi_aviation AS lhi_aviation,
  a.lhi_electionics AS lhi_electionics,
  a.lhi_internet AS lhi_internet,
  a.lhi_credit_card_user AS lhi_credit_card_user,
  a.lhi_beauty_cosmetic AS lhi_beauty_cosmetic,
  a.lhi_asian_ethnic_products AS lhi_asian_ethnic_products,
  a.lhi_dieting AS lhi_dieting,
  a.lhi_science AS lhi_science,
  a.lhi_sweepstakes AS lhi_sweepstakes,
  a.lhi_tobacco AS lhi_tobacco,
  a.lhi_bargain_seekers AS lhi_bargain_seekers,
  a.lhi_publications AS lhi_publications,
  a.lhi_catalogs AS lhi_catalogs,
  a.lhi_hightech AS lhi_hightech,
  a.lhi_apparel_accessories AS lhi_apparel_accessories,
  a.lhi_apparel_mens_fashions AS lhi_apparel_mens_fashions,
  a.lhi_apparel_womens_fashions AS lhi_apparel_womens_fashions,
  a.lhi_auto_racing AS lhi_auto_racing,
  a.lhi_trucks AS lhi_trucks,
  a.lhi_home_office_products AS lhi_home_office_products,
  a.lhi_politically_conservative AS lhi_politically_conservative,
  a.lhi_crocheting AS lhi_crocheting,
  a.lhi_knitting AS lhi_knitting,
  a.lhi_needlepoint AS lhi_needlepoint,
  a.lhi_quilting AS lhi_quilting,
  a.lhi_sewing AS lhi_sewing,
  a.lhi_fiction AS lhi_fiction,
  a.lhi_games AS lhi_games,
  a.lhi_unused_03 AS lhi_unused_03,
  a.lhi_gourmet AS lhi_gourmet,
  a.lhi_history AS lhi_history,
  a.lhi_internet_access AS lhi_internet_access,
  a.lhi_internet_buying AS lhi_internet_buying,
  a.lhi_liberal AS lhi_liberal,
  a.lhi_books_nonfiction AS lhi_books_nonfiction,
  a.lhi_boating_sailing AS lhi_boating_sailing,
  a.lhi_camping_hiking AS lhi_camping_hiking,
  a.lhi_hunting_fishing AS lhi_hunting_fishing,
  a.lhi_photo_processing AS lhi_photo_processing,
  a.lhi_magazine_subscriber AS lhi_magazine_subscriber,
  a.lhi_books_science_fiction AS lhi_books_science_fiction,
  a.lhi_skiing AS lhi_skiing,
  a.lhi_soccer AS lhi_soccer,
  a.lhi_tennis AS lhi_tennis,
  a.lhi_travel_cruises AS lhi_travel_cruises,
  a.lhi_travel_rv AS lhi_travel_rv,
  a.lhi_travel_us AS lhi_travel_us,
  a.tw_greenmodel AS tw_greenmodel,
  a.tw_highvaluesecurityinvestor AS tw_highvaluesecurityinvestor,
  a.tw_highereducation AS tw_highereducation,
  a.tw_highvaluestockinvestor AS tw_highvaluestockinvestor,
  a.tw_heavyinvestmenttraders AS tw_heavyinvestmenttraders,
  a.tw_physicalfitnessclubs AS tw_physicalfitnessclubs,
  a.tw_highriskinvestor AS tw_highriskinvestor,
  a.tw_lowriskinvestor AS tw_lowriskinvestor,
  a.tw_frequentbusinesstraveler AS tw_frequentbusinesstraveler,
  a.tw_frequentflyer AS tw_frequentflyer,
  a.tw_annuities AS tw_annuities,
  a.tw_lifeinsurance AS tw_lifeinsurance,
  a.tw_luxurycarbuyer AS tw_luxurycarbuyer,
  a.tw_cruise AS tw_cruise,
  a.tw_timeshareowner AS tw_timeshareowner,
  a.tw_impulsebuyer AS tw_impulsebuyer,
  a.tw_luxuryhotel AS tw_luxuryhotel,
  a.tw_pbsdonor AS tw_pbsdonor,
  a.tw_safetysecurityconscious AS tw_safetysecurityconscious,
  a.tw_shopaholics AS tw_shopaholics,
  a.tw_avidthemeparkvisitor AS tw_avidthemeparkvisitor,
  a.tw_foreigntravelvacation AS tw_foreigntravelvacation,
  a.tw_conservative AS tw_conservative,
  a.tw_leaningconservative AS tw_leaningconservative,
  a.tw_leaningliberal AS tw_leaningliberal,
  a.tw_religiousdonor AS tw_religiousdonor,
  a.tw_gardenmaintenance AS tw_gardenmaintenance,
  a.tw_heavyfamilyrestaurantvisitor AS tw_heavyfamilyrestaurantvisitor,
  a.tw_heavypayperviewsports AS tw_heavypayperviewsports,
  a.tw_nascar AS tw_nascar,
  a.tw_liberal AS tw_liberal,
  a.tw_dogproducts AS tw_dogproducts,
  a.tw_heavypayperviewmovies AS tw_heavypayperviewmovies,
  a.tw_dietproducts AS tw_dietproducts,
  a.tw_organicfoods AS tw_organicfoods,
  a.tw_heavyvitaminanddietarysupplement AS tw_heavyvitaminanddietarysupplement,
  a.tw_babyproducts AS tw_babyproducts,
  a.tw_heavycouponuser AS tw_heavycouponuser,
  a.tw_opinionleaders AS tw_opinionleaders,
  a.tw_catproducts AS tw_catproducts,
  a.tw_creditcardrewards AS tw_creditcardrewards,
  a.tw_highendsportingequipment AS tw_highendsportingequipment,
  a.tw_homeimprovement AS tw_homeimprovement,
  a.tw_highendelectronicsbuyer AS tw_highendelectronicsbuyer,
  a.tw_avidgamer AS tw_avidgamer,
  a.tw_rentalcar AS tw_rentalcar,
  a.tw_avidcellphoneuser AS tw_avidcellphoneuser,
  a.tw_countryclubmember AS tw_countryclubmember,
  a.tw_professionaltaxpreparation AS tw_professionaltaxpreparation,
  a.tw_onsitetaxpreparationservice AS tw_onsitetaxpreparationservice,
  a.tw_classicalmusicconcerts AS tw_classicalmusicconcerts,
  a.tw_rockmusicconcerts AS tw_rockmusicconcerts,
  a.tw_countrymusicconcerts AS tw_countrymusicconcerts,
  a.tw_livetheater AS tw_livetheater,
  a.tw_heavybookbuyer AS tw_heavybookbuyer,
  a.tw_freshwaterfishing AS tw_freshwaterfishing,
  a.tw_saltwaterfishing AS tw_saltwaterfishing,
  a.tw_hunting AS tw_hunting,
  a.tw_allterrainvehicle AS tw_allterrainvehicle,
  a.tw_powerboating AS tw_powerboating,
  a.tw_outdooractivities AS tw_outdooractivities,
  a.tw_adventureseekers AS tw_adventureseekers,
  a.tw_lowendsportingequipment AS tw_lowendsportingequipment,
  a.tw_professionalbaseballsportsfans AS tw_professionalbaseballsportsfans,
  a.tw_professionalbasketballsportsfans AS tw_professionalbasketballsportsfans,
  a.tw_professionalfootballsportsfans AS tw_professionalfootballsportsfans,
  a.tw_soccersportsfans AS tw_soccersportsfans,
  a.tw_collegebasketballsportsfans AS tw_collegebasketballsportsfans,
  a.tw_collegefootballsportsfans AS tw_collegefootballsportsfans,
  a.tw_golfsportsfans AS tw_golfsportsfans,
  a.tw_tennissportsfans AS tw_tennissportsfans,
  a.tw_professionalwrestlingsportsfans AS tw_professionalwrestlingsportsfans,
  a.tw_internationallongdistance AS tw_internationallongdistance,
  a.tw_heavyfrozendinner AS tw_heavyfrozendinner,
  a.tw_cookfromscratch AS tw_cookfromscratch,
  a.tw_cookforfun AS tw_cookforfun,
  a.tw_winelover AS tw_winelover,
  a.tw_onlinepurchasepersonal AS tw_onlinepurchasepersonal,
  a.tw_onlinepurchasebusiness AS tw_onlinepurchasebusiness,
  a.tw_onlinetravelplan AS tw_onlinetravelplan,
  a.tw_blogwriting AS tw_blogwriting,
  a.tw_voiceoverinternet AS tw_voiceoverinternet,
  a.tw_wifioutsideofhome AS tw_wifioutsideofhome,
  a.tw_wifiinhome AS tw_wifiinhome,
  a.tw_wholesaleclub AS tw_wholesaleclub,
  a.tw_autoclub AS tw_autoclub,
  a.tw_diyautomaintenance AS tw_diyautomaintenance,
  a.tw_onlinegamingactivity AS tw_onlinegamingactivity,
  a.tw_onlineinvestmenttrading AS tw_onlineinvestmenttrading,
  a.tw_onlinebillpayment AS tw_onlinebillpayment,
  a.tw_onlinetvdownload AS tw_onlinetvdownload,
  a.tw_mobileinternetaccess AS tw_mobileinternetaccess,
  a.tw_socialmedianetwork AS tw_socialmedianetwork,
  a.tw_specialtyorganicfoodstore AS tw_specialtyorganicfoodstore,
  a.tw_homeoffice AS tw_homeoffice,
  a.tw_businessbanking AS tw_businessbanking,
  a.tw_moderate_economyhotel AS tw_moderate_economyhotel,
  a.tw_e_reader AS tw_e_reader,
  a.tw_cellphoneonlymodel AS tw_cellphoneonlymodel,
  a.tw_alternativemedicine AS tw_alternativemedicine,
  a.tw_non_religiousdonor AS tw_non_religiousdonor,
  a.tw_majorhomeremodeling AS tw_majorhomeremodeling,
  a.tw_avidsmartphoneusers AS tw_avidsmartphoneusers,
  a.tw_healthinsurance AS tw_healthinsurance,
  a.tw_satellitetv AS tw_satellitetv,
  a.tw_onlinemusicdownload AS tw_onlinemusicdownload,
  a.tw_adulteducation AS tw_adulteducation,
  a.tw_pilates_yoga AS tw_pilates_yoga,
  a.tw_fastfood AS tw_fastfood,
  a.tw_hybridcars AS tw_hybridcars,
  a.tw_heavy_online_buyer AS tw_heavy_online_buyer,
  a.tw_high_end_appparel AS tw_high_end_appparel,
  a.tw_heavy_catalog_buyer AS tw_heavy_catalog_buyer,
  a.tw_gift_buyers AS tw_gift_buyers,
  a.tw_camping AS tw_camping,
  a.tw_sports_fanatics AS tw_sports_fanatics,
  a.tw_financial_planner AS tw_financial_planner,
  a.tw_work_health_insurance AS tw_work_health_insurance,
  a.tw_comprehensive_auto_insurance AS tw_comprehensive_auto_insurance,
  a.tw_small_business_insurance AS tw_small_business_insurance,
  a.tw_heavy_snack_eaters AS tw_heavy_snack_eaters,
  a.tw_heavy_domestic_travelers AS tw_heavy_domestic_travelers,
  a.tw_new_vehicle_buyer AS tw_new_vehicle_buyer,
  a.tw_suv_buyer AS tw_suv_buyer,
  a.tw_minivan_buyer AS tw_minivan_buyer,
  a.tw_auto_loan AS tw_auto_loan,
  a.tw_education_loan AS tw_education_loan,
  a.tw_real_estate_investment AS tw_real_estate_investment,
  a.tw_fantasy_sports AS tw_fantasy_sports,
  a.tw_hockey_buyer AS tw_hockey_buyer,
  a.tw_auto_lease AS tw_auto_lease,
  a.tw_digital_payment AS tw_digital_payment,
  a.tw_high_mileage_usage AS tw_high_mileage_usage,
  a.tw_motorcycle_owner AS tw_motorcycle_owner,
  a.tw_union_member AS tw_union_member,
  a.tw_national_park_visitor AS tw_national_park_visitor,
  a.tw_online_streaming_and_devices AS tw_online_streaming_and_devices,
  a.tw_road_trip AS tw_road_trip,
  a.tw_social_networking_ad_click AS tw_social_networking_ad_click,
  a.tw_satellite_radio AS tw_satellite_radio,
  a.heatingtype_code AS heatingtype_code,
  a.income_producing_assets AS income_producing_assets,
  a.marketarea AS marketarea,
  a.phonenumbertype AS phonenumbertype,
  a.revolver_to_transactor AS revolver_to_transactor,
  a.second_property_ind AS second_property_ind,
  a.consumerstabilityrawscore AS consumerstabilityrawscore,
  a.consumerstabilitypercentile AS consumerstabilitypercentile,
  a.ailments AS ailments,
  CASE WHEN a.haspostal = 'N' AND a.mail_confidence <> '' AND a.mail_confidence IS NOT NULL THEN LEFT(a.mail_confidence, 1) ELSE a.mailscore END AS mailscore,
  a.populationdensity AS populationdensity,
  a.number_of_surnames AS number_of_surnames,
  a.homeowner AS homeowner,
  a.education_model AS education_model,
  a.education_model_ext AS education_model_ext,
  a.income_producing_assets_ext AS income_producing_assets_ext,
  a.titlecode_ext AS titlecode_ext,
  a.boatpropulsioncode_ext AS boatpropulsioncode_ext,
  a.vendorethnicity_ext AS vendorethnicity_ext,
  a.vendorreligion_ext AS vendorreligion_ext,
  a.vendorlanguage_ext AS vendorlanguage_ext,
  a.vendorethnicgroup_ext AS vendorethnicgroup_ext,
  a.countryoforign_ext AS countryoforign_ext,
  a.contributionamount AS contributionamount,
  a.lemsmatchcode AS lemsmatchcode,
  a.mortgagepresent AS mortgagepresent,
  a.discretionary_income_score AS discretionary_income_score,
  a.membercount AS membercount,
  a.sesi AS sesi,
  a.zipfourmatchlevel AS zipfourmatchlevel,
  a.no_stats_indicator AS no_stats_indicator,
  a.vacant_indicator AS vacant_indicator,
  a.timezone AS timezone,
  a.areaprefix AS areaprefix,
  a.correctivelensespresence AS correctivelensespresence,
  a.actualvacation_expense AS actualvacation_expense,
  a.vacation_expense AS vacation_expense,
  a.vacation_expense_dic AS vacation_expense_dic,
  a.nid AS nid,
  NVL(a.nid_description, '') AS nid_description,
  CASE
    WHEN a.fulfillmentflag IN ('B', 'M')
    AND a.mail_confidence IN ('1C', '1F', '1G', '2C', '2D', '2E')
    AND a.location_type NOT IN ('N', 'R')
    AND NOT (
      a.delivery_unit_size_raw BETWEEN 'E'
      AND 'O'
      AND TRIM(a.unit_number) = ''
    )
    AND NOT (
      (
        a.carrier_route_code BETWEEN 'R000'
        AND 'R999'
        OR a.carrier_route_code BETWEEN 'H000'
        AND 'H999'
      )
      AND TRIM(a.house_number) = ''
    ) THEN 'Y'
    ELSE 'N'
  END AS gui_probable,
  CASE
    WHEN a.fulfillmentflag IN ('B', 'M')
    AND a.mail_confidence IN (
      '1A',
      '1C',
      '1F',
      '1G',
      '1H',
      '2C',
      '2D',
      '2E',
      '2F',
      '2G',
      '2H',
      '3B'
    )
    AND a.location_type NOT IN ('N', 'R')
    AND NOT (
      a.delivery_unit_size_raw BETWEEN 'E'
      AND 'O'
      AND TRIM(a.unit_number) = ''
    )
    AND NOT (
      (
        a.carrier_route_code BETWEEN 'R000'
        AND 'R999'
        OR a.carrier_route_code BETWEEN 'H000'
        AND 'H999'
      )
      AND TRIM(a.house_number) = ''
    ) THEN 'Y'
    ELSE 'N'
  END AS gui_probable_mightbe,
  CASE
    WHEN a.fulfillmentflag IN ('B', 'M')
    AND a.mail_confidence IN ('1A', '1C', '1F', '1G', '1H')
    AND a.location_type NOT IN ('N', 'R')
    AND NOT (
      a.delivery_unit_size_raw BETWEEN 'E'
      AND 'O'
      AND TRIM(a.unit_number) = ''
    )
    AND NOT (
      (
        a.carrier_route_code BETWEEN 'R000'
        AND 'R999'
        OR a.carrier_route_code BETWEEN 'H000'
        AND 'H999'
      )
      AND TRIM(a.house_number) = ''
    ) THEN 'Y'
    ELSE 'N'
  END AS gui_accurate,
  CASE
    WHEN (
      a.fulfillmentflag IN ('T', 'B')
      OR a.do_not_call_flag IN ('4', '8', '9')
    ) THEN 'Y'
    ELSE 'N'
  END AS telemarketingflag,
  CASE
    WHEN (
      a.fulfillmentflag IN ('B')
      OR (
        a.fulfillmentflag IN ('M')
        AND a.do_not_call_flag IN ('4', '8', '9')
      )
    ) THEN 'Y'
    ELSE 'N'
  END AS phoneincludeallflag,
  NVL(b.segmentcode_a, '') AS segmentcode_a,
  NVL(b.segmentcode_e, '') AS segmentcode_e,
  NVL(b.segmentcode_f, '') AS segmentcode_f,
  NVL(b.segmentcode_g, '') AS segmentcode_g,
  NVL(b.segmentcode_h, '') AS segmentcode_h,
  NVL(b.segmentcode_i, '') AS segmentcode_i,
  NVL(b.segmentcode_j, '') AS segmentcode_j,
  NVL(a.opt_out_flag, '') AS opt_out_flag,
  NVL(a.emailaddress, '') AS emailaddress,
  NVL(a.vendor_code, '') AS vendor_code,
  NVL(a.categories, '') AS categories,
  NVL(a.domain, '') AS domain,
  NVL(a.top_level_domain, '') AS top_level_domain,
  NVL(a.opt_out_date, '') AS opt_out_date,
  NVL(a.car_make, '') AS car_make,
  NVL(a.car_model, '') AS car_model,
  NVL(a.car_year, '') AS car_year,
  NVL(a.date_entered_yyyymm, '') AS date_entered_yyyymm,
  NVL(a.email_domain, '') AS email_domain,
  NVL(a.open_flag, '') AS open_flag,
  NVL(a.open_date_yyyymm, '') AS open_date_yyyymm,
  a.click_flag AS click_flag,
  NVL(a.click_date_yyyymm, '') AS click_date_yyyymm,
  NVL(a.md5_email_lower, '') AS md5_email_lower,
  NVL(a.md5_email_upper, '') AS md5_email_upper,
 NVL(a.ipaddress,'') AS ipaddress,
  NVL(a.url, '') AS url,
  NVL(a.sha256_email, '') AS sha256_email,
  NVL(a.sha512_email, '') AS sha512_email,
  NVL(a.bv_flag, '') AS bv_flag,
  NVL(a.marigold, '') AS marigold,
  NVL(a.bvt_refresh_date, '') AS bvt_refresh_date,
  NVL(a.ipst_status_code, '') AS ipst_status_code,
  NVL(a.ipst_refresh_date, '') AS ipst_refresh_date,
  a.bestdate AS bestdate,
  NVL(a.reactivation_flag, '') AS reactivation_flag,
  NVL(a.mgen_match_flag, '') AS mgen_match_flag,
  NVL(a.bridge_code, '') AS bridge_code,
  NVL(a.best_date_range, '') AS best_date_range,
  NVL(a.haspostal, 'Y') AS haspostal,
  CASE WHEN a.haspostal = 'N' THEN 'N' ELSE 'Y' END AS infogroupcontact,
  NVL(a.matchcode_lrfs, '') AS matchcode_lrfs,
  CASE WHEN LTRIM(RTRIM(a.emailaddress)) <> '' and a.EmailAddress IS NOT NULL THEN 'Y' ELSE 'N' END AS cinclude,
  NVL(a.matchlevel, 'I') AS matchlevel,
  CASE WHEN a.individual_id is not null and 
            REGEXP_COUNT(a.individual_id,'^[0-9]+$')= 1 THEN a.individual_id::bigint 
       ELSE 0 END AS individual_id_bak,
  CASE WHEN a.company_id is not null and 
            REGEXP_COUNT(a.company_id,'^[0-9]+$')= 1 THEN a.company_id::bigint 
       ELSE 0 END AS company_id_bak,
  a.email_deliverable AS email_deliverable,
  a.email_marketable AS email_marketable,
  a.email_reputation_risk AS email_reputation_risk,
  a.email_deployable AS email_deployable,
  a.markettargetage18_44female AS markettargetage18_44female,
  a.pprofsnl AS pprofsnl,
  a.pwhitcol AS pwhitcol,
  a.pbluecol AS pbluecol,
  a.pcolgrad AS pcolgrad,
  a.routenumber AS routenumber,
  a.boxtype AS boxtype,
  a.boxnumber AS boxnumber,
  a.nameaddresssource AS nameaddresssource,
  a.mailorderbuyerflag AS mailorderbuyerflag,
  a.creditcard_bank AS creditcard_bank,
  a.markettargetage0_11 AS markettargetage0_11,
  a.markettargetage0_17 AS markettargetage0_17,
  a.markettargetage18_24female AS markettargetage18_24female,
  a.markettargetage18_24male AS markettargetage18_24male,
  a.markettargetage18_34female AS markettargetage18_34female,
  a.markettargetage18_34male AS markettargetage18_34male,
  a.markettargetage25_34female AS markettargetage25_34female,
  a.markettargetage25_34male AS markettargetage25_34male,
  a.markettargetage35_44female AS markettargetage35_44female,
  a.markettargetage35_44male AS markettargetage35_44male,
  a.markettargetage35_54female AS markettargetage35_54female,
  a.markettargetage35_54male AS markettargetage35_54male,
  a.markettargetage45_54female AS markettargetage45_54female,
  a.markettargetage45_54male AS markettargetage45_54male,
  a.markettargetage50plus AS markettargetage50plus,
  a.markettargetage55_64female AS markettargetage55_64female,
  a.markettargetage55_64male AS markettargetage55_64male,
  a.markettargetage65plus AS markettargetage65plus,
  a.householdsize1_2 AS householdsize1_2,
  a.householdsize3plus AS householdsize3plus,
  a.phonesourcecat AS phonesourcecat,
  a.secondaryareacode AS secondaryareacode,
  a.secondaryphone AS secondaryphone,
  a.numberoffireplaces AS numberoffireplaces,
  a.pool AS pool,
  a.bedroomcount AS bedroomcount,
  a.bathroomcount AS bathroomcount,
  a.garagetype AS garagetype,
  a.homeownersource AS homeownersource,
  a.salepricesource AS salepricesource,
  a.saledatesource AS saledatesource,
  a.roomcount AS roomcount,
  a.multipleunit AS multipleunit,
  a.purchasing_power_indicator AS purchasing_power_indicator,
  CASE WHEN LTRIM(RTRIM(a.area_code)) LIKE '0%' THEN ''
       ELSE LTRIM(RTRIM(a.area_code)) + LTRIM(RTRIM(a.phonenumber)) END AS phone_number,
  NVL(LTRIM(RTRIM(a.census_state_code_2010)),'') + NVL(LTRIM(RTRIM(a.censuscountycode2010)),'') AS fips_code,
   SUBSTRING (a.lifestyle, 1, 1) AS lifestyle01,
   SUBSTRING (a.lifestyle, 2, 1) AS lifestyle02,
   SUBSTRING (a.lifestyle, 3, 1) AS lifestyle03,
   SUBSTRING (a.lifestyle, 4, 1) AS lifestyle04,
   SUBSTRING (a.lifestyle, 5, 1) AS lifestyle05,
   SUBSTRING (a.lifestyle, 6, 1) AS lifestyle06,
   SUBSTRING (a.lifestyle, 7, 1) AS lifestyle07,
   SUBSTRING (a.lifestyle, 8, 1) AS lifestyle08,
   SUBSTRING (a.lifestyle, 9, 1) AS lifestyle09,
   SUBSTRING (a.lifestyle, 10, 1) AS lifestyle10,
   SUBSTRING (a.lifestyle, 11, 1) AS lifestyle11,
   SUBSTRING (a.lifestyle, 12, 1) AS lifestyle12,
   SUBSTRING (a.lifestyle, 13, 1) AS lifestyle13,
   SUBSTRING (a.lifestyle, 14, 1) AS lifestyle14,
   SUBSTRING (a.lifestyle, 15, 1) AS lifestyle15,
   SUBSTRING (a.lifestyle, 16, 1) AS lifestyle16,
   SUBSTRING (a.lifestyle, 17, 1) AS lifestyle17,
   SUBSTRING (a.lifestyle, 18, 1) AS lifestyle18,
   SUBSTRING (a.lifestyle, 19, 1) AS lifestyle19,
   SUBSTRING (a.lifestyle, 20, 1) AS lifestyle20,
   SUBSTRING (a.lifestyle, 21, 1) AS lifestyle21,
   SUBSTRING (a.lifestyle, 22, 1) AS lifestyle22,
   SUBSTRING (a.lifestyle, 23, 1) AS lifestyle23,
   SUBSTRING (a.lifestyle, 24, 1) AS lifestyle24,
   SUBSTRING (a.lifestyle, 25, 1) AS lifestyle25,
   SUBSTRING (a.lifestyle, 26, 1) AS lifestyle26,
   SUBSTRING (a.lifestyle, 27, 1) AS lifestyle27,
   SUBSTRING (a.lifestyle, 28, 1) AS lifestyle28,
   SUBSTRING (a.lifestyle, 29, 1) AS lifestyle29,
   SUBSTRING (a.lifestyle, 30, 1) AS lifestyle30,
   SUBSTRING (a.lifestyle, 31, 1) AS lifestyle31,
   SUBSTRING (a.lifestyle, 32, 1) AS lifestyle32,
   SUBSTRING (a.lifestyle, 33, 1) AS lifestyle33,
   SUBSTRING (a.lifestyle, 34, 1) AS lifestyle34,
   SUBSTRING (a.lifestyle, 35, 1) AS lifestyle35,
  SUBSTRING (a.ailments, 1, 1) AS ailments_1,
  SUBSTRING (a.ailments, 2, 1) AS ailments_2,
  SUBSTRING (a.ailments, 3, 1) AS ailments_3,
  SUBSTRING (a.ailments, 4, 1) AS ailments_4,
  SUBSTRING (a.ailments, 5, 1) AS ailments_5,
  SUBSTRING (a.ailments, 6, 1) AS ailments_6,
  SUBSTRING (a.ailments, 7, 1) AS ailments_7,
  SUBSTRING (a.ailments, 8, 1) AS ailments_8,
  SUBSTRING (a.ailments, 9, 1) AS ailments_9,
  SUBSTRING (a.ailments, 10, 1) AS ailments_10,
  SUBSTRING (a.ailments, 11, 1) AS ailments_11,
  SUBSTRING (a.ailments, 12, 1) AS ailments_12,
  SUBSTRING (a.ailments, 13, 1) AS ailments_13,
  SUBSTRING (a.ailments, 14, 1) AS ailments_14,
  SUBSTRING (a.ailments, 15, 1) AS ailments_15,
  NVL(LTRIM(RTRIM(a.census_state_code_2010)),'') + NVL(LTRIM(RTRIM(a.censuscountycode2010)),'') + NVL(LTRIM(RTRIM(a.census_tract_2010)),'') + NVL(LTRIM(RTRIM(a.census_block_group_2010)),'') AS census_combined_code,--*****
  NVL(LTRIM(RTRIM(a.census_state_code_2010)),'') + NVL(LTRIM(RTRIM(a.censuscountycode2010)),'') + NVL(LTRIM(RTRIM(a.census_tract_2010)),'') AS census_state_country_tract,---*************
  CASE
    WHEN LTRIM(RTRIM(a.firstname)) + LTRIM(RTRIM(a.middleinitial)) + LTRIM(RTRIM(a.lastname)) IS NOT NULL THEN LTRIM(RTRIM(a.firstname)) + LTRIM(RTRIM(a.middleinitial)) + LTRIM(RTRIM(a.lastname))
    ELSE ''
  END AS contact_name,
 SUBSTRING (a.donor_code, 1, 1) AS donor_code1,
 SUBSTRING (a.donor_code, 2, 1) AS donor_code2,
 substring (a.donor_code, 3, 1) AS donor_code3,
 substring (a.donor_code, 4, 1) AS donor_code4,
 substring (a.donor_code, 5, 1) AS donor_code5,
  CASE
    WHEN a.zip + a.zip_four IS NOT NULL THEN a.zip + a.zip_four
    ELSE ''
  END AS zip9,
  CASE WHEN a.mortgage_interest_rates_old IS NULL OR a.mortgage_interest_rates_old= '' THEN ''
     WHEN a.mortgage_interest_rates_old IS NOT NULL OR a.mortgage_interest_rates_old <> '' THEN LEFT(a.mortgage_interest_rates_old, 2) + '.' + RIGHT(a.mortgage_interest_rates_old, 2)
  END AS mortgage_interest_rates,
  NVL(REPLACE(a.addressline1 + a.city + a.state + a.zip,' ',''),'') AS hardkey,
  NVL(LTRIM(RTRIM(a.census_state_code_2010)),'') + NVL(LTRIM(RTRIM(a.county_code)), '') AS countycodebystatecode,
  NVL(a.zip + a.carrier_route_code, '') AS carrierroutecodebyzipcode,
  SUBSTRING(a.birth_date, 5, 2) AS birthmonth,
  CASE
    WHEN LEN(a.birth_date) = 8 THEN SUBSTRING(a.birth_date, 7, 2)
    ELSE ''
  END AS birthdayofmonth,
  LEFT(a.birth_date, 6) AS birthyearmonth,
  NVL(a.state + a.city, '') AS statecity,
  NVL(LTRIM(RTRIM(a.firstname)) + ' ' + LTRIM(RTRIM(a.lastname)), '') AS fullname,
  NVL(a.title, '') AS title,
  CASE
    WHEN a.location_type NOT IN ('N', 'R')
    AND NOT (
      a.delivery_unit_size_raw BETWEEN 'E'
      AND 'O'
      AND TRIM(a.unit_number) = ''
    )
    AND NOT (
      (
        a.carrier_route_code BETWEEN 'R000'
        AND 'R999'
        OR a.carrier_route_code BETWEEN 'H000'
        AND 'H999'
      )
      AND TRIM(a.house_number) = ''
    ) THEN 'Y'
    ELSE 'N'
  END AS gui_commonsuppression,
  NVL(LTRIM(RTRIM(a.state))+LTRIM(RTRIM(b.ccounty)),'') AS statecountyname,
  NVL(a.state + LTRIM(RTRIM(UPPER(a.city))) + LTRIM(RTRIM(a.zip)), '') AS statecityzip,
  NVL(RIGHT(a.zip, 1), '') AS zipend,
  NVL(a.state + LTRIM(RTRIM(UPPER(a.city))) + LTRIM(RTRIM(a.scf)),'') AS statecityscf,
  NVL(a.State + a.County_Code, '') AS statecountycode,
  CASE
    WHEN a.unit_number <> '' THEN 'Y'
    ELSE 'N'
  END AS unit_number_flag,
  NVL(
    LTRIM(RTRIM(a.addressline1)) + LTRIM(RTRIM(a.zip)),
    ''
  ) AS addresszip,
  NVL(LTRIM(RTRIM(a.state)),'') + NVL(LTRIM(RTRIM(a.nid_description)),'') AS stateneighborhood,---******
  NVL(LTRIM(RTRIM(a.state)),'') + NVL(LTRIM(RTRIM(a.city)),'') + NVL(LTRIM(RTRIM(a.nid_description)), '') AS statecityneighborhood,---***
  NVL(b.mailorderbuyer_01, '') AS mailorderbuyer_01,
  NVL(b.mailorderbuyer_02, '') AS mailorderbuyer_02,
  NVL(b.mailorderbuyer_03, '') AS mailorderbuyer_03,
  NVL(b.mailorderbuyer_04, '') AS mailorderbuyer_04,
  NVL(b.mailorderbuyer_05, '') AS mailorderbuyer_05,
  NVL(b.mailorderbuyer_06, '') AS mailorderbuyer_06,
  NVL(b.mailorderbuyer_07, '') AS mailorderbuyer_07,
  NVL(b.mailorderbuyer_08, '') AS mailorderbuyer_08,
  NVL(b.mailorderbuyer_09, '') AS mailorderbuyer_09,
  NVL(b.mailorderbuyer_10, '') AS mailorderbuyer_10,
  NVL(b.mailorderbuyer_11, '') AS mailorderbuyer_11,
  NVL(b.mailorderbuyer_12, '') AS mailorderbuyer_12,
  NVL(b.mailorderbuyer_13, '') AS mailorderbuyer_13,
  NVL(b.mailorderbuyer_14, '') AS mailorderbuyer_14,
  NVL(b.mailorderbuyer_15, '') AS mailorderbuyer_15,
  NVL(b.mailorderbuyer_16, '') AS mailorderbuyer_16,
  NVL(b.mailorderbuyer_18, '') AS mailorderbuyer_18,
 GETMATCHCODE(REPLACE(NVL(A.firstname, ''), '|', ''),
    REPLACE(NVL(a.lastname, ''), '|', ''),
    REPLACE(NVL(a.addressline1, ''), '|', ''),
    REPLACE(
      CASE
        WHEN a.zip + a.zip_four IS NOT NULL THEN a.zip + a.zip_four
        ELSE ''
      END,
      '|',
      ''
    ),
    '',
    'I'
  ) AS individual_mc,
  GETMATCHCODE(
    REPLACE(NVL(A.firstname, ''), '|', ''),
    REPLACE(NVL(a.lastname, ''), '|', ''),
    REPLACE(NVL(a.addressline1, ''), '|', ''),
    CASE
      WHEN a.zip + a.zip_four IS NOT NULL THEN a.zip + a.zip_four
      ELSE ''
    END,
    REPLACE(NVL(a.lastname, ''), '|', ''),
    'C'
  ) AS company_mc,
  GETMATCHCODE(
    '',
    '',
    REPLACE(NVL(A.addressline1, ''), '|', ''),
    REPLACE(NVL(A.zip, ''), '|', ''),
    '',
    'I'
  ) AS address_mc,
  CASE WHEN b.city IS NOT NULL THEN LTRIM(RTRIM(b.city)) || ' ' || a.state 
     ELSE LTRIM(RTRIM(a.city)) || ' ' || a.state END AS preferredcity,
  b.cellphone AS cellphone,
  b.cellphone_areacode AS cellphone_areacode,
  b.cellphone_number AS cellphone_number,
  b.cellphone_cordcutter AS cellphone_cordcutter,
  b.cellphone_prepaid_indicator AS cellphone_prepaid_indicator,
  b.cellphone_donotcallflag AS cellphone_donotcallflag,
  b.cellphone_verified_code AS cellphone_verified_code,
  b.cellphone_activitystatus AS cellphone_activitystatus,
  b.cellphone_filterflag AS cellphone_filterflag,
  b.cellphone_filterreason AS cellphone_filterreason,
  b.cellphone_isdqi AS cellphone_isdqi,
  b.cellphone_matchlevel AS cellphone_matchlevel,
  b.cellphone_matchscore AS cellphone_matchscore,
  b.cellphone_modifiedscore AS cellphone_modifiedscore,
  b.cellphone_individualmatch AS cellphone_individualmatch,
  NVL(b.nielsen_county_rank_description,'') AS nielsen_county_rank_description,
  NVL(b.income_description,'') AS income_description,
  NVL(b.age_code_description,'') AS age_code_description,
  NVL(b.home_equity_estimate_description,'') AS home_equity_estimate_description,
  NVL(b.home_sale_price_code_description,'') AS home_sale_price_code_description,
  NVL(b.home_value_code_description,'') AS home_value_code_description,
  NVL(b.own_rent_indicator_description,'') AS own_rent_indicator_description,
  NVL(b.expendable_income_rank_description,'') AS expendable_income_rank_description,
  NVL(b.mortgage_amount_description,'') AS mortgage_amount_description,
  NVL(b.mortgage_loan_type_description,'') AS mortgage_loan_type_description,
  NVL(b.loan_to_value_ratio_ltv_description,'') AS loan_to_value_ratio_ltv_description,
  NVL(a.censusstatecode2020,'') AS censusstatecode2020,
	NVL(a.censuscountycode2020,'') AS censuscountycode2020,
	NVL(a.censustract2020,'') AS censustract2020,
	NVL(a.censusblockgroup2020,'') AS censusblockgroup2020,
	NVL(a.secondmortgageamount,'') AS secondmortgageamount,
	NVL(a.secondmortgagetypecode,'') AS secondmortgagetypecode,
	NVL(a.lot_size_sqfeet,'') AS lot_size_sqfeet,
	NVL(a.universal_sqfeet,'') AS universal_sqfeet,
	NVL(a.universal_sqfeet_ind,'') AS universal_sqfeet_ind,
	NVL(a.living_sqfeet,'') AS living_sqfeet,
	NVL(a.groundfloor_sqfeet,'') AS groundfloor_sqfeet,
	NVL(a.gross_sqfeet,'') AS gross_sqfeet,
	NVL(a.adjustedgross_sqfeet,'') AS adjustedgross_sqfeet,
	NVL(a.basement_sqfeet,'') AS basement_sqfeet,
	NVL(a.garage_sqfeet,'') AS garage_sqfeet,
	NVL(a.fullbath,'') AS fullbath,
	NVL(a.halfbath,'') AS halfbath,
	NVL(a.fireplace_ind,'') AS fireplace_ind,
	NVL(a.fireplacetype,'') AS fireplacetype,
	NVL(a.pooltype,'') AS pooltype,
	NVL(a.roofcover,'') AS roofcover,
	NVL(a.roofshape,'') AS roofshape,
	NVL(a.conditiontype,'') AS conditiontype,
	NVL(a.exteriorwalltype,'') AS exteriorwalltype,
	NVL(a.foundationtype,'') AS foundationtype,
	NVL(a.constructionquality,'') AS constructionquality,
	NVL(a.stories,'') AS stories,
	NVL(a.styletype,'') AS styletype,
	NVL(a.airconditioningtype,'') AS airconditioningtype,
	NVL(a.electricwiringtype,'') AS electricwiringtype,
	NVL(a.fueltype,'') AS fueltype,
	NVL(a.sewertype,'') AS sewertype,
	NVL(a.watertype,'') AS watertype,
	NVL(a.construction_type_detail,'') AS construction_type_detail,
	NVL(a.heating_type_detail,'') AS heating_type_detail
FROM {new-load-table} a
LEFT JOIN IG_Consumer_Child_Combined b
ON a.id = b.id;

--drop intermediate tables
DROP TABLE IF EXISTS IG_CONSUMER_NEW_DDValues_Load;
DROP TABLE IF EXISTS IG_CONSUMER_NEW_DDValues_Clean;
DROP TABLE IF EXISTS IG_CONSUMER_NEW_Segcodes_Load;
DROP TABLE IF EXISTS IG_CONSUMER_MailOrderBuyer_ToBeDropped;
DROP TABLE IF EXISTS IG_CONSUMER_NEW_PreferredCity_Load;
DROP TABLE IF EXISTS IG_CONSUMER_NEW_DDValues;
DROP TABLE IF EXISTS MailOrderBuyer;
DROP TABLE IF EXISTS SEGMENTCODE;
DROP TABLE IF EXISTS IG_CONSUMER_EXCLUDE_DONOTCALLFLAG;
DROP TABLE IF EXISTS IG_CONSUMER_Cellphone;
DROP TABLE IF EXISTS IG_CONSUMER_Countycode;
DROP TABLE IF EXISTS IG_CONSUMER_NEW_PreferredCity;


UPDATE IG_CONSUMER_NEW_TEMP
SET FulFillmentFlag = 'Z',
GUI_Probable = 'N',
GUI_Probable_MightBe = 'N',
GUI_Accurate = 'N',
PhoneIncludeAllFlag = 'N',
TeleMarketingFlag = 'N'
FROM IG_CONSUMER_NEW_TEMP tblMain 
INNER JOIN  CONSUMER_1267_TEMPSUPPRESS_ToBeDropped B 
ON B.individual_id = tblMain.individual_id
WHERE tblMain.FulFillmentFlag <> 'Z';

drop table if exists CONSUMER_1267_TEMPSUPPRESS_ToBeDropped;

UPDATE IG_CONSUMER_NEW_TEMP
SET Do_Not_Call_Flag = B.cValue  
FROM IG_CONSUMER_NEW_TEMP tblMain 
INNER JOIN CONSUMER_1267_TEMPFILE_ToBeDropped  B 
ON B.company_id = tblMain.company_id 
WHERE tblMain.Do_Not_Call_Flag <> B.cValue;

UPDATE tblDQI_CellPhone 
SET Donotcallflag = B.cvalue
FROM tblDQI_CellPhone A 
INNER JOIN CONSUMER_1267_TEMPFILE_ToBeDropped B 
ON A.company_id = B.company_id_int
WHERE A.Donotcallflag <> '8' AND B.cvalue = '8';

UPDATE IG_CONSUMER_NEW_TEMP 
SET CellPhone_Donotcallflag = B.Donotcallflag
FROM IG_CONSUMER_NEW_TEMP tblMain  
INNER JOIN tblDQI_CellPhone B 
ON tblMain.CellPhone_Number = B.CellPhone_Number; 

UPDATE IG_CONSUMER_NEW_TEMP 
SET PhoneIncludeAllFlag = 'Y'
FROM IG_CONSUMER_NEW_TEMP tblMain 
WHERE (FULFILLMENTFLAG IN ('B') OR (FULFILLMENTFLAG IN ('M') AND DO_NOT_CALL_FLAG IN ('4','8','9')));

UPDATE IG_CONSUMER_NEW_TEMP 
SET TeleMarketingFlag = 'Y'
FROM IG_CONSUMER_NEW_TEMP tblMain
WHERE (FULFILLMENTFLAG IN ('T','B') OR  DO_NOT_CALL_FLAG IN ('4','8','9'));

DROP TABLE IF EXISTS CONSUMER_1267_TEMPFILE_ToBeDropped;
