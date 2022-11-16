DROP TABLE IF EXISTS nosuchtable;
DROP TABLE IF EXISTS temp_tblChild14_{build_id}_{build};
CREATE TABLE temp_tblChild14_{build_id}_{build}
AS 
SELECT 
    id as id,
    ','+ CASE WHEN tw_greenmodel IN ('7','8','9') THEN '1,' ELSE '' END  +
    CASE WHEN tw_highvaluesecurityinvestor  IN ('7','8','9') THEN '2,' ELSE '' END  +
    CASE WHEN tw_highereducation  IN ('7','8','9') THEN '3,' ELSE '' END  +
    CASE WHEN tw_highvaluestockinvestor IN ('7','8','9') THEN '4,' ELSE '' END  +
    CASE WHEN tw_heavyinvestmenttraders IN ('7','8','9') THEN '5,' ELSE '' END  +
    CASE WHEN tw_physicalfitnessclubs IN ('7','8','9') THEN '6,' ELSE '' END  +
    CASE WHEN tw_highriskinvestor IN ('7','8','9') THEN '7,' ELSE '' END  +
    CASE WHEN tw_lowriskinvestor  IN ('7','8','9') THEN '8,' ELSE '' END  +
    CASE WHEN tw_frequentbusinesstraveler IN ('7','8','9') THEN '9,' ELSE '' END  +
    CASE WHEN tw_frequentflyer  IN ('7','8','9') THEN '10,' ELSE '' END  +  
    CASE WHEN tw_annuities  IN ('7','8','9') THEN '11,' ELSE '' END  +  
    CASE WHEN tw_lifeinsurance  IN ('7','8','9') THEN '12,' ELSE '' END  +  
    CASE WHEN tw_luxurycarbuyer IN ('7','8','9') THEN '13,' ELSE '' END  +  
    CASE WHEN tw_cruise IN ('7','8','9') THEN '14,' ELSE '' END  +  
    CASE WHEN tw_timeshareowner IN ('7','8','9') THEN '15,' ELSE '' END  +  
    CASE WHEN tw_impulsebuyer IN ('7','8','9') THEN '16,' ELSE '' END  +  
    CASE WHEN tw_luxuryhotel  IN ('7','8','9') THEN '17,' ELSE '' END  +  
    CASE WHEN tw_pbsdonor IN ('7','8','9') THEN '18,' ELSE '' END  +  
    CASE WHEN tw_safetysecurityconscious  IN ('7','8','9') THEN '19,' ELSE '' END  +  
    CASE WHEN tw_shopaholics  IN ('7','8','9') THEN '20,' ELSE '' END  +  
    CASE WHEN tw_avidthemeparkvisitor IN ('7','8','9') THEN '21,' ELSE '' END  +  
    CASE WHEN tw_foreigntravelvacation  IN ('7','8','9') THEN '22,' ELSE '' END  +  
    CASE WHEN tw_conservative IN ('7','8','9') THEN '23,' ELSE '' END  +  
    CASE WHEN tw_leaningconservative  IN ('7','8','9') THEN '24,' ELSE '' END  +  
    CASE WHEN tw_leaningliberal IN ('7','8','9') THEN '25,' ELSE '' END  +  
    CASE WHEN tw_religiousdonor IN ('7','8','9') THEN '26,' ELSE '' END  +  
    CASE WHEN tw_gardenmaintenance  IN ('7','8','9') THEN '27,' ELSE '' END  +  
    CASE WHEN tw_heavyfamilyrestaurantvisitor IN ('7','8','9') THEN '28,' ELSE '' END  +  
    CASE WHEN tw_heavypayperviewsports  IN ('7','8','9') THEN '29,' ELSE '' END  +  
    CASE WHEN tw_nAScar IN ('7','8','9') THEN '30,' ELSE '' END  +  
    CASE WHEN tw_liberal  IN ('7','8','9') THEN '31,' ELSE '' END  +  
    CASE WHEN tw_dogproducts  IN ('7','8','9') THEN '32,' ELSE '' END  +  
    CASE WHEN tw_heavypayperviewmovies  IN ('7','8','9') THEN '33,' ELSE '' END  +  
    CASE WHEN tw_dietproducts IN ('7','8','9') THEN '34,' ELSE '' END  +  
    CASE WHEN tw_organicfoods IN ('7','8','9') THEN '35,' ELSE '' END  +  
    CASE WHEN tw_heavyvitaminanddietarysupplement IN ('7','8','9') THEN '36,' ELSE '' END  +  
    CASE WHEN tw_babyproducts IN ('7','8','9') THEN '37,' ELSE '' END  +  
    CASE WHEN tw_heavycouponuser  IN ('7','8','9') THEN '38,' ELSE '' END  +  
    CASE WHEN tw_opinionleaders IN ('7','8','9') THEN '39,' ELSE '' END  +  
    CASE WHEN tw_catproducts  IN ('7','8','9') THEN '40,' ELSE '' END  +  
    CASE WHEN tw_creditcardrewards  IN ('7','8','9') THEN '41,' ELSE '' END  +  
    CASE WHEN tw_highENDsportingequipment IN ('7','8','9') THEN '42,' ELSE '' END  +  
    CASE WHEN tw_homeimprovement  IN ('7','8','9') THEN '43,' ELSE '' END  +  
    CASE WHEN tw_highENDelectronicsbuyer  IN ('7','8','9') THEN '44,' ELSE '' END  +  
    CASE WHEN tw_avidgamer  IN ('7','8','9') THEN '45,' ELSE '' END  +  
    CASE WHEN tw_rentalcar  IN ('7','8','9') THEN '46,' ELSE '' END  +  
    CASE WHEN tw_avidcellphoneuser  IN ('7','8','9') THEN '47,' ELSE '' END  +  
    CASE WHEN tw_countryclubmember  IN ('7','8','9') THEN '48,' ELSE '' END  +  
    CASE WHEN tw_professionaltaxpreparation IN ('7','8','9') THEN '49,' ELSE '' END  +  
    CASE WHEN tw_onsitetaxpreparationservice  IN ('7','8','9') THEN '50,' ELSE '' END  +  
    CASE WHEN tw_clASsicalmusicconcerts IN ('7','8','9') THEN '51,' ELSE '' END  +  
    CASE WHEN tw_rockmusicconcerts  IN ('7','8','9') THEN '52,' ELSE '' END  +  
    CASE WHEN tw_countrymusicconcerts IN ('7','8','9') THEN '53,' ELSE '' END  +  
    CASE WHEN tw_livetheater  IN ('7','8','9') THEN '54,' ELSE '' END  +  
    CASE WHEN tw_heavybookbuyer IN ('7','8','9') THEN '55,' ELSE '' END  +
    CASE WHEN tw_freshwaterfishing  IN ('7','8','9') THEN '56,' ELSE '' END  +
    CASE WHEN tw_saltwaterfishing IN ('7','8','9') THEN '57,' ELSE '' END  +
    CASE WHEN tw_hunting  IN ('7','8','9') THEN '58,' ELSE '' END  +
    CASE WHEN tw_allterrainvehicle  IN ('7','8','9') THEN '59,' ELSE '' END  +
    CASE WHEN tw_powerboating IN ('7','8','9') THEN '60,' ELSE '' END  +
    CASE WHEN tw_outdooractivities  IN ('7','8','9') THEN '61,' ELSE '' END  +
    CASE WHEN tw_adventureseekers IN ('7','8','9') THEN '62,' ELSE '' END  +
    CASE WHEN tw_lowENDsportingequipment  IN ('7','8','9') THEN '63,' ELSE '' END  +
    CASE WHEN tw_professionalbASeballsportsfans IN ('7','8','9') THEN '64,' ELSE '' END  +
    CASE WHEN tw_professionalbASketballsportsfans IN ('7','8','9') THEN '65,' ELSE '' END  +
    CASE WHEN tw_professionalfootballsportsfans IN ('7','8','9') THEN '66,' ELSE '' END  +
    CASE WHEN tw_soccersportsfans IN ('7','8','9') THEN '67,' ELSE '' END  +
    CASE WHEN tw_collegebASketballsportsfans  IN ('7','8','9') THEN '68,' ELSE '' END  +
    CASE WHEN tw_collegefootballsportsfans  IN ('7','8','9') THEN '69,' ELSE '' END  +
    CASE WHEN tw_golfsportsfans IN ('7','8','9') THEN '70,' ELSE '' END  +
    CASE WHEN tw_tennissportsfans IN ('7','8','9') THEN '71,' ELSE '' END  +
    CASE WHEN tw_professionalwrestlingsportsfans  IN ('7','8','9') THEN '72,' ELSE '' END  +
    CASE WHEN tw_internationallongdistance  IN ('7','8','9') THEN '73,' ELSE '' END  +
    CASE WHEN tw_heavyfrozENDinner  IN ('7','8','9') THEN '74,' ELSE '' END  +
    CASE WHEN tw_cookfromscratch  IN ('7','8','9') THEN '75,' ELSE '' END  +
    CASE WHEN tw_cookforfun IN ('7','8','9') THEN '76,' ELSE '' END  +
    CASE WHEN tw_winelover  IN ('7','8','9') THEN '77,' ELSE '' END  +
    CASE WHEN tw_onlinepurchASepersonal IN ('7','8','9') THEN '78,' ELSE '' END  +
    CASE WHEN tw_onlinepurchASebusiness IN ('7','8','9') THEN '79,' ELSE '' END  +
    CASE WHEN tw_onlinetravelplan IN ('7','8','9') THEN '80,' ELSE '' END  +
    CASE WHEN tw_blogwriting  IN ('7','8','9') THEN '81,' ELSE '' END  +
    CASE WHEN tw_voiceoverinternet  IN ('7','8','9') THEN '82,' ELSE '' END  +
    CASE WHEN tw_wifioutsideofhome  IN ('7','8','9') THEN '83,' ELSE '' END  +
    CASE WHEN tw_wifiinhome IN ('7','8','9') THEN '84,' ELSE '' END  +
    CASE WHEN tw_wholesaleclub  IN ('7','8','9') THEN '85,' ELSE '' END  +
    CASE WHEN tw_autoclub IN ('7','8','9') THEN '86,' ELSE '' END  +
    CASE WHEN tw_diyautomaintenance IN ('7','8','9') THEN '87,' ELSE '' END  +
    CASE WHEN tw_onlinegamingactivity IN ('7','8','9') THEN '88,' ELSE '' END  +
    CASE WHEN tw_onlineinvestmenttrading  IN ('7','8','9') THEN '89,' ELSE '' END  +
    CASE WHEN tw_onlinebillpayment  IN ('7','8','9') THEN '90,' ELSE '' END  +
    CASE WHEN tw_onlinetvdownload IN ('7','8','9') THEN '91,' ELSE '' END  +
    CASE WHEN tw_mobileinternetaccess IN ('7','8','9') THEN '92,' ELSE '' END  +
    CASE WHEN tw_socialmedianetwork IN ('7','8','9') THEN '93,' ELSE '' END  +
    CASE WHEN tw_specialtyorganicfoodstore  IN ('7','8','9') THEN '94,' ELSE '' END  +
    CASE WHEN tw_homeoffice IN ('7','8','9') THEN '95,' ELSE '' END  +
    CASE WHEN tw_businessbanking  IN ('7','8','9') THEN '96,' ELSE '' END  +
    CASE WHEN tw_moderate_economyhotel  IN ('7','8','9') THEN '97,' ELSE '' END  +
    CASE WHEN tw_e_reader IN ('7','8','9') THEN '98,' ELSE '' END  +
    CASE WHEN tw_cellphoneonlymodel IN ('7','8','9') THEN '99,' ELSE '' END  +
    CASE WHEN tw_alternativemedicine  IN ('7','8','9') THEN '100,' ELSE '' END  +
    CASE WHEN tw_non_religiousdonor IN ('7','8','9') THEN '101,' ELSE '' END  +
    CASE WHEN tw_majorhomeremodeling IN ('7','8','9') THEN '102,' ELSE '' END  +
    CASE WHEN tw_avidsmartphoneusers  IN ('7','8','9') THEN '103,' ELSE '' END  +
    CASE WHEN tw_healthinsurance  IN ('7','8','9') THEN '104,' ELSE '' END  +
    CASE WHEN tw_satellitetv  IN ('7','8','9') THEN '105,' ELSE '' END  +
    CASE WHEN tw_onlinemusicdownload  IN ('7','8','9') THEN '106,' ELSE '' END  +
    CASE WHEN tw_adulteducation IN ('7','8','9') THEN '107,' ELSE '' END  +
    CASE WHEN tw_pilates_yoga IN ('7','8','9') THEN '108,' ELSE '' END  +
    CASE WHEN tw_fAStfood IN ('7','8','9') THEN '109,' ELSE '' END  +
    CASE WHEN tw_hybridcars IN ('7','8','9') THEN '110,' ELSE '' END  +
    CASE WHEN tw_heavy_online_buyer IN ('7','8','9') THEN '111,' ELSE '' END  +
    CASE WHEN tw_high_END_appparel  IN ('7','8','9') THEN '112,' ELSE '' END  +
    CASE WHEN tw_heavy_catalog_buyer  IN ('7','8','9') THEN '113,' ELSE '' END  +
    CASE WHEN tw_gift_buyers  IN ('7','8','9') THEN '114,' ELSE '' END  +
    CASE WHEN tw_camping  IN ('7','8','9') THEN '115,' ELSE '' END  +
    CASE WHEN tw_sports_fanatics  IN ('7','8','9') THEN '116,' ELSE '' END  +
    CASE WHEN tw_financial_planner IN ('7','8','9') THEN '117,' ELSE '' END  +
    CASE WHEN tw_work_health_insurance  IN ('7','8','9') THEN '118,' ELSE '' END  +
    CASE WHEN tw_comprehensive_auto_insurance IN ('7','8','9') THEN '119,' ELSE '' END  +
    CASE WHEN tw_small_business_insurance IN ('7','8','9') THEN '120,' ELSE '' END  +
    CASE WHEN tw_heavy_snack_eaters IN ('7','8','9') THEN '121,' ELSE '' END  +
    CASE WHEN tw_heavy_domestic_travelers IN ('7','8','9') THEN '122,' ELSE '' END  +
    CASE WHEN tw_new_vehicle_buyer  IN ('7','8','9') THEN '123,' ELSE '' END  +
    CASE WHEN tw_suv_buyer  IN ('7','8','9') THEN '124,' ELSE '' END  +
    CASE WHEN tw_minivan_buyer  IN ('7','8','9') THEN '125,' ELSE '' END  +
    CASE WHEN tw_auto_loan  IN ('7','8','9') THEN '126,' ELSE '' END  +
    CASE WHEN tw_education_loan IN ('7','8','9') THEN '127,' ELSE '' END  +
    CASE WHEN tw_real_estate_investment IN ('7','8','9') THEN '128,' ELSE '' END  +
    CASE WHEN tw_fantASy_sports IN ('7','8','9') THEN '129,' ELSE '' END  +
    CASE WHEN tw_hockey_buyer IN ('7','8','9') THEN '130,' ELSE '' END  +
    CASE WHEN tw_auto_leASe IN ('7','8','9') THEN '131,' ELSE '' END  +
    CASE WHEN tw_digital_payment  IN ('7','8','9') THEN '132,' ELSE '' END  +
    CASE WHEN tw_high_mileage_usage IN ('7','8','9') THEN '133,' ELSE '' END  +
    CASE WHEN tw_motorcycle_owner IN ('7','8','9') THEN '134,' ELSE '' END  +
    CASE WHEN tw_union_member IN ('7','8','9') THEN '135,' ELSE '' END  +
    CASE WHEN tw_national_park_visitor  IN ('7','8','9') THEN '136,' ELSE '' END  +
    CASE WHEN tw_online_streaming_and_devices IN ('7','8','9') THEN '137,' ELSE '' END  +
    CASE WHEN tw_road_trip  IN ('7','8','9') THEN '138,' ELSE '' END  +
    CASE WHEN tw_social_networking_ad_click IN ('7','8','9') THEN '139,' ELSE '' END  +
    CASE WHEN tw_satellite_radio  IN ('7','8','9') THEN '140,' ELSE '' END as tw_flags,
    ','+ CASE WHEN lifestyle01 = '1' THEN '1,' ELSE '' END +
    CASE WHEN lifestyle02 = '1' THEN '2,' ELSE '' END +
    CASE WHEN lifestyle03 = '1' THEN '3,' ELSE '' END +
    CASE WHEN lifestyle04 = '1' THEN '4,' ELSE '' END +
    CASE WHEN lifestyle05 = '1' THEN '5,' ELSE '' END +
    CASE WHEN lifestyle06 = '1' THEN '6,' ELSE '' END +
    CASE WHEN lifestyle07 = '1' THEN '7,' ELSE '' END +
    CASE WHEN lifestyle08 = '1' THEN '8,' ELSE '' END +
    CASE WHEN lifestyle09 = '1' THEN '9,' ELSE '' END +
    CASE WHEN lifestyle10 = '1' THEN '10,' ELSE '' END +
    CASE WHEN lifestyle11 = '1' THEN '11,' ELSE '' END +
    CASE WHEN lifestyle12 = '1' THEN '12,' ELSE '' END +
    CASE WHEN lifestyle13 = '1' THEN '13,' ELSE '' END +
    CASE WHEN lifestyle14 = '1' THEN '14,' ELSE '' END +
    CASE WHEN lifestyle15 = '1' THEN '15,' ELSE '' END +
    CASE WHEN lifestyle16 = '1' THEN '16,' ELSE '' END +
    CASE WHEN lifestyle17 = '1' THEN '17,' ELSE '' END +
    CASE WHEN lifestyle18 = '1' THEN '18,' ELSE '' END +
    CASE WHEN lifestyle19 = '1' THEN '19,' ELSE '' END +
    CASE WHEN lifestyle20 = '1' THEN '20,' ELSE '' END +
    CASE WHEN lifestyle21 = '1' THEN '21,' ELSE '' END +
    CASE WHEN lifestyle22 = '1' THEN '22,' ELSE '' END +
    CASE WHEN lifestyle23 = '1' THEN '23,' ELSE '' END +
    CASE WHEN lifestyle24 = '1' THEN '24,' ELSE '' END +
    CASE WHEN lifestyle25 = '1' THEN '25,' ELSE '' END +
    CASE WHEN lifestyle26 = '1' THEN '26,' ELSE '' END +
    CASE WHEN lifestyle27 = '1' THEN '27,' ELSE '' END +
    CASE WHEN lifestyle28 = '1' THEN '28,' ELSE '' END +
    CASE WHEN lifestyle29 = '1' THEN '29,' ELSE '' END +
    CASE WHEN lifestyle30 = '1' THEN '30,' ELSE '' END +
    CASE WHEN lifestyle31 = '1' THEN '31,' ELSE '' END +
    CASE WHEN lifestyle32 = '1' THEN '32,' ELSE '' END +
    CASE WHEN lifestyle33 = '1' THEN '33,' ELSE '' END +
    CASE WHEN lifestyle34 = '1' THEN '34,' ELSE '' END +
    CASE WHEN lifestyle35 = '1' THEN '35,' ELSE '' END as lifestyle_flags,
    ','+ CASE WHEN ailments_1 ='1' THEN '1,' ELSE '' END +
    CASE WHEN ailments_2 ='1' THEN '2,' ELSE '' END +
    CASE WHEN ailments_3 ='1' THEN '3,' ELSE '' END +
    CASE WHEN ailments_4 ='1' THEN '4,' ELSE '' END +
    CASE WHEN ailments_5 ='1' THEN '5,' ELSE '' END +
    CASE WHEN ailments_6 ='1' THEN '6,' ELSE '' END +
    CASE WHEN ailments_7 ='1' THEN '7,' ELSE '' END +
    CASE WHEN ailments_8 ='1' THEN '8,' ELSE '' END +
    CASE WHEN ailments_9 ='1' THEN '9,' ELSE '' END +
    CASE WHEN ailments_10 ='1' THEN '10,' ELSE '' END +
    CASE WHEN ailments_11 ='1' THEN '11,' ELSE '' END +
    CASE WHEN ailments_12 ='1' THEN '12,' ELSE '' END +
    CASE WHEN ailments_13 ='1' THEN '13,' ELSE '' END +
    CASE WHEN ailments_14 ='1' THEN '14,' ELSE '' END +
    CASE WHEN ailments_15 ='1' THEN '15,' ELSE '' END as ailments_flags,
    ','+ CASE WHEN donor_code1  ='1' THEN '1,' ELSE '' END +
    CASE WHEN donor_code2  ='1' THEN '2,' ELSE '' END +
    CASE WHEN donor_code3  ='1' THEN '3,' ELSE '' END +
    CASE WHEN donor_code4  ='1' THEN '4,' ELSE '' END +
    CASE WHEN donor_code5  ='1' THEN '5,' ELSE '' END AS donor_flags,
    ','+ CASE WHEN childrenbygender0 IN ('U','B') OR 
              childrenbygender1 IN ('U','B') OR 
              childrenbygender2 IN ('U','B') 
        THEN '1,' ELSE '' END +
        CASE WHEN childrenbygender0 ='F' OR 
              childrenbygender1 ='F' OR 
              childrenbygender2 ='F' 
        THEN '2,' ELSE '' END +
        CASE WHEN childrenbygender0 ='M' OR 
              childrenbygender1 ='M' OR 
              childrenbygender2 ='M' 
        THEN '3,' ELSE '' END +
        CASE WHEN childrenbygender3 IN ('U','B') OR 
              childrenbygender4 IN ('U','B') OR 
              childrenbygender5 IN ('U','B') 
        THEN '4,' ELSE '' END +
        CASE WHEN childrenbygender3 ='F' OR 
              childrenbygender4 ='F' OR 
              childrenbygender5 ='F' 
        THEN '5,' ELSE '' END +
        CASE WHEN childrenbygender3 ='M' OR 
              childrenbygender4 ='M' OR 
              childrenbygender5 ='M' 
        THEN '6,' ELSE '' END +
        CASE WHEN childrenbygender6 IN ('U','B') OR 
              childrenbygender7 IN ('U','B') OR 
              childrenbygender8 IN ('U','B') OR 
              childrenbygender9 IN ('U','B') OR 
              childrenbygender10 IN ('U','B') 
        THEN '7,' ELSE '' END +
        CASE WHEN childrenbygender6 ='F' OR 
              childrenbygender7 ='F' OR 
              childrenbygender8 ='F' OR 
              childrenbygender9 ='F' OR 
              childrenbygender10 ='F' 
        THEN '8,' ELSE '' END +
        CASE WHEN childrenbygender6 ='M' OR 
              childrenbygender7 ='M' OR 
              childrenbygender8 ='M' OR 
              childrenbygender9 ='M' OR 
              childrenbygender10 ='M' 
        THEN '9,' ELSE '' END +
        CASE WHEN childrenbygender11 IN ('U','B') OR 
              childrenbygender12 IN ('U','B') OR 
              childrenbygender13 IN ('U','B') OR 
              childrenbygender14 IN ('U','B') OR 
              childrenbygender15 IN ('U','B')
        THEN '10,' ELSE '' END +
        CASE WHEN childrenbygender11 ='F' OR 
              childrenbygender12 ='F' OR 
              childrenbygender13 ='F' OR 
              childrenbygender14 ='F' OR 
              childrenbygender15 ='F' 
        THEN '11,' ELSE '' END +
        CASE WHEN childrenbygender11 ='M' OR 
              childrenbygender12 ='M' OR 
              childrenbygender13 ='M' OR 
              childrenbygender14 ='M' OR 
              childrenbygender15 ='M' 
        THEN '12,' ELSE '' END +
        CASE WHEN childrenbygender16 IN ('U','B') OR 
              childrenbygender17 IN ('U','B') 
        THEN '13,' ELSE '' END +
        CASE WHEN childrenbygender16 ='F' OR 
              childrenbygender17 ='F' 
        THEN '14,' ELSE '' END +
        CASE WHEN childrenbygender16 ='M' OR 
              childrenbygender17 ='M' 
        THEN '15,' ELSE '' END AS children_age_presence_flag,
    ','+ CASE WHEN childrenbygender0 IN ('U','B') THEN '1,' ELSE '' END +
    CASE WHEN childrenbygender1 IN ('U','B') THEN '2,' ELSE '' END +
    CASE WHEN childrenbygender2 IN ('U','B') THEN '3,' ELSE '' END +
    CASE WHEN childrenbygender3 IN ('U','B') THEN '4,' ELSE '' END +
    CASE WHEN childrenbygender4 IN ('U','B') THEN '5,' ELSE '' END +
    CASE WHEN childrenbygender5 IN ('U','B') THEN '6,' ELSE '' END +
    CASE WHEN childrenbygender6 IN ('U','B') THEN '7,' ELSE '' END +
    CASE WHEN childrenbygender7 IN ('U','B') THEN '8,' ELSE '' END +
    CASE WHEN childrenbygender8 IN ('U','B') THEN '9,' ELSE '' END +
    CASE WHEN childrenbygender9 IN ('U','B') THEN '10,' ELSE '' END +
    CASE WHEN childrenbygender10 IN ('U','B') THEN '11,' ELSE '' END +
    CASE WHEN childrenbygender11 IN ('U','B') THEN '12,' ELSE '' END +
    CASE WHEN childrenbygender12 IN ('U','B') THEN '13,' ELSE '' END +
    CASE WHEN childrenbygender13 IN ('U','B') THEN '14,' ELSE '' END +
    CASE WHEN childrenbygender14 IN ('U','B') THEN '15,' ELSE '' END +
    CASE WHEN childrenbygender15 IN ('U','B') THEN '16,' ELSE '' END +
    CASE WHEN childrenbygender16 IN ('U','B') THEN '17,' ELSE '' END +
    CASE WHEN childrenbygender17 IN ('U','B') THEN '18,' ELSE '' END +
    CASE WHEN childrenbygender0 = 'F' THEN '19,' ELSE '' END +
    CASE WHEN childrenbygender1 = 'F' THEN '20,' ELSE '' END +
    CASE WHEN childrenbygender2 = 'F' THEN '21,' ELSE '' END +
    CASE WHEN childrenbygender3 = 'F' THEN '22,' ELSE '' END +
    CASE WHEN childrenbygender4 = 'F' THEN '23,' ELSE '' END +
    CASE WHEN childrenbygender5 = 'F' THEN '24,' ELSE '' END +
    CASE WHEN childrenbygender6 = 'F' THEN '25,' ELSE '' END +
    CASE WHEN childrenbygender7 = 'F' THEN '26,' ELSE '' END +
    CASE WHEN childrenbygender8 = 'F' THEN '27,' ELSE '' END +
    CASE WHEN childrenbygender9 = 'F' THEN '28,' ELSE '' END +
    CASE WHEN childrenbygender10 = 'F' THEN '29,' ELSE '' END +
    CASE WHEN childrenbygender11 = 'F' THEN '30,' ELSE '' END +
    CASE WHEN childrenbygender12 = 'F' THEN '31,' ELSE '' END +
    CASE WHEN childrenbygender13 = 'F' THEN '32,' ELSE '' END +
    CASE WHEN childrenbygender14 = 'F' THEN '33,' ELSE '' END +
    CASE WHEN childrenbygender15 = 'F' THEN '34,' ELSE '' END +
    CASE WHEN childrenbygender16 = 'F' THEN '35,' ELSE '' END +
    CASE WHEN childrenbygender17 = 'F' THEN '36,' ELSE '' END +
    CASE WHEN childrenbygender0 = 'M' THEN '37,' ELSE '' END +
    CASE WHEN childrenbygender1 = 'M' THEN '38,' ELSE '' END +
    CASE WHEN childrenbygender2 = 'M' THEN '39,' ELSE '' END +
    CASE WHEN childrenbygender3 = 'M' THEN '40,' ELSE '' END +
    CASE WHEN childrenbygender4 = 'M' THEN '41,' ELSE '' END +
    CASE WHEN childrenbygender5 = 'M' THEN '42,' ELSE '' END +
    CASE WHEN childrenbygender6 = 'M' THEN '43,' ELSE '' END +
    CASE WHEN childrenbygender7 = 'M' THEN '44,' ELSE '' END +
    CASE WHEN childrenbygender8 = 'M' THEN '45,' ELSE '' END +
    CASE WHEN childrenbygender9 = 'M' THEN '46,' ELSE '' END +
    CASE WHEN childrenbygender10 = 'M' THEN '47,' ELSE '' END +
    CASE WHEN childrenbygender11 = 'M' THEN '48,' ELSE '' END +
    CASE WHEN childrenbygender12 = 'M' THEN '49,' ELSE '' END +
    CASE WHEN childrenbygender13 = 'M' THEN '50,' ELSE '' END +
    CASE WHEN childrenbygender14 = 'M' THEN '51,' ELSE '' END +
    CASE WHEN childrenbygender15 = 'M' THEN '52,' ELSE '' END +
    CASE WHEN childrenbygender16 = 'M' THEN '53,' ELSE '' END +
    CASE WHEN childrenbygender17 = 'M' THEN '54,' ELSE '' END AS children_Age_by_gender
FROM {maintable_name};

DROP TABLE IF EXISTS tblChild14_{build_id}_{build};
CREATE TABLE tblChild14_{build_id}_{build}
(
    id INT DISTKEY SORTKEY ENCODE AZ64,
    tw_flags VARCHAR(460) ENCODE ZSTD,
    lifestyle_flags VARCHAR(100) ENCODE ZSTD,
    ailments_flags VARCHAR(40) ENCODE ZSTD,
    donor_flags VARCHAR(20) ENCODE ZSTD,
    children_age_presence_flag Varchar(60) ENCODE ZSTD,
    children_Age_by_gender VARCHAR(160) ENCODE ZSTD
);

INSERT INTO tblChild14_{build_id}_{build}
(
    id,
    tw_flags,
    lifestyle_flags,
    ailments_flags,
    donor_flags,
    children_age_presence_flag,
    children_Age_by_gender
)
SELECT 
    id,
    CASE WHEN tw_flags = ',' THEN '' ELSE tw_flags end,
    CASE WHEN lifestyle_flags  = ',' THEN '' ELSE lifestyle_flags end,
    CASE WHEN ailments_flags  = ',' THEN '' ELSE ailments_flags end,
    CASE WHEN donor_flags = ',' THEN '' ELSE donor_flags end,
    CASE WHEN children_age_presence_flag = ',' THEN '' ELSE children_age_presence_flag end,
    CASE WHEN children_Age_by_gender = ',' THEN '' ELSE children_Age_by_gender end
FROM temp_tblChild14_{build_id}_{build};

DROP TABLE IF EXISTS temp_tblChild14_{build_id}_{build};