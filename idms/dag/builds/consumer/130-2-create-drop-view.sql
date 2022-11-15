DROP TABLE IF EXISTS nosuchtable;
DROP VIEW IF EXISTS tblChild1_{build_id}_{build};
CREATE VIEW tblChild1_{build_id}_{build}  
AS SELECT cbsacode AS cbsa_code , cbtitle FROM EXCLUDE_CBSA_Descriptions;


--Aircraft_Type_Description
DROP VIEW IF EXISTS tblChild2_{build_id}_{build}; 
CREATE VIEW tblChild2_{build_id}_{build}
AS SELECT code AS aircraft_type_code,description AS aircraft_type_description FROM EXCLUDE_AircraftTypeCode;

--PotentialInvestorConsumer 
DROP VIEW IF EXISTS tblChild3_{build_id}_{build};
CREATE VIEW tblChild3_{build_id}_{build} 
AS SELECT code AS potential_investor_consumer, description AS potential_investor_consumer_description FROM EXCLUDE_PotentialInvestorConsumer;

--PurchasingPowerIncome
DROP VIEW IF EXISTS tblChild4_{build_id}_{build};
CREATE VIEW tblChild4_{build_id}_{build}
AS SELECT code AS purchasing_power_income,description AS purchasing_power_income_description FROM  EXCLUDE_PurchasingPowerIncome;

--Wealth_Finder
DROP VIEW IF EXISTS tblChild5_{build_id}_{build};
CREATE VIEW tblChild5_{build_id}_{build}  AS SELECT code AS wealth_finder,DESCRIPTION AS wealth_finder_description FROM  EXCLUDE_WealthFinder;

--InfopersonaCluster
DROP VIEW IF EXISTS tblChild6_{build_id}_{build};
CREATE VIEW tblChild6_{build_id}_{build}
AS SELECT code AS infopersona_cluster,DESCRIPTION AS infopersona_cluster_description FROM  EXCLUDE_InfopersonaCluster;

--PoliticalPartyAffiliation
DROP VIEW IF EXISTS tblChild7_{build_id}_{build};
CREATE VIEW tblChild7_{build_id}_{build}
AS select code AS political_party_affiliation,DESCRIPTION AS political_party_affiliation_description FROM  EXCLUDE_PoliticalPartyAffiliation;

--InfopersonaSupercluster
DROP VIEW IF EXISTS tblChild8_{build_id}_{build};
CREATE VIEW tblChild8_{build_id}_{build}
AS SELECT code AS infopersona_supercluster,DESCRIPTION AS infopersona_supercluster_description FROM  EXCLUDE_InfopersonaSupercluster;


--PilotLicenseCode
DROP VIEW IF EXISTS tblChild9_{build_id}_{build};
CREATE VIEW tblChild9_{build_id}_{build}
AS SELECT code AS pilot_license_code,DESCRIPTION AS pilot_license_code_description FROM EXCLUDE_PilotLicenseCode;


--LastPartyContributed
DROP VIEW IF EXISTS tblChild10_{build_id}_{build};
CREATE VIEW tblChild10_{build_id}_{build}
AS SELECT code AS last_party_contributed_to,DESCRIPTION AS last_party_contributed_to_description FROM  EXCLUDE_LastPartyContributed;


DROP VIEW IF EXISTS tblChild11_{build_id}_{build};
CREATE VIEW tblChild11_{build_id}_{build}
AS SELECT ccode AS statecountycode, cdescription AS countyname FROM CountyState_Decode;

--SF 01.21.2020 Create a new view on apogee L2 table for use
--DROP VIEW IF EXISTS tblChild12_{build_id}_{build};
--CREATE VIEW tblChild12_{build_id}_{build}
--AS SELECT * FROM tblExternal45_191_201206;

--SF 04.06.2020 Add DMA description as a child table (view)
DROP VIEW IF EXISTS tblChild13_{build_id}_{build};
-- CB 2022.08.02 dmacode is changed to marketarea in consumer load
--CREATE VIEW tblChild13_{build_id}_{build}
--AS SELECT * FROM EXCLUDE_DMACode_Descriptions;
CREATE VIEW tblChild13_{build_id}_{build}
AS SELECT dmacode as marketarea, descriptions FROM EXCLUDE_DMACode_Descriptions;