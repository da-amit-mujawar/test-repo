-- --------------------------------------------
-- Finalize Child tables from build process:
-- --------------------------------------------

 --20221003 CB Remove FEC from build

----------------------------------------------
--- Monthly Updates
----------------------------------------------

-- 1 HH Summary - Non Profit Donors
DROP VIEW IF EXISTS tblChild1_{build_id}_{build};
CREATE VIEW tblChild1_{build_id}_{build}
AS 
SELECT * FROM public.exclude_nonprofit_child1_{dbid}
WITH NO SCHEMA BINDING;

-- 2 mGen
DROP VIEW IF EXISTS tblChild2_{build_id}_{build};
CREATE VIEW tblChild2_{build_id}_{build}
AS 
SELECT * FROM public.exclude_nonprofit_child2_{dbid}
WITH NO SCHEMA BINDING;


-- 3 Transaction - Non Profit Donors
DROP VIEW IF EXISTS tblChild3_{build_id}_{build};
CREATE VIEW tblChild3_{build_id}_{build}
AS
SELECT * FROM public.exclude_nonprofit_transactions_{dbid}
WITH NO SCHEMA BINDING;

/*
-- 4 Transaction - FEC Donors  (FET)
DROP VIEW IF EXISTS tblChild4_{build_id}_{build};
CREATE VIEW tblChild4_{build_id}_{build}
AS 
SELECT * FROM public.exclude_nonprofit_transactions_fec_{dbid}
WITH NO SCHEMA BINDING;
-- 5 HH Summary - FEC Donors  (FES)
DROP VIEW IF EXISTS tblChild5_{build_id}_{build};
CREATE VIEW tblChild5_{build_id}_{build}
AS 
SELECT * FROM public.exclude_nonprofit_tblchild5_fec_{dbid}
WITH NO SCHEMA BINDING;
-- 6 HH Summary - Comb NonProfit and FEC  (COM)
DROP VIEW IF EXISTS tblChild6_{build_id}_{build};
CREATE VIEW tblChild6_{build_id}_{build}
AS 
SELECT * FROM public.exclude_nonprofit_tblchild6_comb_{dbid}
WITH NO SCHEMA BINDING;
*/

-- 7 L2 Enhanced Voter Data  (deprecated, formerly view of ext45)


-- 8 Haystaq For Selections
DROP VIEW IF EXISTS tblChild8_{build_id}_{build};
CREATE VIEW tblChild8_{build_id}_{build}
AS 
SELECT * FROM public.tblexternal44_191_201206 
WITH NO SCHEMA BINDING;

-- 9 Consumer Email (for matching only)
DROP VIEW IF EXISTS tblChild9_{build_id}_{build};
CREATE VIEW tblChild9_{build_id}_{build}
AS 
SELECT * FROM public.exclude_nonprofit_tblchild9_{dbid}
WITH NO SCHEMA BINDING;


--10 Cell Phone (view of tblDQI_CellPhone)
DROP VIEW IF EXISTS tblChild10_{build_id}_{build};
CREATE VIEW tblChild10_{build_id}_{build}
AS 
SELECT * FROM public.tbldqi_cellphone
WITH NO SCHEMA BINDING;

--11 Premover Daily for Apogee has no usage (deprecated)

/* CB 2022.08.22: child12 not needed, all cat fields are added to exclude_nonprofit_child1_{dbid}
--12 HH Summary NP - M12 and M48 by Category
DROP VIEW IF EXISTS tblchild12_{build_id}_{build};
CREATE VIEW tblchild12_{build_id}_{build} 
AS
SELECT * FROM public.exclude_sum_cat_np 
WITH NO SCHEMA BINDING;
 */

--13 HH Summary NP - M48 Mos Since Last Donation by List
DROP VIEW IF EXISTS tblchild13_{build_id}_{build};
CREATE VIEW tblchild13_{build_id}_{build} 
AS
SELECT * FROM public.exclude_sum_mos_list_np 
WITH NO SCHEMA BINDING;

--14 HH Summary NP - M48 Average Dollar Donation by List
DROP VIEW IF EXISTS tblchild14_{build_id}_{build};
CREATE VIEW tblchild14_{build_id}_{build} 
AS
SELECT * FROM public.exclude_sum_avg_list_np 
WITH NO SCHEMA BINDING;

--15 HH Summary NP - M48 Number Of Donations by List
DROP VIEW IF EXISTS tblchild15_{build_id}_{build};
CREATE VIEW tblchild15_{build_id}_{build} 
AS
SELECT * FROM public.exclude_sum_nbr_list_np 
WITH NO SCHEMA BINDING;

--16 HH Summary NP - M48 Total Dollar Donations by List
DROP VIEW IF EXISTS tblchild16_{build_id}_{build};
CREATE VIEW tblchild16_{build_id}_{build}
AS
SELECT * FROM public.exclude_sum_amt_list_np 
WITH NO SCHEMA BINDING;


/*Enable this when we go to production. Drop physical table first*/

-- Apogee External Link Table for other IDMS databases
DROP VIEW IF EXISTS tblexternal39_191_201206;
CREATE VIEW tblexternal39_191_201206
AS
SELECT * FROM public.exclude_nonprofit_tblexternal39_{dbid}
WITH NO SCHEMA BINDING;



