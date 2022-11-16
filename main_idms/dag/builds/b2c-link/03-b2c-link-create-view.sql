-- create tblchild1
DROP VIEW IF EXISTS tblchild1_{build_id}_{build};
CREATE VIEW tblchild1_{build_id}_{build} AS
    SELECT sic_code AS bus_primarysiccode, 
           sic_description 
    FROM public.sic_code_6digits_descriptions WITH NO SCHEMA BINDING;

--child2
DROP VIEW IF EXISTS tblchild2_{build_id}_{build};
CREATE VIEW tblchild2_{build_id}_{build} AS
     SELECT ccode AS bus_employeesizecode, 
           cDescription AS bus_location_employment_size_description 
      FROM public.employeesize_decode WITH NO SCHEMA BINDING;


--tblchild3
DROP VIEW IF EXISTS tblchild3_{build_id}_{build};
CREATE VIEW tblchild3_{build_id}_{build} AS
    SELECT cCode AS bus_corporate_employment_size_code, 
           cDescription AS bus_corporate_employment_size_Description 
    FROM public.employeesize_decode WITH NO SCHEMA BINDING;

--tblchild4
DROP VIEW IF EXISTS tblchild4_{build_id}_{build};
CREATE VIEW tblchild4_{build_id}_{build} AS
    SELECT ccode AS bus_locationoutputvolumecode, 
           cDescription AS bus_location_sales_volume_description 
    FROM public.sales_volume_decode WITH NO SCHEMA BINDING;

--tblchild5
DROP VIEW IF EXISTS tblchild5_{build_id}_{build};
CREATE VIEW tblchild5_{build_id}_{build} AS
    SELECT cCode AS bus_corporate_sales_volume_code, 
           cDescription AS bus_corporate_sales_volume_description 
    FROM public.sales_volume_decode WITH NO SCHEMA BINDING;

--tblchild6
DROP VIEW IF EXISTS tblchild6_{build_id}_{build};
CREATE VIEW tblchild6_{build_id}_{build} AS
    SELECT ccode AS bus_square_footage, 
           cdescription AS bus_square_footage_description 
    FROM public.square_footage_descriptions WITH NO SCHEMA BINDING;

--tblchild7
DROP VIEW IF EXISTS tblchild7_{build_id}_{build};
CREATE VIEW tblchild7_{build_id}_{build} AS
    SELECT ccode AS bus_departmentcode, 
           cdescription AS bus_departmentcode_description 
      FROM public.exclude_busdepartmentcode WITH NO SCHEMA BINDING;

--tblchild8
DROP VIEW IF EXISTS tblchild8_{build_id}_{build};
CREATE VIEW tblchild8_{build_id}_{build} AS
    SELECT ccode AS bus_levelcode, 
           cdescription AS bus_levelcode_description 
    FROM public.exclude_buslevelcode WITH NO SCHEMA BINDING;

--tblchild9
DROP VIEW IF EXISTS tblchild9_{build_id}_{build};
CREATE VIEW tblchild9_{build_id}_{build} AS
    SELECT ccode AS bus_functionalareacode, 
           cdescription AS bus_functionalareacode_description 
    FROM public.exclude_busfunctionalareacode WITH NO SCHEMA BINDING;

--tblchild10
DROP VIEW IF EXISTS tblchild10_{build_id}_{build};
CREATE VIEW tblchild10_{build_id}_{build} AS
    SELECT ccode AS bus_rolecode, 
           cdescription AS bus_rolecode_description 
      FROM public.exclude_busrolecode WITH NO SCHEMA BINDING;

--tblchild36
DROP VIEW IF EXISTS tblchild36_{build_id}_{build};
CREATE VIEW tblchild36_{build_id}_{build} AS 
    SELECT exclude_tblintent_topic_final_1150.company_mc, exclude_tblintent_topic_final_1150.main_cat_code as intent_main_cat_code, exclude_tblintent_topic_final_1150.sub_cat_code as intent_sub_cat_code, exclude_tblintent_topic_final_1150.topic as intent_topic
   FROM exclude_tblintent_topic_final_1150;