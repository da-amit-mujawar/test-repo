ALTER TABLE public.exclude_sum_fec RENAME TO tblchild29_apogee_fec_temp;

ALTER TABLE public.exclude_sum_fec_list3 RENAME COLUMN Company_ID TO Company_ID_23;
ALTER TABLE public.exclude_sum_fec_list0 RENAME COLUMN Company_ID TO Company_ID_24;
ALTER TABLE public.exclude_sum_fec_list2 RENAME COLUMN Company_ID TO Company_ID_25;
ALTER TABLE public.exclude_sum_fec_list1 RENAME COLUMN Company_ID TO Company_ID_26;
ALTER TABLE public.tblchild29_apogee_fec_temp RENAME COLUMN Company_ID TO Company_ID_29;

-- ALTER TABLE tblchild29_{build_id}_{build} RENAME COLUMN ID TO Table_ID;
CREATE TABLE public.exclude_sum_fec
DISTSTYLE KEY DISTKEY (Company_ID)
SORTKEY (Company_ID)
AS
SELECT ROW_NUMBER() OVER () AS ID,
       COALESCE(company_id_29, company_id_23, company_id_24, company_id_25, company_id_26) Company_ID,
       t29.*, t23.*, t24.*, t25.*, t26.*
  FROM public.tblchild29_apogee_fec_temp t29
       FULL OUTER JOIN public.exclude_sum_fec_list3 t23 ON t29.company_id_29 = t23.company_id_23
       FULL OUTER JOIN public.exclude_sum_fec_list0 t24 ON t29.company_id_29 = t24.company_id_24
       FULL OUTER JOIN public.exclude_sum_fec_list2 t25 ON t29.company_id_29 = t25.company_id_25
       FULL OUTER JOIN public.exclude_sum_fec_list1 t26 ON t29.company_id_29 = t26.company_id_26 ;

ALTER TABLE public.exclude_sum_fec DROP COLUMN company_id_23;
ALTER TABLE public.exclude_sum_fec DROP COLUMN company_id_24;
ALTER TABLE public.exclude_sum_fec DROP COLUMN company_id_25;
ALTER TABLE public.exclude_sum_fec DROP COLUMN company_id_26;
ALTER TABLE public.exclude_sum_fec DROP COLUMN company_id_29;

DROP TABLE IF EXISTS public.exclude_sum_fec_list3 ;
DROP TABLE IF EXISTS public.exclude_sum_fec_list0 ;
DROP TABLE IF EXISTS public.exclude_sum_fec_list2 ;
DROP TABLE IF EXISTS public.exclude_sum_fec_list1 ;
DROP TABLE IF EXISTS public.tblchild29_apogee_fec_temp ;
