DROP TABLE IF EXISTS public.tblchild99_apogee_temp;

ALTER TABLE public.exclude_sum_ltd_np RENAME TO tblchild99_apogee_temp;

ALTER TABLE public.tblChild17_apogee_months RENAME COLUMN company_id TO company_id_17;
ALTER TABLE public.tblChild18_apogee_custom RENAME COLUMN company_id TO company_id_18;

CREATE TABLE public.exclude_sum_ltd_np
DISTSTYLE KEY DISTKEY (Company_ID)
SORTKEY (Company_ID)
AS
SELECT T1.*, T18.*, T17.*
FROM public.tblchild99_apogee_temp as T1
    FULL OUTER JOIN public.tblChild17_apogee_months as T17 ON T1.company_id = t17.company_id_17
    FULL OUTER JOIN public.tblChild18_apogee_custom as T18 ON T1.company_id = t18.company_id_18;

ALTER TABLE public.exclude_sum_ltd_np DROP COLUMN company_id_17;
ALTER TABLE public.exclude_sum_ltd_np DROP COLUMN company_id_18;

DROP TABLE public.tblchild99_apogee_temp;
DROP TABLE public.tblChild17_apogee_months;
DROP TABLE public.tblChild18_apogee_custom;
