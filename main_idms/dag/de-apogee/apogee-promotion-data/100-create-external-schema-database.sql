-- DROP SCHEMA IF EXISTS spectrumdb
-- DROP EXTERNAL DATABASE CASCADE;

-- create external schema spectrumdb
--     from data catalog
--     database 'apogee_promo'
-- --iam_role 'arn:aws:iam::250245842722:role/da-idms-redshift-role'
--     iam_role '{iam}'
--     create external database if not exists;
select getdate()
