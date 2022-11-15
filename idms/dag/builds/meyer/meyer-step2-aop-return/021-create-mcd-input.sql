-- Create MCD input
/*1,234,567,890
individual = enterprise
household = collection
    sequence varchar(10),   --req, file_id+maintable.id to match mcd return file
    file_no varchar(3),     --req, default 100 for non-match
    run varchar(10),        --req, default null
    enterpriseid varchar(20), --opt, The ENTERPRISE_ID will always be null unless the MCD processing has been configured where the project team is keeping the MCD history, versus the MCD history being kept in a historical file.
                                 --In this case, the MZP INDIV ID will be assigned to this field.  On the initial run, this data will not be available.
    collection varchar(20),  --opt, The COLLECTION_ID will always be null unless the MCD processing has been configured where the project team is keeping the MCD history, versus the MCD history being kept in a historical file.
                                 --In this case, the MZP HH ID will be assigned to this field.  On the initial run, this data will not be available.
    channel	varchar(3),     --req, default 1 (used for multiple emails/phones)

 */
drop table if exists meyer_mcd_input;

create table meyer_mcd_input (
    sequence varchar(10),
    file_no varchar(3),
    run varchar(10),
    enterpriseid varchar(20),
    collection varchar(20),
    channel	varchar(3),
    title	varchar(30),
    first	varchar(30),
    middle	varchar(30),
    last	varchar(30),
    generational	varchar(30),
    gender	varchar(1),
    dob	varchar(8),
    address	varchar(100),
    secondary	varchar(100),
    unit	varchar(10),
    city	varchar(50),
    province	varchar(30),
    postal	varchar(6),
    plus4	varchar(4),
    account	varchar(100),
    phone	varchar(20),
    email	varchar(75),
    company	varchar(100),
    division	varchar(30),
    since	varchar(30),
    mailability	varchar(62)
);

