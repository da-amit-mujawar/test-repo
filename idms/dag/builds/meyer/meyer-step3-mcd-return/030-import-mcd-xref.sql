CREATE TABLE IF NOT EXISTS meyer_mcd_xref
(
    new_mcdindividualid VARCHAR(20),
    new_collection      VARCHAR(20),
    old_mcdindividualid VARCHAR(20),
    old_collection      VARCHAR(20),
    created_dt          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_dt          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS meyer_mcd_xref_tmp;

CREATE TABLE meyer_mcd_xref_tmp
(
    new_mcdindividualid VARCHAR(20),
    new_collection      VARCHAR(20),
    old_mcdindividualid VARCHAR(20),
    old_collection      VARCHAR(20)
);

COPY meyer_mcd_xref_tmp
    FROM 's3://idms-7933-aop-output/a05_meyers_xref.dat'
    IAM_ROLE '{iam}'
    DELIMITER ';'
    ACCEPTINVCHARS;

DELETE
  FROM meyer_mcd_xref_tmp
 WHERE old_mcdindividualid = new_mcdindividualid;

INSERT INTO meyer_mcd_xref(new_mcdindividualid, new_collection,
                           old_mcdindividualid, old_collection)
SELECT new_mcdindividualid, new_collection, old_mcdindividualid, old_collection
  FROM meyer_mcd_xref_tmp tmp
 where not exists(select 1
                    from meyer_mcd_xref xref
                   where xref.new_mcdindividualid = tmp.new_mcdindividualid
                     and xref.old_mcdindividualid = tmp.old_mcdindividualid);


UPDATE meyer_mcd_xref
   SET new_mcdindividualid = meyer_mcd_xref_tmp.new_mcdindividualid
  FROM meyer_mcd_xref_tmp
 WHERE meyer_mcd_xref.new_mcdindividualid =
       meyer_mcd_xref_tmp.old_mcdindividualid;
