DROP TABLE IF EXISTS RoyaltyOrderIds_{database_id}_ToBeDropped;
CREATE TABLE RoyaltyOrderIds_{database_id}_ToBeDropped
 (  
      OrderId varchar(10),	
      cTableName varchar(50),	
      CreatedBy varchar(50),
      ShippedBy varchar(50),
      Mailer varchar(100),                
      cNextMarkOrderNo varchar(50),
      PO varchar(50) ,
      OrderDescription varchar(50),
      ListName varchar(50) ,
      OrderQty varchar(12) ,
      ShipDate varchar(10),
      cNotes varchar(50),
      Bus_ContactName_Exported varchar(3), 
      Bus_EmailAddress_Exported varchar(3), 
      Cons_EmailAddress_Exported varchar(3) 
);


COPY RoyaltyOrderIds_{database_id}_ToBeDropped(
      OrderId,
      cTableName,
      CreatedBy,
      ShippedBy,
      Mailer,                     
      cNextMarkOrderNo,
      PO,
      OrderDescription,
      ListName,
      OrderQty,
      ShipDate,
      cNotes,
      Bus_ContactName_Exported,
      Bus_EmailAddress_Exported,
      Cons_EmailAddress_Exported  
)
FROM 's3://{s3-internal}{s3-key1}'
iam_role '{iam}'
delimiter '|';


DROP TABLE IF EXISTS  RoyaltyFields_{database_id}_ToBeDropped;
CREATE TABLE RoyaltyFields_{database_id}_ToBeDropped (cFieldName varchar(100));

COPY RoyaltyFields_{database_id}_ToBeDropped(cFieldName)
FROM 's3://{s3-internal}{s3-key2}'
iam_role '{iam}'
delimiter '|';


DELETE FROM RoyaltyFields_{database_id}_ToBeDropped 
WHERE (LTRIM(RTRIM(cFieldName))='' or cFieldName is null);
