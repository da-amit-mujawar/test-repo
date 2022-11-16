DROP TABLE IF EXISTS RoyaltyOrderIds_{database_id}_ToBeDropped;
CREATE TABLE RoyaltyOrderIds_{database_id}_ToBeDropped
 (  
      OrderId varchar(10),	
      cTableName varchar(50),	
      ContactFlag Varchar(1) NULL,
      EmailFlag Varchar(1) NULL,
      ShippedBy varchar(50),
      Mailer varchar(100),               
      cNextMarkOrderNo varchar(50),
      PO varchar(50),
      OrderDescription varchar(50),
      ListName varchar(50),
      OrderQty varchar(12),
      ShipDate varchar(10),
      OESSOrderID varchar(30),
      OESSAccountNumber varchar(30),
      OESSInvoiceTotal varchar(12),
      SalesRepName varchar (50),
      DivisionNumber varchar (50), 
      cNotes varchar(100)
);
        --	CreatedBy varchar(50),


COPY RoyaltyOrderIds_{database_id}_ToBeDropped(
      OrderId,
      cTableName,
      ContactFlag,  
      EmailFlag,
      ShippedBy,
      Mailer,                      
      cNextMarkOrderNo,
      PO,
      OrderDescription,
      ListName,
      OrderQty,
      ShipDate,
      OESSOrderID,
      OESSAccountNumber,
      OESSInvoiceTotal,
      SalesRepName,
      DivisionNumber,
      cNotes
)
FROM 's3://{s3-internal}{s3-key1}'
iam_role '{iam}'
delimiter '|';
      --	CreatedBy '|',


DROP TABLE IF EXISTS  RoyaltyFields_{database_id}_ToBeDropped;
CREATE TABLE RoyaltyFields_{database_id}_ToBeDropped (cFieldName varchar(100));

INSERT INTO RoyaltyFields_{database_id}_ToBeDropped
      VALUES  ('Vendor_CODE'); 
