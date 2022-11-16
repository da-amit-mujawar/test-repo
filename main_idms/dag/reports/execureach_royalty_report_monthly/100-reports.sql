-- Adding first select for qouted headings
UNLOAD ('
SELECT  Report.CreatedBy,
        Report.ShippedBy,
        Report.Orderid,
        Report.Mailer,
        Report.cNextMarkOrderNo,
        Report.PO,
        Report.OrderDescription,
        Report.Listname,
        Report.OrderQty,
        Report.ShipDate,
        Report.FieldName,
        Report."Vendor ID",
        Report."Qty Shipped",
        Report.Bus_ContactName_Exported,
        Report.Bus_EmailAddress_Exported,
        Report.Cons_EmailAddress_Exported,
        Report.Notes
FROM RoyaltyOutput_ForExport_{database_id}_ToBeDropped Report
ORDER BY Report.iOrderBy ASC;
')
to 's3://{s3-internal}{reportname1}'
iam_role '{iam}'
kms_key_id '{kmskey}'
ENCRYPTED
DELIMITER AS ','
ALLOWOVERWRITE
parallel off
;
