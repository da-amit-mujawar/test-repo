DROP TABLE IF EXISTS spectrumdb.dotcompaymentgateway_customer; 
  CREATE EXTERNAL TABLE spectrumdb.dotcompaymentgateway_customer 
(
CustomerId VARCHAR(100)
,Timestamp VARCHAR(64)
,Created TIMESTAMP
,LastAccessed TIMESTAMP
,CustomerReference VARCHAR(30)
,SmsAccount VARCHAR(60)
,Email VARCHAR(80)
,Phone VARCHAR(32)
,Company VARCHAR(80)
,FirstName VARCHAR(20)
,LastName VARCHAR(40)
,Address1 VARCHAR(80)
,Address2 VARCHAR(80)
,City VARCHAR(30)
,StateOrProvince VARCHAR(30)
,PostalCode VARCHAR(15)
,Country VARCHAR(30)
,ThinkCustomerNo INT
,VatRegistrationNo VARCHAR(50)
,CustomerNo DECIMAL(18,0)
,ShipToAddressNo DECIMAL(18,0)
,BillToAddressNo DECIMAL(18,0)
)

  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
  WITH SERDEPROPERTIES ( 
  'separatorChar' = '|', 
  'quoteChar' = '"', 
  'escapeChar' = '\\' 
  ) 
  STORED AS textfile 
  LOCATION 's3://axle-internal-sources/raw/oess/dotcompaymentgateway_customer/'
  TABLE PROPERTIES ('compression_type'='gzip');
DROP VIEW IF EXISTS interna.dotcompg_customer; 
CREATE VIEW interna.dotcompg_customer AS SELECT * FROM spectrumdb.dotcompaymentgateway_customer WITH NO SCHEMA BINDING; 
SELECT COUNT(*) FROM interna.dotcompg_customer; 
SELECT TOP 1000 * FROM interna.dotcompg_Customer;

DROP TABLE IF EXISTS spectrumdb.dotcompaymentgateway_invoice; 
  CREATE EXTERNAL TABLE spectrumdb.dotcompaymentgateway_invoice 
(
InvoiceId VARCHAR(100)
,InvoiceDate timestamp
,CustomerId VARCHAR(100)
,AuthorizationId VARCHAR(100)
,OrderId VARCHAR(100)
,SmsOrder NVARCHAR(160)
,SmsAccount NVARCHAR(160)
,CustomerReference VARCHAR(64)
,InvoiceNumber VARCHAR(64)
,RetailPrice DECIMAL(15,4)
,FinalPrice DECIMAL(15,4)
,Quantity DECIMAL(25,13)
,UnitPrice DECIMAL(15,4)
,Freight DECIMAL(15,4)
,Created timestamp
,LastAccessed timestamp
,Assisted INT
,GhostRep INT
,PrimarySalesRep VARCHAR(64)
,SecondarySalesRep VARCHAR(64)
,OracleProductId VARCHAR(64)
,PaymentTerms VARCHAR(16)
,TransactionType VARCHAR(16)
,SegmentCode VARCHAR(16)
,LegacyProductCode VARCHAR(16)
,Text VARCHAR(MAX)
,Timestamp Varchar(64)
,InvoiceStatus VARCHAR(32)
,Recurring VARCHAR(16)
,SettlementSystem VARCHAR(32)
,Tax DECIMAL(15,4)
,BillToCustomerNo DECIMAL(18,0)
,BillToAddressNo DECIMAL(18,0)
,ShipToCustomerNo DECIMAL(18,0)
,ShipToAddressNo DECIMAL(18,0)
,OrganizationNo VARCHAR(16)
,FirstTransaction INT
,InstallmentNo INT
)


  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
  WITH SERDEPROPERTIES ( 
  'separatorChar' = '|', 
  'quoteChar' = '"', 
  'escapeChar' = '\\' 
  ) 
  STORED AS textfile 
  LOCATION 's3://axle-internal-sources/raw/oess/dotcompaymentgateway_invoice/' 
  TABLE PROPERTIES ('compression_type'='gzip');
DROP VIEW IF EXISTS interna.dotcompg_invoice; 
CREATE VIEW interna.dotcompg_invoice AS SELECT * FROM spectrumdb.dotcompaymentgateway_invoice WITH NO SCHEMA BINDING; 
SELECT COUNT(*) FROM interna.dotcompg_invoice; 
SELECT TOP 1000 * FROM interna.dotcompg_invoice;


DROP TABLE IF EXISTS spectrumdb.dotcompaymentgateway_order; 
  CREATE EXTERNAL TABLE spectrumdb.dotcompaymentgateway_order 
(
OrderId Varchar(100)
,CustomerId Varchar(100)
,OriginApplication VARCHAR(16)
,SmsOrder VARCHAR(50)
,DatabaseType VARCHAR(32)
,PaymentMode VARCHAR(32)
,FullAuth INT
,PaymentProfileId Varchar(100)
,Timestamp Varchar(64)
,CheckoutState VARCHAR(16)
,Recurring VARCHAR(16)
,Quantity DECIMAL(25,13)
,AuthAmount DECIMAL(15,4)
,RetailPrice DECIMAL(15,4)
,FinalPrice DECIMAL(15,4)
,ShippingAmount DECIMAL(15,4)
,TaxAmount DECIMAL(15,4)
,Created TIMESTAMP
,LastAccessed TIMESTAMP
,VendorId VARCHAR(16)
,ParentVendorId VARCHAR(16)
,AccountSalesRep VARCHAR(64)
,OracleProductId VARCHAR(32)
,MediaCode VARCHAR(32)
,ShipAddress1 VARCHAR(80)
,ShipAddress2 VARCHAR(80)
,ShipCity VARCHAR(30)
,ShipState VARCHAR(30)
,ShipPostalCode VARCHAR(15)
,ShipCountry VARCHAR(30)
,ShippingMethod VARCHAR(32)
,PurchaseOrder VARCHAR(32)
,SettlementSystem VARCHAR(32)
,NumberBillingCycles INT
,ShipToCompany VARCHAR(80)
,AnnualShippingAmount DECIMAL(15,4)
,AnnualTaxAmount DECIMAL(15,4)
,RetailAnnualAmount DECIMAL(15,4)
,FinalAnnualAmount DECIMAL(15,4)
,ShipToContact VARCHAR(80)
,ThinkOrderNo INT
,ShipPhone VARCHAR(32)
,CurrencyCode VARCHAR(3)
,UpdateVersion INT
,Canceled INT
,Held INT
,TaxExempt INT
)


  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
  WITH SERDEPROPERTIES ( 
  'separatorChar' = '|', 
  'quoteChar' = '"', 
  'escapeChar' = '\\' 
  ) 
  STORED AS textfile 
  LOCATION 's3://axle-internal-sources/raw/oess/dotcompaymentgateway_order/'
  TABLE PROPERTIES ('compression_type'='gzip');
DROP VIEW IF EXISTS interna.dotcompg_order; 
CREATE VIEW interna.dotcompg_order AS SELECT * FROM spectrumdb.dotcompaymentgateway_order WITH NO SCHEMA BINDING; 
SELECT COUNT(*) FROM interna.dotcompg_order; 
SELECT TOP 1000 * FROM interna.dotcompg_order;


DROP TABLE IF EXISTS spectrumdb.dotcompaymentgateway_product; 
  CREATE EXTERNAL TABLE spectrumdb.dotcompaymentgateway_product 
(
ProductId varchar(64)
,OrderId varchar(64)
,ProductNo VARCHAR(16)
,Description VARCHAR(255)
,Quantity DECIMAL(25,13)
,UnitPrice DECIMAL(15,4)
,Subtotal DECIMAL(15,4)
,Timestamp BIGINT
,TaxExempt INT
,Recurring VARCHAR(16)
)

  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
  WITH SERDEPROPERTIES ( 
  'separatorChar' = '|', 
  'quoteChar' = '"', 
  'escapeChar' = '\\' 
  ) 
  STORED AS textfile 
  LOCATION 's3://axle-internal-sources/raw/oess/dotcompaymentgateway_product/'
  TABLE PROPERTIES ('compression_type'='gzip'); 
DROP VIEW IF EXISTS interna.dotcompg_product; 
CREATE VIEW interna.dotcompg_product AS SELECT * FROM spectrumdb.dotcompaymentgateway_product WITH NO SCHEMA BINDING; 
SELECT COUNT(*) FROM interna.dotcompg_product; 
SELECT TOP 1000 * FROM interna.dotcompg_product; 

DROP TABLE IF EXISTS spectrumdb.dotcompaymentgateway_scheduledpayment; 

  CREATE EXTERNAL TABLE spectrumdb.dotcompaymentgateway_scheduledpayment 
(
ScheduledPaymentId varchar(64)
,OrderId varchar(64)
,InvoiceId varchar(64)
,PaymentDate TIMESTAMP
,ShippingAmount DECIMAL(15,4)
,TaxAmount DECIMAL(15,4)
,Timestamp BIGINT
,Created TIMESTAMP
,LastAccessed TIMESTAMP
,FirstTransaction INT
,RetailPrice DECIMAL(15,4)
,FinalPrice DECIMAL(15,4)
,OrderIdentity INT
)

  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
  WITH SERDEPROPERTIES ( 
  'separatorChar' = '|', 
  'quoteChar' = '"', 
  'escapeChar' = '\\' 
  ) 
  STORED AS textfile 
  LOCATION 's3://axle-internal-sources/raw/oess/dotcompaymentgateway_scheduledpayment/'
  TABLE PROPERTIES ('compression_type'='gzip');

DROP VIEW IF EXISTS interna.dotcompg_scheduledpayment; 

CREATE VIEW interna.dotcompg_scheduledpayment AS SELECT * FROM spectrumdb.dotcompaymentgateway_scheduledpayment WITH NO SCHEMA BINDING; 

SELECT COUNT(*) FROM interna.dotcompg_scheduledpayment;
 
SELECT TOP 1000 * FROM interna.dotcompg_scheduledpayment; 


DROP TABLE IF EXISTS spectrumdb.dotcompaymentgateway_thinkorder; 

  CREATE EXTERNAL TABLE spectrumdb.dotcompaymentgateway_thinkorder 
(
ThinkOrderId VARCHAR(64)
,ThinkContractId VARCHAR(64)
,OrderId VARCHAR(64)
,Timestamp BIGINT
,OrderStartDate TIMESTAMP
,OrderEndDate TIMESTAMP
,InvoiceOption VARCHAR(32)
,SalesRepInfo VARCHAR(MAX)
,ExchangeInfo VARCHAR(MAX)
,PrepaidNumber VARCHAR(64)
,CustomerRep VARCHAR(64)
,EngagementCode VARCHAR(64)
,ShowDatesOnInvoice INT
,InvoiceComments VARCHAR(240)
,IsEvergreen INT
,RenewalFor INT
,SecondaryOrderNo VARCHAR(32)
,TeamNumber VARCHAR(32)
,BusinessUnit VARCHAR(32)
,SubscriptionId INT
,OriginalSubscriptionDate TIMESTAMP
,InvoicingEmailList VARCHAR(300)
,WatchEmailList VARCHAR(300)
)
  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
  WITH SERDEPROPERTIES ( 
  'separatorChar' = '|', 
  'quoteChar' = '"', 
  'escapeChar' = '\\' 
  ) 
  STORED AS textfile 
  LOCATION 's3://axle-internal-sources/raw/oess/dotcompaymentgateway_thinkorder/'
  TABLE PROPERTIES ('compression_type'='gzip');

DROP VIEW IF EXISTS interna.dotcompg_thinkorder; 

CREATE VIEW interna.dotcompg_thinkorder AS SELECT * FROM spectrumdb.dotcompaymentgateway_thinkorder WITH NO SCHEMA BINDING; 

SELECT COUNT(*) FROM interna.dotcompg_thinkorder;
 
SELECT TOP 1000 * FROM interna.dotcompg_thinkorder; 


DROP TABLE IF EXISTS spectrumdb.dotcompaymentgateway_thinkorderitem; 

  CREATE EXTERNAL TABLE spectrumdb.dotcompaymentgateway_thinkorderitem 
(
ThinkOrderItemId VARCHAR(64)
,ThinkOrderId VARCHAR(64)
,ThinkContractLineId VARCHAR(64)
,ProductNo VARCHAR(16)
,Description VARCHAR(2000)
,QuantityOrdered DECIMAL(25,13)
,QuantityBilled DECIMAL(25,13)
,Amount DECIMAL(15,4)
,TaxExempt INT
,LineItemNo INT
,LineItemType VARCHAR(32)
,Recurring VARCHAR(32)
,FirstPaymentDate TIMESTAMP
,HoldBilling INT
,Timestamp BIGINT
,ListPrice DECIMAL(15,4)
,UseCustomDescription INT
,UnitPrice DECIMAL(15,4)
,UnitOfMeasure VARCHAR(32)
,CustomDescription VARCHAR(2000)
,PackageInformation VARCHAR(200)
)

  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
  WITH SERDEPROPERTIES ( 
  'separatorChar' = '|', 
  'quoteChar' = '"', 
  'escapeChar' = '\\' 
  ) 
  STORED AS textfile 
  LOCATION 's3://axle-internal-sources/raw/oess/dotcompaymentgateway_thinkorderitem/' 
  TABLE PROPERTIES ('compression_type'='gzip');

DROP VIEW IF EXISTS interna.dotcompg_thinkorderitem; 

CREATE VIEW interna.dotcompg_thinkorderitem AS SELECT * FROM spectrumdb.dotcompaymentgateway_thinkorderitem WITH NO SCHEMA BINDING; 

SELECT COUNT(*) FROM interna.dotcompg_thinkorderitem;
 
SELECT TOP 1000 * FROM interna.dotcompg_thinkorderitem; 


DROP TABLE IF EXISTS spectrumdb.dotcompaymentgateway_thinkscheduledpayment; 

  CREATE EXTERNAL TABLE spectrumdb.dotcompaymentgateway_thinkscheduledpayment 
(
ThinkScheduledPaymentId VARCHAR(64)
,ThinkContractLineId VARCHAR(64)
,ThinkOrderId VARCHAR(64)
,InvoiceId VARCHAR(64)
,Timestamp BIGINT
,InstallmentNo INT
,PaymentDate TIMESTAMP
,Recurring VARCHAR(64)
,LineItemNo INT
,ProductNo VARCHAR(64)
,QuantityOrdered DECIMAL(25,13)
,QuantityInvoiced DECIMAL(25,13)
,ExtendedAmount DECIMAL(15,4)
,ThinkOrderItemId VARCHAR(64)
,PaymentStatus VARCHAR(32)
,PaymentMode VARCHAR(32)
,InvoiceOption VARCHAR(32)
,AccessStartDate TIMESTAMP
,AccessEndDate TIMESTAMP
,CustomDescription VARCHAR(2000)
,BilledMonths INT
,ShippingAmount DECIMAL(15,4)
)


  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
  WITH SERDEPROPERTIES ( 
  'separatorChar' = '|', 
  'quoteChar' = '"', 
  'escapeChar' = '\\' 
  ) 
  STORED AS textfile 
  LOCATION 's3://axle-internal-sources/raw/oess/dotcompaymentgateway_thinkscheduledpayment/' 
  TABLE PROPERTIES ('compression_type'='gzip');

DROP VIEW IF EXISTS interna.dotcompg_thinkscheduledpayment; 

CREATE VIEW interna.dotcompg_thinkscheduledpayment AS SELECT * FROM spectrumdb.dotcompaymentgateway_thinkscheduledpayment WITH NO SCHEMA BINDING; 

SELECT COUNT(*) FROM interna.dotcompg_thinkscheduledpayment;
 
SELECT TOP 1000 * FROM interna.dotcompg_thinkscheduledpayment; 


DROP TABLE IF EXISTS spectrumdb.oess_customer; 

  CREATE EXTERNAL TABLE spectrumdb.oess_customer 
(
Id INT
,OracleId INT
,SfId VARCHAR(18)
,Phone VARCHAR(15)
,DefaultBillToAddressSeq INT
,DefaultShipToAddressSeq INT
,PhoneCountryCode VARCHAR(4)
,OracleContactId INT
,OracleEmailId INT
,AuthId VARCHAR(64)
,PasswordUpdateInAuth INT
,PhoneExtension VARCHAR(10)
,Enterprise INT
,DoNotUpdateFromAccount INT
,ExcludeFromRenewalPriceIncrease INT
,SupportPhoneNumber VARCHAR(15)
,AllowedEmailDomains VARCHAR(8000)
,OrderPageCustomerName VARCHAR(8000)
,OrderPagePricingText VARCHAR(255)
,Email VARCHAR(256)
,FirstName VARCHAR(40)
,LastName VARCHAR(40)
,Company VARCHAR(80)
,ParentId INT
,CustomerType CHAR(1)
,WhiteLabelId INT
,PaymentTypeIdsCSV VARCHAR(255)
,IsArchived INT
)
  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
  WITH SERDEPROPERTIES ( 
  'separatorChar' = '|', 
  'quoteChar' = '"', 
  'escapeChar' = '\\' 
  ) 
  STORED AS textfile 
  LOCATION 's3://axle-internal-sources/raw/oess/oess_customer/'
  TABLE PROPERTIES ('compression_type'='gzip');

DROP VIEW IF EXISTS interna.oess_customer; 

CREATE VIEW interna.oess_customer AS SELECT * FROM spectrumdb.oess_customer WITH NO SCHEMA BINDING; 

SELECT COUNT(*) FROM interna.oess_customer;
 
SELECT TOP 1000 * FROM interna.oess_customer; 


DROP TABLE IF EXISTS spectrumdb.oess_customeraddress; 
  CREATE EXTERNAL TABLE spectrumdb.oess_customeraddress 
  (
 Id INT
,CustomerId INT
,OldCustomerAddressSeq INT
,OracleId INT
,AddressType INT
,ContactId INT
,Zip varchar(15)
,Address1 varchar(80)
,Address2 varchar(80)
,City varchar(50)
,State varchar(50)
,FirstName varchar(40)
,LastName varchar(40)
,IsArchived INT
,CreatedDate timestamp
,LastModifiedDate timestamp
,CreatedByUserId INT
,LastModifiedByUserId INT
,Company varchar(80)
)
  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
  WITH SERDEPROPERTIES ( 
  'separatorChar' = '|', 
  'quoteChar' = '"', 
  'escapeChar' = '\\' 
  ) 
  STORED AS textfile 
  LOCATION 's3://axle-internal-sources/raw/oess/oess_customeraddress/' 
  TABLE PROPERTIES ('compression_type'='gzip');
DROP VIEW IF EXISTS interna.oess_customeraddress; 
CREATE VIEW interna.oess_customeraddress AS SELECT * FROM spectrumdb.oess_customeraddress WITH NO SCHEMA BINDING; 
SELECT COUNT(*) FROM interna.oess_customeraddress; 
SELECT TOP 100 * FROM interna.oess_customeraddress;

DROP TABLE IF EXISTS spectrumdb.oess_fulfillmentsystem; 
  CREATE EXTERNAL TABLE spectrumdb.oess_fulfillmentsystem 
  (
   Id INT
  ,Code VARCHAR(50)
  ,Description VARCHAR(250)
  ,IsActive INT
  )
  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
  WITH SERDEPROPERTIES ( 
  'separatorChar' = '|', 
  'quoteChar' = '"', 
  'escapeChar' = '\\' 
  ) 
  STORED AS textfile 
  LOCATION 's3://axle-internal-sources/raw/oess/oess_fulfillmentsystem/'
  TABLE PROPERTIES ('compression_type'='gzip');
DROP VIEW IF EXISTS interna.oess_fulfillmentsystem; 
CREATE VIEW interna.oess_fulfillmentsystem AS SELECT * FROM spectrumdb.oess_fulfillmentsystem WITH NO SCHEMA BINDING; 
SELECT COUNT(*) FROM interna.oess_fulfillmentsystem; 
SELECT TOP 100 * FROM interna.oess_fulfillmentsystem; 

DROP TABLE IF EXISTS spectrumdb.oess_lookupheader; 
  CREATE EXTERNAL TABLE spectrumdb.oess_lookupheader 
 (
Id INT
,Name VARCHAR(50)
,LookupDataTypeId INT
,IsActive int
)
  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
  WITH SERDEPROPERTIES ( 
  'separatorChar' = '|', 
  'quoteChar' = '"', 
  'escapeChar' = '\\' 
  ) 
  STORED AS textfile 
  LOCATION 's3://axle-internal-sources/raw/oess/oess_lookupheader/' 
  TABLE PROPERTIES ('compression_type'='gzip');
DROP VIEW IF EXISTS interna.oess_lookupheader; 
CREATE VIEW interna.oess_lookupheader AS SELECT * FROM spectrumdb.oess_lookupheader WITH NO SCHEMA BINDING; 
SELECT COUNT(*) FROM interna.oess_lookupheader; 
SELECT TOP 1000 * FROM interna.oess_lookupheader; 

DROP TABLE IF EXISTS spectrumdb.oess_lookupitem; 
  CREATE EXTERNAL TABLE spectrumdb.oess_lookupitem 
(
LookupItemId INT
,LookupId INT
,Value VARCHAR(50)
,EnumKey VARCHAR(50)
,DropDownText VARCHAR(75)
,Description VARCHAR(255)
,IsActive int
,IsVisible INT
)
  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
  WITH SERDEPROPERTIES ( 
  'separatorChar' = '|', 
  'quoteChar' = '"', 
  'escapeChar' = '\\' 
  ) 
  STORED AS textfile 
  LOCATION 's3://axle-internal-sources/raw/oess/oess_lookupitem/'
  TABLE PROPERTIES ('compression_type'='gzip');
DROP VIEW IF EXISTS interna.oess_lookupitem; 
CREATE VIEW interna.oess_lookupitem AS SELECT * FROM spectrumdb.oess_lookupitem WITH NO SCHEMA BINDING; 
SELECT COUNT(*) FROM interna.oess_lookupitem; 
SELECT TOP 1000 * FROM interna.oess_lookupitem;


 DROP TABLE IF EXISTS spectrumdb.oess_order; 
  CREATE EXTERNAL TABLE spectrumdb.oess_order 
(
Id INT
,ProcessFlowType SMALLINT
,BusinessUnit SMALLINT
,OrderName VARCHAR(255)
,ExternalOrderId VARCHAR(250)
,BillingInstruction VARCHAR(2000)
,TeamId INT
,EmailList VARCHAR(2000)
,CreatedDate TIMESTAMP
,LastModifiedDate TIMESTAMP
,CreatedByUserId INT
,LastModifiedByUserId INT
,ShipByDate TIMESTAMP
,CreateSFOppertunity INT
,QuotedAmount DECIMAL(18,2)
,DPOrderId INT
,AOPOrderType SMALLINT
,AOPType CHAR(1)
,ContractNumberText VARCHAR(100)
,PromoCode VARCHAR(20)
,OriginalSubscriptionId INT
,StatusPriorToError SMALLINT
,WorkflowStatusPriorToError SMALLINT
,SuccessRedirectUrl VARCHAR(2500)
,FailureRedirectUrl VARCHAR(2500)
,CancelAndReturnUrl VARCHAR(2500)
,DownloadUrl VARCHAR(2500)
,ExternalOrderType SMALLINT
,ActiveSubscriptionId INT
,OrderStatus SMALLINT
,WorkflowStatus SMALLINT
,StatusChangeSource VARCHAR(250)
,PrimarySalesRepNum VARCHAR(6)
,PrimarySalesRepPercentage NUMERIC
,PrimarySalesRepCurrencyCode VARCHAR(3)
,SecondarySalesRepNum VARCHAR(6)
,SecondarySalesRepPercentage NUMERIC
,SecondarySalesRepCurrencyCode VARCHAR(3)
,TertiarySalesRepNum VARCHAR(6)
,TertiarySalesRepPercentage NUMERIC
,TertiarySalesRepCurrencyCode VARCHAR(3)
,DueDate TIMESTAMP
,SalesforceOpportunityId VARCHAR(20)
,PrepaidNumber VARCHAR(150)
,PaymentType SMALLINT
,DeliveryFrequency SMALLINT
,OrderType SMALLINT
,CustomerApproval INT
,CustomerApprovalDate TIMESTAMP
,CustomerApprovalIpAddress VARCHAR(15)
,OriginalOrderId INT
,ExternalSourceCodeId VARCHAR(25)
,OpportunityDescription VARCHAR(255)
,MediaCode VARCHAR(32)
,IsRenewal INT
,Division SMALLINT
,InvoiceType SMALLINT
,SatisfactionGuarantee INT
,ContractTermCount SMALLINT
,Vertical VARCHAR(100)
,ContractObligation SMALLINT
,ContractUpdateFrequency SMALLINT
,FullfillmentType VARCHAR(100)
,ShippingSpeed SMALLINT
,QuoteExpirationDays SMALLINT
,FullfillmentSystem VARCHAR(100)
,IsCurrent INT
,AmendToOrderId INT
,IsAmendment INT
,ShippingAmount NUMERIC
,InvoiceHandlingInstructions VARCHAR(2000)
,LastCreditApprovedDate TIMESTAMP
,LastCreditApprovedAmount NUMERIC
,LastCreditRequestedAmount NUMERIC
,WatchList VARCHAR(1000)
,LastCreditApprovedPaymentType SMALLINT
,SkipQuoteEmail INT
,PaymentFrequency SMALLINT
,BillToAddressSeq INT
,ShipToAddressSeq INT
,InvoiceEmailList VARCHAR(150)
,IsArchived INT
,AOPChildOrderStatus INT
,EmailOption SMALLINT
,ForcedEndDate TIMESTAMP
,StartDate TIMESTAMP
,EndDate TIMESTAMP
,CustomerId INT
,IsEverGreen INT
,PONumber VARCHAR(50)
,BrandingType SMALLINT
,QuoteEmailSent TIMESTAMP
,QuoteEmailSentCounter INT
,AutoResendQuoteEmail INT
,QuoteEmailSentState INT
,CreatedBySourceApplication SMALLINT
,CurrencyCode VARCHAR(3)
,ShipVia VARCHAR(1)
,ShipType VARCHAR(1)
,IsAlreadyShipped INT
,RenewToFrequency SMALLINT
,ESignAgreementId VARCHAR(2048)
,IsESignRequired INT
,RenewalOption INT
)

  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
  WITH SERDEPROPERTIES ( 
  'separatorChar' = '|', 
  'quoteChar' = '"', 
  'escapeChar' = '\\' 
  ) 
  STORED AS textfile 
  LOCATION 's3://axle-internal-sources/raw/oess/oess_order/' 
  TABLE PROPERTIES ('compression_type'='gzip');

DROP VIEW IF EXISTS interna.oess_order; 

CREATE VIEW interna.oess_order AS SELECT * FROM spectrumdb.oess_order WITH NO SCHEMA BINDING; 

SELECT COUNT(*) FROM interna.oess_order;
 
SELECT TOP 1000 * FROM interna.oess_order;

 
DROP TABLE IF EXISTS spectrumdb.oess_ordercancel; 
  CREATE EXTERNAL TABLE spectrumdb.oess_ordercancel 
 (
OrderId INT
,CancelReasonCode varchar(15)
,CancelReason varchar(128)
,PreviousOrderStatus INT
,Created timestamp
,Updated timestamp
,RequestIsFromCredit INT
,CancelRequestDate timestamp
,CancelNote VARCHAR(140)
,CancelOrderStatus Smallint
,OriginalSubscriptionId INT
,CancelWorkflowStatus smallint
,SequenceNo INT
,CancelDate timestamp
)
  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
  WITH SERDEPROPERTIES ( 
  'separatorChar' = '|', 
  'quoteChar' = '"', 
  'escapeChar' = '\\' 
  ) 
  STORED AS textfile 
  LOCATION 's3://axle-internal-sources/raw/oess/oess_ordercancel/'
  TABLE PROPERTIES ('compression_type'='gzip');
DROP VIEW IF EXISTS interna.oess_ordercancel; 
CREATE VIEW interna.oess_ordercancel AS SELECT * FROM spectrumdb.oess_ordercancel WITH NO SCHEMA BINDING; 
SELECT COUNT(*) FROM interna.oess_ordercancel; 
SELECT TOP 1000 * FROM interna.oess_ordercancel; 

DROP TABLE IF EXISTS spectrumdb.oess_orderfulfillmentsystems; 
  CREATE EXTERNAL TABLE spectrumdb.oess_orderfulfillmentsystems 
(

Id INT
,OrderId INT
,FulfillmentSystemId INT
,ExternalReferenceId VARCHAR(100)
,CreatedByUserId INT
,CreatedDate timestamp
,LastModifiedByUserId INT
,LastModifiedDate timestamp
)

  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
  WITH SERDEPROPERTIES ( 
  'separatorChar' = '|', 
  'quoteChar' = '"', 
  'escapeChar' = '\\' 
  ) 
  STORED AS textfile 
  LOCATION 's3://axle-internal-sources/raw/oess/oess_orderfulfillmentsystems/'
  TABLE PROPERTIES ('compression_type'='gzip');
DROP VIEW IF EXISTS interna.oess_orderfulfillmentsystems; 
CREATE VIEW interna.oess_orderfulfillmentsystems AS SELECT * FROM spectrumdb.oess_orderfulfillmentsystems WITH NO SCHEMA BINDING; 
SELECT COUNT(*) FROM interna.oess_orderfulfillmentsystems; 
SELECT TOP 1000 * FROM interna.oess_orderfulfillmentsystems; 

DROP TABLE IF EXISTS spectrumdb.oess_orderitem; 
  CREATE EXTERNAL TABLE spectrumdb.oess_orderitem 
 (
Id INT
,OrderId INT
,OldSequenceNo INT2
,OESSProductCode VARCHAR(200)
,PricePerThousand Varchar(30) 
,InvoiceDescription VARCHAR(8000)
,CreatedDate Timestamp
,LastModifiedDate timestamp
,CreatedByUserId INT
,LastModifiedByUserId INT
,RateCard INT2
,Discount boolean
,PgOrderItemSeq INT
,PackageEditionCode VARCHAR(50)
,CreditQueueStatus INT2
,PriceOverrideReason VARCHAR(255)
,OESSBasePrice Varchar(30) 
,OESSFinalPrice varchar(30) 
,MailCardId INT
,EstimatedQty INT
,UnitPrice INTEGER
,UnitOfMeasure INT2
,EstimatedAmount INTEGER
,CreditRange VARCHAR(50)
,CustomPackageId INT
,ProductSubTypeId INT
,Credits INT
,Quantity INT
,DisplayOrder INT
,IsToSendShippingProductUpdate boolean
,BookEditionCode VARCHAR(10)
,PolkClassCode VARCHAR(10)
,PolkTitleId INT
)
  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
  WITH SERDEPROPERTIES ( 
  'separatorChar' = '|', 
  'quoteChar' = '"', 
  'escapeChar' = '\\' 
  ) 
  STORED AS textfile 
  LOCATION 's3://axle-internal-sources/raw/oess/oess_orderitem/'
  TABLE PROPERTIES ('compression_type'='gzip');
DROP view IF EXISTS interna.oess_orderitem; 
CREATE VIEW interna.oess_orderitem AS SELECT * FROM spectrumdb.oess_orderitem WITH NO SCHEMA BINDING; 
SELECT COUNT(*) FROM interna.oess_orderitem; 
SELECT TOP 1000 * FROM interna.oess_orderitem; 


DROP TABLE IF EXISTS spectrumdb.oess_ordertype; 
  CREATE EXTERNAL TABLE spectrumdb.oess_ordertype 
(
Id INT
,Code VARCHAR(50)
,Description VARCHAR(250)
,IsActive INT
)

  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
  WITH SERDEPROPERTIES ( 
  'separatorChar' = '|', 
  'quoteChar' = '"', 
  'escapeChar' = '\\' 
  ) 
  STORED AS textfile 
  LOCATION 's3://axle-internal-sources/raw/oess/oess_ordertype/'
  TABLE PROPERTIES ('compression_type'='gzip');
DROP VIEW IF EXISTS interna.oess_ordertype; 
CREATE VIEW interna.oess_ordertype AS SELECT * FROM spectrumdb.oess_ordertype WITH NO SCHEMA BINDING; 
SELECT COUNT(*) FROM interna.oess_ordertype; 
SELECT TOP 1000 * FROM interna.oess_ordertype; 


DROP TABLE IF EXISTS spectrumdb.oess_productlookup; 
  CREATE EXTERNAL TABLE spectrumdb.oess_productlookup 
 (
Id INT
,PaymentFrequencyId Smallint
,ModuleId INT
,Currency VARCHAR(10)
,ProductCode VARCHAR(100)
,IsActive INT
,ProductType Smallint
,RateClassId INT
,ProcessFlowType INT
,IsVisible INT
,SupressQueueMessage INT
,Groups VARCHAR(5000)
,ExcludeOnReadActiveModule INT
,CategoryId INT
,SiteId Smallint
,AllowMultiplePerOrder INT
,AllowModuleSwap INT
,ScheduleInOESS INT
,DepositAllCredit INT
,Acronym VARCHAR(10)
,PLProductTypeId Smallint
,OrderCode VARCHAR(30)
,Description NVARCHAR(80)
,OracleProductCode VARCHAR(80)
,IsLicenseRequired Smallint
,IsShippable INT
,PolkClassCodeConfig VARCHAR(20)
)

  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
  WITH SERDEPROPERTIES ( 
  'separatorChar' = '|', 
  'quoteChar' = '"', 
  'escapeChar' = '\\' 
  ) 
  STORED AS textfile 
  LOCATION 's3://axle-internal-sources/raw/oess/oess_productlookup/'
  TABLE PROPERTIES ('compression_type'='gzip');
DROP View IF EXISTS interna.oess_productlookup; 
CREATE VIEW interna.oess_productlookup AS SELECT * FROM spectrumdb.oess_productlookup WITH NO SCHEMA BINDING; 
SELECT COUNT(*) FROM interna.oess_productlookup; 
SELECT TOP 1000 * FROM interna.oess_productlookup; 


DROP TABLE IF EXISTS spectrumdb.origin_accounts; 

  CREATE EXTERNAL TABLE spectrumdb.origin_accounts 
(
Id INT
,ThinkId INT
,AccountNumber CHAR(10)
,Name VARCHAR(80)
,Address1 VARCHAR(50)
,Address2 VARCHAR(50)
,City VARCHAR(50)
,State VARCHAR(20)
,ZipCode VARCHAR(20)
,Country VARCHAR(50)
,Phone VARCHAR(25)
,Email VARCHAR(256)
,SalesForceId VARCHAR(20)
,MediaCode VARCHAR(20)
,SanNumber VARCHAR(18)
,SanExpires DATE
,IsSanReadonly INT
,MaxUsers INT
,IsApproved INT
,IsSingleSignOn INT
,IsDemo INT
,IsThinkCompleted INT
,CaptchaInternal INT
,CaptchaExternal INT
,AllowScraping INT
,HideInactiveModules INT
,RedirectToDefaultSite INT
,CreditSubscription INT
,UsesAllocation INT
,UsesSharedCredits INT
,LiveBall INT
,HasCorporateSuppression INT
,HasSalesDeskAccountSuppression INT
,HasEnterpriseContactManager INT
,CanSaveSearches INT
,HasLiveChat INT
,HasUserSan INT
,HasUserCreditSchedule INT
,IsArchiveCompleted INT
,IsPolkOnly INT
,IsEnterprise INT
,HideWaiver INT
,HasEcommerceFlow INT
,SanOrganizationId VARCHAR(14)
,StartDate TIMESTAMP
,EndDate TIMESTAMP
,CreateDate TIMESTAMP
,AccountType SMALLINT
,SiteId INT
,SalesRepId INT
,SalesDeskInstance INT
,LegacyIsSource INT
,ThinkMigrationId INT
,Version BIGINT
,Url VARCHAR(255)
,MigratedPurchasesStatus SMALLINT
,ApplyAccountSuppression INT
,ExpirePurchasedRecords INT
,OracleSalesRepId VARCHAR(10)
,IsActive INT
,AllowUnAssistedCheckout INT
,EnforceDEMinimumOrder INT
,CaSanNumber VARCHAR(18)
,CaRanNumber VARCHAR(18)
,CaDownloadKey VARCHAR(18)
,CaSanExpires DATE
,UseCreditsForEmailCampaign INT
,DpCreateBoughtINTsets INT
,HasClientApi INT
,HasOracleIntegration INT
,CanChangeUsername INT
,SuppressSalesForce INT
,CanAccessEmailRetention INT
,CustomerDataOverridesCorpSuppression INT
,DisplaySalesforceExports INT
,HasOracleAccountIntegration INT
,IsTeamManagementEnabled INT
,CustomerDataNoFileSizeLimit INT
,RequiresStrongPassword INT
,HideLinkedLogins INT
,DatalynxId INT
,IsAccountLevelLeadStatus INT
,AccountLevelLeadStatusId INT
,RequiresTwoFactorAuth INT
,Is360Enabled INT
,FileManagementId VARCHAR(64)
,HideEnterpriseSettings INT
,WhiteLabelId INT
,DmUseCardOnFile INT
,DMDueUponReceipt INT
,CrmSettingsId INT
,IsFollowUpEmailDigestEnabled INT
)
  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
  WITH SERDEPROPERTIES ( 
  'separatorChar' = '|', 
  'quoteChar' = '"', 
  'escapeChar' = '\\' 
  ) 
  STORED AS textfile 
  LOCATION 's3://axle-internal-sources/raw/oess/origin_accounts/'
TABLE PROPERTIES ('compression_type'='gzip');  

DROP VIEW IF EXISTS interna.origin_accounts; 

CREATE VIEW interna.origin_accounts AS SELECT * FROM spectrumdb.origin_accounts WITH NO SCHEMA BINDING; 

SELECT COUNT(*) FROM interna.origin_accounts;
 
SELECT TOP 1000 * FROM interna.origin_accounts; 


DROP TABLE IF EXISTS spectrumdb.origin_eventsummarybyuser; 

  CREATE EXTERNAL TABLE spectrumdb.origin_eventsummarybyuser 
(
Date DATE
,AccountId INT
,UserId INT
,EventType SMALLINT
,EventSubType SMALLINT
,Count INT
,Sum INT
,LegacyIsSource INT
,IsAdjustment INT
)

  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
  WITH SERDEPROPERTIES ( 
  'separatorChar' = '|', 
  'quoteChar' = '"', 
  'escapeChar' = '\\' 
  ) 
  STORED AS textfile 
  LOCATION 's3://axle-internal-sources/raw/oess/origin_eventsummarybyuser/' 
 TABLE PROPERTIES ('compression_type'='gzip');

DROP VIEW IF EXISTS interna.origin_eventsummarybyuser; 

CREATE VIEW interna.origin_eventsummarybyuser AS SELECT * FROM spectrumdb.origin_eventsummarybyuser WITH NO SCHEMA BINDING; 

SELECT COUNT(*) FROM interna.origin_eventsummarybyuser;
 
SELECT TOP 1000 * FROM interna.origin_eventsummarybyuser; 


DROP TABLE IF EXISTS spectrumdb.origin_users; 

  CREATE EXTERNAL TABLE spectrumdb.origin_users 
(
Id INT
,AccountId INT
,ThinkId INT
,UserName VARCHAR(256)
,Password VARCHAR(128)
,PasswordSalt VARCHAR(40)
,FirstName VARCHAR(128)
,LastName VARCHAR(128)
,Email VARCHAR(256)
,SanNumber VARCHAR(18)
,SanExpires DATE
,IsSanReadonly INT
,EmailVerifyRequest TIMESTAMP
,EmailVerified TIMESTAMP
,IsApproved INT
,IsThinkCompleted INT
,IsDeleted INT
,IsExternal INT
,IsLockedOut INT
,IsEnterprise INT
,PasswordChangeRequired INT
,FailedPasswordAttemptCount INT
,LastPasswordChangedDate TIMESTAMP
,LastLockoutDate TIMESTAMP
,StartDate TIMESTAMP
,EndDate TIMESTAMP
,CreateDate TIMESTAMP
,LegacyId INT
,LegacyIsSource INT
,ClassicPassword VARCHAR(40)
,MigratedUser INT
,MigratedFirstTimeLogin INT
,ThinkMigrationId INT
,Version BIGINT
,IsUnderMaintenance INT
,BlockSiteLogin INT
,UserType INT
,MaintenanceStatus SMALLINT
,SubscriptionType SMALLINT
,SalesForceId VARCHAR(20)
,SanOrganizationId VARCHAR(14)
,SubscriberDiscount INT
,ReblastDiscount INT
,ReblastDiscountDays INT
,OracleSalesRepId VARCHAR(10)
,DataEnhancementDiscount INT
,IsInternal INT
,DirectMailDiscount INT
,PricePerLead DECIMAL(5,2)
,Is24HrPassUsed INT
,HasContactManager INT
,HasActiveModule INT
,CaSanNumber VARCHAR(18)
,CaRanNumber VARCHAR(18)
,CaDownloadKey VARCHAR(18)
,CaSanExpires DATE
,IsEmailVerified INT
,AuthId VARCHAR(64)
,IsActive INT
,DatalynxId INT
,IsCCDeclined INT
,TimeZone VARCHAR(35)
,FileManagementId VARCHAR(64)
,TriedSubscribeOnline INT
)
  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
  WITH SERDEPROPERTIES ( 
  'separatorChar' = '|', 
  'quoteChar' = '"', 
  'escapeChar' = '\\' 
  ) 
  STORED AS textfile 
  LOCATION 's3://axle-internal-sources/raw/oess/origin_users/' 
  TABLE PROPERTIES ('compression_type'='gzip');

DROP VIEW IF EXISTS interna.origin_users; 

CREATE VIEW interna.origin_users AS SELECT * FROM spectrumdb.origin_users WITH NO SCHEMA BINDING; 

SELECT COUNT(*) FROM interna.origin_users;
 
SELECT TOP 1000 * FROM interna.origin_users; 


DROP TABLE IF EXISTS spectrumdb.dotcompaymentgateway_invoicelineitem; 

  CREATE EXTERNAL TABLE spectrumdb.dotcompaymentgateway_invoicelineitem 
(
InvoiceLineItemId Varchar(64)
,InvoiceId Varchar(64)
,ThinkContractLineId Varchar(64)
,Timestamp Bigint
,LineItemType VARCHAR(32)
,LineItemNo INT
,ProductNo VARCHAR(16)
,QuantityOrdered DECIMAL(25,13)
,QuantityInvoiced DECIMAL(25,13)
,UnitOfMeasure VARCHAR(16)
,ExtendedAmount DECIMAL(15,4)
,SalesRepInfo VARCHAR(MAX)
,CommentsInfo VARCHAR(MAX)
,Description VARCHAR(1500)
,UnitPrice DECIMAL(15,4)
)
  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
  WITH SERDEPROPERTIES ( 
  'separatorChar' = '|', 
  'quoteChar' = '"', 
  'escapeChar' = '\\' 
  ) 
  STORED AS textfile 
  LOCATION 's3://axle-internal-sources/raw/oess/dotcompaymentgateway_invoicelineitem/'
 TABLE PROPERTIES ('compression_type'='gzip');

DROP VIEW IF EXISTS interna.dotcompg_invoicelineitem; 

CREATE VIEW interna.dotcompg_invoicelineitem AS SELECT * FROM spectrumdb.dotcompaymentgateway_invoicelineitem WITH NO SCHEMA BINDING; 

SELECT COUNT(*) FROM interna.dotcompg_invoicelineitem;
 
SELECT TOP 1000 * FROM interna.dotcompg_invoicelineitem; 


DROP TABLE IF EXISTS spectrumdb.oess_license; 

  CREATE EXTERNAL TABLE spectrumdb.oess_license 
(
ThinkOrderId INT
,ThinkOrderItemSeq INT
,LicenseSeq INT
,ThinkCustomerId INT
)
  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
  WITH SERDEPROPERTIES ( 
  'separatorChar' = '|', 
  'quoteChar' = '"', 
  'escapeChar' = '\\' 
  ) 
  STORED AS textfile 
  LOCATION 's3://axle-internal-sources/raw/oess/oess_license/' 
 TABLE PROPERTIES ('compression_type'='gzip'); 

DROP View IF EXISTS interna.oess_license; 

CREATE VIEW interna.oess_license AS SELECT * FROM spectrumdb.oess_license WITH NO SCHEMA BINDING; 

SELECT COUNT(*) FROM interna.oess_license;
 
SELECT TOP 1000 * FROM interna.oess_license; 

DROP TABLE IF EXISTS spectrumdb.oess_rpt_order_detail; 
CREATE EXTERNAL TABLE spectrumdb.oess_rpt_order_detail 
(
"Order Id" VARCHAR(12)
,ProductId VARCHAR(12)
,"Sales Rep ID" VARCHAR(30)
,"OESS Customer Number" VARCHAR(12)
,"Oracle Customer Number" VARCHAR(12)
,"Contact Name" varchar(81)
,email VARCHAR(256)
,"Contact Phone" VARCHAR(15)
,"Contact Salesforce ID" VARCHAR(18)
,"Order Status" VARCHAR(50)
,"Order Created Date" timestamp
,"Order Start Date" timestamp
,"Order End Date" timestamp
,ProductDescription varchar(80)
,ListPrice VARCHAR(41)
,Subtotal VARCHAR(41)
,DiscountReason VARCHAR(255)
,NoOfUsers VARCHAR(12)
,"Credits Selected" VARCHAR(12)
,SourceCode VARCHAR(5)
,Currency VARCHAR(3)
,BusinessUnit VARCHAR(4)
,ProcessFlowType VARCHAR(4)
,zzaux_infogroup_type VARCHAR(12)
,zzaux_origing_application VARCHAR(50)
,"Salesforce Opportunity ID" VARCHAR(20)
,"Media Code" VARCHAR(32)
,"Promo Code" VARCHAR(20)
,PromoCodeDescription VARCHAR(250)
,PackageEditionCode VARCHAR(50)
,PackageEditionDescription VARCHAR(200)
,zzaux_original_order_id VARCHAR(12)
,Evergreen VARCHAR(1)
,zzaux_is_renewal VARCHAR(1)
,OriginalSubscriptionId VARCHAR(12)
,"Billing Address" varchar(80)
,"Billing City" varchar(50)
,"Billing State" varchar(50)
,"Billing Zip" varchar(15)
,"OrderAmountBeforeCancel" VARCHAR(41)
,ExternalOrderId VARCHAR(250)
,ExternalOrderType VARCHAR(255)
,Vertical VARCHAR(100)
,Site VARCHAR(250)
,ProductType VARCHAR(250)
,MailClass VARCHAR(250)
,MailCardMaterial VARCHAR(250)
,MailerType VARCHAR(250)
,MailSize VARCHAR(50)
,ContractObligation VARCHAR(4)
,ContractUpdateFrequency VARCHAR(4)
,Delivery VARCHAR(1000)
,FullfillmentSystem VARCHAR(1000)
,SecondarySalesRepNum VARCHAR(6)
,TertiarySalesRepNum VARCHAR(6)
,PrimarySalesRepPercentage VARCHAR(41)
,SecondarySalesRepPercentage VARCHAR(41)
,TertiarySalesRepPercentage VARCHAR(41)
,ContractAmendmentDate timestamp
,IsAmendment VARCHAR(1)
,OrderName VARCHAR(255)
,IsCurrent VARCHAR(1)
,InvoiceDescription VARCHAR(8000)
,EstimatedAmount VARCHAR(41)
,DiscountApplied VARCHAR(41)
,DiscountPercentage VARCHAR(41)
,"Sales Rep Name" varchar(100)
,"Division Number" VARCHAR(30)
,"OESS Account ID" VARCHAR(12)
,"Account Phone" VARCHAR(15)
,"Account Salesforce ID" VARCHAR(18)
,customer_id VARCHAR(12)
,"Account Name" varchar(80)
,"Account Address" varchar(80)
,"Account City" varchar(50)
,"Account State" varchar(50)
,"Account Zip" varchar(15)
,"Total Licenses" VARCHAR(12)
,"Total Credits on Order" VARCHAR(12)
,"Order Final Price" VARCHAR(41)
,"Order Retail Price" VARCHAR(41)
,"Original Order Id" VARCHAR(12)
,"Recurring Payment Frequency" VARCHAR(50)
,TotalOrderAmountBeforeCancel VARCHAR(41)
,"Customer Login" VARCHAR(100)
,TotalEstimatedAmount VARCHAR(41)
,WhiteLabelText VARCHAR(255)
,CreatedByEmail VARCHAR(100)
,CreatedByFirstName VARCHAR(50)
,CreatedByLastName VARCHAR(50)
,CreatedByLogin VARCHAR(50)
,LastModifiedEmail VARCHAR(100)
,LastModifiedFirstName VARCHAR(50)
,LastModifiedLastName VARCHAR(50)
,LastModifiedLogin VARCHAR(50)
,"Override End Date" timestamp
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
'separatorChar' = '|', 
'quoteChar' = '"', 
'escapeChar' = '~' 
) 
STORED AS textfile 
LOCATION 's3://axle-internal-sources/raw/oess/oess_view_rpt_orders_detail/'
TABLE PROPERTIES ('compression_type'='gzip');  
DROP VIEW IF EXISTS interna.oess_rpt_order_detail; 
CREATE VIEW interna.oess_rpt_order_detail AS SELECT * FROM spectrumdb.oess_rpt_order_detail WITH NO SCHEMA BINDING; 
SELECT COUNT(*) FROM interna.oess_rpt_order_detail; 
SELECT TOP 100 * FROM interna.oess_rpt_order_detail;


