/*    ==Scripting Parameters==

    Source Server Version : SQL Server 2014 (12.0.6024)
    Source Database Engine Edition : Microsoft SQL Server Standard Edition
    Source Database Engine Type : Standalone SQL Server

    Target Server Version : SQL Server 2014
    Target Database Engine Edition : Microsoft SQL Server Standard Edition
    Target Database Engine Type : Standalone SQL Server
*/

USE [DotcomPaymentGateway]
GO

/****** Object:  View [dbo].[View_Order_DSG]    Script Date: 5/8/2021 12:59:49 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER OFF
GO

ALTER view [dbo].[View_Order_DSG] As Select
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),OrderId),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),CustomerId),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),OriginApplication),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),SmsOrder),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),DatabaseType),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),PaymentMode),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),FullAuth),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),PaymentProfileId),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(BIGINT,Timestamp),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),CheckoutState),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),Recurring),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),Quantity),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),AuthAmount),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),RetailPrice),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),FinalPrice),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),ShippingAmount),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),TaxAmount),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(FORMAT(Created,'yyyy-MM-dd HH:mm:ss'),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(FORMAT(LastAccessed,'yyyy-MM-dd HH:mm:ss'),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),VendorId),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),ParentVendorId),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),AccountSalesRep),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),OracleProductId),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),MediaCode),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),ShipAddress1),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),ShipAddress2),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),ShipCity),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),ShipState),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),ShipPostalCode),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),ShipCountry),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),ShippingMethod),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),PurchaseOrder),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),SettlementSystem),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),NumberBillingCycles),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),ShipToCompany),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),AnnualShippingAmount),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),AnnualTaxAmount),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),RetailAnnualAmount),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),FinalAnnualAmount),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),ShipToContact),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),ThinkOrderNo),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),ShipPhone),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),CurrencyCode),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),UpdateVersion),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),Canceled),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),Held),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),TaxExempt),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"' as cline from [DotComPaymentGateway].[dbo].[Order] 
GO


