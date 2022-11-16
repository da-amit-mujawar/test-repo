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

/****** Object:  View [dbo].[View_Scheduledpayment_DSG]    Script Date: 5/9/2021 10:02:09 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER OFF
GO

ALTER view [dbo].[View_Scheduledpayment_DSG] As Select '"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),ScheduledPaymentId),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  '"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),OrderId),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  '"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),InvoiceId),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  '"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(FORMAT(PaymentDate,'yyyy-MM-dd HH:mm:ss'),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  '"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),ShippingAmount),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  '"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),TaxAmount),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  '"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(BIGINT,Timestamp),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  '"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(FORMAT(Created,'yyyy-MM-dd HH:mm:ss'),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  '"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(FORMAt(LastAccessed,'yyyy-MM-dd HH:mm:ss'),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  '"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),FirstTransaction),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  '"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),RetailPrice),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  '"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),FinalPrice),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  '"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),OrderIdentity),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"' as cline from [DotComPaymentGateway].[dbo].[Scheduledpayment]  
GO


