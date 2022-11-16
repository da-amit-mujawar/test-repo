/*    ==Scripting Parameters==

    Source Server Version : SQL Server 2014 (12.0.6024)
    Source Database Engine Edition : Microsoft SQL Server Standard Edition
    Source Database Engine Type : Standalone SQL Server

    Target Server Version : SQL Server 2014
    Target Database Engine Edition : Microsoft SQL Server Standard Edition
    Target Database Engine Type : Standalone SQL Server
*/

USE [OESS]
GO

/****** Object:  View [dbo].[View_OrderFulFillmentSystems_DSG]    Script Date: 5/7/2021 4:53:15 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER OFF
GO

ALTER view [dbo].[View_OrderFulFillmentSystems_DSG] As Select '"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),Id),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  '"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),OrderId),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  '"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),FulfillmentSystemId),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  '"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),ExternalReferenceId),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  '"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),CreatedByUserId),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  '"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(FORMAT(CreatedDate,'yyyy-MM-dd HH:mm:ss'),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  '"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),LastModifiedByUserId),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  '"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(FORMAT(LastModifiedDate,'yyyy-MM-dd HH:mm:ss'),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"' as cline from [OESS].[dbo].[OrderFulFillmentSystems]  
GO


