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

/****** Object:  View [dbo].[View_OrderCancel_DSG]    Script Date: 5/7/2021 4:39:19 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER OFF
GO



ALTER view [dbo].[View_OrderCancel_DSG] As Select '"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),OrderId),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  '"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),CancelReasonCode),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  '"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),CancelReason),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  '"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),PreviousOrderStatus),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  '"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(FORMAT(Created,'yyyy-MM-dd HH:mm:ss'),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  '"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(FORMAT(Updated,'yyyy-MM-dd HH:mm:ss'),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  '"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),RequestIsFromCredit),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  '"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(FORMAT(CancelRequestDate,'yyyy-MM-dd HH:mm:ss'),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  '"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),CancelNote),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  '"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),CancelOrderStatus),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  '"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),OriginalSubscriptionId),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  '"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),CancelWorkflowStatus),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  '"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),SequenceNo),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  '"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(FORMAT(CancelDate,'yyyy-MM-dd HH:mm:ss'),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"' as cline from [OESS].[dbo].[OrderCancel]  
GO

