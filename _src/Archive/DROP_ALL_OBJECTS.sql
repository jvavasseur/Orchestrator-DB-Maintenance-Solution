SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

/*
SELECT * FROM sys.schemas
SELECT * FROM sys.tables
SELECT * FROM sys.procedures
*/

DROP PROCEDURE IF EXISTS [Maintenance].[AddRunMessage]
DROP PROCEDURE IF EXISTS [Maintenance].[DeleteRuns]
DROP PROCEDURE IF EXISTS [Maintenance].[ValidateASyncArchiveObjectsLogs]
DROP PROCEDURE IF EXISTS [Maintenance].[ValidateASyncArchiveObjects]
DROP PROCEDURE IF EXISTS [Maintenance].[ASyncCleanupLogs]
DROP TABLE IF EXISTS [Maintenance].[Messages]
DROP TABLE IF EXISTS [Maintenance].[Runs]
DROP TABLE IF EXISTS [Maintenance].[ASyncStatus_Logs]
DROP SCHEMA  IF EXISTS  [Maintenance]

