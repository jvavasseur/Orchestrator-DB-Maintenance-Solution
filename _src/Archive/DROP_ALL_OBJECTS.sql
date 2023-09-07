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

DROP PROCEDURE IF EXISTS [Maintenance].[ValidateArchiveObjects]
DROP PROCEDURE IF EXISTS [Maintenance].[ValidateArchiveObjectsLogs]
DROP PROCEDURE IF EXISTS [Maintenance].[ParseJsonArchiveLogs]
DROP PROCEDURE IF EXISTS [Maintenance].[ParseJsonArchiveLogs]
DROP PROCEDURE IF EXISTS [Maintenance].[AddArchiveTriggerLogs]
DROP PROCEDURE IF EXISTS [Maintenance].[ArchiveLogs]
DROP PROCEDURE IF EXISTS [Maintenance].[CleanupSyncedLogs]

DROP TABLE IF EXISTS [Maintenance].[Messages]
DROP TABLE IF EXISTS [Maintenance].[Runs]
DROP TABLE IF EXISTS [Maintenance].[ASyncStatus_Logs]
DROP TABLE IF EXISTS [Maintenance].[Delete_Logs]
DROP TABLE IF EXISTS [Maintenance].[Filter_Logs]
DROP TABLE IF EXISTS [Maintenance].[Sync_Logs]
DROP TABLE IF EXISTS [Maintenance].[Archive_Logs]
DROP SCHEMA  IF EXISTS  [Maintenance]


