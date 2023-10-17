SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

/*
SELECT [Schema] = OBJECT_SCHEMA_NAME(object_id), [Procedure] = [name], [DROP] =  N'DROP PROCEDURE IF EXISTS ' + QUOTENAME(OBJECT_SCHEMA_NAME(object_id)) + N'.' + QUOTENAME(name) + N';' FROM sys.procedures WHERE OBJECT_SCHEMA_NAME(object_id) = N'Maintenance' ORDER BY [name] ASC;
SELECT [Schema] = OBJECT_SCHEMA_NAME(object_id), [Synonym] = [name], [DROP] =  N'IF EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = N''' + [name] + N''' AND OBJECT_SCHEMA_NAME(object_id) = N''' + OBJECT_SCHEMA_NAME(object_id) + ''') DROP SYNONYM ' + QUOTENAME(OBJECT_SCHEMA_NAME(object_id)) + N'.' + QUOTENAME(name) + N';' FROM sys.synonyms WHERE OBJECT_SCHEMA_NAME(object_id) = N'Maintenance' ORDER BY [name] ASC;
SELECT [Schema] = OBJECT_SCHEMA_NAME(object_id), [External Table] = [name], [DROP] =  N'IF EXISTS(SELECT 1 FROM sys.external_tables WHERE [name] = N''' + [name] + N''' AND OBJECT_SCHEMA_NAME(object_id) = N''' + OBJECT_SCHEMA_NAME(object_id) + ''') DROP EXTERNAL TABLE ' + QUOTENAME(OBJECT_SCHEMA_NAME(object_id)) + N'.' + QUOTENAME(name) + N';' FROM sys.external_tables WHERE OBJECT_SCHEMA_NAME(object_id) = N'Maintenance' ORDER BY [name] ASC;
SELECT [Schema] = OBJECT_SCHEMA_NAME(object_id), [External Table] = [name], [DROP] =  N'IF EXISTS(SELECT 1 FROM sys.external_tables WHERE [name] = N''' + [name] + N''' AND OBJECT_SCHEMA_NAME(object_id) = N''' + OBJECT_SCHEMA_NAME(object_id) + ''') DROP EXTERNAL TABLE ' + QUOTENAME(OBJECT_SCHEMA_NAME(object_id)) + N'.' + QUOTENAME(name) + N';' FROM sys.external_tables WHERE OBJECT_SCHEMA_NAME(object_id) = N'dbo' AND [name] IN (N'AuditLogEntities', N'AuditLogs', N'Jobs', N'Logs', N'QueueItemComments', N'QueueItemEvents', N'QueueItems', N'RobotLicenseLogs', N'Tenants') ORDER BY [name] ASC;
SELECT [Schema] = OBJECT_SCHEMA_NAME(object_id), [View] = [name], [DROP] =  N'DROP VIEW IF EXISTS ' + QUOTENAME(OBJECT_SCHEMA_NAME(object_id)) + N'.' + QUOTENAME(name) + N';' FROM sys.views WHERE OBJECT_SCHEMA_NAME(object_id) = N'Maintenance' ORDER BY [name] ASC;
SELECT [Schema] = OBJECT_SCHEMA_NAME(object_id), [Table] = OBJECT_NAME(parent_object_id), [Constraint] = [name], [DROP] =  N'IF OBJECT_ID(N''' + QUOTENAME(OBJECT_SCHEMA_NAME(parent_object_id)) + N'.' + QUOTENAME(OBJECT_NAME(parent_object_id)) + ''') IS NOT NULL ALTER TABLE ' + QUOTENAME(OBJECT_SCHEMA_NAME(parent_object_id)) + N'.' + QUOTENAME(OBJECT_NAME(parent_object_id)) + N' DROP CONSTRAINT IF EXISTS ' + QUOTENAME([name]) + N';' FROM sys.foreign_keys WHERE [type] = N'F' AND OBJECT_SCHEMA_NAME(parent_object_id) = N'Maintenance' ORDER BY REVERSE(OBJECT_NAME(parent_object_id)) DESC, [name] ASC;
SELECT [Schema] = OBJECT_SCHEMA_NAME(object_id), [Table] = [name], [DROP] =  N'DROP TABLE IF EXISTS ' + QUOTENAME(OBJECT_SCHEMA_NAME(object_id)) + N'.' + QUOTENAME(name) + N';' FROM sys.tables WHERE OBJECT_SCHEMA_NAME(object_id) = N'Maintenance' AND is_external = 0 ORDER BY REVERSE([name]) DESC;

SELECT [Schema] = OBJECT_SCHEMA_NAME(object_id), [Procedure] = [name], N'==> object'= N'==>', * FROM sys.procedures;
SELECT [Schema] = OBJECT_SCHEMA_NAME(object_id), [Synonym] = [name], N'==> object'= N'==>', * FROM sys.synonyms;
SELECT [Schema] = OBJECT_SCHEMA_NAME(object_id), [External Table] = [name], N'==> object'= N'==>', * FROM sys.external_tables;
SELECT [Schema] = OBJECT_SCHEMA_NAME(object_id), [View] = [name], N'==> object'= N'==>', * FROM sys.views;
SELECT [Schema] = OBJECT_SCHEMA_NAME(object_id), [Table] = OBJECT_NAME(parent_object_id), [Foreign Key] = [name], N'==> object'= N'==>', * FROM sys.foreign_keys WHERE [type] = N'F'
SELECT [Schema] = OBJECT_SCHEMA_NAME(object_id), [Table] = [name], N'==> object'= N'==>', * FROM sys.tables;
SELECT [Schema] = [name], N'==> object'= N'==>', * FROM sys.schemas;

SELECT [Schema] = OBJECT_SCHEMA_NAME(object_id), [External Table] = [name], [DROP] =  N'IF EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = N''' + [name] + N''' AND OBJECT_SCHEMA_NAME(object_id) = N''' + OBJECT_SCHEMA_NAME(object_id) + ''') DROP SYNONYM ' + QUOTENAME(OBJECT_SCHEMA_NAME(object_id)) + N'.' + QUOTENAME(name) + N';' FROM sys.synonyms WHERE OBJECT_SCHEMA_NAME(object_id) = N'Maintenance' ORDER BY [name] ASC;

SELECT * FROM sys.synonyms
*/

----------------------------------------------------------------------------------------------------
-- Procedures
----------------------------------------------------------------------------------------------------
-- Procedures - Archive DB
DROP PROCEDURE IF EXISTS [Maintenance].[AddArchiveTriggerAuditLogs];
DROP PROCEDURE IF EXISTS [Maintenance].[AddArchiveTriggerJobs];
DROP PROCEDURE IF EXISTS [Maintenance].[AddArchiveTriggerLogs];
DROP PROCEDURE IF EXISTS [Maintenance].[AddArchiveTriggerQueues];
DROP PROCEDURE IF EXISTS [Maintenance].[AddArchiveTriggerRobotLicenseLogs];
DROP PROCEDURE IF EXISTS [Maintenance].[AddRunMessage];
DROP PROCEDURE IF EXISTS [Maintenance].[ArchiveAuditLogs];
DROP PROCEDURE IF EXISTS [Maintenance].[ArchiveJobs];
DROP PROCEDURE IF EXISTS [Maintenance].[ArchiveLogs];
DROP PROCEDURE IF EXISTS [Maintenance].[ArchiveQueues];
DROP PROCEDURE IF EXISTS [Maintenance].[ArchiveRobotLicenseLogs];
DROP PROCEDURE IF EXISTS [Maintenance].[CleanupSyncedAuditLogs];
DROP PROCEDURE IF EXISTS [Maintenance].[CleanupSyncedJobs];
DROP PROCEDURE IF EXISTS [Maintenance].[CleanupSyncedLogs];
DROP PROCEDURE IF EXISTS [Maintenance].[CleanupSyncedQueues];
DROP PROCEDURE IF EXISTS [Maintenance].[CleanupSyncedRobotLicenseLogs];
DROP PROCEDURE IF EXISTS [Maintenance].[CreateArchivingExternalTable];
DROP PROCEDURE IF EXISTS [Maintenance].[DeleteRuns];
DROP PROCEDURE IF EXISTS [Maintenance].[ParseJsonArchiveAuditLogs];
DROP PROCEDURE IF EXISTS [Maintenance].[ParseJsonArchiveJobs];
DROP PROCEDURE IF EXISTS [Maintenance].[ParseJsonArchiveLogs];
DROP PROCEDURE IF EXISTS [Maintenance].[ParseJsonArchiveQueues];
DROP PROCEDURE IF EXISTS [Maintenance].[ParseJsonArchiveRobotLicenseLogs];
DROP PROCEDURE IF EXISTS [Maintenance].[SetArchiveTable];
DROP PROCEDURE IF EXISTS [Maintenance].[SetArchiveTableAuditLogEntities];
DROP PROCEDURE IF EXISTS [Maintenance].[SetArchiveTableAuditLogs];
DROP PROCEDURE IF EXISTS [Maintenance].[SetArchiveTableJobs];
DROP PROCEDURE IF EXISTS [Maintenance].[SetArchiveTableLogs];
DROP PROCEDURE IF EXISTS [Maintenance].[SetArchiveTableQueueItemComments];
DROP PROCEDURE IF EXISTS [Maintenance].[SetArchiveTableQueueItemEvents];
DROP PROCEDURE IF EXISTS [Maintenance].[SetArchiveTableQueueItems];
DROP PROCEDURE IF EXISTS [Maintenance].[SetArchiveTableRobotLicenseLogs];
DROP PROCEDURE IF EXISTS [Maintenance].[SetOrchestratorArchiveTables];
DROP PROCEDURE IF EXISTS [Maintenance].[SetOrchestratorDBSourceTables];
DROP PROCEDURE IF EXISTS [Maintenance].[SetSourceTable];
DROP PROCEDURE IF EXISTS [Maintenance].[SetSourceTableASyncStatus_AuditLogs];
DROP PROCEDURE IF EXISTS [Maintenance].[SetSourceTableASyncStatus_Jobs];
DROP PROCEDURE IF EXISTS [Maintenance].[SetSourceTableASyncStatus_Logs];
DROP PROCEDURE IF EXISTS [Maintenance].[SetSourceTableASyncStatus_Queues];
DROP PROCEDURE IF EXISTS [Maintenance].[SetSourceTableASyncStatus_RobotLicenseLogs];
DROP PROCEDURE IF EXISTS [Maintenance].[SetSourceTableAuditLogEntities];
DROP PROCEDURE IF EXISTS [Maintenance].[SetSourceTableAuditLogs];
DROP PROCEDURE IF EXISTS [Maintenance].[SetSourceTableJobs];
DROP PROCEDURE IF EXISTS [Maintenance].[SetSourceTableLogs];
DROP PROCEDURE IF EXISTS [Maintenance].[SetSourceTableQueueItemComments];
DROP PROCEDURE IF EXISTS [Maintenance].[SetSourceTableQueueItemEvents];
DROP PROCEDURE IF EXISTS [Maintenance].[SetSourceTableQueueItems];
DROP PROCEDURE IF EXISTS [Maintenance].[SetSourceTableRobotLicenseLogs];
DROP PROCEDURE IF EXISTS [Maintenance].[SetSourceTableTenants];
DROP PROCEDURE IF EXISTS [Maintenance].[SimpleArchivingAuditLogs];
DROP PROCEDURE IF EXISTS [Maintenance].[SimpleArchivingJobs];
DROP PROCEDURE IF EXISTS [Maintenance].[SimpleArchivingLogs];
DROP PROCEDURE IF EXISTS [Maintenance].[SimpleArchivingQueues];
DROP PROCEDURE IF EXISTS [Maintenance].[SimpleArchivingRobotLicenseLogs];
DROP PROCEDURE IF EXISTS [Maintenance].[ValidateArchiveObjects];
DROP PROCEDURE IF EXISTS [Maintenance].[ValidateArchiveObjectsAuditLogs];
DROP PROCEDURE IF EXISTS [Maintenance].[ValidateArchiveObjectsJobs];
DROP PROCEDURE IF EXISTS [Maintenance].[ValidateArchiveObjectsLogs];
DROP PROCEDURE IF EXISTS [Maintenance].[ValidateArchiveObjectsQueues];
DROP PROCEDURE IF EXISTS [Maintenance].[ValidateArchiveObjectsRobotLicenseLogs];
-- Procedures - Orchestrator DB
DROP PROCEDURE IF EXISTS [Maintenance].[ASyncCleanupAuditLogs];
DROP PROCEDURE IF EXISTS [Maintenance].[ASyncCleanupJobs];
DROP PROCEDURE IF EXISTS [Maintenance].[ASyncCleanupLogs];
DROP PROCEDURE IF EXISTS [Maintenance].[ASyncCleanupQueues];
DROP PROCEDURE IF EXISTS [Maintenance].[ASyncCleanupRobotLicenseLogs];
DROP PROCEDURE IF EXISTS [Maintenance].[CreateArchivingExternalTable];
DROP PROCEDURE IF EXISTS [Maintenance].[SetArchiveDBSourceTables];
DROP PROCEDURE IF EXISTS [Maintenance].[SetSourceTable];
DROP PROCEDURE IF EXISTS [Maintenance].[ValidateASyncArchiveObjects];
DROP PROCEDURE IF EXISTS [Maintenance].[ValidateASyncArchiveObjectsAuditLogs];
DROP PROCEDURE IF EXISTS [Maintenance].[ValidateASyncArchiveObjectsJobs];
DROP PROCEDURE IF EXISTS [Maintenance].[ValidateASyncArchiveObjectsLogs];
DROP PROCEDURE IF EXISTS [Maintenance].[ValidateASyncArchiveObjectsQueues];
DROP PROCEDURE IF EXISTS [Maintenance].[ValidateASyncArchiveObjectsRobotLicenseLogs];

----------------------------------------------------------------------------------------------------
-- Synonyms
----------------------------------------------------------------------------------------------------
-- Synonyms - Archive DB
IF EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = N'Synonym_Archive_AuditLogEntities' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP SYNONYM [Maintenance].[Synonym_Archive_AuditLogEntities];
IF EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = N'Synonym_Archive_AuditLogs' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP SYNONYM [Maintenance].[Synonym_Archive_AuditLogs];
IF EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = N'Synonym_Archive_Jobs' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP SYNONYM [Maintenance].[Synonym_Archive_Jobs];
IF EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = N'Synonym_Archive_Logs' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP SYNONYM [Maintenance].[Synonym_Archive_Logs];
IF EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = N'Synonym_Archive_QueueItemComments' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP SYNONYM [Maintenance].[Synonym_Archive_QueueItemComments];
IF EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = N'Synonym_Archive_QueueItemEvents' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP SYNONYM [Maintenance].[Synonym_Archive_QueueItemEvents];
IF EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = N'Synonym_Archive_QueueItems' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP SYNONYM [Maintenance].[Synonym_Archive_QueueItems];
IF EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = N'Synonym_Archive_RobotLicenseLogs' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP SYNONYM [Maintenance].[Synonym_Archive_RobotLicenseLogs];
IF EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = N'Synonym_ArchivingListOrchestratorDBTables' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP SYNONYM [Maintenance].[Synonym_ArchivingListOrchestratorDBTables];
IF EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = N'Synonym_Source_ASyncStatus_AuditLogs' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP SYNONYM [Maintenance].[Synonym_Source_ASyncStatus_AuditLogs];
IF EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = N'Synonym_Source_ASyncStatus_Jobs' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP SYNONYM [Maintenance].[Synonym_Source_ASyncStatus_Jobs];
IF EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = N'Synonym_Source_ASyncStatus_Logs' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP SYNONYM [Maintenance].[Synonym_Source_ASyncStatus_Logs];
IF EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = N'Synonym_Source_ASyncStatus_Queues' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP SYNONYM [Maintenance].[Synonym_Source_ASyncStatus_Queues];
IF EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = N'Synonym_Source_ASyncStatus_RobotLicenseLogs' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP SYNONYM [Maintenance].[Synonym_Source_ASyncStatus_RobotLicenseLogs];
IF EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = N'Synonym_Source_AuditLogEntities' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP SYNONYM [Maintenance].[Synonym_Source_AuditLogEntities];
IF EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = N'Synonym_Source_AuditLogs' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP SYNONYM [Maintenance].[Synonym_Source_AuditLogs];
IF EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = N'Synonym_Source_Jobs' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP SYNONYM [Maintenance].[Synonym_Source_Jobs];
IF EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = N'Synonym_Source_Logs' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP SYNONYM [Maintenance].[Synonym_Source_Logs];
IF EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = N'Synonym_Source_QueueItemComments' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP SYNONYM [Maintenance].[Synonym_Source_QueueItemComments];
IF EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = N'Synonym_Source_QueueItemEvents' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP SYNONYM [Maintenance].[Synonym_Source_QueueItemEvents];
IF EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = N'Synonym_Source_QueueItems' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP SYNONYM [Maintenance].[Synonym_Source_QueueItems];
IF EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = N'Synonym_Source_RobotLicenseLogs' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP SYNONYM [Maintenance].[Synonym_Source_RobotLicenseLogs];
IF EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = N'Synonym_Source_Tenants' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP SYNONYM [Maintenance].[Synonym_Source_Tenants];
-- Synonyms - Orchestrator DB
IF EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = N'Synonym_ArchivingListASyncOrchestratorDBTables' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP SYNONYM [Maintenance].[Synonym_ArchivingListASyncOrchestratorDBTables];
IF EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = N'Synonym_ASyncDelete_AuditLogs' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP SYNONYM [Maintenance].[Synonym_ASyncDelete_AuditLogs];
IF EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = N'Synonym_ASyncDelete_Jobs' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP SYNONYM [Maintenance].[Synonym_ASyncDelete_Jobs];
IF EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = N'Synonym_ASyncDelete_Logs' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP SYNONYM [Maintenance].[Synonym_ASyncDelete_Logs];
IF EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = N'Synonym_ASyncDelete_Queues' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP SYNONYM [Maintenance].[Synonym_ASyncDelete_Queues];
IF EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = N'Synonym_ASyncDelete_RobotLicenseLogs' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP SYNONYM [Maintenance].[Synonym_ASyncDelete_RobotLicenseLogs];
IF EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = N'Synonym_ASyncSync_AuditLogs' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP SYNONYM [Maintenance].[Synonym_ASyncSync_AuditLogs];
IF EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = N'Synonym_ASyncSync_Jobs' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP SYNONYM [Maintenance].[Synonym_ASyncSync_Jobs];
IF EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = N'Synonym_ASyncSync_Logs' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP SYNONYM [Maintenance].[Synonym_ASyncSync_Logs];
IF EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = N'Synonym_ASyncSync_Queues' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP SYNONYM [Maintenance].[Synonym_ASyncSync_Queues];
IF EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = N'Synonym_ASyncSync_RobotLicenseLogs' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP SYNONYM [Maintenance].[Synonym_ASyncSync_RobotLicenseLogs];

----------------------------------------------------------------------------------------------------
-- External Tables (Maintenance)
----------------------------------------------------------------------------------------------------
-- External Tables - Archive DB
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE [name] = N'ArchivingListOrchestratorDBTables' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP EXTERNAL TABLE [Maintenance].[ArchivingListOrchestratorDBTables];
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE [name] = N'ASyncStatus_AuditLogs' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP EXTERNAL TABLE [Maintenance].[ASyncStatus_AuditLogs];
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE [name] = N'ASyncStatus_Jobs' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP EXTERNAL TABLE [Maintenance].[ASyncStatus_Jobs];
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE [name] = N'ASyncStatus_Logs' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP EXTERNAL TABLE [Maintenance].[ASyncStatus_Logs];
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE [name] = N'ASyncStatus_Queues' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP EXTERNAL TABLE [Maintenance].[ASyncStatus_Queues];
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE [name] = N'ASyncStatus_RobotLicenseLogs' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP EXTERNAL TABLE [Maintenance].[ASyncStatus_RobotLicenseLogs];
-- External Tables - Orchestrator DB
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE [name] = N'ArchivingListArchiveDBTables' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP EXTERNAL TABLE [Maintenance].[ArchivingListArchiveDBTables];
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE [name] = N'ArchivingListASyncOrchestratorDBTables' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP EXTERNAL TABLE [Maintenance].[ArchivingListASyncOrchestratorDBTables];
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE [name] = N'Delete_AuditLogs' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP EXTERNAL TABLE [Maintenance].[Delete_AuditLogs];
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE [name] = N'Delete_Jobs' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP EXTERNAL TABLE [Maintenance].[Delete_Jobs];
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE [name] = N'Delete_Logs' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP EXTERNAL TABLE [Maintenance].[Delete_Logs];
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE [name] = N'Delete_Queues' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP EXTERNAL TABLE [Maintenance].[Delete_Queues];
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE [name] = N'Delete_RobotLicenseLogs' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP EXTERNAL TABLE [Maintenance].[Delete_RobotLicenseLogs];
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE [name] = N'Sync_AuditLogs' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP EXTERNAL TABLE [Maintenance].[Sync_AuditLogs];
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE [name] = N'Sync_Jobs' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP EXTERNAL TABLE [Maintenance].[Sync_Jobs];
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE [name] = N'Sync_Logs' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP EXTERNAL TABLE [Maintenance].[Sync_Logs];
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE [name] = N'Sync_Queues' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP EXTERNAL TABLE [Maintenance].[Sync_Queues];
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE [name] = N'Sync_RobotLicenseLogs' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP EXTERNAL TABLE [Maintenance].[Sync_RobotLicenseLogs];

----------------------------------------------------------------------------------------------------
-- External Tables (dbo)
----------------------------------------------------------------------------------------------------
-- External Tables (dbo) - Archive DB
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE [name] = N'AuditLogEntities' AND OBJECT_SCHEMA_NAME(object_id) = N'dbo') DROP EXTERNAL TABLE [dbo].[AuditLogEntities];
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE [name] = N'AuditLogs' AND OBJECT_SCHEMA_NAME(object_id) = N'dbo') DROP EXTERNAL TABLE [dbo].[AuditLogs];
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE [name] = N'Jobs' AND OBJECT_SCHEMA_NAME(object_id) = N'dbo') DROP EXTERNAL TABLE [dbo].[Jobs];
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE [name] = N'Logs' AND OBJECT_SCHEMA_NAME(object_id) = N'dbo') DROP EXTERNAL TABLE [dbo].[Logs];
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE [name] = N'QueueItemComments' AND OBJECT_SCHEMA_NAME(object_id) = N'dbo') DROP EXTERNAL TABLE [dbo].[QueueItemComments];
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE [name] = N'QueueItemEvents' AND OBJECT_SCHEMA_NAME(object_id) = N'dbo') DROP EXTERNAL TABLE [dbo].[QueueItemEvents];
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE [name] = N'QueueItems' AND OBJECT_SCHEMA_NAME(object_id) = N'dbo') DROP EXTERNAL TABLE [dbo].[QueueItems];
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE [name] = N'RobotLicenseLogs' AND OBJECT_SCHEMA_NAME(object_id) = N'dbo') DROP EXTERNAL TABLE [dbo].[RobotLicenseLogs];
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE [name] = N'Tenants' AND OBJECT_SCHEMA_NAME(object_id) = N'dbo') DROP EXTERNAL TABLE [dbo].[Tenants];
-- External Tables (dbo) - Orchestrator DB
--

----------------------------------------------------------------------------------------------------
-- Views
----------------------------------------------------------------------------------------------------
-- Views - Archive DB
DROP VIEW IF EXISTS [Maintenance].[ArchivingListASyncOrchestratorDBTables];
-- Views - Orchestrator DB
DROP VIEW IF EXISTS [Maintenance].[ArchivingListOrchestratorDBTables];

----------------------------------------------------------------------------------------------------
-- Foreign Keys
----------------------------------------------------------------------------------------------------
-- Foreign Keys - Archive DB
IF OBJECT_ID(N'[Maintenance].[Filter_AuditLogs]') IS NOT NULL ALTER TABLE [Maintenance].[Filter_AuditLogs] DROP CONSTRAINT IF EXISTS [FK_Maintenance.Filter_AuditLogs.Sync_AuditLogs];
IF OBJECT_ID(N'[Maintenance].[Delete_AuditLogs]') IS NOT NULL ALTER TABLE [Maintenance].[Delete_AuditLogs] DROP CONSTRAINT IF EXISTS [FK_Maintenance.Delete_AuditLogs.Sync_AuditLogs];
IF OBJECT_ID(N'[Maintenance].[Sync_AuditLogs]') IS NOT NULL ALTER TABLE [Maintenance].[Sync_AuditLogs] DROP CONSTRAINT IF EXISTS [FK_Maintenance.Sync_AuditLogs.Archive_AuditLogs];
IF OBJECT_ID(N'[Maintenance].[Filter_RobotLicenseLogs]') IS NOT NULL ALTER TABLE [Maintenance].[Filter_RobotLicenseLogs] DROP CONSTRAINT IF EXISTS [FK_Maintenance.Filter_RobotLicenseLogs.Sync_RobotLicenseLogs];
IF OBJECT_ID(N'[Maintenance].[Delete_RobotLicenseLogs]') IS NOT NULL ALTER TABLE [Maintenance].[Delete_RobotLicenseLogs] DROP CONSTRAINT IF EXISTS [FK_Maintenance.Delete_RobotLicenseLogs.Sync_RobotLicenseLogs];
IF OBJECT_ID(N'[Maintenance].[Sync_RobotLicenseLogs]') IS NOT NULL ALTER TABLE [Maintenance].[Sync_RobotLicenseLogs] DROP CONSTRAINT IF EXISTS [FK_Maintenance.Sync_RobotLicenseLogs.Archive_RobotLicenseLogs];
IF OBJECT_ID(N'[Maintenance].[Filter_Logs]') IS NOT NULL ALTER TABLE [Maintenance].[Filter_Logs] DROP CONSTRAINT IF EXISTS [FK_Maintenance.Filter_Logs.Sync_Logs];
IF OBJECT_ID(N'[Maintenance].[Delete_Logs]') IS NOT NULL ALTER TABLE [Maintenance].[Delete_Logs] DROP CONSTRAINT IF EXISTS [FK_Maintenance.Delete_Logs.Sync_Logs];
IF OBJECT_ID(N'[Maintenance].[Sync_Logs]') IS NOT NULL ALTER TABLE [Maintenance].[Sync_Logs] DROP CONSTRAINT IF EXISTS [FK_Maintenance.Sync_Logs.Archive_Logs];
IF OBJECT_ID(N'[Maintenance].[Filter_Queues]') IS NOT NULL ALTER TABLE [Maintenance].[Filter_Queues] DROP CONSTRAINT IF EXISTS [FK_Maintenance.Filter_Queues-Sync_Queues];
IF OBJECT_ID(N'[Maintenance].[Delete_Queues]') IS NOT NULL ALTER TABLE [Maintenance].[Delete_Queues] DROP CONSTRAINT IF EXISTS [FK_Maintenance.Delete_Queues.Sync_Queues];
IF OBJECT_ID(N'[Maintenance].[Sync_Queues]') IS NOT NULL ALTER TABLE [Maintenance].[Sync_Queues] DROP CONSTRAINT IF EXISTS [FK_Maintenance.Sync_Queues.Archive_Queues];
IF OBJECT_ID(N'[Maintenance].[Messages]') IS NOT NULL ALTER TABLE [Maintenance].[Messages] DROP CONSTRAINT IF EXISTS [FK_RunId];
IF OBJECT_ID(N'[Maintenance].[Filter_Jobs]') IS NOT NULL ALTER TABLE [Maintenance].[Filter_Jobs] DROP CONSTRAINT IF EXISTS [FK_Maintenance.Filter_Jobs-Sync_Jobs];
IF OBJECT_ID(N'[Maintenance].[Delete_Jobs]') IS NOT NULL ALTER TABLE [Maintenance].[Delete_Jobs] DROP CONSTRAINT IF EXISTS [FK_Maintenance.Delete_Jobs.Sync_Jobs];
IF OBJECT_ID(N'[Maintenance].[Sync_Jobs]') IS NOT NULL ALTER TABLE [Maintenance].[Sync_Jobs] DROP CONSTRAINT IF EXISTS [FK_Maintenance.Sync_Jobs.Archive_Jobs];
-- Foreign Keys - Orchestrator DB
--

----------------------------------------------------------------------------------------------------
-- Tables
----------------------------------------------------------------------------------------------------
-- Tables - Archive DB
DROP TABLE IF EXISTS [Maintenance].[Runs];
DROP TABLE IF EXISTS [Maintenance].[Filter_AuditLogs];
DROP TABLE IF EXISTS [Maintenance].[Archive_AuditLogs];
DROP TABLE IF EXISTS [Maintenance].[Delete_AuditLogs];
DROP TABLE IF EXISTS [Maintenance].[Sync_AuditLogs];
DROP TABLE IF EXISTS [Maintenance].[Filter_RobotLicenseLogs];
DROP TABLE IF EXISTS [Maintenance].[Archive_RobotLicenseLogs];
DROP TABLE IF EXISTS [Maintenance].[Delete_RobotLicenseLogs];
DROP TABLE IF EXISTS [Maintenance].[Sync_RobotLicenseLogs];
DROP TABLE IF EXISTS [Maintenance].[Filter_Logs];
DROP TABLE IF EXISTS [Maintenance].[Archive_Logs];
DROP TABLE IF EXISTS [Maintenance].[Delete_Logs];
DROP TABLE IF EXISTS [Maintenance].[Sync_Logs];
DROP TABLE IF EXISTS [Maintenance].[Filter_Queues];
DROP TABLE IF EXISTS [Maintenance].[Archive_Queues];
DROP TABLE IF EXISTS [Maintenance].[Delete_Queues];
DROP TABLE IF EXISTS [Maintenance].[Sync_Queues];
DROP TABLE IF EXISTS [Maintenance].[Messages];
DROP TABLE IF EXISTS [Maintenance].[Filter_Jobs];
DROP TABLE IF EXISTS [Maintenance].[Archive_Jobs];
DROP TABLE IF EXISTS [Maintenance].[Delete_Jobs];
DROP TABLE IF EXISTS [Maintenance].[Sync_Jobs];
-- Tables - Orchestrator DB
DROP TABLE IF EXISTS [Maintenance].[ASyncStatus_AuditLogs];
DROP TABLE IF EXISTS [Maintenance].[ASyncStatus_RobotLicenseLogs];
DROP TABLE IF EXISTS [Maintenance].[ASyncStatus_Logs];
DROP TABLE IF EXISTS [Maintenance].[ASyncStatus_Queues];
DROP TABLE IF EXISTS [Maintenance].[ASyncStatus_Jobs];

/*
BEGIN TRAN
IF EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = N'Synonym_Archive_AuditLogEntities' AND OBJECT_SCHEMA_NAME(object_id) = N'Maintenance') DROP SYNONYM [Maintenance].[Synonym_Archive_AuditLogEntities];
ROLLBACK

*/

