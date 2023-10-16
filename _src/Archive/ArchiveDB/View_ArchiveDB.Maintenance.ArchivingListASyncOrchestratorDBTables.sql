SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- DROP VIEW [Maintenance].[ArchivingListASyncOrchestratorDBTables]
----------------------------------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[ArchivingListASyncOrchestratorDBTables]') AND type in (N'V'))
BEGIN
    PRINT '  + CREATE VIEW: [Maintenance].[ArchivingListASyncOrchestratorDBTables]';
END
ELSE PRINT '  ~ ALTER VIEW: [Maintenance].[ArchivingListASyncOrchestratorDBTables]'
GO

CREATE OR ALTER VIEW [Maintenance].[ArchivingListASyncOrchestratorDBTables]
AS
    WITH list([schema], [table], [group], [IsArchived]) AS (
        SELECT CAST(LTRIM(RTRIM([schema])) AS nvarchar(128)), CAST(LTRIM(RTRIM([table])) AS nvarchar(128)), CAST(NULLIF(LTRIM(RTRIM([group])), '') AS nvarchar(128)), CAST([IsArchived] AS bit)
        FROM (VALUES
            ('Maintenance', 'Sync_AuditLogs', 'AuditLogs', 0)
            , ('Maintenance', 'Delete_AuditLogs', 'AuditLogs', 0)
            , ('Maintenance', 'Sync_Jobs', 'Jobs', 0)
            , ('Maintenance', 'Delete_Jobs', 'Jobs', 0)
            , ('Maintenance', 'Sync_Logs', 'Logs', 0)
            , ('Maintenance', 'Delete_Logs', 'Logs', 0)
            , ('Maintenance', 'Sync_RobotLicenseLogs', 'RobotLicenseLogs', 0)
            , ('Maintenance', 'Delete_RobotLicenseLogs', 'RobotLicenseLogs', 0)
            , ('Maintenance', 'Sync_Queues', 'Queues', 0)
            , ('Maintenance', 'Delete_Queues', 'Queues', 0)
        ) list([schema], [table], [group], [IsArchived]) 
    ), tables([schema], [table], [group], [object_id], [IsArchived]) AS (
        SELECT lst.[schema], lst.[table], lst.[group], tbl.object_id, lst.[IsArchived]
        FROM [list] lst
        LEFT JOIN sys.tables tbl ON tbl.[name] = lst.[table]
        LEFT JOIN sys.schemas sch ON tbl.schema_id = sch.schema_id AND sch.[name] = lst.[schema] 
    )
    SELECT tbl.[group], tbl.[schema], tbl.[table], [IsArchived]--, lst.object_id
        , [exists] = CAST(IIF(tbl.object_id IS NULL, 0, 1) AS bit)
        , [isvalid] = CAST(IIF(NOT EXISTS(SELECT 1 FROM list WHERE ([group] = tbl.[group] OR [group] IS NULL) AND object_id IS NULL), 1, 0) AS bit)
        , columns = (
            SELECT TOP(1000) [column] = col.[name], [id] = col.column_id
                , [datatype] =   tpe.[name] + 
                    CASE WHEN tpe.[name] IN (N'varchar', N'char', N'varbinary', N'binary', N'text') THEN '(' + CASE WHEN col.max_length = -1 THEN 'MAX' ELSE CAST(col.max_length AS VARCHAR(5)) END + ')'
                    WHEN tpe.[name] IN (N'nvarchar', N'nchar', N'ntext') THEN '(' + CASE WHEN col.max_length = -1 THEN 'MAX' ELSE CAST(col.max_length / 2 AS VARCHAR(5)) END + ')'
                    WHEN tpe.[name] IN (N'datetime2', N'time2', N'datetimeoffset') THEN '(' + CAST(col.scale AS VARCHAR(5)) + ')'
                    WHEN tpe.[name] IN (N'decimal', N'numeric') THEN '(' + CAST(col.[precision] AS VARCHAR(5)) + ',' + CAST(col.scale AS VARCHAR(5)) + ')'
                    WHEN tpe.[name] IN (N'float') THEN '(' + CAST(col.[precision] AS VARCHAR(5)) + ')'
                    ELSE '' END
            FROM sys.columns AS col
            INNER JOIN sys.types AS tpe ON col.system_type_id = tpe.system_type_id AND tpe.system_type_id = tpe.user_type_id
            WHERE col.object_id = tbl.object_id AND tpe.[name] <> N'timestamp' AND NOT EXISTS(SELECT 1 FROM tables WHERE ([group] = tbl.[group] OR [group] IS NULL) AND object_id IS NULL)
            FOR JSON PATH 
        )
    FROM tables tbl
GO
