SET NOEXEC OFF;
GO
----------------------------------------------------------------------------------------------------
-- 1. Orchestrator Database must be selected
-- 2. The value of @OrchestratorDatabaseName must be set to the name of the Orchestrator Database
----------------------------------------------------------------------------------------------------
--\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/
DECLARE @OrchestratorDatabaseName sysname = N'<orchestrator database>'; --<== UPDATE NAME
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
--!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
----------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------
-- DB Check
----------------------------------------------------------------------------------------------------
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
SET NOCOUNT ON;

DECLARE @message nvarchar(1000) = N'The Orchestrator Database must be selected and @OrchestratorDatabaseName value must match the Orchestrator Databse Name';
IF DB_NAME() <> @OrchestratorDatabaseName 
BEGIN
    RAISERROR(N'The Orchestrator Database must be selected and @OrchestratorDatabaseName value must match the Orchestrator Databse Name', 16, 1);
    SET @message = N'Current Database ==> ''' + DB_NAME() + N''' <=='; RAISERROR(@message, 10, 1);
    SET @message = N'@OrchestratorDatabaseName ==> ''' + @OrchestratorDatabaseName + N''' <=='; RAISERROR(@message, 10, 1);
    RAISERROR(N'Script execution canceled', 16, 1);
    SET NOEXEC ON;
END
IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- ### [Object]: Schema [Maintenance]
-- ### [Version]: 2023-10-17T11:47:17+02:00
-- ### [Source]: _src/Schemas/Schema_Maintenance.sql
-- ### [Hash]: 5d13ce3 [SHA256-8DBBEB9BEA51AD2A1ED12F90085A891C2105686DEB253181FB306F50ACEA3CB7]
-- ### [Docs]: https://???.???
----------------------------------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Maintenance')
BEGIN 
	PRINT ' + Create Schema [Maintenance]';
	EXEC sp_executesql N'CREATE SCHEMA [Maintenance]';
END
ELSE PRINT ' = Schema already exists: [Maintenance]';
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- ### [Object]: TABLE [Maintenance].[Runs]
-- ### [Version]: 2022-02-11T14:24:45+01:00
-- ### [Source]: _src/Runs/Table_Maintenance.Runs.sql
-- ### [Hash]: 977a791 [SHA256-8F55CD3EB205659CB8F4A11E5D5ED5A12440E40D22A93B18E9289EC368FAF640]
-- ### [Docs]: https://???.???
----------------------------------------------------------------------------------------------------
IF NOT EXISTS(SELECT 1 FROM sys.tables WHERE name = N'Runs' AND SCHEMA_NAME(schema_id) = N'Maintenance')
BEGIN
    CREATE TABLE [Maintenance].[Runs]
    (
        Id int IDENTITY(0, 1) CONSTRAINT [PK_Maintenance.Run] PRIMARY KEY CLUSTERED(Id)
        , [Type] nvarchar(128) NOT NULL
        , [info] nvarchar(max)
        , [StartTime] datetime2 NOT NULL CONSTRAINT df_StartTime DEFAULT SYSDATETIME()
        , [EndDate] datetime2
        , [ErrorStatus] tinyint
    );
    PRINT '  + TABLE CREATED: [Maintenance].[Runs]';
END
ELSE
BEGIN
    PRINT '  = TABLE [Maintenance].[Runs] already exists' 

    -- Update existing sysname column to nvarchar(max)
    IF EXISTS( SELECT col.name FROM sys.tables tbl 
        INNER JOIN sys.columns col ON tbl.object_id = col.object_id
        WHERE tbl.name = N'Runs' AND SCHEMA_NAME(tbl.schema_id) = N'Maintenance' AND col.name = N'Type' AND col.system_type_id = 231 AND col.user_type_id <> 231
    )
    BEGIN
        PRINT '  ~ UPDATE TABLE [Maintenance].[Runs] COLUMN: [Type] nvarchar(128) NOT NULL' 
        ALTER TABLE [Maintenance].[Runs] ALTER COLUMN [Type] nvarchar(128) NOT NULL;
    END

END
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- ### [Object]: TABLE [Maintenance].[Messages]
-- ### [Version]: 2022-02-11T14:24:45+01:00
-- ### [Source]: _src/Runs/Table_Maintenance.Messages.sql
-- ### [Hash]: 977a791 [SHA256-E8CFF249EA3841977F65C278341B910301AACCE921684E10FEDC2F3E740E90C9]
-- ### [Docs]: https://???.???
----------------------------------------------------------------------------------------------------
IF NOT EXISTS(SELECT 1 FROM sys.tables WHERE name = N'Messages' AND SCHEMA_NAME(schema_id) = N'Maintenance')
BEGIN
    CREATE TABLE [Maintenance].[Messages]
    (
        [Id] bigint IDENTITY(0, 1) CONSTRAINT [PK_Maintenance.Messages] PRIMARY KEY CLUSTERED(Id)
        , [RunId] int NOT NULL CONSTRAINT [FK_RunId] FOREIGN KEY(RunId) REFERENCES [Maintenance].[Runs](Id) ON DELETE CASCADE
        , [Date] datetime2 NOT NULL CONSTRAINT CK_Date DEFAULT SYSDATETIME()
        , [Procedure] nvarchar(max) NOT NULL
        , [Message] nvarchar(max) NOT NULL
        , [Severity] tinyint NOT NULL
        , [State] tinyint NOT NULL
        , [Number] int
        , [Line] int
    );
    PRINT '  + TABLE CREATED: [Maintenance].[Messages]';
END
ELSE
BEGIN
    PRINT '  = TABLE [Maintenance].[Messages] already exists' 

    -- Update existing sysname column to nvarchar(max)
    IF EXISTS( SELECT col.name FROM sys.tables tbl 
        INNER JOIN sys.columns col ON tbl.object_id = col.object_id
        WHERE tbl.name = N'Messages' AND SCHEMA_NAME(tbl.schema_id) = N'Maintenance' AND col.name = N'Procedure' AND col.system_type_id = 231 AND col.max_length <> -1
    )
    BEGIN
        PRINT '  ~ UPDATE TABLE [Maintenance].[Messages] COLUMN: [Procedure] nvarchar(max) NOT NULL' 
        ALTER TABLE [Maintenance].[Messages] ALTER COLUMN [Procedure] nvarchar(max) NOT NULL;
    END
END
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- PROCEDURE [Maintenance].[AddRunMessage]
----------------------------------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[AddRunMessage]') AND type in (N'P'))
BEGIN
    EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [Maintenance].[AddRunMessage] AS'
    PRINT '  + PROCEDURE CREATED: [Maintenance].[AddRunMessage]';
END
ELSE PRINT '  = PROCEDURE [Maintenance].[AddRunMessage] already exists' 
GO

ALTER PROCEDURE [Maintenance].[AddRunMessage]
----------------------------------------------------------------------------------------------------
-- ### [Object]: PROCEDURE [Maintenance].[AddRunMessage]
-- ### [Version]: 2023-01-31T12:58:44+00:00
-- ### [Source]: _src/Runs/Procedure_Maintenance.AddRunMessage.sql
-- ### [Hash]: d0ca361 [SHA256-AB8A92454EE6D7FD1A73DF40513F33C0DD9DDE342C9D900F558E46A2E3D91BAB]
-- ### [Docs]: https://???.???
----------------------------------------------------------------------------------------------------
    @RunId int
	, @Procedure nvarchar(max)
	, @Message nvarchar(max)
	, @Severity tinyint
    , @State tinyint
	, @Number int = NULL
	, @Line int = NULL
    , @VerboseLevel tinyint
	, @LogToTable bit
    , @RaiseError bit = 1
    , @MessagesStack xml = NULL OUTPUT
AS
BEGIN
    SET ARITHABORT ON;
    SET NOCOUNT ON;
    SET NUMERIC_ROUNDABORT OFF;

    BEGIN TRY
        DECLARE @ERROR_NUMBER INT, @ERROR_SEVERITY INT, @ERROR_STATE INT, @ERROR_PROCEDURE NVARCHAR(126), @ERROR_LINE INT, @ERROR_MESSAGE NVARCHAR(2048) ;
        DECLARE @count int;
        DECLARE @date datetime;
        DECLARE @log xml;
        /*IF @@TRANCOUNT <> 0 
        BEGIN
            RAISERROR(N'Can''t run when the transaction count is bigger than 0.', 16, 1);
            SET @RunId = NULL;
        END*/
        SET @VerboseLevel = ISNULL(@VerboseLevel, 10);
        SET @Procedure = ISNULL(@Procedure, QUOTENAME(COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?')) + N'.' + QUOTENAME(COALESCE(OBJECT_NAME(@@PROCID), N'?')));
        SET @date = SYSDATETIME();
        SET @MessagesStack = COALESCE(@MessagesStack, N'<messages></messages>');
        SET @Message = COALESCE(@Message, N'');

        IF @LogToTable = 1 
        BEGIN
            INSERT INTO [Maintenance].[Messages](RunId, [Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
            SELECT @RunId, @date, @Procedure, @Message, @Severity, @state, @Number, @Line WHERE @RunId IS NOT NULL;
        END

        SET @log = (  SELECT * FROM (SELECT SYSDATETIME(), @Procedure, @Message, @Severity, @state, @Number, @Line) x([Date], [Procedure], [Message], [Severity], [State], [Number], [Line]) FOR XML PATH('message') )
        SET @MessagesStack.modify('insert sql:variable("@log") as last into (/messages)[1]')

        IF @Severity >= @VerboseLevel 
        BEGIN
            IF @Severity < 10 SET @Severity = 10;
            IF @Severity > 10
            BEGIN
                IF @RaiseError <> 1 SET @Severity = 10;
                --ELSE RAISERROR(@Message, 10, @State);
            END
            RAISERROR(@Message, @Severity, @State);
        END
    END TRY
    BEGIN CATCH
        SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
        --IF @@TRANCOUNT > 0 ROLLBACK;
        IF @Message = @ERROR_MESSAGE RAISERROR(@ERROR_MESSAGE, @ERROR_SEVERITY, @ERROR_STATE);
        ELSE THROW;
    END CATCH

    RETURN 0
END

GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- PROCEDURE [Maintenance].[DeleteRuns]
----------------------------------------------------------------------------------------------------

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[DeleteRuns]') AND type in (N'P'))
BEGIN
    EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [Maintenance].[DeleteRuns] AS'
    PRINT '  + PROCEDURE CREATED: [Maintenance].[DeleteRuns]';
END
ELSE PRINT '  = PROCEDURE [Maintenance].[DeleteRuns] already exists' 
GO

ALTER PROCEDURE [Maintenance].[DeleteRuns]
----------------------------------------------------------------------------------------------------
-- ### [Object]: PROCEDURE [Maintenance].[DeleteRuns]
-- ### [Version]: 2023-01-31T12:58:44+00:00
-- ### [Source]: _src/Runs/Procedure_Maintenance.DeleteRuns.sql
-- ### [Hash]: d0ca361 [SHA256-C80C680B2B24A52204AA835431EE11A3CBB83FB7C32FA62BFDA5E114FD1A4692]
-- ### [Docs]: https://???.???
----------------------------------------------------------------------------------------------------
    @CleanupAfterDays tinyint = 30
    , @RunId int = NULL
    , @Procedure nvarchar(max) = NULL
    , @MessagesStack xml = NULL OUTPUT
AS
BEGIN
    SET ARITHABORT ON;
    SET NOCOUNT ON;
    SET NUMERIC_ROUNDABORT OFF;

    BEGIN TRY
        DECLARE @ERROR_NUMBER INT, @ERROR_SEVERITY INT, @ERROR_STATE INT, @ERROR_PROCEDURE NVARCHAR(126), @ERROR_LINE INT, @ERROR_MESSAGE NVARCHAR(2048) ;
        DECLARE @count int;
    	DECLARE @localRunId int;
        DECLARE @message nvarchar(max);
    
        IF @@TRANCOUNT <> 0 
        BEGIN
            RAISERROR(N'Can''t run when the transaction count is bigger than 0.', 16, 1);
        END
        SET @CleanupAfterDays = ISNULL(ABS(@CleanupAfterDays), 30);
        SET @Procedure = ISNULL(@Procedure, QUOTENAME(COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?')) + N'.' + QUOTENAME(COALESCE(OBJECT_NAME(@@PROCID), N'?')));
        
        BEGIN TRY
			SET @localRunId = @RunId
            IF @localRunId IS NULL 
            BEGIN
                INSERT INTO [Maintenance].[Runs]([Type], [Info], [StartTime]) VALUES
                    (N'Cleanup Runs', N'PROCEDURE ' + @Procedure, SYSDATETIME());
                SELECT @localRunId = @@IDENTITY;
            END
/*
            INSERT INTO [Maintenance].[Messages](RunId, [Procedure], [Message], [Severity], [State])
                VALUES(@localRunId, @Procedure, N'Runs cleanup started', 10, 1)
                    , (@localRunId, @Procedure, N'Keep Runs from past days: ' + CAST(@CleanupAfterDays AS nvarchar(10)), 10, 1)
                    , (@localRunId, @Procedure, N'Delete Runs before: ' + CONVERT(nvarchar(20), DATEADD(DAY, - @CleanupAfterDays, SYSDATETIME()), 121), 10, 1)
            ;*/

            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @Procedure, @Message = N'Runs cleanup started', @Severity = 10, @State = 1, @VerboseLevel = 10, @LogToTable = 1, @MessagesStack = @MessagesStack OUTPUT;
            SET @message = N'Keep Runs from past days: ' + CAST(@CleanupAfterDays AS nvarchar(10));
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @Procedure, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = 10, @LogToTable = 1, @MessagesStack = @MessagesStack OUTPUT;
            SET @message =  N'Delete Runs before: ' + CONVERT(nvarchar(20), DATEADD(DAY, - @CleanupAfterDays, SYSDATETIME()), 121);
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @Procedure, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = 10, @LogToTable = 1, @MessagesStack = @MessagesStack OUTPUT;

            BEGIN TRAN

            DELETE FROM [Maintenance].[Runs] WHERE [StartTime] < DATEADD(DAY, - @CleanupAfterDays, SYSDATETIME()) AND [Id] <> @localRunId;
            SET @count = @@ROWCOUNT;

            IF @@TRANCOUNT > 0 COMMIT;
/*
            INSERT INTO [Maintenance].[Messages](RunId, [Procedure], [Message], [Severity], [State]) VALUES
                (@localRunId, @Procedure, N'Runs deleted: ' + CAST(@count AS nvarchar(10)), 10, 1)
                , (@localRunId, @Procedure, N'Runs Cleanup Finished', 10, 1);
*/
            SET @message = N'Runs deleted: ' + CAST(@count AS nvarchar(10));
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @Procedure, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = 10, @LogToTable = 1, @MessagesStack = @MessagesStack OUTPUT;
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @Procedure, @Message = N'Runs Cleanup Finished', @Severity = 10, @State = 1, @VerboseLevel = 10, @LogToTable = 1, @MessagesStack = @MessagesStack OUTPUT;

            UPDATE [Maintenance].[Runs] SET [EndDate] = SYSDATETIME(), [ErrorStatus] = 0 WHERE Id = @localRunId AND @RunId IS NULL;
        END TRY
        BEGIN CATCH
            SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
            IF @@TRANCOUNT > 0 ROLLBACK;

            IF @localRunId IS NOT NULL
            BEGIN
                SET @message = N'Error: ' + @ERROR_MESSAGE;
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @Procedure, @Message = @message, @Severity = @ERROR_SEVERITY, @State = @ERROR_STATE, @VerboseLevel = 10, @LogToTable = 1, @RaiseError = 0, @MessagesStack = @MessagesStack OUTPUT;
            END; 

            THROW;

        END CATCH
    END TRY
    BEGIN CATCH
        SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
        IF @@TRANCOUNT > 0 ROLLBACK;

        IF @localRunId IS NOT NULL 
        BEGIN
            -- Save Unknown Error
--            INSERT INTO [Maintenance].[Messages](RunId, [Procedure], [Message], [Severity], [State]) VALUES
  --              (@localRunId, @Procedure, N'Runs Cleanup Finished with error(s)', 16, 1);
                
            SET @message = N'Runs Cleanup Finished with error(s)';
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @Procedure, @Message = @message, @Severity = @ERROR_SEVERITY, @State = @ERROR_STATE, @VerboseLevel = 10, @LogToTable = 1, @RaiseError = 0, @MessagesStack = @MessagesStack OUTPUT;

            UPDATE [Maintenance].[Runs] SET [EndDate] = SYSDATETIME(), [ErrorStatus] = 4 WHERE Id = @localRunId;
        END;

        THROW;
    END CATCH
END

GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- DROP VIEW [Maintenance].[ArchivingListOrchestratorDBTables]
----------------------------------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[ArchivingListOrchestratorDBTables]') AND type in (N'V'))
BEGIN
    PRINT '  + CREATE VIEW: [Maintenance].[ArchivingListOrchestratorDBTables]';
END
ELSE PRINT '  ~ ALTER VIEW: [Maintenance].[ArchivingListOrchestratorDBTables]'
GO

CREATE OR ALTER VIEW [Maintenance].[ArchivingListOrchestratorDBTables]
AS
    WITH list([schema], [table], [group], [cluster], [IsArchived]) AS (
        SELECT CAST(LTRIM(RTRIM([schema])) AS nvarchar(128)), CAST(LTRIM(RTRIM([table])) AS nvarchar(128)), CAST(NULLIF(LTRIM(RTRIM([group])), '') AS nvarchar(128)), CAST(NULLIF(LTRIM(RTRIM([cluster])), '') AS nvarchar(128)), CAST([IsArchived] AS bit)
        FROM (VALUES
            ('dbo', 'AuditLogEntities', 'AuditLogs', N'Id', 1), ('dbo', 'AuditLogs', 'AuditLogs', N'Id', 1), ('Maintenance', 'ASyncStatus_AuditLogs', 'AuditLogs', NULL, 0)
            , ('dbo', 'Jobs', 'Jobs', N'Id', 1), ('Maintenance', 'ASyncStatus_Jobs', 'Jobs', NULL, 0)
            , ('dbo', 'Logs', 'Logs', N'Id', 1), ('Maintenance', 'ASyncStatus_Logs', 'Logs', NULL, 0)
            , ('dbo', 'QueueItems', 'Queues', N'Id', 1), ('dbo', 'QueueItemComments', 'Queues', N'Id', 1), ('dbo', 'QueueItemEvents', 'Queues', N'Id', 1), ('Maintenance', 'ASyncStatus_Queues', 'Queues', NULL, 0)
            , ('dbo', 'RobotLicenseLogs', 'RobotLicenseLogs', N'Id', 1), ('Maintenance', 'ASyncStatus_RobotLicenseLogs', 'RobotLicenseLogs', NULL, 0)
            , ('dbo', 'Tenants', NULL, NULL, 0)
        ) list([schema], [table], [group], [cluster], [IsArchived]) 
    ), tables([schema], [table], [group], [object_id], [cluster], [IsArchived]) AS (
        SELECT lst.[schema], lst.[table], lst.[group], tbl.object_id, lst.[cluster], lst.[IsArchived]
        FROM [list] lst
        LEFT JOIN sys.tables tbl ON tbl.[name] = lst.[table]
        LEFT JOIN sys.schemas sch ON tbl.schema_id = sch.schema_id AND sch.[name] = lst.[schema] 
    )
    SELECT tbl.[group], tbl.[schema], tbl.[table], [cluster], [IsArchived]--, lst.object_id
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

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- ### [Object]: TABLE [Maintenance].[ASyncStatus_Logs]
-- ### [Version]: 2023-09-07T18:14:12+02:00
-- ### [Source]: _src/Archive/OrchestratorDB/Logs/Table_OrchestratorDB.Maintenance.ASyncStatus_Logs.sql
-- ### [Hash]: 914e4af [SHA256-336F6B0B179A9ECEA25601525EFFF2A0C0FA7A08814E4F428B9118CD4FAAC47E]
-- ### [Docs]: https://???.???
-- !!! ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!!!
-- !!! ~~~~~~~~~ NOT OFFICIALLY SUPPORTED BY UIPATH 
----------------------------------------------------------------------------------------------------
IF NOT EXISTS(SELECT 1 FROM sys.tables WHERE name = N'ASyncStatus_Logs' AND SCHEMA_NAME(schema_id) = N'Maintenance')
BEGIN
    PRINT '  + CREATE TABLE: [Maintenance].[ASyncStatus_Logs]';

	CREATE TABLE [Maintenance].[ASyncStatus_Logs](
		[SyncId] [bigint] NOT NULL
		, [IsDeleted] [bit] NOT NULL CONSTRAINT [DF_Maintenance.ASyncStatus_Logs.IsDeleted] DEFAULT 0
		, [DeletedOnDate] [datetime] NULL
		, [FirstASyncId] [bigint] NULL
		, [LastAsyncId] [bigint] NULL
		, CONSTRAINT [PK_Maintenance.ASyncStatus_Logs] PRIMARY KEY CLUSTERED ([SyncId] ASC)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
	) ON [PRIMARY]
END
ELSE PRINT '  = Table already exists: [Maintenance].[ASyncStatus_Logs]';

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- DROP PROCEDURE [Maintenance].[ValidateASyncArchiveObjects]
----------------------------------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[ValidateASyncArchiveObjects]') AND type in (N'P'))
BEGIN
    EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [Maintenance].[ValidateASyncArchiveObjects] AS'
    PRINT '  + CREATE PROCEDURE: [Maintenance].[ValidateASyncArchiveObjects]';
END
ELSE PRINT '  = PROCEDURE [Maintenance].[ValidateASyncArchiveObjects] already exists' 
GO

PRINT '  ~ UPDATE PROCEDURE: [Maintenance].[ValidateASyncArchiveObjects]'
GO

ALTER PROCEDURE [Maintenance].[ValidateASyncArchiveObjects]
----------------------------------------------------------------------------------------------------
-- ### [Object]: PROCEDURE [Maintenance].[ValidateASyncArchiveObjects]
-- ### [Version]: 2023-09-06T16:29:11+02:00
-- ### [Source]: _src/Archive/OrchestratorDB/Procedure_OrchestratorDB.Maintenance.ValidateASyncArchiveObjects.sql
-- ### [Hash]: 8de71a8 [SHA256-632DB57590CF2A8F1092EE8479E15E22228349EC432A52B790B0FDBCF0E4A20C]
-- ### [Docs]: https://???.???
-- !!! ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!!!
-- !!! ~~~~~~~~~ NOT OFFICIALLY SUPPORTED BY UIPATH 
-- !!! ~~~~~~~~~ SQL Server >= 2016 SP1
----------------------------------------------------------------------------------------------------
    @SynonymASyncDeleteName nvarchar(256)
    , @SynonymASyncDeleteSchema nvarchar(256)
    , @SynonymArchiveSyncName nvarchar(256)
    , @SynonymArchiveSyncSchema nvarchar(256)
	, @ASyncDeleteTableFullParts nvarchar(250)
    , @ASyncDeleteExpectedColumns nvarchar(MAX) = NULL
    , @ArchiveSyncTableFullParts nvarchar(256) = NULL
    , @ArchiveSyncExpectedColumns nvarchar(MAX) = NULL
    , @CreateOrUpdateSynonyms bit = 0
    , @IsValid bit = 0 OUTPUT
    , @Messages nvarchar(MAX) OUTPUT
AS
BEGIN
    BEGIN TRY 
        SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
		SET LOCK_TIMEOUT 5000;
        SET ARITHABORT ON;
        SET NOCOUNT ON;
        SET NUMERIC_ROUNDABORT OFF;

        ----------------------------------------------------------------------------------------------------
        -- ASync Delete Variables
        ----------------------------------------------------------------------------------------------------
        DECLARE @asyncDeleteIsValid bit = 0;
        DECLARE @asyncDeleteTable nvarchar(256);
        DECLARE @asyncDeleteTable4Parts nvarchar(256);
        DECLARE @paramsASyncDeleteTableChecks nvarchar(MAX) = N'@Message nvarchar(MAX) OUTPUT, @Columns nvarchar(MAX) OUTPUT';
        DECLARE @stmtASyncDeleteTableChecks nvarchar(MAX) = N'';
        DECLARE @asyncDeleteJsonColumns nvarchar(MAX);
        DECLARE @expectedASyncDeleteColumns nvarchar(MAX)
        ----------------------------------------------------------------------------------------------------
        -- ASync Archive Sync variables
        ----------------------------------------------------------------------------------------------------
        DECLARE @archiveSyncTable nvarchar(256);
        DECLARE @archiveSyncSchema nvarchar(128);
        DECLARE @archiveSyncTable4Parts nvarchar(256);
        DECLARE @paramsArchiveSyncTableChecks nvarchar(MAX) = N'@Message nvarchar(MAX) OUTPUT, @Columns nvarchar(MAX) OUTPUT';
        DECLARE @stmtArchiveSyncTableChecks nvarchar(MAX);
        DECLARE @archiveSyncJsonColumns nvarchar(MAX);
        DECLARE @expectedArchiveSyncColumns nvarchar(MAX)
        ----------------------------------------------------------------------------------------------------
        -- Misc      
        ----------------------------------------------------------------------------------------------------
        DECLARE @sql nvarchar(max);
        ----------------------------------------------------------------------------------------------------      
        -- Message / Error Handling
        ----------------------------------------------------------------------------------------------------
        DECLARE @message nvarchar(MAX);
        DECLARE @ERROR_NUMBER INT, @ERROR_SEVERITY INT, @ERROR_STATE INT, @ERROR_PROCEDURE NVARCHAR(126), @ERROR_LINE INT, @ERROR_MESSAGE NVARCHAR(2048);
        DECLARE @space tinyint = 2;
        DECLARE @tab tinyint = 0;
        DECLARE @json_errors TABLE([id] tinyint NOT NULL, [severity] int NOT NULL, [message] nvarchar(MAX) NOT NULL);

        ----------------------------------------------------------------------------------------------------      
        -- Checks Synonyms and Tables
        ----------------------------------------------------------------------------------------------------
        -- Check Missing Synonym
        BEGIN TRY
            -- Synonym for ASync Delete table
            SELECT @asyncDeleteTable = ISNULL(LTRIM(RTRIM(@ASyncDeleteTableFullParts)), N'');
            SELECT @asyncDeleteTable4Parts = ISNULL(QUOTENAME(LTRIM(RTRIM(PARSENAME(@asyncDeleteTable, 4)))) + N'.', N'') + ISNULL(QUOTENAME(LTRIM(RTRIM(PARSENAME(@asyncDeleteTable, 3)))) + N'.', N'') + ISNULL(QUOTENAME(LTRIM(RTRIM(PARSENAME(@asyncDeleteTable, 2)))) + N'.', N'') + ISNULL(QUOTENAME(LTRIM(RTRIM(PARSENAME(@asyncDeleteTable, 1)))), N'');

            INSERT INTO @json_errors([id], [severity], [message]) 
            SELECT 0, 16, N'ERROR[SS1]: Synonym ' + QUOTENAME(@synonymASyncDeleteSchema) + N'.' + QUOTENAME(@synonymASyncDeleteName) + N' not found and @ASyncDeleteTableFullParts not provided' WHERE NOT EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = @synonymASyncDeleteName AND schema_id = SCHEMA_ID(@synonymASyncDeleteSchema)) AND @asyncDeleteTable = N''
            UNION ALL SELECT 1, 16, N'ERROR[SS2]: Synonym ' + QUOTENAME(@synonymASyncDeleteSchema) + N'.' + QUOTENAME(@synonymASyncDeleteName) + N' refers to an invalid @ASyncDeleteTableFullParts''s name' WHERE @asyncDeleteTable <> N'' AND @asyncDeleteTable4Parts = N''
            UNION ALL SELECT 2, 16, N'ERROR[SS3]: Synonym ' + QUOTENAME(@synonymASyncDeleteSchema) + N'.' + QUOTENAME(@synonymASyncDeleteName) + N' not found and @CreateSynonym not enabled' WHERE NOT EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = @synonymASyncDeleteName AND schema_id = SCHEMA_ID(@synonymASyncDeleteSchema)) AND @asyncDeleteTable <> N'' AND @asyncDeleteTable4Parts <> N'' AND @CreateOrUpdateSynonyms <> 1
            UNION ALL SELECT 3, 16, N'ERROR[SS4]: Synonym ' + QUOTENAME(@synonymASyncDeleteSchema) + N'.' + QUOTENAME(@synonymASyncDeleteName) + N' must refers to a 2 (or more) parts name with at least a schema and table name: [schema_name].[table_name]' WHERE @asyncDeleteTable <> N'' AND @asyncDeleteTable4Parts <> N'' AND PARSENAME(@asyncDeleteTable, 2) IS NULL
            ;
        END TRY
        BEGIN CATCH;
            IF @@TRANCOUNT > 0 ROLLBACK;
            SET @message = N'ERROR[SS0]: error(s) occured while checking ASyncDelete synonym';
            THROW;
        END CATCH; 

        BEGIN TRY
            -- Synonym for Archive Sync table
            SELECT @archiveSyncTable = ISNULL(LTRIM(RTRIM(@archiveSyncTableFullParts)), N'');
            SELECT @archiveSyncTable4Parts = ISNULL(QUOTENAME(LTRIM(RTRIM(PARSENAME(@archiveSyncTable, 4)))) + N'.', N'') + ISNULL(QUOTENAME(LTRIM(RTRIM(PARSENAME(@archiveSyncTable, 3)))) + N'.', N'') + ISNULL(QUOTENAME(LTRIM(RTRIM(PARSENAME(@archiveSyncTable, 2)))) + N'.', N'') + ISNULL(QUOTENAME(LTRIM(RTRIM(PARSENAME(@archiveSyncTable, 1)))), N'');

            INSERT INTO @json_errors([id], [severity], [message])
            SELECT 11, 16, N'ERROR[ST1]: Synonym ' + QUOTENAME(@synonymArchiveSyncSchema) + N'.' + QUOTENAME(@synonymArchiveSyncName) + N' not found and @ArchiveSyncTableFullParts not provided' WHERE NOT EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = @synonymArchiveSyncName AND schema_id = SCHEMA_ID(@synonymArchiveSyncSchema)) AND  @archiveSyncTable = N''
            UNION ALL SELECT 12, 16, N'ERROR[ST2]: Synonym ' + QUOTENAME(@synonymArchiveSyncSchema) + N'.' + QUOTENAME(@synonymArchiveSyncName) + N' not found and @ArchiveSyncTableFullParts''s name is invalid' WHERE @archiveSyncTable <> N'' AND @archiveSyncTable4Parts = N''
            UNION ALL SELECT 13, 16, N'ERROR[ST3]: Synonym ' + QUOTENAME(@synonymArchiveSyncSchema) + N'.' + QUOTENAME(@synonymArchiveSyncName) + N' not found and @CreateOrUpdateSynonyms not enabled' WHERE  NOT EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = @synonymArchiveSyncName AND schema_id = SCHEMA_ID(@synonymArchiveSyncSchema)) AND @archiveSyncTable <> N'' AND @archiveSyncTable4Parts <> N'' AND @CreateOrUpdateSynonyms <> 1
            UNION ALL SELECT 3, 16, N'ERROR[ST4]: Synonym ' + QUOTENAME(@synonymArchiveSyncSchema) + N'.' + QUOTENAME(@synonymArchiveSyncName) + N' must refers to a 2 (or more) parts name with at least a schema and table name: [schema_name].[table_name]' WHERE @archiveSyncTable <> N'' AND @archiveSyncTable4Parts <> N'' AND PARSENAME(@archiveSyncTable, 2) IS NULL
            ;
        END TRY
        BEGIN CATCH;
            IF @@TRANCOUNT > 0 ROLLBACK;
            SET @message = N'ERROR[ST0]: error(s) occured while checking Archive Sync synonyms';
            THROW;
        END CATCH; 

        -- Check ASync Delete Table
        IF NOT EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10)
        BEGIN
            BEGIN TRY
                SELECT @asyncDeleteTable4Parts = base_object_name FROM sys.synonyms WHERE [name] = @synonymASyncDeleteName AND schema_id = SCHEMA_ID(@synonymASyncDeleteSchema) AND (@archiveSyncTable4Parts IS NULL OR @asyncDeleteTable4Parts = N'');

                SELECT @stmtASyncDeleteTableChecks = N'
                DROP TABLE IF EXISTS #tempASTable;
                BEGIN TRY
                    SELECT TOP(0) * INTO #tempArchiveSyncTable FROM ' + @asyncDeleteTable4Parts + N';

                    SELECT @Columns = (
                        SELECT col.column_id, [column] = col.name, [type] = tpe.name, col.max_length, col.precision, col.scale
                        FROM tempdb.sys.columns AS col
                        INNER JOIN tempdb.sys.types AS tpe ON col.system_type_id = tpe.system_type_id AND tpe.system_type_id = tpe.user_type_id
                        WHERE [object_id] = OBJECT_ID(N''tempdb.dbo.#tempArchiveSyncTable'')
                        FOR JSON PATH
                    );
                END TRY
                BEGIN CATCH
                    THROW;
                END CATCH
                ';
                -- retrieve ASync Delete columns
                EXEC sp_executesql @stmt = @stmtASyncDeleteTableChecks, @params = @paramsASyncDeleteTableChecks, @Message = NULL, @Columns = @asyncDeleteJsonColumns OUTPUT;

                -- Set default columns if not provided
                SELECT @expectedASyncDeleteColumns = ISNULL(LTRIM(RTRIM(@ASyncDeleteExpectedColumns)), N'[{"column":"SyncId","type":"bigint"},{"column":"Id","type":"bigint"}]' );
                -- check columns
                WITH exp([name], [type], [max_length]) AS (
                    SELECT [name], [type], [max_length]/*, [precision], [scale]*/ FROM OPENJSON(@expectedASyncDeleteColumns, N'$')
                    WITH ([name] nvarchar(128) N'$.column', [type] nvarchar(128), max_length smallint/*, precision tinyint, scale tinyint*/)                
                ), col([name], [type], [max_length]) AS(
                    SELECT [name], [type], [max_length] FROM OPENJSON(@asyncDeleteJsonColumns, N'$')
                    WITH ([name] nvarchar(128) N'$.column', [type] nvarchar(128), max_length smallint)
                )
                INSERT INTO @json_errors([id], [severity], [message])
                SELECT 20, 16, N'ERROR[TS1]: No column retrieved from remote Delete table ' + @asyncDeleteTable4Parts + N' refered by synonym ' + QUOTENAME(@synonymASyncDeleteSchema) + N'.' + QUOTENAME(@synonymASyncDeleteName) WHERE NOT EXISTS(SELECT 1 FROM col)
                UNION ALL SELECT 20, 16, N'ERROR[TS2]: Expected column ' + QUOTENAME(x.[name]) + N' not found in remote Delete table ' + @asyncDeleteTable4Parts + N' refered by synonym ' + QUOTENAME(@synonymASyncDeleteSchema) + N'.' + QUOTENAME(@synonymASyncDeleteName) FROM exp x WHERE NOT EXISTS(SELECT 1 FROM col WHERE [name] = x.[name])
                UNION ALL SELECT 20, 16, N'ERROR[TS3]: Invalid type '+ QUOTENAME(c.[type]) + N' for column ' + QUOTENAME(x.[name]) + N' in remote Delete table ' + @asyncDeleteTable4Parts + N' refered by synonym ' + QUOTENAME(@synonymASyncDeleteSchema) + N'.' + QUOTENAME(@synonymASyncDeleteName) + N' (' + QUOTENAME(x.[type]) + N' expected)' FROM exp x INNER JOIN col c ON x.[name] = c.[name] AND x.[type] <> c.[type]
                ;
            END TRY
            BEGIN CATCH
                SET @message = N'ERROR[TS0]: error(s) occured while checking remote Delete table';
                THROW;
            END CATCH
        END    
        -- Check Archive Sync Table
        IF NOT EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10)
        BEGIN
            BEGIN TRY
                SELECT @archiveSyncTable4Parts = base_object_name FROM sys.synonyms WHERE [name] = @synonymArchiveSyncName AND schema_id = SCHEMA_ID(@synonymArchiveSyncSchema) AND (@archiveSyncTable4Parts IS NULL OR @archiveSyncTable4Parts = N'');

                SELECT @stmtArchiveSyncTableChecks = N'
                DROP TABLE IF EXISTS #tempArchiveSyncTable;
                BEGIN TRY
                    SELECT TOP(0) * INTO #tempArchiveSyncTable FROM ' + @archiveSyncTable4Parts + N';

                    SELECT @Columns = (
                        SELECT col.column_id, [column] = col.name, [type] = tpe.name, col.max_length, col.precision, col.scale
                        FROM tempdb.sys.columns AS col
                        INNER JOIN tempdb.sys.types AS tpe ON col.system_type_id = tpe.system_type_id AND tpe.system_type_id = tpe.user_type_id
                        WHERE [object_id] = OBJECT_ID(N''tempdb.dbo.#tempArchiveSyncTable'')
                        FOR JSON PATH
                    );
                END TRY
                BEGIN CATCH
                    THROW;
                END CATCH
                ';
                -- retrieve Archive Sync columns
                EXEC sp_executesql @stmt = @stmtArchiveSyncTableChecks, @params = @paramsArchiveSyncTableChecks, @Message = NULL, @Columns = @archiveSyncJsonColumns OUTPUT;

                -- Set default columns if not provided
                SELECT @expectedArchiveSyncColumns = ISNULL(LTRIM(RTRIM(@ArchiveSyncExpectedColumns)), N'[{"column":"Id","type":"bigint"},{"column":"IsArchived","type":"bit"},{"column":"IsDeleted","type":"bit"},{"column":"DeleteAfterDatetime","type":"datetime"},{"column":"FirstASyncId","type":"bigint"},{"column":"LastASyncId","type":"bigint"},{"column":"CountASyncIds","type":"bigint"}]' );
                -- check columns
                WITH exp([name], [type], [max_length]) AS (
                    SELECT [name], [type], [max_length]/*, [precision], [scale]*/ FROM OPENJSON(@expectedArchiveSyncColumns, N'$')
                    WITH ([name] nvarchar(128) N'$.column', [type] nvarchar(128), max_length smallint/*, precision tinyint, scale tinyint*/)                
                ), col([name], [type], [max_length]) AS(
                    SELECT [name], [type], [max_length] FROM OPENJSON(@archiveSyncJsonColumns, N'$')
                    WITH ([name] nvarchar(128) N'$.column', [type] nvarchar(128), max_length smallint)
                )
                INSERT INTO @json_errors([id], [severity], [message])
                SELECT 20, 16, N'ERROR[TS1]: No column retrieved from remote Sync table ' + @archiveSyncTable4Parts + N' refered by synonym ' + QUOTENAME(@synonymArchiveSyncSchema) + N'.' + QUOTENAME(@synonymArchiveSyncName) WHERE NOT EXISTS(SELECT 1 FROM col)
                UNION ALL SELECT 20, 16, N'ERROR[TS2]: Expected column ' + QUOTENAME(x.[name]) + N' not found in remote Sync table ' + @archiveSyncTable4Parts + N' refered by synonym ' + QUOTENAME(@synonymArchiveSyncSchema) + N'.' + QUOTENAME(@synonymArchiveSyncName) FROM exp x WHERE NOT EXISTS(SELECT 1 FROM col WHERE [name] = x.[name])
                UNION ALL SELECT 20, 16, N'ERROR[TS3]: Invalid type '+ QUOTENAME(c.[type]) + N' for column ' + QUOTENAME(x.[name]) + N' in remote Sync table ' + @archiveSyncTable4Parts + N' refered by synonym ' + QUOTENAME(@synonymArchiveSyncSchema) + N'.' + QUOTENAME(@synonymArchiveSyncName) + N' (' + QUOTENAME(x.[type]) + N' expected)' FROM exp x INNER JOIN col c ON x.[name] = c.[name] AND x.[type] <> c.[type]
                ;
            END TRY
            BEGIN CATCH
                SET @message = N'ERROR[TS0]: error(s) occured while checking remote Sync table';
                THROW;
            END CATCH
        END        
        ----------------------------------------------------------------------------------------------------      
        -- Create or Update Synonym(s) / Table / Columns
        ----------------------------------------------------------------------------------------------------
        IF NOT EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10)
        BEGIN
            -- Start Transaction for all upcoming schema changes
            BEGIN TRAN
            -- Create ASync Delete synonym if missing or outdated
            BEGIN TRY
                IF NOT EXISTS (SELECT 1 FROM sys.synonyms WHERE [name] = @synonymASyncDeleteName AND schema_id = SCHEMA_ID(@synonymASyncDeleteSchema) AND base_object_name = @asyncDeleteTable4Parts) AND @asyncDeleteTable4Parts <> N'' AND @CreateOrUpdateSynonyms = 1
                BEGIN
                    SET @sql = NULL;
                    INSERT INTO @json_errors([id], [severity], [message]) SELECT 0, 10, N'Create or alter ASync Delete Synonym '+ QUOTENAME(@synonymASyncDeleteSchema) + N'.' + QUOTENAME(@synonymASyncDeleteName) + ' with base object ' + @asyncDeleteTable4Parts;
                    SELECT @sql = N'DROP SYNONYM ' + QUOTENAME(@synonymASyncDeleteSchema) + N'.' + QUOTENAME(@synonymASyncDeleteName) + N';' FROM sys.synonyms WHERE [name] = @synonymASyncDeleteName AND schema_id = SCHEMA_ID(@synonymASyncDeleteSchema);
                    SELECT @sql = ISNULL(@sql, N'') + N'CREATE SYNONYM ' + QUOTENAME(@synonymASyncDeleteSchema) + N'.' + QUOTENAME(@synonymASyncDeleteName) + N' FOR ' + @asyncDeleteTable4Parts + N';';
                    EXEC sp_executesql @statement = @sql;
                END
            END TRY
            BEGIN CATCH
                SET @message = N'ERROR[SU0]: error(s) occured while creating or updating source synomym';
                THROW;
            END CATCH
            -- Create Archive Sync synonym if missing or outdated
            BEGIN TRY
                IF NOT EXISTS (SELECT 1 FROM sys.synonyms WHERE [name] = @synonymArchiveSyncName AND schema_id = SCHEMA_ID(@synonymArchiveSyncSchema) AND base_object_name = @archiveSyncTable4Parts) AND @archiveSyncTable4Parts <> N'' AND @CreateOrUpdateSynonyms = 1
                BEGIN
                    SET @sql = NULL;
                    INSERT INTO @json_errors([id], [severity], [message]) SELECT 10, 10, N'Create or alter Archive Sync Synonym '+ QUOTENAME(@synonymArchiveSyncSchema) + N'.' + QUOTENAME(@synonymArchiveSyncName) + ' with base object: ' + @archiveSyncTable4Parts;
                    SELECT @sql = N'DROP SYNONYM ' + QUOTENAME(@synonymArchiveSyncSchema) + N'.' + QUOTENAME(@synonymArchiveSyncName) + N';' FROM sys.synonyms WHERE [name] = @synonymArchiveSyncName AND schema_id = SCHEMA_ID(@synonymArchiveSyncSchema);
                    SELECT @sql = ISNULL(@sql, N'') + N'CREATE SYNONYM ' + QUOTENAME(@synonymArchiveSyncSchema) + N'.' + QUOTENAME(@synonymArchiveSyncName) + N' FOR ' + @archiveSyncTable4Parts + N';';
                    EXEC sp_executesql @statement = @sql;
                END
            END TRY
            BEGIN CATCH
                IF @@TRANCOUNT > 0 ROLLBACK;
                SET @message = N'ERROR[SU1]: error(s) occured while creating or updating Archive Sync synomym';
                THROW;
            END CATCH
            SET @message = NULL;
            IF @@TRANCOUNT > 0 COMMIT
        END
    END TRY
    BEGIN CATCH
        SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
        IF @@TRANCOUNT > 0 ROLLBACK;
    END CATCH

--    SELECT TOP(100) 'message'= 'output', [Message] = LEFT([message], 4000), [Severity] = [severity], [State] = 1 FROM @json_errors ORDER BY [id] ASC
    -- Check / Set @IsValid flag
    SET @IsValid = IIF(NOT EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10) AND @ERROR_NUMBER IS NULL AND @message IS NULL, 1, 0);

    SET @Messages = --ISNULL(
    ( 
        SELECT [Procedure] = QUOTENAME(COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?')) + N'.' + QUOTENAME(COALESCE(OBJECT_NAME(@@PROCID), N'?')), [Message], [Severity], [State] 
        FROM (
            SELECT [Message], [Severity], [State] FROM (
                SELECT TOP(100) [Message] = LEFT([message], 4000), [Severity] = [severity], [State] = 1 FROM @json_errors ORDER BY [id] ASC
            ) err
            UNION ALL SELECT N'ERROR: ' + @ERROR_MESSAGE, 16, 1 WHERE @ERROR_MESSAGE IS NOT NULL
            UNION ALL SELECT @message, 16, 1 WHERE @message IS NOT NULL
        ) jsn
        FOR JSON PATH)
    --    , N'[]')
    ;
    RETURN 0;
END
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- DROP PROCEDURE [Maintenance].[ValidateASyncArchiveObjectsLogs]
----------------------------------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[ValidateASyncArchiveObjectsLogs]') AND type in (N'P'))
BEGIN
    EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [Maintenance].[ValidateASyncArchiveObjectsLogs] AS'
    PRINT '  + CREATE PROCEDURE: [Maintenance].[ValidateASyncArchiveObjectsLogs]';
END
ELSE PRINT '  = PROCEDURE [Maintenance].[ValidateASyncArchiveObjectsLogs] already exists' 
GO

PRINT '  ~ UPDATE PROCEDURE: [Maintenance].[ValidateASyncArchiveObjectsLogs]'
GO

ALTER PROCEDURE [Maintenance].[ValidateASyncArchiveObjectsLogs]
----------------------------------------------------------------------------------------------------
-- ### [Object]: PROCEDURE [Maintenance].[ValidateASyncArchiveObjectsLogs]
-- ### [Version]: 2023-09-07T18:14:12+02:00
-- ### [Source]: _src/Archive/OrchestratorDB/Logs/Procedure_OrchestratorDB.Maintenance.ValidateASyncArchiveObjectsLogs.sql
-- ### [Hash]: 914e4af [SHA256-350CD80EFFE665FF803C89B2D061C745C91124F4FA40556B66597D9F058645A6]
-- ### [Docs]: https://???.???
-- !!! ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!!!
-- !!! ~~~~~~~~~ NOT OFFICIALLY SUPPORTED BY UIPATH 
-- !!! ~~~~~~~~~ SQL Server >= 2016 SP1
----------------------------------------------------------------------------------------------------
	@ASyncDeleteTableFullParts nvarchar(256) = NULL
	, @ArchiveSyncTableFullParts nvarchar(256) = NULL
    , @CreateOrUpdateSynonyms bit = 0
    , @IsValid bit = 0 OUTPUT
    , @Messages nvarchar(MAX) = NULL OUTPUT
AS
BEGIN
    BEGIN TRY 
        SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
		SET LOCK_TIMEOUT 5000;
        SET ARITHABORT ON;
        SET NOCOUNT ON;
        SET NUMERIC_ROUNDABORT OFF;

        ----------------------------------------------------------------------------------------------------
        -- Settings
        ----------------------------------------------------------------------------------------------------
        DECLARE @synonymASyncDeleteName nvarchar(256) = N'Synonym_ASyncDelete_Logs';
        DECLARE @synonymASyncDeleteSchema nvarchar(256) = N'Maintenance';
        DECLARE @synonymArchiveSyncName nvarchar(256) = N'Synonym_ArchiveSync_Logs';
        DECLARE @synonymArchiveSyncSchema nvarchar(256) = N'Maintenance';
        DECLARE @synonymASyncStatusName nvarchar(256) = N'Synonym_ASyncStatus_Logs';
        DECLARE @synonymASyncStatusSchema nvarchar(256) = N'Maintenance';
        DECLARE @clusteredName nvarchar(128) = N'Id';
        ----------------------------------------------------------------------------------------------------      
        -- Message / Error Handling
        ----------------------------------------------------------------------------------------------------
        DECLARE @message nvarchar(MAX);
        DECLARE @ERROR_NUMBER INT, @ERROR_SEVERITY INT, @ERROR_STATE INT, @ERROR_PROCEDURE NVARCHAR(126), @ERROR_LINE INT, @ERROR_MESSAGE NVARCHAR(2048) ;

        ----------------------------------------------------------------------------------------------------
        -- Call Main Checks Procedure      
        ----------------------------------------------------------------------------------------------------
        EXEC [Maintenance].[ValidateASyncArchiveObjects]
            @SynonymASyncDeleteName = @synonymASyncDeleteName
            , @SynonymASyncDeleteSchema = @synonymASyncDeleteSchema
            , @SynonymArchiveSyncName = @synonymArchiveSyncName
            , @SynonymArchiveSyncSchema = @synonymArchiveSyncSchema
            , @ASyncDeleteTableFullParts = @ASyncDeleteTableFullParts
            , @ArchiveSyncTableFullParts = @ArchiveSyncTableFullParts
            , @CreateOrUpdateSynonyms = @CreateOrUpdateSynonyms
            , @IsValid = @IsValid OUTPUT
            , @Messages = @Messages OUTPUT
        ;
    END TRY
    BEGIN CATCH
        SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
        SET @message = N'ERROR[CH0]: error(s) occured while checking ASync and Delete objects';
        SET @Messages = 
        ( 
            SELECT [Procedure] = QUOTENAME(COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?')) + N'.' + QUOTENAME(COALESCE(OBJECT_NAME(@@PROCID), N'?')), [Message], [Severity], [State] 
            FROM (
                SELECT [Message]  = N'ERROR: ' + @ERROR_MESSAGE, [Severity] = 16, [State] = 1 WHERE @ERROR_MESSAGE IS NOT NULL
                UNION ALL SELECT @message, 16, 1 WHERE @message IS NOT NULL
            ) jsn
            FOR JSON PATH
        );
        SET @IsValid = 0;
    END CATCH
    RETURN 0;
END
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- DROP PROCEDURE [Maintenance].[CreateArchivingExternalTable]
----------------------------------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[CreateArchivingExternalTable]') AND type in (N'P'))
BEGIN
    EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [Maintenance].[CreateArchivingExternalTable] AS'
    PRINT '  + CREATE PROCEDURE: [Maintenance].[CreateArchivingExternalTable]';
END
ELSE PRINT '  = PROCEDURE [Maintenance].[CreateArchivingExternalTable] already exists' 
GO

PRINT '  ~ UPDATE PROCEDURE: [Maintenance].[CreateArchivingExternalTable]'
GO

ALTER PROCEDURE [Maintenance].[CreateArchivingExternalTable]
----------------------------------------------------------------------------------------------------
-- ### [Object]: PROCEDURE [Maintenance].[CreateArchivingExternalTable]
-- ### [Version]: 2023-10-16T18:16:29+02:00
-- ### [Source]: _src/Archive/Procedure_Maintenance.CreateArchivingExternalTable.sql
-- ### [Hash]: 085ff9b [SHA256-833D83081AF8B257DB988014284AAE0FDB68525C72089E8311234E42654C3417]
-- ### [Docs]: https://???.???
-- !!! ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!!!
-- !!! ~~~~~~~~~ NOT OFFICIALLY SUPPORTED BY UIPATH 
-- !!! ~~~~~~~~~ SQL Server >= 2016 SP1
----------------------------------------------------------------------------------------------------
    @ExternalDataSource nvarchar(128)
    , @ExternalName nvarchar(128)
    , @ExternalSchema nvarchar(128)
    , @Columns nvarchar(MAX)
    , @ShowMessages bit = 1
    , @ThrowError bit = 1
--    , @Columns nvarchar(MAX) = NULL OUTPUT
    , @IsValid bit = 0 OUTPUT
    , @Messages nvarchar(MAX) = NULL OUTPUT
AS
BEGIN
    BEGIN TRY 
        SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
		SET LOCK_TIMEOUT 5000;
        SET ARITHABORT ON;
        SET NOCOUNT ON;
        SET NUMERIC_ROUNDABORT OFF;

        ----------------------------------------------------------------------------------------------------
        -- Table Variables
        ----------------------------------------------------------------------------------------------------
        DECLARE @datasourceId int;
        DECLARE @tableId int
        DECLARE @replaceTable bit = 0;
        DECLARE @listExisting TABLE([column] nvarchar(128), [datatype] nvarchar(128))
        DECLARE @listColumns TABLE([id] int IDENTITY(0, 1), [column] nvarchar(128), [datatype] nvarchar(128))
        ----------------------------------------------------------------------------------------------------
        -- Misc      
        ----------------------------------------------------------------------------------------------------
        DECLARE @sql nvarchar(max);
        DECLARE @sqlVars nvarchar(MAX);
        DECLARE @sqlValues nvarchar(MAX);
        DECLARE @sqlCols nvarchar(MAX);
        ----------------------------------------------------------------------------------------------------      
        -- Message / Error Handling
        ----------------------------------------------------------------------------------------------------
        DECLARE @message nvarchar(MAX);
        DECLARE @ERROR_NUMBER INT, @ERROR_SEVERITY INT, @ERROR_STATE INT, @ERROR_PROCEDURE NVARCHAR(126), @ERROR_LINE INT, @ERROR_MESSAGE NVARCHAR(2048);
        DECLARE @space tinyint = 2;
        DECLARE @tab tinyint = 0;
        DECLARE @json_errors TABLE([id] tinyint NOT NULL, [severity] int NOT NULL, [message] nvarchar(MAX) NOT NULL);

        ----------------------------------------------------------------------------------------------------      
        -- Checks Data Source and External Table
        ----------------------------------------------------------------------------------------------------
        SELECT @datasourceId = data_source_id FROM sys.external_data_sources WHERE [name] = @ExternalDataSource;
        SELECT @tableId = object_id, @replaceTable = @replaceTable | IIF([data_source_id] <> @datasourceId, 1, 0) FROM sys.external_tables WHERE [name] = @ExternalName AND [schema_id] = SCHEMA_ID(@ExternalSchema);

        INSERT INTO @listExisting([column], [datatype])
        SELECT TOP(1000) col.[name] 
            , [datatype] =   tpe.[name] + 
                CASE WHEN tpe.[name] IN (N'varchar', N'char', N'varbinary', N'binary', N'text') THEN '(' + CASE WHEN col.max_length = -1 THEN 'MAX' ELSE CAST(col.max_length AS VARCHAR(5)) END + ')'
                WHEN tpe.[name] IN (N'nvarchar', N'nchar', N'ntext') THEN '(' + CASE WHEN col.max_length = -1 THEN 'MAX' ELSE CAST(col.max_length / 2 AS VARCHAR(5)) END + ')'
                WHEN tpe.[name] IN (N'datetime2', N'time2', N'datetimeoffset') THEN '(' + CAST(col.scale AS VARCHAR(5)) + ')'
                WHEN tpe.[name] IN (N'decimal', N'numeric') THEN '(' + CAST(col.[precision] AS VARCHAR(5)) + ',' + CAST(col.scale AS VARCHAR(5)) + ')'
                WHEN tpe.[name] IN (N'float') THEN '(' + CAST(col.[precision] AS VARCHAR(5)) + ')'
                ELSE '' END
        FROM sys.columns AS col
        INNER JOIN sys.types AS tpe ON col.system_type_id = tpe.system_type_id AND tpe.system_type_id = tpe.user_type_id
        WHERE col.object_id = @tableId AND tpe.[name] <> N'timestamp'

        IF ISJSON(@Columns) = 1
        BEGIN
            INSERT INTO @listColumns([column], [datatype])
            SELECT[column], [datatype]
            FROM OPENJSON(@Columns) WITH([column] nvarchar(128), [datatype] nvarchar(128))
            WHERE [column] IS NOT NULL AND [datatype] IS NOT NULL;
        END

        SELECT @replaceTable = 1 WHERE EXISTS(SELECT [column], [datatype] FROM @listExisting EXCEPT SELECT [column], [datatype] FROM @listColumns);
        SELECT @replaceTable = 1 WHERE EXISTS(SELECT [column], [datatype] FROM @listColumns EXCEPT SELECT [column], [datatype] FROM @listExisting);

        INSERT INTO @json_errors([id], [severity], [message]) 
        SELECT 0, 16, N'Data Source does not exists: ' + @ExternalDataSource WHERE @datasourceId IS NULL
        UNION ALL SELECT 0, 1, N'Data Source exists: ' + @ExternalDataSource WHERE @datasourceId IS NOT NULL
        UNION ALL SELECT 1, 16, N'@Columns is not a valid JSON string' WHERE ISJSON(@Columns) = 0
        UNION ALL SELECT 1, 16, N'No column name found in @Columns' WHERE NOT EXISTS(SELECT 1 FROM @listColumns)
        UNION ALL SELECT 2, 16, N'External table not found but an object with this name already exists and must be replaced: ' +  QUOTENAME(@ExternalSchema) + N'.' + QUOTENAME(@ExternalName) FROM sys.objects WHERE [name] = @ExternalName AND [schema_id] = SCHEMA_ID(@ExternalSchema) AND @tableId IS NULL
        UNION ALL SELECT 2, 1, N'External table already exists: ' +  QUOTENAME(@ExternalSchema) + N'.' + QUOTENAME(@ExternalName) WHERE @tableId IS NOT NULL AND @replaceTable = 0
        ;

        SELECT @message = N'ERROR[PR0]: error(s) occured while checking parameters' WHERE EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10);
        
        IF NOT EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10) AND @replaceTable = 1 AND @tableId IS NOT NULL
        BEGIN
            BEGIN TRY
                INSERT INTO @json_errors([id], [severity], [message]) SELECT 10, 10, N'Drop external table: ' +  QUOTENAME(@ExternalSchema) + N'.' + QUOTENAME(@ExternalName)
                UNION ALL SELECT 2, 10, N'External Table exists but will be replaced: ' + QUOTENAME(@ExternalSchema) + N'.' + QUOTENAME(@ExternalName) WHERE @datasourceId IS NOT NULL AND @tableId IS NOT NULL AND @replaceTable = 1;
                SELECT @sql = N'DROP EXTERNAL TABLE '+ QUOTENAME(@ExternalSchema) + N'.' + QUOTENAME(@ExternalName) + N';';
                EXEC sp_executesql @stmt = @sql;
            END TRY
            BEGIN CATCH
                SET @message = N'ERROR[DT0]: error(s) occured while removing external table';
                THROW;
            END CATCH
        END

        -- Prepare columns Name/Type string
        SELECT @sqlVars = NULL;
        SELECT @sqlVars = COALESCE(@sqlVars + N', @c' + CAST([id] AS nvarchar(10)) + N' ' + [datatype], N'DECLARE @c' + CAST([id] AS nvarchar(10)) + N' ' + [datatype]) 
            , @sqlValues = COALESCE(@sqlValues + N', @c' + CAST([id] AS nvarchar(10)) + N' = ' + QUOTENAME([column]), N'@c' + CAST([id] AS nvarchar(10)) + N' = ' + QUOTENAME([column])) 
            , @sqlCols = COALESCE(@sqlCols + N', ' + QUOTENAME([column]) + N' ' + [datatype], N'' + QUOTENAME([column])  + N' ' + [datatype]) 
        FROM @listColumns;

        -- Create External Table
        IF NOT EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10) AND (@tableId IS NULL OR @replaceTable = 1)
        BEGIN
            BEGIN TRY
                INSERT INTO @json_errors([id], [severity], [message]) SELECT 11, 10, N'Create external table: ' +  QUOTENAME(@ExternalSchema) + N'.' + QUOTENAME(@ExternalName)
                SELECT @sql = N'CREATE EXTERNAL TABLE '+ QUOTENAME(@ExternalSchema) + N'.' + QUOTENAME(@ExternalName) + N'(' + @sqlCols + N') WITH ( DATA_SOURCE = ' + QUOTENAME(@ExternalDataSource) + N');';

                EXEC sp_executesql @stmt = @sql;
            END TRY
            BEGIN CATCH
                SET @message = N'ERROR[XT0]: error(s) occured while creating external table';
                THROW;
            END CATCH
        END

        -- Test External Table
        IF NOT EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10)
        BEGIN
            BEGIN TRY
--                INSERT INTO @json_errors([id], [severity], [message]) SELECT 21, 10, N'Test external table: ' +  QUOTENAME(@ExternalSchema) + N'.' + QUOTENAME(@ExternalName)
                SELECT @sql = N'
                    BEGIN TRY
                        ' + @sqlVars + N';
                        SELECT TOP(1) ' + @sqlValues + N' FROM ' + QUOTENAME(@ExternalSchema) + N'.' + QUOTENAME(@ExternalName) + N';
                        SELECT @error_message = NULL
                    END TRY
                    BEGIN CATCH
                        SELECT @error_message = ERROR_MESSAGE();
                        THROW;
                    END CATCH
                ';
                -- Test external table
                EXEC sp_executesql @stmt = @sql, @params = N'@error_message nvarchar(2048) OUTPUT', @error_message = @ERROR_MESSAGE OUTPUT;
                INSERT INTO @json_errors([id], [severity], [message]) SELECT 22, 10, N'External table is valid: ' +  QUOTENAME(@ExternalSchema) + N'.' + QUOTENAME(@ExternalName) WHERE @ERROR_MESSAGE IS NULL
                INSERT INTO @json_errors([id], [severity], [message]) SELECT 20, 16, 'y'+ @ERROR_MESSAGE WHERE @ERROR_MESSAGE IS NOT NULL;
            END TRY
            BEGIN CATCH
                SET @message = N'ERROR[TS0]: error(s) occured while testing external table';
                THROW;
            END CATCH
        END
    END TRY
    BEGIN CATCH
        SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
        IF @@TRANCOUNT > 0 ROLLBACK;
    END CATCH

--    SELECT TOP(100) 'message'= 'output', [Message] = LEFT([message], 4000), [Severity] = [severity], [State] = 1 FROM @json_errors ORDER BY [id] ASC
    -- Check / Set @IsValid flag
    SET @IsValid = IIF(NOT EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10) AND @ERROR_NUMBER IS NULL AND @message IS NULL, 1, 0);

    SET @Messages = --ISNULL(
    ( 
        SELECT [Procedure] = QUOTENAME(COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?')) + N'.' + QUOTENAME(COALESCE(OBJECT_NAME(@@PROCID), N'?')), [Message], [Severity], [State] 
        FROM (
            SELECT [Message], [Severity], [State] FROM (
                SELECT TOP(100) [Message] = LEFT([message], 4000), [Severity] = [severity], [State] = 1 FROM @json_errors WHERE [severity] >= 10 OR @IsValid = 0 OR @ShowMessages = 1 ORDER BY [id] ASC
            ) err
            UNION ALL SELECT N'ERROR: ' + @ERROR_MESSAGE, 16, 1 WHERE @ERROR_MESSAGE IS NOT NULL
            UNION ALL SELECT @message, 16, 1 WHERE @message IS NOT NULL
        ) jsn
        FOR JSON PATH)
    --    , N'[]')
    ;
    IF @ShowMessages = 1 OR (@IsValid = 0 AND @ThrowError = 1) SELECT [Message] = LEFT([Message], 1000), [Severity], [Error] = IIF([Severity] > 10, 1, 0) FROM OPENJSON(@Messages) WITH([Message] nvarchar(MAX), [Severity] int)
    IF @IsValid = 0 AND @ThrowError = 1 THROW 50000, 'Error(s) occured. See output dataset', 1;
    RETURN 0;
END
GO

/*
EXEC [Maintenance].[CreateArchivingExternalTable] @ExternalDataSource = 'ArchivingDatasourceForOrchestratorDB', @ExternalName = N'ArchivingListOrchestratorDBTables', @ExternalSchema = N'Maintenance'
, @Columns = N'[{"column":"group","datatype":"nvarchar(128)"},{"column":"schema","datatype":"nvarchar(128)"},{"column":"table","datatype":"nvarchar(128)"},{"column":"exists","datatype":"bit"},{"column":"isvalid","datatype":"bit"},{"column":"columns","datatype":"nvarchar(MAX)"}]'
, @ShowMessages = 1
, @ThrowError = 0;
GO
*/

/*
drop external data source [ArchivingDatasourceForOrchestratorDB]
drop external table [Maintenance].[ArchivingListOrchestratorDBTables]

SELECT * FROM sys.external_data_sources WHERE [name] = @ExternalDataSource
SELECT * FROM sys.external_tables 
SELECT 'drop external table ' +  quotename(OBJECT_SCHEMA_NAME(object_id)) + '.'+ quotename(name), * FROM sys.external_tables 

SELECT * FROM [Maintenance].[ArchivingOrchestratorDBTables]
SELECT * FROM [Maintenance].[ArchivingOrchestratorDBTablesx]
*/

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- DROP PROCEDURE [Maintenance].[SetSourceTable]
----------------------------------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[SetSourceTable]') AND type in (N'P'))
BEGIN
    EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [Maintenance].[SetSourceTable] AS'
    PRINT '  + CREATE PROCEDURE: [Maintenance].[SetSourceTable]';
END
ELSE PRINT '  = PROCEDURE [Maintenance].[SetSourceTable] already exists' 
GO

PRINT '  ~ UPDATE PROCEDURE: [Maintenance].[SetSourceTable]'
GO

ALTER PROCEDURE [Maintenance].[SetSourceTable]
----------------------------------------------------------------------------------------------------
-- ### [Object]: PROCEDURE [Maintenance].[SetSourceTable]
-- ### [Version]: 2023-10-16T18:16:29+02:00
-- ### [Source]: _src/Archive/Procedure_ArchiveDB.Maintenance.SetSourceTable.sql
-- ### [Hash]: 085ff9b [SHA256-6BDE73946CA205EBF1DEFDC7BDEE6D45725C697B0EC16C8A411647CD5315DDEA]
-- ### [Docs]: https://???.???
-- !!! ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!!!
-- !!! ~~~~~~~~~ NOT OFFICIALLY SUPPORTED BY UIPATH 
-- !!! ~~~~~~~~~ SQL Server >= 2016 SP1
----------------------------------------------------------------------------------------------------
    @SynonymName nvarchar(256)
    , @SynonymSchema nvarchar(256)
	, @SourceTableFullParts nvarchar(250) = NULL
    , @SourceExpectedColumns nvarchar(MAX) = NULL
    , @CreateOrUpdateSynonym bit = 0
    , @Columns nvarchar(MAX) = NULL OUTPUT
    , @ShowMessages bit = 0
    , @ThrowError bit = 0
    , @IsValid bit = 0 OUTPUT
    , @Messages nvarchar(MAX) OUTPUT
AS
BEGIN
    BEGIN TRY 
        SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
		SET LOCK_TIMEOUT 5000;
        SET ARITHABORT ON;
        SET NOCOUNT ON;
        SET NUMERIC_ROUNDABORT OFF;

        ----------------------------------------------------------------------------------------------------
        -- ASync Delete Variables
        ----------------------------------------------------------------------------------------------------
        DECLARE @sourceTable nvarchar(256);
        DECLARE @sourceTable4Parts nvarchar(256);
        DECLARE @paramsSourceTableChecks nvarchar(MAX) = N'@Message nvarchar(MAX) OUTPUT, @Columns nvarchar(MAX) OUTPUT';
        DECLARE @stmtSourceTableChecks nvarchar(MAX) = N'';
        DECLARE @sourceJsonColumns nvarchar(MAX);
        DECLARE @expectedSourceColumns nvarchar(MAX)
        ----------------------------------------------------------------------------------------------------
        -- Misc      
        ----------------------------------------------------------------------------------------------------
        DECLARE @sql nvarchar(max);
        ----------------------------------------------------------------------------------------------------      
        -- Message / Error Handling
        ----------------------------------------------------------------------------------------------------
        DECLARE @message nvarchar(MAX);
        DECLARE @ERROR_NUMBER INT, @ERROR_SEVERITY INT, @ERROR_STATE INT, @ERROR_PROCEDURE NVARCHAR(126), @ERROR_LINE INT, @ERROR_MESSAGE NVARCHAR(2048);
        DECLARE @space tinyint = 2;
        DECLARE @tab tinyint = 0;
        DECLARE @json_errors TABLE([id] tinyint NOT NULL, [severity] int NOT NULL, [message] nvarchar(MAX) NOT NULL);

        SET @Messages = NULL
        ----------------------------------------------------------------------------------------------------      
        -- Checks Synonyms and Tables
        ----------------------------------------------------------------------------------------------------
        -- Check Missing Synonym
        BEGIN TRY
            -- Synonym for Source table
            SELECT @sourceTable = ISNULL(LTRIM(RTRIM(@SourceTableFullParts)), N'');
            SELECT @sourceTable4Parts = ISNULL(QUOTENAME(LTRIM(RTRIM(PARSENAME(@sourceTable, 4)))) + N'.', N'') + ISNULL(QUOTENAME(LTRIM(RTRIM(PARSENAME(@sourceTable, 3)))) + N'.', N'') + ISNULL(QUOTENAME(LTRIM(RTRIM(PARSENAME(@sourceTable, 2)))) + N'.', N'') + ISNULL(QUOTENAME(LTRIM(RTRIM(PARSENAME(@sourceTable, 1)))), N'');

            INSERT INTO @json_errors([id], [severity], [message]) 
            SELECT 0, 16, N'ERROR[SS1]: Synonym ' + QUOTENAME(@synonymSchema) + N'.' + QUOTENAME(@synonymName) + N' not found and @SourceTableFullParts not provided' WHERE NOT EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = @synonymName AND schema_id = SCHEMA_ID(@synonymSchema)) AND @sourceTable = N''
            UNION ALL SELECT 1, 16, N'ERROR[SS2]: Synonym ' + QUOTENAME(@synonymSchema) + N'.' + QUOTENAME(@synonymName) + N' refers to an invalid @SourceTableFullParts''s name' WHERE @sourceTable <> N'' AND @sourceTable4Parts = N''
            UNION ALL SELECT 2, 16, N'ERROR[SS3]: Synonym ' + QUOTENAME(@synonymSchema) + N'.' + QUOTENAME(@synonymName) + N' not found and @CreateSynonym not enabled' WHERE NOT EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = @synonymName AND schema_id = SCHEMA_ID(@synonymSchema)) AND @sourceTable <> N'' AND @sourceTable4Parts <> N'' AND @CreateOrUpdateSynonym <> 1
            UNION ALL SELECT 3, 16, N'ERROR[SS4]: Synonym ' + QUOTENAME(@synonymSchema) + N'.' + QUOTENAME(@synonymName) + N' must refers to a 2 (or more) parts name with at least a schema and table name: [schema_name].[table_name]' WHERE @sourceTable <> N'' AND @sourceTable4Parts <> N'' AND PARSENAME(@sourceTable, 2) IS NULL
            ;
        END TRY
        BEGIN CATCH;
            IF @@TRANCOUNT > 0 ROLLBACK;
            SET @message = N'ERROR[SS0]: error(s) occured while checking synonym';
            THROW;
        END CATCH; 

        -- Check Source Table
        IF NOT EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10)
        BEGIN
            BEGIN TRY
                SELECT @sourceTable4Parts = base_object_name FROM sys.synonyms WHERE [name] = @synonymName AND schema_id = SCHEMA_ID(@synonymSchema) AND (@sourceTable4Parts IS NULL OR @sourceTable4Parts = N'');

                SELECT @stmtSourceTableChecks = N'
                DROP TABLE IF EXISTS #tempASTable;
                BEGIN TRY
                    SELECT TOP(0) * INTO #tempSourceTableCheck FROM ' + @sourceTable4Parts + N';

                    SELECT @Columns = (
                        SELECT col.column_id, [column] = col.name, [type] = tpe.name, col.max_length, col.precision, col.scale
                            , [datatype] =   tpe.[name] + 
                                CASE WHEN tpe.[name] IN (N''varchar'', N''char'', N''varbinary'', N''binary'', N''text'') THEN ''('' + CASE WHEN col.max_length = -1 THEN ''MAX'' ELSE CAST(col.max_length AS VARCHAR(5)) END + '')''
                                WHEN tpe.[name] IN (N''nvarchar'', N''nchar'', N''ntext'') THEN ''('' + CASE WHEN col.max_length = -1 THEN ''MAX'' ELSE CAST(col.max_length / 2 AS VARCHAR(5)) END + '')''
                                WHEN tpe.[name] IN (N''datetime2'', N''time2'', N''datetimeoffset'') THEN ''('' + CAST(col.scale AS VARCHAR(5)) + '')''
                                WHEN tpe.[name] IN (N''decimal'', N''numeric'') THEN ''('' + CAST(col.[precision] AS VARCHAR(5)) + '','' + CAST(col.scale AS VARCHAR(5)) + '')''
                                WHEN tpe.[name] IN (N''float'') THEN ''('' + CAST(col.[precision] AS VARCHAR(5)) + '')''
                                ELSE '''' END
                        FROM tempdb.sys.columns AS col
                        INNER JOIN tempdb.sys.types AS tpe ON col.system_type_id = tpe.system_type_id AND tpe.system_type_id = tpe.user_type_id
                        WHERE [object_id] = OBJECT_ID(N''tempdb.dbo.#tempSourceTableCheck'') AND tpe.[name] <> N''timestamp''
                        FOR JSON PATH
                    );
                END TRY
                BEGIN CATCH
                    THROW;
                END CATCH
                ';
                -- retrieve Source columns
                EXEC sp_executesql @stmt = @stmtSourceTableChecks, @params = @paramsSourceTableChecks, @Message = NULL, @Columns = @sourceJsonColumns OUTPUT;
                -- Set default columns if not provided
                SELECT @expectedSourceColumns = ISNULL(LTRIM(RTRIM(@sourceExpectedColumns)), N'[{"column":"Id","type":"bigint"}]' );

                -- check columns
                WITH exp([name], [datatype], [type], [max_length]) AS (
                    SELECT [name], [datatype], [type], [max_length]/*, [precision], [scale]*/ FROM OPENJSON(@expectedSourceColumns, N'$')
                    WITH ([name] nvarchar(128) N'$.column', [datatype] nvarchar(128), [type] nvarchar(128), max_length smallint/*, precision tinyint, scale tinyint*/)                
                ), col([name], [datatype], [type], [max_length]) AS(
                    SELECT [name], [datatype], [type], [max_length] FROM OPENJSON(@sourceJsonColumns, N'$')
                    WITH ([name] nvarchar(128) N'$.column', [datatype] nvarchar(128), [type] nvarchar(128), max_length smallint)
                )
                INSERT INTO @json_errors([id], [severity], [message])
                SELECT 20, 16, N'ERROR[TS1]: No column retrieved from  Source table ' + @sourceTable4Parts + N' refered by synonym ' + QUOTENAME(@synonymSchema) + N'.' + QUOTENAME(@synonymName) WHERE NOT EXISTS(SELECT 1 FROM col)
                UNION ALL SELECT 20, 16, N'ERROR[TS2]: Expected column ' + QUOTENAME(x.[name]) + N' not found in  Source table ' + @sourceTable4Parts + N' refered by synonym ' + QUOTENAME(@synonymSchema) + N'.' + QUOTENAME(@synonymName) FROM exp x WHERE NOT EXISTS(SELECT 1 FROM col WHERE [name] = x.[name])
                UNION ALL SELECT 20, 16, N'ERROR[TS3]: Invalid type '+ QUOTENAME(c.[type]) + N' for column ' + QUOTENAME(x.[name]) + N' in  Source table ' + @sourceTable4Parts + N' refered by synonym ' + QUOTENAME(@synonymSchema) + N'.' + QUOTENAME(@synonymName) + N' (' + QUOTENAME(x.[type]) + N' expected)' FROM exp x INNER JOIN col c ON x.[name] = c.[name] AND x.[type] <> c.[type]
                ;
                SET @Columns = @sourceJsonColumns;
            END TRY
            BEGIN CATCH
                SET @message = N'ERROR[TS0]: error(s) occured while checking Source table';
                THROW;
            END CATCH
        END    

        ----------------------------------------------------------------------------------------------------      
        -- Create or Update Synonym(s) / Table / Columns
        ----------------------------------------------------------------------------------------------------
        IF NOT EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10)
        BEGIN
            -- Start Transaction for all upcoming schema changes
            BEGIN TRAN
            -- Create synonym if missing or outdated
            BEGIN TRY
                IF NOT EXISTS (SELECT 1 FROM sys.synonyms WHERE [name] = @synonymName AND schema_id = SCHEMA_ID(@synonymSchema) AND base_object_name = @sourceTable4Parts) AND @sourceTable4Parts <> N'' AND @CreateOrUpdateSynonym = 1
                BEGIN
                    SET @sql = NULL;
                    INSERT INTO @json_errors([id], [severity], [message]) SELECT 0, 10, N'Create or alter Synonym '+ QUOTENAME(@synonymSchema) + N'.' + QUOTENAME(@synonymName) + ' with base object ' + @sourceTable4Parts;
                    SELECT @sql = N'DROP SYNONYM ' + QUOTENAME(@synonymSchema) + N'.' + QUOTENAME(@synonymName) + N';' FROM sys.synonyms WHERE [name] = @synonymName AND schema_id = SCHEMA_ID(@synonymSchema);
                    SELECT @sql = ISNULL(@sql, N'') + N'CREATE SYNONYM ' + QUOTENAME(@synonymSchema) + N'.' + QUOTENAME(@synonymName) + N' FOR ' + @sourceTable4Parts + N';';
                    EXEC sp_executesql @statement = @sql;
                END
            END TRY
            BEGIN CATCH
                SET @message = N'ERROR[SU0]: error(s) occured while creating or updating source synomym';
                THROW;
            END CATCH
            SET @message = NULL;
            IF @@TRANCOUNT > 0 COMMIT
        END
    END TRY
    BEGIN CATCH
        SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
        IF @@TRANCOUNT > 0 ROLLBACK;
    END CATCH

--    SELECT TOP(100) 'message'= 'output', [Message] = LEFT([message], 4000), [Severity] = [severity], [State] = 1 FROM @json_errors ORDER BY [id] ASC
    -- Check / Set @IsValid flag
    SET @IsValid = IIF(NOT EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10) AND @ERROR_NUMBER IS NULL AND @message IS NULL, 1, 0);
    INSERT INTO @json_errors([id], [severity], [message]) SELECT 100, 10, N'Source Synonym is valid: '+ QUOTENAME(@synonymSchema) + N'.' + QUOTENAME(@synonymName) + ' => ' + @sourceTable4Parts WHERE @IsValid = 1;

    SET @Messages = --ISNULL(
    ( 
        SELECT [Procedure] = QUOTENAME(COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?')) + N'.' + QUOTENAME(COALESCE(OBJECT_NAME(@@PROCID), N'?')), [Message], [Severity], [State] 
        FROM (
            SELECT [Message], [Severity], [State] FROM (
                SELECT TOP(100) [Message] = LEFT([message], 4000), [Severity] = [severity], [State] = 1 FROM @json_errors ORDER BY [id] ASC
            ) err
            UNION ALL SELECT N'ERROR: ' + @ERROR_MESSAGE, 16, 1 WHERE @ERROR_MESSAGE IS NOT NULL
            UNION ALL SELECT @message, 16, 1 WHERE @message IS NOT NULL
        ) jsn
        FOR JSON PATH)
    --    , N'[]')
    ;
    IF @ShowMessages = 1 OR (@IsValid = 0 AND @ThrowError = 1) SELECT [Message] = LEFT([Message], 1000), [Severity], [Error] = IIF([Severity] > 10, 1, 0) FROM OPENJSON(@Messages) WITH([Message] nvarchar(MAX), [Severity] int)
    IF @IsValid = 0 AND @ThrowError = 1 THROW 50000, 'Error(s) occured. See output dataset', 1;

    RETURN 0;
END
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- DROP PROCEDURE [Maintenance].[SetArchiveDBSourceTables]
----------------------------------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[SetArchiveDBSourceTables]') AND type in (N'P'))
BEGIN
    EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [Maintenance].[SetArchiveDBSourceTables] AS'
    PRINT '  + CREATE PROCEDURE: [Maintenance].[SetArchiveDBSourceTables]';
END
ELSE PRINT '  = PROCEDURE [Maintenance].[SetArchiveDBSourceTables] already exists' 
GO

PRINT '  ~ UPDATE PROCEDURE: [Maintenance].[SetArchiveDBSourceTables]'
GO

ALTER PROCEDURE [Maintenance].[SetArchiveDBSourceTables]
----------------------------------------------------------------------------------------------------
-- ### [Object]: PROCEDURE [Maintenance].[SetArchiveDBSourceTables]
-- ### [Version]: 2023-10-17T10:53:15+02:00
-- ### [Source]: _src/Archive/OrchestratorDB/Procedure_OrchestratorDB.Maintenance.SetArchiveDBSourceTables.sql
-- ### [Hash]: 5ee9393 [SHA256-0B3A14B238BC49D71524FAC536D21760F551B80CB0270A9ACED7F0061B2AC3BB]
-- ### [Docs]: https://???.???
-- !!! ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!!!
-- !!! ~~~~~~~~~ NOT OFFICIALLY SUPPORTED BY UIPATH 
-- !!! ~~~~~~~~~ SQL Server >= 2016 SP1
----------------------------------------------------------------------------------------------------
    @DataSource nvarchar(128)
    , @IsExternal bit = 0
    , @ShowMessages bit = 1
    , @ThrowError bit = 1
    , @IsValid bit = 0 OUTPUT
    , @Messages nvarchar(MAX) = NULL OUTPUT
AS
BEGIN
    BEGIN TRY 
        SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
		SET LOCK_TIMEOUT 5000;
        SET ARITHABORT ON;
        SET NOCOUNT ON;
        SET NUMERIC_ROUNDABORT OFF;

        ----------------------------------------------------------------------------------------------------
        -- 
        ----------------------------------------------------------------------------------------------------
        DECLARE @externalListName nvarchar(128) = N'ArchivingListASyncOrchestratorDBTables';
        DECLARE @externalListSchema nvarchar(128) = N'Maintenance';
        DECLARE @listTableColuns nvarchar(MAX);
        ----------------------------------------------------------------------------------------------------
        -- Local 
        ----------------------------------------------------------------------------------------------------
        DECLARE @outputIsValid bit;
        DECLARE @outputMessages nvarchar(MAX);
        ----------------------------------------------------------------------------------------------------
        -- Misc      
        ----------------------------------------------------------------------------------------------------
        DECLARE @sql nvarchar(max);
        ----------------------------------------------------------------------------------------------------
        -- Cursor
        ----------------------------------------------------------------------------------------------------
        DECLARE @cursorGroup nvarchar(128);
        DECLARE @cursorSchema nvarchar(128);
        DECLARE @cursorTable nvarchar(128);
        DECLARE @cursorExists bit;
        DECLARE @cursorIsValid bit;
        DECLARE @cursorColumns nvarchar(MAX);
        DECLARE @synonymName nvarchar(128);
        DECLARE @sourceTableFullParts nvarchar(128);
        ----------------------------------------------------------------------------------------------------      
        -- Message / Error Handling
        ----------------------------------------------------------------------------------------------------
        DECLARE @message nvarchar(MAX);
        DECLARE @ERROR_NUMBER INT, @ERROR_SEVERITY INT, @ERROR_STATE INT, @ERROR_PROCEDURE NVARCHAR(126), @ERROR_LINE INT, @ERROR_MESSAGE NVARCHAR(2048);
        DECLARE @space tinyint = 2;
        DECLARE @tab tinyint = 0;
        DECLARE @json_errors TABLE([id] tinyint IDENTITY(0, 1), [procedure] nvarchar(128), [severity] int NOT NULL, [message] nvarchar(MAX) NOT NULL);

        ----------------------------------------------------------------------------------------------------      
        -- Checks Synonyms and Tables
        ----------------------------------------------------------------------------------------------------
        SELECT @listTableColuns = N'[{"column":"group","datatype":"nvarchar(128)"},{"column":"schema","datatype":"nvarchar(128)"},{"column":"table","datatype":"nvarchar(128)"},{"column":"isarchived","datatype":"bit"},{"column":"exists","datatype":"bit"},{"column":"isvalid","datatype":"bit"},{"column":"columns","datatype":"nvarchar(MAX)"}]';
        -- Add (IF External) Table ArchivingListArchiveDBTables
        IF @IsExternal = 1
        BEGIN
            BEGIN TRY
                EXEC [Maintenance].[CreateArchivingExternalTable] @ExternalDataSource = @DataSource, @ExternalName = @externalListName, @ExternalSchema = @externalListSchema, @Columns = @listTableColuns
                    , @ShowMessages = 0, @ThrowError = 0, @Messages = @outputMessages OUTPUT , @IsValid = @outputIsValid OUTPUT;
            END TRY
            BEGIN CATCH
                SET @message = N'ERROR[TS0]: error(s) occured while checking ' + QUOTENAME(@externalListName) + N' table';
                THROW;
            END CATCH
            INSERT INTO @json_errors([procedure], [severity], [message]) SELECT N'[Maintenance].[CreateArchivingExternalTable]', [severity], [message] FROM OPENJSON(@outputMessages) WITH([Message] nvarchar(MAX), [Severity] int)
            SELECT @sourceTableFullParts = QUOTENAME(@externalListSchema) + N'.' + QUOTENAME(@externalListName);
        END
        ELSE SELECT @sourceTableFullParts = ISNULL(@DataSource + N'.', '') + QUOTENAME(@externalListSchema) + N'.' + QUOTENAME(@externalListName);;
        
        IF NOT EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10)
        BEGIN
            BEGIN TRY
                SELECT @SynonymName = N'Synonym_' + @externalListName;
                EXEC [Maintenance].[SetSourceTable]
                    @SynonymName = @SynonymName, @SynonymSchema = N'Maintenance'
                    , @SourceTableFullParts = @sourceTableFullParts
                    , @SourceExpectedColumns = @listTableColuns
                    , @CreateOrUpdateSynonym = 1    
                    , @ShowMessages = 0, @IsValid = @outputIsValid OUTPUT, @Messages = @outputMessages OUTPUT
                ;
            END TRY
            BEGIN CATCH
                SET @message = N'ERROR[TS0]: error(s) occured while checking ' + QUOTENAME(@externalListName) + N' Synonym';
                THROW;
            END CATCH
        END
        INSERT INTO @json_errors([procedure], [severity], [message]) SELECT N'[Maintenance].[SetSourceTable]', [severity], [message] FROM OPENJSON(@outputMessages) WITH([Message] nvarchar(MAX), [Severity] int)

        IF NOT EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10) AND @outputIsValid = 1
        BEGIN
            BEGIN TRY

                DECLARE CursorTables CURSOR FAST_FORWARD LOCAL FOR 
                    SELECT lst.[group], [schema], [table], [exists], [isvalid], [columns] FROM [Maintenance].[ArchivingListASyncOrchestratorDBTables] lst
                    INNER JOIN (VALUES(N'AuditLogs', 1), (N'Jobs', 1), (N'Logs', 1), (N'Queues', 1), (N'RobotLicenseLogs', 1)) AS v([group], [include]) ON v.[group] = lst.[group]
                    WHERE v.[include] >= 1;

                OPEN CursorTables;
                FETCH CursorTables INTO @cursorGroup, @cursorSchema, @cursorTable, @cursorExists, @cursorIsValid, @cursorColumns;

                IF CURSOR_STATUS('local', 'CursorTables') = 1
                BEGIN
                    WHILE @@FETCH_STATUS = 0
                    BEGIN;
                        IF @cursorIsValid = 0 
                        BEGIN
                            INSERT INTO @json_errors([severity], [message]) SELECT 10, N'Missing or invalid source table: Group = ' + ISNULL(@cursorGroup, '-') + N', Table = ' + @cursorSchema + N'.' + @cursorTable + N' ';
                            CONTINUE;
                        END
                        --INSERT INTO @json_errors([severity], [message]) SELECT 10, N'Add table: Group = ' + ISNULL(@cursorGroup, '-') + N', Table = ' + @cursorSchema + N'.' + @cursorTable + N' ';

                        BEGIN TRY
                            EXEC [Maintenance].[CreateArchivingExternalTable] @ExternalDataSource = @DataSource, @ExternalName = @cursorTable, @ExternalSchema = @cursorSchema, @Columns = @cursorColumns
                                , @ShowMessages = 0, @ThrowError = 0, @Messages = @outputMessages OUTPUT , @IsValid = @outputIsValid OUTPUT;
                        END TRY
                        BEGIN CATCH
                            INSERT INTO @json_errors([severity], [message]) SELECT 16, ERROR_MESSAGE()
                            INSERT INTO @json_errors([severity], [message]) SELECT 16, N'ERROR[XT]: error(s) occured while checking external table' + QUOTENAME(@cursorSchema) + N'.' + QUOTENAME(@cursorTable);
                            --THROW;
                        END CATCH
                        INSERT INTO @json_errors([procedure], [severity], [message]) SELECT N'[Maintenance].[CreateArchivingExternalTable]', [severity], [message] FROM OPENJSON(@outputMessages) WITH([Message] nvarchar(MAX), [Severity] int)
                        
                        IF NOT EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10)
                        BEGIN
                            BEGIN TRY
                                SELECT @synonymName = N'Synonym_ASync' + @cursorTable, @sourceTableFullParts =  QUOTENAME(@cursorSchema) + N'.' + QUOTENAME(@cursorTable)

                                EXEC [Maintenance].[SetSourceTable]
                                    @SynonymName = @synonymName
                                    , @SynonymSchema = N'Maintenance'
                                    , @SourceTableFullParts =  @sourceTableFullParts
                                    , @SourceExpectedColumns = @cursorColumns
                                    , @CreateOrUpdateSynonym = 1
                                    , @ShowMessages = 0, @IsValid = @outputIsValid OUTPUT, @Messages = @outputMessages OUTPUT
                                ;
                            END TRY
                            BEGIN CATCH
                                INSERT INTO @json_errors([severity], [message]) SELECT 16, ERROR_MESSAGE()
                                INSERT INTO @json_errors([severity], [message]) SELECT 16, N'ERROR[SY0]: error(s) occured while checking synonym for table: ' + QUOTENAME(@cursorSchema) + N'.' + QUOTENAME(@cursorTable);
                --                THROW;
                            END CATCH
                        END
                        INSERT INTO @json_errors([procedure], [severity], [message]) SELECT N'[Maintenance].[CreateArchivingExternalTable]', [severity], [message] FROM OPENJSON(@outputMessages) WITH([Message] nvarchar(MAX), [Severity] int)

                        FETCH CursorTables INTO @cursorGroup, @cursorSchema, @cursorTable, @cursorExists, @cursorIsValid, @cursorColumns;
                    END
                END
                ELSE 
                BEGIN
                    SET @message = 'Execution has been canceled: Error Opening Table list Cursor';

                    INSERT INTO @json_errors([severity], [message]) VALUES (16, @message);
                    RAISERROR(@message, 16, 1);
                END 
                IF CURSOR_STATUS('local', 'CursorTables') >= 0 CLOSE CursorTables;
                IF CURSOR_STATUS('local', 'CursorTables') >= -1 DEALLOCATE CursorTables;
            END TRY
            BEGIN CATCH
                SET @message = N'ERROR[XT0]: error(s) occured while processing tables from ' + QUOTENAME(@externalListName);
                THROW;
            END CATCH
        END
    END TRY
    BEGIN CATCH
        SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
        IF @@TRANCOUNT > 0 ROLLBACK;
    END CATCH

    -- Check / Set @IsValid flag
    SET @IsValid = IIF(NOT EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10) AND @ERROR_NUMBER IS NULL AND @message IS NULL, 1, 0);

    SET @Messages = --ISNULL(
    ( 
        SELECT [Procedure] = QUOTENAME(COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?')) + N'.' + QUOTENAME(COALESCE(OBJECT_NAME(@@PROCID), N'?')), [Message], [Severity], [State] 
        FROM (
            SELECT [Message], [Severity], [State] FROM (
                SELECT TOP(100) [Message] = LEFT([message], 4000), [Severity] = [severity], [State] = 1 FROM @json_errors ORDER BY [id] ASC
            ) err
            UNION ALL SELECT N'ERROR: ' + @ERROR_MESSAGE, 16, 1 WHERE @ERROR_MESSAGE IS NOT NULL
            UNION ALL SELECT @message, 16, 1 WHERE @message IS NOT NULL
        ) jsn
        FOR JSON PATH)
    --    , N'[]')
    ;
    IF @ShowMessages = 1 OR (@IsValid = 0 AND @ThrowError = 1) SELECT [Message] = LEFT([Message], 1000), [Severity], [Error] = IIF([Severity] > 10, 1, 0) FROM OPENJSON(@Messages) WITH([Message] nvarchar(MAX), [Severity] int)
    IF @IsValid = 0 AND @ThrowError = 1 THROW 50000, 'Error(s) occured. See output dataset', 1;
    RETURN 0;
END
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- DROP PROCEDURE [Maintenance].[ASyncCleanupLogs]
----------------------------------------------------------------------------------------------------

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[ASyncCleanupLogs]') AND type in (N'P'))
BEGIN
    EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [Maintenance].[ASyncCleanupLogs] AS'
    PRINT '  + CREATE PROCEDURE: [Maintenance].[ASyncCleanupLogs]';
END
ELSE PRINT '  = PROCEDURE [Maintenance].[ASyncCleanupLogs] already exists' 
GO

PRINT '  ~ UPDATE PROCEDURE: [Maintenance].[ASyncCleanupLogs]'
GO  

ALTER PROCEDURE [Maintenance].[ASyncCleanupLogs]
----------------------------------------------------------------------------------------------------
-- ### [Object]: PROCEDURE [Maintenance].[ASyncCleanupLogs]
-- ### [Version]: 2023-09-07T18:14:12+02:00
-- ### [Source]: _src/Archive/OrchestratorDB/Logs/Procedure_OrchestratorDB.Maintenance.ASyncCleanupLogs.sql
-- ### [Hash]: 914e4af [SHA256-A4A8749B99622BD610B69227364C43A3E0952C8B275E46CE89A50736F829F903]
-- ### [Docs]: https://???.???
-- !!! ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!!!
-- !!! ~~~~~~~~~ NOT OFFICIALLY SUPPORTED BY UIPATH 
-- !!! ~~~~~~~~~ SQL Server >= 2016 SP1
----------------------------------------------------------------------------------------------------
    /* Row count settings */
    @RowsDeletedForEachLoop int = 10000 -- Don't go above 50.000 (min = 1000, Max = 100.000)
    /* Loop Limits */
    , @MaxRunMinutes int = NULL -- NULL or 0 = unlimited
    /* Dry Run */
--    , @DryRunOnly nvarchar(MAX) = NULL -- Y{es} or N{o} => Only Check Parameters (default if NULL = Y)
    /* Error Handling */
    , @OnErrorRetry tinyint = NULL -- between 0 and 20 => retry up to 20 times (default if NULL = 10)
    , @OnErrorWaitMillisecond smallint = 1000 -- wait for milliseconds between each Retry (default if NULL = 1000ms)
    /* Messge Logging */
    , @SaveMessagesToTable nvarchar(MAX) = 'Y' -- Y{es} or N{o} => Save to [maintenance].[messages] table (default if NULL = Y)
    , @SavedMessagesRetentionDays smallint = 30
    , @SavedToRunId int = NULL OUTPUT
    , @OutputMessagesToDataset nvarchar(MAX) = 'N' -- Y{es} or N{o} => Output Messages Result set
    , @Verbose nvarchar(MAX) = NULL -- Y{es} = Print all messages < 10
AS
BEGIN
    BEGIN TRY 
        SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
		SET LOCK_TIMEOUT 5000;
        SET ARITHABORT ON;
        SET NOCOUNT ON;
        SET NUMERIC_ROUNDABORT OFF;

        ----------------------------------------------------------------------------------------------------
        -- Local Run Variables
        ----------------------------------------------------------------------------------------------------
        DECLARE @runId int;  
        DECLARE @dryRun bit;
        DECLARE @isTimeOut bit = 0;

        DECLARE @startTime datetime = SYSDATETIME();
        DECLARE @startTimeFloat float(53);

		DECLARE @loopStart datetime;
        DECLARE @maxRunDateTime datetime;
        DECLARE @logToTable bit;
        DECLARE @MaxErrorRetry tinyint;
        DECLARE @errorDelay smallint;
        DECLARE @errorWait datetime;
        DECLARE @returnValue int = 1;
        ----------------------------------------------------------------------------------------------------
        -- Sync Cursor
        ----------------------------------------------------------------------------------------------------
--        DROP TABLE IF EXISTS #tempListSync;
        DECLARE @tempListSync TABLE([Id] [bigint] PRIMARY KEY NOT NULL, [DeleteAfterDatetime] [datetime] NOT NULL, [FirstASyncId] [bigint] NULL, [LastAsyncId] [bigint] NULL, [CountASyncIds] [bigint] NULL, [IsDeleted] bit NULL, [IsSynced] bit NULL);
        DECLARE @cursorSyncId bigint;
        DECLARE @cursorDeleteAfterDatetime datetime;
        DECLARE @cursorFirstASyncId bigint;
        DECLARE @cursorLastAsyncId bigint;
        DECLARE @cursorCountASyncIds bigint;
        DECLARE @cursorIsDeleted bit;
        DECLARE @cursorIsSynced bit;

        ----------------------------------------------------------------------------------------------------
        -- Delete Loop
        ----------------------------------------------------------------------------------------------------
        DECLARE @maxLoopDeleteRows int;
        DECLARE @firstId bigint;
        DECLARE @lastId bigint;
        ----------------------------------------------------------------------------------------------------
        -- Count row Ids
        ----------------------------------------------------------------------------------------------------
        DECLARE @countIds bigint;
        DECLARE @totalIds bigint;
        DECLARE @globalIds bigint;
        ----------------------------------------------------------------------------------------------------
        -- Constant / Default value
        ----------------------------------------------------------------------------------------------------
        DECLARE @maxDeleteRows int = 100*1000; -- Raise an error if @RowsDeletedForEachLoop is bigger than this value
        DECLARE @minDeleteRows int = 1*1000; -- Raise an error if @RowsDeletedForEachLoop is smaller than this value
        DECLARE @defaultDeleteRows int = 10*1000;
        DECLARE @verboseBelowLevel int = 10; -- don't print message with Severity < 10 unless Verbose is set to Y

        DECLARE @paramsYesNo TABLE ([id] tinyint IDENTITY(0, 1) PRIMARY KEY CLUSTERED, [parameter] nvarchar(MAX), [value] int)
        DECLARE @floatingDay float(53) = 1;
        DECLARE @floatingHour float(53) = @floatingDay / 24;
        DECLARE @constDefaultDeleteDelay int = 0;
        DECLARE @constDefaultAfterHours int = NULL;
        ----------------------------------------------------------------------------------------------------
        -- Server Info 
        ----------------------------------------------------------------------------------------------------
        DECLARE @productVersion nvarchar(MAX) = CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(MAX));
        DECLARE @engineEdition int =  CAST(ISNULL(SERVERPROPERTY('EngineEdition'), 0) AS int);
        DECLARE @minProductVersion nvarchar(MAX) = N'13.0.4001.0 (SQL Server 2016 SP1)'
        DECLARE @version numeric(18, 10);
        DECLARE @minVersion numeric(18, 10) = 13.0040010000;
        DECLARE @minCompatibilityLevel int = 130;
        DECLARE @hostPlatform nvarchar(256); 
        ----------------------------------------------------------------------------------------------------
        -- Proc Info
        ----------------------------------------------------------------------------------------------------
        DECLARE @paramsGetProcInfo nvarchar(MAX) = N'@procid int, @info nvarchar(MAX), @output nvarchar(MAX) OUTPUT'
        DECLARE @stmtGetProcInfo nvarchar(MAX) = N'
            DECLARE @definition nvarchar(MAX) = OBJECT_DEFINITION(@procid), @keyword nvarchar(MAX) = REPLICATE(''-'', 2) + SPACE(1) + REPLICATE(''#'', 3) + SPACE(1) + QUOTENAME(LTRIM(RTRIM(@info))) + '':'';
			DECLARE @eol char(1) = IIF(CHARINDEX( CHAR(13) , @definition) > 0, CHAR(13), CHAR(10));
			SET @output = ''''+ LTRIM(RTRIM( SUBSTRING(@definition, NULLIF(CHARINDEX(@keyword, @definition), 0 ) + LEN(@keyword), CHARINDEX( @eol , @definition, CHARINDEX(@keyword, @definition) + LEN(@keyword) + 1) - CHARINDEX(@keyword, @definition) - LEN(@keyword) ))) + '''';
        ';
        DECLARE @procSchemaName nvarchar(MAX) = COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?');
        DECLARE @procObjecttName nvarchar(MAX) = COALESCE(OBJECT_NAME(@@PROCID), N'?');
        DECLARE @procName nvarchar(MAX) = QUOTENAME(COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?')) + N'.' + QUOTENAME(COALESCE(OBJECT_NAME(@@PROCID), N'?')); 
        DECLARE @versionDatetime nvarchar(MAX);
        ----------------------------------------------------------------------------------------------------
        -- Synonym
        ----------------------------------------------------------------------------------------------------
        DECLARE @synonymMessages nvarchar(MAX);
        DECLARE @synonymIsValid bit;
        ----------------------------------------------------------------------------------------------------      
        -- Message / Error Handling
        ----------------------------------------------------------------------------------------------------
        /* Format */
        DECLARE @lineSeparator nvarchar(MAX) = N'----------------------------------------------------------------------------------------------------';
        DECLARE @lineBreak nvarchar(MAX) = N'';
        DECLARE @message nvarchar(MAX);
        DECLARE @string nvarchar(MAX);
        DECLARE @tab tinyint = 2;
        /* Log Stack */
        DECLARE @stmtEmptyMessagesStack nvarchar(MAX) = N'
            SELECT 
                [Date] = m.value(''Date[1]'', ''datetime'')
                , [Procedure] = m.value(''Procedure[1]'', ''nvarchar(MAX)'')
                , [Message] = m.value(''Message[1]'', ''nvarchar(MAX)'')
                , [Severity] = m.value(''Severity[1]'', ''int'')
                , [State] = m.value(''State[1]'', ''int'')
                , [Number] = m.value(''Number[1]'', ''int'')
                , [Line] = m.value(''Line[1]'', ''int'')
            FROM @MessagesStack.nodes(''/messages/message'') x(m)
            SET @MessagesStack = NULL;
        ';
        DECLARE @paramsEmptyMessagesStack nvarchar(MAX) = N'@MessagesStack xml = NULL OUTPUT';
        DECLARE @MessagesStack xml;
        /* Errors */
        DECLARE @ERROR_NUMBER INT, @ERROR_SEVERITY INT, @ERROR_STATE INT, @ERROR_PROCEDURE NVARCHAR(126), @ERROR_LINE INT, @ERROR_MESSAGE NVARCHAR(2048) ;
        DECLARE @errorCount int = 0;
        DECLARE @countErrorRetry tinyint;
        /* Messages */
        DECLARE @messages TABLE(id int IDENTITY(0, 1) PRIMARY KEY, [date] datetime2 DEFAULT SYSDATETIME(), [procedure] nvarchar(MAX) NOT NULL DEFAULT QUOTENAME(COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?')) + N'.' + QUOTENAME(COALESCE(OBJECT_NAME(@@PROCID), N'?')), [message] nvarchar(MAX) NOT NULL, severity tinyint NOT NULL, state tinyint NOT NULL, [number] int, [line] int);
        DECLARE @levelVerbose int;    
        DECLARE @outputDataset bit;
        /* Cursor */ 
        DECLARE CursorMessages CURSOR FAST_FORWARD LOCAL FOR SELECT [Date], [Procedure], [Message], [Severity], [State], [Number], [Line] FROM @messages ORDER BY [ID] ASC;
        DECLARE @cursorDate datetime;
        DECLARE @cursorProcedure nvarchar(MAX);
        DECLARE @cursorMessage nvarchar(MAX);
        DECLARE @cursorSeverity tinyint;
        DECLARE @cursorState tinyint;
        DECLARE @cursorNumber int;
        DECLARE @cursorLine int;    

        ----------------------------------------------------------------------------------------------------
        -- START
        ----------------------------------------------------------------------------------------------------
        SELECT @startTime = SYSDATETIME();
        SELECT @startTimeFloat = CAST(@startTime AS float(53));

        ----------------------------------------------------------------------------------------------------
        -- Gather General & Server Info
        ----------------------------------------------------------------------------------------------------

        -- Get Run Time limit or NULL (unlimited)
	    SET @maxRunDateTime = CASE WHEN ABS(@MaxRunMinutes) >= 1 THEN DATEADD(MINUTE, ABS(@MaxRunMinutes), @startTime) ELSE NULL END;

        -- Output Start & Stop info
        INSERT INTO @messages([Message], Severity, [State]) VALUES 
            ( @lineSeparator, 10, 1)
            , (N'Start Time = ' + CONVERT(nvarchar(MAX), @startTime, 121), 10, 1 )
            , (N'MAX Run Time = ' + ISNULL(CONVERT(nvarchar(MAX), @maxRunDateTime, 121), N'NULL (=> unlimited)'), 10, 1 )
            , (@lineBreak, 10, 1);
/*        -- Get Host info
        IF @version >= 14 SELECT @hostPlatform = host_platform FROM sys.dm_os_host_info ELSE SET @HostPlatform = 'Windows'
*/
        ----------------------------------------------------------------------------------------------------
        -- Check SQL Server Requierements
        ----------------------------------------------------------------------------------------------------
        -- Get Proc Version
--        EXEC sp_executesql @stmt = @stmtGetProcInfo, @params = @paramsGetProcInfo, @procid = @@PROCID, @info = N'Version', @output = @versionDatetime OUTPUT;

        -- Get SQL Server Server Version
        WITH release (id, position, version) AS
        (
            SELECT id = 0, position = CHARINDEX(N'.', @productVersion, 1)
                , version = LEFT(@productVersion, CHARINDEX(N'.', @productVersion, 1))
            UNION ALL
            SELECT id = r.id + 1, position = CHARINDEX(N'.', @productVersion, position + 1)
                , version = r.version + RIGHT(v.[space] + SUBSTRING(@productVersion, position + 1, COALESCE( NULLIF(CHARINDEX('.', @productVersion, position + 1), 0), LEN(@productVersion) + 1) - position - 1), LEN(v.[space]) )
            FROM release r INNER JOIN (VALUES(0, '00'), (1, '00'), (2, '0000'), (3, '0000') ) AS v(id, space) ON v.id = r.id + 1
            WHERE position < LEN(@productVersion) + 1
        )
        SELECT TOP(1) @version = CAST(version AS numeric(18, 10))
        FROM release
        ORDER BY Id DESC

        -- Get Host info
        IF @version >= 14 SELECT @hostPlatform = host_platform FROM sys.dm_os_host_info ELSE SET @HostPlatform = 'Windows'

        INSERT INTO @messages([Message], Severity, [State])
        -- Check min required version
        SELECT N'ERROR: Current SQL Server version is ' + @productVersion + N'. Only version ' + @minProductVersion + + N' or higher is supported.', 16, 1 WHERE @version < @minVersion
        -- Check Database Compatibility Level
        UNION ALL SELECT 'ERROR: Database ' + QUOTENAME(DB_NAME(DB_ID())) + ' Compatibility Level is set to '+ CAST([compatibility_level] AS nvarchar(MAX)) + '. Compatibility level 130 or higher is requiered.', 16, 1 FROM sys.databases WHERE database_id = DB_ID() AND [compatibility_level] < @minCompatibilityLevel
        -- Check opened transation(s)
        --UNION ALL SELECT 'The transaction count is not 0.', 16, 1 WHERE @@TRANCOUNT <> 0
        -- Check uses_ansi_nulls
        UNION ALL SELECT 'ERROR: ANSI_NULLS must be set to ON for this Stored Procedure', 16, 1 FROM sys.sql_modules WHERE [object_id] = @@PROCID AND uses_ansi_nulls <> 1
        -- Check uses_quoted_identifier
        UNION ALL SELECT 'ERROR: QUOTED_IDENTIFIER must be set to ON for this Stored Procedure', 16, 1 FROM sys.sql_modules WHERE [object_id] = @@PROCID AND uses_quoted_identifier <> 1;

        ----------------------------------------------------------------------------------------------------
        -- Parameters
        ----------------------------------------------------------------------------------------------------
        SET @levelVerbose = @VerboseBelowLevel;

        -- Convert Yes / No varations to bit
        INSERT INTO @paramsYesNo([parameter], [value]) VALUES(N'NO', 0), (N'N', 0), (N'0', 0), (N'YES', 1), (N'Y', 1), (N'1', 1);

        ----------------------------------------------------------------------------------------------------
        -- Parameters' Validation
        ----------------------------------------------------------------------------------------------------
       INSERT INTO @messages([Message], Severity, [State])
        VALUES 
            ( @lineSeparator, 10, 1)
            , ( N'Validation', 10, 1)
            , ( @lineSeparator, 10, 1)

        -- Check @RowsDeletedForEachLoop
        SET @maxLoopDeleteRows = ISNULL(NULLIF(@RowsDeletedForEachLoop, 0), @defaultDeleteRows);
        IF @maxLoopDeleteRows < @minDeleteRows OR @maxLoopDeleteRows > @MaxDeleteRows 
        BEGIN
            INSERT INTO @messages ([Message], Severity, [State]) VALUES
                ( 'Parameter @RowsDeletedForEachLoop is invalid: ' + LTRIM(RTRIM(CAST(@RowsDeletedForEachLoop AS nvarchar(MAX)))), 10, 1)
                , ('USAGE: use a value between ' + CAST(@minDeleteRows AS nvarchar(MAX)) + N' and ' + CAST(@maxDeleteRows AS nvarchar(MAX)) +  N'', 10, 1)
                , ('Parameter @RowsDeletedForEachLoop is invalid', 16, 1);
        END

		----------------------------------------------------------------------------------------------------
        -- Check Output Settings
        ----------------------------------------------------------------------------------------------------
        -- Check @OutputMessagesToDataset parameter
        SELECT @outputDataset = [value] FROM @paramsYesNo WHERE [parameter] = ISNULL(LTRIM(RTRIM(@OutputMessagesToDataset)), N'N');
        IF @outputDataset IS NULL 
        BEGIN
            INSERT INTO @messages ([Message], Severity, [State]) VALUES
                ( 'Parameter @OutputMessagesToDataset is invalid: ' + LTRIM(RTRIM(@OutputMessagesToDataset)), 10, 1)
                , ('Usage: @OutputMessagesToDataset = Y{es} or N{o}', 10, 1)
                , ('Parameter @OutputMessagesToDataset is invalid: ' + LTRIM(RTRIM(@OutputMessagesToDataset)), 16, 1);
        END

		----------------------------------------------------------------------------------------------------
        -- Check Permissions
        ----------------------------------------------------------------------------------------------------
        -- Check SELECT & DELETE permission on [dbo].[Logs]
        INSERT INTO @messages ([Message], Severity, [State])
        SELECT 'Permission not effectively granted: ' + UPPER(p.permission_name), 10, 1
        FROM (VALUES(N'', N'SELECT'), (N'', N'DELETE')) AS p (subentity_name, permission_name)
        LEFT JOIN sys.fn_my_permissions(N'[dbo].[Logs]', N'OBJECT') eff ON eff.subentity_name = p.subentity_name AND eff.permission_name = p.permission_name
        WHERE eff.permission_name IS NULL
        ORDER BY p.permission_name;

        IF @@ROWCOUNT > 0 
        BEGIN
            INSERT INTO @messages ([Message], Severity, [State]) VALUES
                (N'Error: missing permission', 10, 1)
                , (N'SELECT and DELETE permissions are required on [dbo].[Logs] table', 16, 1);
        END

        -- Check @SaveMessagesToTable parameter
        SELECT @logToTable = [value] FROM @paramsYesNo WHERE [parameter] = ISNULL(LTRIM(RTRIM(@SaveMessagesToTable)), N'Y');
        IF @logToTable IS NULL 
        BEGIN
            INSERT INTO @messages ([Message], Severity, [State]) VALUES
                ( 'Parameter @SaveMessagesToTable is invalid: ' + LTRIM(RTRIM(@SaveMessagesToTable)), 10, 1)
                , ('Usage: @SaveMessagesToTable = Y{es} or N{o}', 10, 1)
                , ('Parameter @SaveMessagesToTable is invalid: ' + LTRIM(RTRIM(@SaveMessagesToTable)), 16, 1);
        END

        -- Check Messages' Tables
        INSERT INTO @messages ([Message], Severity, [State])
        SELECT N'The table ' + QUOTENAME(t.[schema]) + N'.' + QUOTENAME(t.name) + ' is missing.', 10, 1
        FROM ( VALUES('Maintenance', 'Runs'), ('Maintenance', 'Messages')) as t([schema], [name])
        LEFT JOIN sys.objects obj ON obj.name = t.name
        LEFT JOIN sys.schemas sch ON sch.[schema_id] = obj.[schema_id] AND sch.name = t.[schema]
        WHERE (obj.[type] <> 'U' OR  obj.object_id IS NULL) AND obj.object_id IS NULL AND @logToTable = 1;

        -- Update @logToTable ON missing table(s)
        IF @@ROWCOUNT > 0 
        BEGIN
            SET @logToTable = 0;
            INSERT INTO @messages ([Message], Severity, [State]) VALUES
                ('Procedure''s execution (Errors & Messages) can''t be saved to [Messages] table', 16, 1);
        END 
        ELSE
        BEGIN
            -- Check missing permissions on Runs and Messages Table
            WITH p AS(
                SELECT entity_name, subentity_name, permission_name --'Permission not effectively granted: ' + UPPER(p.permission_name)
                FROM (VALUES
                    (N'[Maintenance].[Runs]', N'', N'INSERT')
                    , (N'[Maintenance].[Runs]', N'', N'DELETE')
                    , (N'[Maintenance].[Messages]', N'', N'INSERT')
                    , (N'[Maintenance].[Messages]', N'', N'DELETE')
                ) AS p (entity_name, subentity_name, permission_name)
            )
            INSERT INTO @messages ([Message], Severity, [State])
            SELECT 'Permission not effectively granted on ' + p.entity_name + N': ' + UPPER(p.permission_name), 10, 1
            FROM p
            LEFT JOIN (
                SELECT ca.entity_name, ca.subentity_name, ca.permission_name FROM (SELECT DISTINCT entity_name FROM p) AS t
                CROSS APPLY sys.fn_my_permissions(t.entity_name, N'OBJECT') ca
            ) eff ON eff.entity_name = p.entity_name AND eff.subentity_name = p.subentity_name AND eff.permission_name = p.permission_name
            WHERE eff.permission_name IS NULL 
            ORDER BY p.entity_name, p.permission_name;

            IF @@ROWCOUNT > 0 
            BEGIN
                INSERT INTO @messages ([Message], Severity, [State]) VALUES
                    (N'Error: missing permission', 10, 1)
                    , (N'WHEN @SaveMessagesToTable is set to Y or YES, INSERT and DELETE permissions are required on [Maintenance].[Runs] and [Maintenance].[Messages] tables', 16, 1);
            END
        END

        ----------------------------------------------------------------------------------------------------
        -- Check Error(s) count
        ----------------------------------------------------------------------------------------------------

        SELECT @errorCount = COUNT(*) FROM @messages WHERE severity > 10;
        IF @errorCount > 0 INSERT INTO @messages ([Message], Severity, [State]) SELECT N'End, see previous Error(s): ' + CAST(@errorCount AS nvarchar(10)) + N' found', 16, 1;

        ----------------------------------------------------------------------------------------------------
        -- Settings
        ----------------------------------------------------------------------------------------------------

        IF  NOT EXISTS(SELECT 1 FROM @messages WHERE severity > 10)
        BEGIN
            -- Check @OnErrorRetry
            SET @MaxErrorRetry = @OnErrorRetry;
            IF @MaxErrorRetry IS NULL 
            BEGIN
                SET @MaxErrorRetry = 5;
                INSERT INTO @messages ([Message], Severity, [State]) VALUES (N'@OnErrorRetry is NULL. Default value will be used (5 times).', 10, 1);
            END
            IF @MaxErrorRetry > 20
            BEGIN
                SET @MaxErrorRetry = 20;
                INSERT INTO @messages ([Message], Severity, [State]) VALUES (N'@OnErrorRetry is bigger than 20. Max value will be used (20 times)', 10, 1);
            END

            -- Check @OnErrorWaitMillisecond
            IF @OnErrorWaitMillisecond IS NULL 
            BEGIN
                INSERT INTO @messages ([Message], Severity, [State]) VALUES (N'@OnErrorWaitMillisecond is NULL. Default value will be used (1000 ms).', 10, 1);
            END
            SELECT @errorDelay = ISNULL(ABS(@OnErrorWaitMillisecond), 1000);
            SELECT @errorWait = DATEADD(MILLISECOND, @errorDelay, 0);

            INSERT INTO @messages ([Message], Severity, [State]) VALUES (N'@@LOCK_TIMEOUT = ' + CAST(@@LOCK_TIMEOUT AS nvarchar(MAX)), 10 , 1);

/*            -- Check Dry Run
            SELECT @dryRun = [value] FROM @paramsYesNo WHERE [parameter] = LTRIM(RTRIM(@DryRunOnly));
            IF @dryRun IS NULL 
            BEGIN
                SET @dryRun = 1;
                INSERT INTO @messages ([Message], Severity, [State]) VALUES (N'@DryRunOnly is NULL. Default value will be used (Yes).', 10, 1);
            END*/

            -- Verbose Level
            SELECT @levelVerbose = CASE WHEN [value] = 1 THEN 0 ELSE @verboseBelowLevel END FROM @paramsYesNo WHERE [parameter] = LTRIM(RTRIM(@Verbose));
            IF @levelVerbose IS NULL 
            BEGIN
                SET @levelVerbose = @verboseBelowLevel;
                INSERT INTO @messages ([Message], Severity, [State]) VALUES (N'@Verbose is NULL. Default value will be used (No).', 10, 1);
            END

            INSERT INTO @messages ([Message], Severity, [State]) VALUES
                (N'Run until = ' + ISNULL(CONVERT(nvarchar(MAX), @maxRunDateTime, 121), N'unlimited'), 10, 1 )
		END

        ----------------------------------------------------------------------------------------------------
        -- Create new Run Id
        ----------------------------------------------------------------------------------------------------
        INSERT INTO @messages ([Message], Severity, [State]) 
        SELECT N'Parameter Run Id [' + CAST(@SavedToRunId AS nvarchar(100)) + N'] is already ended.' , 10, 1 FROM [Maintenance].[Runs] WHERE Id = @SavedToRunId AND EndDate IS NOT NULL
        UNION ALL SELECT N'Parameter Run Id [' + CAST(@SavedToRunId AS nvarchar(100)) + N'] not found.', 10, 1 WHERE @SavedToRunId IS NOT NULL AND NOT EXISTS (SELECT 1 FROM [Maintenance].[Runs] WHERE Id = @SavedToRunId)
        
        IF @SavedToRunId IS NULL OR NOT EXISTS(SELECT 1 FROM [Maintenance].[Runs] WHERE Id = @SavedToRunId AND EndDate IS NULL)
        BEGIN
            INSERT INTO [Maintenance].[Runs]([Type], [Info], [StartTime]) SELECT N'Add Archive Logs Trigger', N'PROCEDURE ' + @procName, @startTime;
            SELECT @runId = @@IDENTITY, @SavedToRunId = @@IDENTITY;
            INSERT INTO @messages ([Message], Severity, [State]) VALUES 
                (N'Messages saved to new Run Id: ' + CONVERT(nvarchar(MAX), @runId), 10, 1);
        END
        ELSE SELECT @runId = @SavedToRunId;

        ----------------------------------------------------------------------------------------------------
        -- Check Main Archive objects
        ----------------------------------------------------------------------------------------------------
        IF NOT EXISTS(SELECT 1 FROM @messages WHERE severity > 10)
        BEGIN
            -- Check Synonyms and source/Archive tables
            BEGIN TRY            
                EXEC [Maintenance].[ValidateASyncArchiveObjectsLogs] @Messages = @synonymMessages OUTPUT, @IsValid = @synonymIsValid OUTPUT;

                INSERT INTO @messages ([Procedure], [Message], Severity, [State])
                SELECT [Procedure], [Message], [Severity], [State] FROM OPENJSON(@synonymMessages, N'$') WITH ([Procedure] nvarchar(MAX), [Message] nvarchar(MAX), [Severity] tinyint, [State] tinyint);

                INSERT INTO @messages ([Message], Severity, [State])
                SELECT 'ERORR: Synonyms and ASync tables checks failed, see previous errors', 16, 1 WHERE ISNULL(@synonymIsValid, 0) = 0
            END TRY
            BEGIN CATCH
                INSERT INTO @messages ([Procedure], [Message], Severity, [State])
                SELECT [Procedure], [Message], [Severity], [State] FROM OPENJSON(@synonymMessages, N'$') WITH ([Procedure] nvarchar(MAX), [Message] nvarchar(MAX), [Severity] tinyint, [State] tinyint);

                INSERT INTO @messages ([Message], Severity, [State]) VALUES 
                    (N'ERORR: error(s) occured while checking synonyms and Async tables', 16, 1);
                THROW;
            END CATCH
        END
    END TRY
    BEGIN CATCH
        -- Get Unknown error
        SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
        -- Save Unknwon Errror
        INSERT INTO @messages ([Message], Severity, [State], [Number], [Line])
        SELECT @ERROR_MESSAGE, @ERROR_SEVERITY, @ERROR_STATE, @ERROR_NUMBER, @ERROR_LINE;
    END CATCH

    ----------------------------------------------------------------------------------------------------
    -- Print & Save Errors & Messages
    ----------------------------------------------------------------------------------------------------
    SELECT @errorCount = COUNT(*) FROM @messages WHERE severity > 10;
    OPEN CursorMessages;
    FETCH CursorMessages INTO @cursorDate, @cursorProcedure, @cursorMessage, @cursorSeverity, @cursorState, @cursorNumber, @cursorLine;

    IF CURSOR_STATUS('local', 'CursorMessages') = 1
    BEGIN
        WHILE @@FETCH_STATUS = 0
        BEGIN;
            IF @logToTable = 0 OR @errorCount > 0 OR @cursorSeverity >= @levelVerbose OR @cursorSeverity > 10 RAISERROR('%s', @cursorSeverity, @cursorState, @cursorMessage) WITH NOWAIT;
            IF @cursorSeverity >= 16 RAISERROR('', 10, 1) WITH NOWAIT;
            FETCH CursorMessages INTO @cursorDate, @cursorProcedure, @cursorMessage, @cursorSeverity, @cursorState, @cursorNumber, @cursorLine;
        END
    END
    ELSE 
    BEGIN
        SET @message = 'Execution has been canceled: Error Opening Messages Cursor';
        INSERT INTO @messages ([Message], Severity, [State]) VALUES (@message, 16, 1);
        SET @errorCount = @errorCount +1;
        RAISERROR(@message, 16, 1);
    END 

	IF CURSOR_STATUS('local', 'CursorMessages') >= 0 CLOSE CursorMessages;
	IF CURSOR_STATUS('local', 'CursorMessages') >= -1 DEALLOCATE CursorMessages;

    BEGIN TRY 
        -- Records existing messages
        IF @runId IS NOT NULL 
        BEGIN
            INSERT INTO [Maintenance].[Messages](RunId, [Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
            SELECT @runId, [Date], [Procedure], [Message], [Severity], [State], [Number], [Line]
            FROM @messages 
            ORDER BY Id ASC;
        END

        ----------------------------------------------------------------------------------------------------
        -- End Run on Error(s)
        ----------------------------------------------------------------------------------------------------
        IF EXISTS(SELECT 1 FROM @messages WHERE severity > 10)
        BEGIN
            SET @returnValue = 3;
            SET @message = N'Incorrect Parameters or Settings, see previous Error(s): ' + CAST(@errorCount AS nvarchar(MAX)) + N' found';
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 16, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = 0, @MessagesStack = @MessagesStack OUTPUT;
            -- RETURN 123; => catch
        END
        ----------------------------------------------------------------------------------------------------
        -- End Run on Dry Run
        ----------------------------------------------------------------------------------------------------
        IF @dryRun <> 0 
        BEGIN
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineBreak, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

            INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
            EXEC sp_executesql @stmt = @stmtEmptyMessagesStack, @params = @paramsEmptyMessagesStack, @MessagesStack = @MessagesStack OUTPUT;

            SET @returnValue = 1;

            SET @message = 'DRY RUN ONLY (check output and parameters and set @DryRunOnly to No when ready)';
            EXEC [Maintenance].[AddRunMessage] @RunId = @RunId, @Procedure = @procName, @Message = @message, @Severity = 11, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = 0, @MessagesStack = @MessagesStack OUTPUT;
            -- RETURN 1; => catch
        END
        ----------------------------------------------------------------------------------------------------
        -- Remove Saved messages
        ----------------------------------------------------------------------------------------------------
        --DELETE FROM @messages;

        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineBreak, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

        ----------------------------------------------------------------------------------------------------
        -- Archive
        ----------------------------------------------------------------------------------------------------
        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = 'Start ASync Logs Cleanup', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

        INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
        EXEC sp_executesql @stmt = @stmtEmptyMessagesStack, @params = @paramsEmptyMessagesStack, @MessagesStack = @MessagesStack OUTPUT;

        INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
        EXEC sp_executesql @stmt = @stmtEmptyMessagesStack, @params = @paramsEmptyMessagesStack, @MessagesStack = @MessagesStack OUTPUT;

        BEGIN TRY
            INSERT INTO @tempListSync([Id], [DeleteAfterDatetime], [FirstASyncId], [LastAsyncId], [CountASyncIds], [IsDeleted], [IsSynced])
            SELECT Id, DeleteAfterDatetime, FirstASyncId, LastASyncId, CountASyncIds, IsDeleted, IsSynced
            FROM [Maintenance].[Synonym_ArchiveSync_Logs] 
            WHERE ( CountASyncIds > 0 AND IsArchived = 1 AND IsDeleted <> 1 AND DeleteAfterDatetime < @startTime )
            UNION
            SELECT syn.Id, syn.DeleteAfterDatetime, syn.FirstASyncId, syn.LastASyncId, syn.CountASyncIds, syn.IsDeleted, syn.IsSynced
            FROM [Maintenance].[ASyncStatus_Logs] sts 
            INNER JOIN [Maintenance].[Synonym_ArchiveSync_Logs] syn ON syn.Id = sts.SyncId
           WHERE sts.IsDeleted = 1;

            IF @@ROWCOUNT = 0
            BEGIN 
                SET @message = N'Nothing to cleanup';
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 0, @Number = @ERROR_NUMBER, @Line = @ERROR_LINE, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
            END
        END TRY
        BEGIN CATCH
            -- Get Unknown error
            SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
            -- Save Unknwon Errror
            SET @message = N'  Error(s) occured while retrieving archived Sync Id(s)';
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = @ERROR_STATE, @Number = @ERROR_NUMBER, @Line = @ERROR_LINE, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

            THROW;
        END CATCH

        DROP TABLE IF EXISTS #tempIds
        CREATE TABLE #tempIds(Id bigint)
        SET @globalIds = 0;

        IF CURSOR_STATUS('local', 'CursorSyncIds') >= 0 CLOSE CursorSyncIds;
        IF CURSOR_STATUS('local', 'CursorSyncIds') >= -1 DEALLOCATE CursorSyncIds;

        DECLARE CursorSyncIds CURSOR FAST_FORWARD LOCAL FOR 
            SELECT [Id], [DeleteAfterDatetime], [FirstASyncId], [LastAsyncId], [CountASyncIds], [IsDeleted], [IsSynced] FROM @tempListSync ORDER BY [DeleteAfterDatetime] ASC;

        OPEN CursorSyncIds;
        FETCH CursorSyncIds INTO @cursorSyncId, @cursorDeleteAfterDatetime, @cursorFirstASyncId, @cursorLastAsyncId, @cursorCountASyncIds, @cursorIsDeleted, @cursorIsSynced;

        IF CURSOR_STATUS('local', 'CursorSyncIds') = 1
        BEGIN 
            WHILE @@FETCH_STATUS = 0 --> Cursor Loop Sync Id
            BEGIN;
                SET @message = N'Sync Id [' + CAST(@cursorSyncId AS nvarchar(100)) + N']: ';
                IF EXISTS(SELECT 1 FROM [Maintenance].[ASyncStatus_Logs] WHERE [SyncId] = @cursorSyncId AND [IsDeleted] = 1)
                BEGIN
                    SELECT @message = @message + IIF(@cursorIsSynced = 1, N' remove (already deleted and synced)', N'skip (deleted but not synced yet)')
                    EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                    DELETE FROM [Maintenance].[ASyncStatus_Logs] WHERE [SyncId] = @cursorSyncId AND [IsDeleted] = 1 AND @cursorIsSynced = 1;
                    FETCH CursorSyncIds INTO @cursorSyncId, @cursorDeleteAfterDatetime, @cursorFirstASyncId, @cursorLastAsyncId, @cursorCountASyncIds, @cursorIsDeleted, @cursorIsSynced;
                    CONTINUE;
                END

                SELECT @message = @message + N'Start';
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                INSERT INTO [Maintenance].[ASyncStatus_Logs]([SyncId], [FirstASyncId]) SELECT @cursorSyncId, @cursorFirstASyncId WHERE NOT EXISTS(SELECT 1 FROM [Maintenance].[ASyncStatus_Logs] WHERE [SyncId] = @cursorSyncId)
                SELECT @firstId = COALESCE([LastASyncId] + 1, [FirstASyncId], @cursorFirstASyncId) FROM [Maintenance].[ASyncStatus_Logs] WHERE [SyncId] = @cursorSyncId
                UPDATE [Maintenance].[ASyncStatus_Logs] SET [FirstASyncId] = @firstId WHERE [SyncId] = @cursorSyncId

                SET @totalIds = 0;
                WHILE 0 >= 0
                BEGIN
                    INSERT INTO #tempIds(Id)
                    SELECT TOP(@maxLoopDeleteRows) Id FROM [Maintenance].[Synonym_ASyncDelete_Logs] WHERE [SyncId] = @cursorSyncId AND Id >= @firstId ORDER BY Id ASC;
                    IF @@ROWCOUNT = 0
                    BEGIN
                        SET @message = SPACE(@tab * 1) + 'Cleanup finished (total = ' + CAST(@totalIds AS nvarchar(100)) + N')';
                        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                        UPDATE stt SET [IsDeleted] = 1, [DeletedOnDate] = SYSDATETIME() FROM [Maintenance].[ASyncStatus_Logs] stt WHERE [SyncId] = @cursorSyncId;
                        BREAK;
                    END
                    SELECT @firstId = MIN(Id), @lastId = MAX(Id), @countIds = COUNT(*) FROM #tempIds;

                    BEGIN TRAN
                    DELETE lgs FROM [dbo].[logs] lgs INNER JOIN #tempIds ids ON ids.Id = lgs.Id;                    
                    UPDATE stt SET [LastAsyncId] = @lastId FROM [Maintenance].[ASyncStatus_Logs] stt WHERE [SyncId] = @cursorSyncId;
                    IF @@TRANCOUNT > 0 COMMIT
                    TRUNCATE TABLE #tempIds;

                    SET @message = SPACE(@tab * 1) + 'delete: ' + CAST(@firstId AS nvarchar(100)) + N' - ' + CAST(@lastId AS nvarchar(100)) + N' (' + CAST(@countIds AS nvarchar(100)) + N')';
                    EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                    SELECT @firstId = @lastId + 1, @totalIds = @totalIds + @countIds, @globalIds = @globalIds + @countIds;                    

                    IF SYSDATETIME() > @maxRunDateTime
                    BEGIN
                        SET @isTimeOut = 1;
                        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineBreak, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                        SELECT @message = N'TIME OUT:' + (SELECT CAST(DATEDIFF(MINUTE, @startTime, SYSDATETIME()) AS nvarchar(MAX)) ) + N' min (@MaxRunMinutes = ' + ISNULL(CAST(@MaxRunMinutes AS nvarchar(MAX)), N'NULL' ) + N')';
                        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                        BREAK;
                    END 

                END --> end loop
                IF @isTimeOut = 1 BREAK;

                FETCH CursorSyncIds INTO @cursorSyncId, @cursorDeleteAfterDatetime, @cursorFirstASyncId, @cursorLastAsyncId, @cursorCountASyncIds, @cursorIsDeleted, @cursorIsSynced;
            END --> Cursor Loop Sync Id
        END
        ELSE 
        BEGIN
            SET @message = 'Execution has been canceled: Error Opening Sync Ids Cursor';
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
            RAISERROR(@message, 16, 1);
        END

        IF CURSOR_STATUS('local', 'CursorSyncIds') >= 0 CLOSE CursorSyncIds;
        IF CURSOR_STATUS('local', 'CursorSyncIds') >= -1 DEALLOCATE CursorSyncIds;

        SET @message = 'Sync Id Cleanup finished' + IIF(@isTimeOut = 1, N' (TIME OUT)', N'');
        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
        SET @message = SPACE(@tab * 1) + 'Rows deleted = ' + ISNULL(CAST(@globalIds AS nvarchar(100)), 0);
        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

        INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
        EXEC sp_executesql @stmt = @stmtEmptyMessagesStack, @params = @paramsEmptyMessagesStack, @MessagesStack = @MessagesStack OUTPUT;

        ----------------------------------------------------------------------------------------------------
        -- Runs Cleanup
        ----------------------------------------------------------------------------------------------------
        IF @logToTable = 1 
        BEGIN
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineBreak, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = 'Old Runs cleanup', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

            EXEC [Maintenance].[DeleteRuns] @CleanupAfterDays = @SavedMessagesRetentionDays, @RunId = @runId, @Procedure = @procName, @MessagesStack = @MessagesStack OUTPUT;
        END

		EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineBreak, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
		EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
		EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = 'END (SUCCESS)', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

        INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
        EXEC sp_executesql @stmt = @stmtEmptyMessagesStack, @params = @paramsEmptyMessagesStack, @MessagesStack = @MessagesStack OUTPUT;

        SET @returnValue = 0; -- Success
    END TRY
    BEGIN CATCH
        SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();

        -- Record error message
        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @ERROR_MESSAGE, @Severity = @ERROR_SEVERITY, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @RaiseError = 0, @MessagesStack = @MessagesStack OUTPUT;

        IF @@TRANCOUNT > 0 ROLLBACK TRAN;

        INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
        EXEC sp_executesql @stmt = @stmtEmptyMessagesStack, @params = @paramsEmptyMessagesStack, @MessagesStack = @MessagesStack OUTPUT;

        IF @errorCount = 0
        BEGIN
            IF @dryRun <> 0 
            BEGIN 
                -- Output Message result set
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = 'END (DRY RUN)', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
            END
            ELSE 
            BEGIN
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineBreak, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                SET @message = N'Execution finished with error(s)'
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                SET @message = N'Rows deleted: ' + ISNULL(CAST(@totalIds AS nvarchar(MAX)), '0');
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                SET @message = N'Elapsed time : ' + CAST(CAST(DATEADD(SECOND, DATEDIFF(SECOND, @startTime, SYSDATETIME()), 0) AS time) AS nvarchar(MAX));
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineBreak, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = 'END (FAIL)', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                SET @returnValue = 4
            END
        END
        ELSE
        BEGIN
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = 'END (INCORRECT PARAMETERS)', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
        END

        RAISERROR(@ERROR_MESSAGE, @ERROR_SEVERITY, @ERROR_STATE);

        SET @returnValue = ISNULL(@returnValue, 255);
    END CATCH

    ----------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------
    INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
    EXEC sp_executesql @stmt = @stmtEmptyMessagesStack, @params = @paramsEmptyMessagesStack, @MessagesStack = @MessagesStack OUTPUT;

    -- Output Messages result set
    IF @outputDataset = 1 SELECT [RunId] = @RunId, [Date], [Procedure], [Message], [Severity], [State], [Number], [Line] FROM @messages ORDER BY [id] ASC;

    SET @returnValue = ISNULL(@returnValue, 255);
    IF @runId IS NOT NULL UPDATE [Maintenance].[Runs] SET [EndDate] = SYSDATETIME(), [ErrorStatus] = @returnValue WHERE Id = @runId;

    ----------------------------------------------------------------------------------------------------
    -- End
    ----------------------------------------------------------------------------------------------------
    RETURN @returnValue;
END
GO
