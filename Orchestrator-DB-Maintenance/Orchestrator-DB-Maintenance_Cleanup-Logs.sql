SET NOCOUNT ON;
GO

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
-- TABLE [Maintenance].[Runs]
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
-- TABLE [Maintenance].[Messages]
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
-- ### [Version]: 2021-03-11T10:11:59+01:00
-- ### [Hash]: 560870c
-- ### [Docs]: https://XxXxXxX
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
    , @LogsStack xml = NULL OUTPUT
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
        SET @LogsStack = COALESCE(@logsStack, N'<messages></messages>');
        SET @Message = COALESCE(@Message, N'');

        IF @LogToTable = 1 
        BEGIN
            INSERT INTO [Maintenance].[Messages](RunId, [Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
            SELECT @RunId, @date, @Procedure, @Message, @Severity, @state, @Number, @Line WHERE @RunId IS NOT NULL;
        END

        SET @log = (  SELECT * FROM (SELECT SYSDATETIME(), @Procedure, @Message, @Severity, @state, @Number, @Line) x([Date], [Procedure], [Message], [Severity], [State], [Number], [Line]) FOR XML PATH('message') )
        SET @logsStack.modify('insert sql:variable("@log") as last into (/messages)[1]')

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
-- ### [Version]: 2021-03-11T10:11:59+01:00
-- ### [Hash]: 560870c
-- ### [Docs]: https://XxXxXxX
----------------------------------------------------------------------------------------------------
    @CleanupAfterDays tinyint = 30
    , @RunId int = NULL
    , @Procedure nvarchar(max) = NULL
    , @LogsStack xml = NULL OUTPUT
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

            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @Procedure, @Message = N'Runs cleanup started', @Severity = 10, @State = 1, @VerboseLevel = 10, @LogToTable = 1, @LogsStack = @logsStack OUTPUT;
            SET @message = N'Keep Runs from past days: ' + CAST(@CleanupAfterDays AS nvarchar(10));
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @Procedure, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = 10, @LogToTable = 1, @LogsStack = @logsStack OUTPUT;
            SET @message =  N'Delete Runs before: ' + CONVERT(nvarchar(20), DATEADD(DAY, - @CleanupAfterDays, SYSDATETIME()), 121);
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @Procedure, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = 10, @LogToTable = 1, @LogsStack = @logsStack OUTPUT;

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
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @Procedure, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = 10, @LogToTable = 1, @LogsStack = @logsStack OUTPUT;
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @Procedure, @Message = N'Runs Cleanup Finished', @Severity = 10, @State = 1, @VerboseLevel = 10, @LogToTable = 1, @LogsStack = @logsStack OUTPUT;

            UPDATE [Maintenance].[Runs] SET [EndDate] = SYSDATETIME(), [ErrorStatus] = 0 WHERE Id = @localRunId AND @RunId IS NULL;
        END TRY
        BEGIN CATCH
            SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
            IF @@TRANCOUNT > 0 ROLLBACK;

            IF @localRunId IS NOT NULL
            BEGIN
                SET @message = N'Error: ' + @ERROR_MESSAGE;
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @Procedure, @Message = @message, @Severity = @ERROR_SEVERITY, @State = @ERROR_STATE, @VerboseLevel = 10, @LogToTable = 1, @RaiseError = 0, @LogsStack = @logsStack OUTPUT;
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
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @Procedure, @Message = @message, @Severity = @ERROR_SEVERITY, @State = @ERROR_STATE, @VerboseLevel = 10, @LogToTable = 1, @RaiseError = 0, @LogsStack = @logsStack OUTPUT;

            UPDATE [Maintenance].[Runs] SET [EndDate] = SYSDATETIME(), [ErrorStatus] = 4 WHERE Id = @localRunId;
        END;

        THROW;
    END CATCH
END

GO

/*
USE [Uipath] -- Orchestrator Database
GO
*/

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- PROCEDURE [Maintenance].[CleanupLogs]
----------------------------------------------------------------------------------------------------

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[CleanupLogs]') AND type in (N'P'))
BEGIN
    EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [Maintenance].[CleanupLogs] AS'
    PRINT '  + CREATE PROCEDURE: [Maintenance].[CleanupLogs]';
END
ELSE PRINT '  = PROCEDURE [Maintenance].[CleanupLogs] already exists' 
GO

PRINT '  ~ UPDATE PROCEDURE: [Maintenance].[CleanupLogs]'
GO

ALTER PROCEDURE [Maintenance].[CleanupLogs]
----------------------------------------------------------------------------------------------------
-- ### [Version]: 2021-03-11T10:11:59+01:00
-- ### [Hash]: 560870c
-- ### [Docs]: https://XxXxXxX
----------------------------------------------------------------------------------------------------
    @HoursToKeep int = NULL -- i.e. 168h = 7*24h = 7 days => value can't be NULL and must be bigger than 0 if @CleanupBeforeDate is not set
    , @CleanupBeforeDate datetime = NULL -- Use either @CleanupBeforeDate or @HoursToKeep BUT not both
    , @RowsDeletedForEachLoop int = 10000 -- Don't go above 50.000 (min = 1000, max = 100.000)
    , @CleanupBelowId bigint = NULL -- Provide Max Log Id to procedure
   , @MaxRunMinutes int = NULL -- NULL or 0 = unlimited
    -- 
    , @DryRunOnly nvarchar(max) = NULL -- Y{es} or N{o} => Only Check Parameters (default if NULL = Y)
    /* Error Handling */
    , @OnErrorRetry tinyint = NULL -- between 0 and 20 => retry up to 20 times (default if NULL = 10)
    , @OnErrorWaitMillisecond smallint = 1000 -- wait for milliseconds between each Retry (default if NULL = 1000ms)
    /* Index handling */
    , @DisableIndex nvarchar(max) = NULL -- Y{es} or N{o} => Disables all non clustered indexes (default if NULL = N)
    , @RebuildIndex nvarchar(max) = NULL -- Y{es} or N{o} => Rebuild all indexes (default if NULL = N)
    , @RebuildIndexOnline nvarchar(max) = NULL -- Y{es} or N{o} => Rebuild Online on Enterprise edition Only
    /* Messge Logging */
    , @SaveMessagesToTable nvarchar(max) = 'Y' -- Y{es} or N{o} => Save to [maintenance].[messages] table (default if NULL = Y)
    , @SavedMessagesRetentionDays smallint = 30
    , @SavedToRunId int = NULL OUTPUT
    , @OutputMessagesToDataset nvarchar(max) = 'N' -- Y{es} or N{o} => Output Messages Result set
    , @Verbose nvarchar(max) = NULL -- Y{es} = Print all messages < 10
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
        DECLARE @startTime datetime2 = SYSDATETIME();
		DECLARE @loopStart datetime;
        DECLARE @maxRunDateTime datetime2;
        DECLARE @logToTable bit;
        DECLARE @deleteTopRows int;
        DECLARE @maxCreationTime datetime;
        DECLARE @maxErrorRetry tinyint;
        DECLARE @errorDelay smallint;
        DECLARE @errorWait datetime;
        DECLARE @indexDisable bit;
        DECLARE @indexRebuild bit;
        DECLARE @indexRebuildOnline bit;
        DECLARE @returnValue int = 1;
        ----------------------------------------------------------------------------------------------------
        -- Cleanup
        ----------------------------------------------------------------------------------------------------
        DECLARE @dryRun bit;
        DECLARE @minId bigint = 0;
        DECLARE @maxId bigint;
        DECLARE @curIdStart bigint
        DECLARE @curIdEnd bigint              
        DECLARE @totalIds bigint;
        DECLARE @countIds bigint;
        DECLARE @countErrorRetry tinyint;
        ----------------------------------------------------------------------------------------------------
        -- Constant / Default value
        ----------------------------------------------------------------------------------------------------
        DECLARE @maxDeleteRows int = 100*1000; -- Raise an error if @RowsDeletedForEachLoop is bigger than this value
        DECLARE @minDeleteRows int = 1*1000; -- Raise an error if @RowsDeletedForEachLoop is smaller than this value
        DECLARE @VerboseBelowLevel int = 10; -- don't print message with Severity < 10 unless Verbose is set to Y
        ----------------------------------------------------------------------------------------------------
        DECLARE @lineSeparator nvarchar(max) = N'----------------------------------------------------------------------------------------------------';
        DECLARE @lineBreak nvarchar(max) = N'';
        DECLARE @message nvarchar(max);
        DECLARE @string nvarchar(max);
        DECLARE @paramsYesNo TABLE ([id] tinyint IDENTITY(0, 1) PRIMARY KEY CLUSTERED, [parameter] nvarchar(max), [value] int)
        ----------------------------------------------------------------------------------------------------
        -- Server Info 
        ----------------------------------------------------------------------------------------------------
        DECLARE @productVersion nvarchar(max) = CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max));
        DECLARE @engineEdition int =  CAST(ISNULL(SERVERPROPERTY('EngineEdition'), 0) AS int);
        DECLARE @minProductVersion nvarchar(max) = N'11.0.2100.60 (SQL Server 2012 RTM)'
        DECLARE @version numeric(18, 10);
        DECLARE @minVersion numeric(18, 10) = 11.0210060;
        DECLARE @hostPlatform nvarchar(256); 
        ----------------------------------------------------------------------------------------------------
        -- Proc Info
        ----------------------------------------------------------------------------------------------------
        DECLARE @paramsGetProcInfo nvarchar(max) = N'@procid int, @info nvarchar(max), @output nvarchar(max) OUTPUT'
        DECLARE @stmtGetProcInfo nvarchar(max) = N'
            DECLARE @definition nvarchar(max) = OBJECT_DEFINITION(@procid), @keyword nvarchar(max) = REPLICATE(''-'', 2) + SPACE(1) + REPLICATE(''#'', 3) + SPACE(1) + QUOTENAME(LTRIM(RTRIM(@info))) + '':'';
            SET @output = ''=''+ LTRIM(RTRIM( SUBSTRING(@definition, NULLIF(CHARINDEX(@keyword, @definition), 0 ) + LEN(@keyword), CHARINDEX( CHAR(13) , @definition, CHARINDEX(@keyword, @definition) + LEN(@keyword) + 1) - CHARINDEX(@keyword, @definition) - LEN(@keyword) ))) + ''='';
        ';
        DECLARE @procSchemaName nvarchar(max) = COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?');
        DECLARE @procObjecttName nvarchar(max) = COALESCE(OBJECT_NAME(@@PROCID), N'?');
        DECLARE @procName nvarchar(max) = QUOTENAME(COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?')) + N'.' + QUOTENAME(COALESCE(OBJECT_NAME(@@PROCID), N'?')); 
        DECLARE @versionDatetime nvarchar(max);
        ----------------------------------------------------------------------------------------------------      
        -- Message / Error Handling
        ----------------------------------------------------------------------------------------------------
        /*DECLARE @stmtLogMessage nvarchar(max) = N'
            IF @LogToTable = 1 
            BEGIN
                INSERT INTO [Maintenance].[Messages](RunId, [Procedure], [Message], [Severity], [State], [Number], [Line])
                    OUTPUT INSERTED.[date], INSERTED.[Procedure], INSERTED.[Message], INSERTED.[Severity], INSERTED.[State], INSERTED.[Number], INSERTED.[Line]
                SELECT @RunId, @Procedure, @Message, @Severity, @state, @Number, @Line WHERE @RunId IS NOT NULL;
            END
            ELSE 
            BEGIN
                SELECT SYSDATETIME(), @Procedure, @Message, @Severity, @state, @Number, @Line;
            END

            IF @Severity >= @VerboseLevel 
            BEGIN
                IF @Severity < 10 SET @Severity = 10;
                RAISERROR(@Message, @Severity, @State);
            END
        ';
        DECLARE @paramsLogMessage nvarchar(max) = N'@RunId int, @Procedure sysname, @Message nvarchar(max), @Severity tinyint, @Number int = NULL, @Line int = NULL, @State tinyint, @VerboseLevel tinyint, @LogToTable bit';
        */
        DECLARE @stmtEmptyLogsStack nvarchar(max) = N'
            SELECT 
                [Date] = m.value(''Date[1]'', ''datetime'')
                , [Procedure] = m.value(''Procedure[1]'', ''nvarchar(max)'')
                , [Message] = m.value(''Message[1]'', ''nvarchar(max)'')
                , [Severity] = m.value(''Severity[1]'', ''int'')
                , [State] = m.value(''State[1]'', ''int'')
                , [Number] = m.value(''Number[1]'', ''int'')
                , [Line] = m.value(''Line[1]'', ''int'')
            FROM @LogsStack.nodes(''/messages/message'') x(m)
            SET @LogsStack = NULL;
        ';
        DECLARE @paramsEmptyLogsStack nvarchar(max) = N'@LogsStack xml = NULL OUTPUT';
        DECLARE @logsStack xml;

        DECLARE @ERROR_NUMBER INT, @ERROR_SEVERITY INT, @ERROR_STATE INT, @ERROR_PROCEDURE NVARCHAR(126), @ERROR_LINE INT, @ERROR_MESSAGE NVARCHAR(2048) ;
        DECLARE @errorCount int = 0;
        DECLARE @messages TABLE(id int IDENTITY(0, 1) PRIMARY KEY, [date] datetime2 DEFAULT SYSDATETIME(), [procedure] nvarchar(max) NOT NULL DEFAULT QUOTENAME(COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?')) + N'.' + QUOTENAME(COALESCE(OBJECT_NAME(@@PROCID), N'?')), [message] nvarchar(max) NOT NULL, severity tinyint NOT NULL, state tinyint NOT NULL, [number] int, [line] int);
        DECLARE CursorMessages CURSOR FAST_FORWARD LOCAL FOR SELECT [Date], [Procedure], [Message], [Severity], [State], [Number], [Line] FROM @messages ORDER BY [ID] ASC;
        DECLARE @cursorDate datetime2;
        DECLARE @cursorProcedure nvarchar(max);
        DECLARE @cursorMessage nvarchar(max);
        DECLARE @cursorSeverity tinyint;
        DECLARE @cursorState tinyint;
        DECLARE @cursorNumber int;
        DECLARE @cursorLine int;    
        DECLARE @levelVerbose int;    
        DECLARE @outputDataset bit;

        ----------------------------------------------------------------------------------------------------
        -- START
        ----------------------------------------------------------------------------------------------------

        ----------------------------------------------------------------------------------------------------
        -- Gather General & Server Info
        ----------------------------------------------------------------------------------------------------

        -- Get Run Time limit or NULL (unlimited)
	    SET @maxRunDateTime = CASE WHEN ABS(@MaxRunMinutes) >= 1 THEN DATEADD(MINUTE, ABS(@MaxRunMinutes), @startTime) ELSE NULL END;

        -- Output Start & Stop info
        INSERT INTO @messages([Message], Severity, [State]) VALUES 
            ( @lineSeparator, 10, 1)
            , (N'Start Time = ' + CONVERT(nvarchar(max), @startTime, 121), 10, 1 )
            , (N'Max Run Time = ' + ISNULL(CONVERT(nvarchar(max), @maxRunDateTime, 121), N'NULL (=> unlimited)'), 10, 1 )
            , (@lineBreak, 10, 1);

        -- Get Proc Version
        EXEC sp_executesql @stmt = @stmtGetProcInfo, @params = @paramsGetProcInfo, @procid = @@PROCID, @info = N'Version', @output = @versionDatetime OUTPUT;

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

        -- Output Server Info
        INSERT INTO @messages([Message], Severity, [State]) VALUES 
             ( @lineSeparator, 10, 1)
            , ( N'Server Info', 10, 1)
            , ( @lineSeparator, 10, 1)
            , ( N'Host Platform = ' + @hostPlatform, 10, 1)
            , ( N'Server Name = ' + ISNULL(CAST(SERVERPROPERTY('ServerName') AS nvarchar(128)), N'?'), 10, 1)
            , ( N'Machine Name = ' + ISNULL(CAST(SERVERPROPERTY('MachineName') AS nvarchar(128)), N'?'), 10, 1)
            , ( N'Host Netbios Name = ' + ISNULL(CAST(SERVERPROPERTY('ComputerNamePhysicalNetBIOS') AS nvarchar(128)), N'?'), 10, 1)
            , ( N'Instance Name = ' + ISNULL(CAST(SERVERPROPERTY('InstanceName') AS nvarchar(128)), N''), 10, 1)
            , ( N'Is Clustered = ' + ISNULL(CAST(SERVERPROPERTY('IsClustered') AS nvarchar(10)), N'?'), 10, 1)
            , ( N'Is Hadr Enabled = ' + ISNULL(CAST(SERVERPROPERTY('IsHadrEnabled') AS nvarchar(10)), N'?'), 10, 1)
            , ( N'SQL Server version = ' + @productVersion, 10, 1)
            , ( N'Edition = ' + ISNULL(CAST(SERVERPROPERTY('Edition') AS nvarchar(128)), N'?'), 10, 1)
            , ( N'Engine Edition = ' + ISNULL(CAST(@engineEdition AS nvarchar(10)), N''), 10, 1)
            , ( N'ProductLevel = ' + ISNULL(CAST(SERVERPROPERTY('ProductLevel') AS nvarchar(128)), N'?'), 10, 1)
            , ( N'Database name = ' + QUOTENAME(DB_NAME(DB_ID())), 10, 1)
            , ( N'Compatibility Level = ' + @productVersion, 10, 1)
            , ( N'Procedure object name = ' + QUOTENAME(@procObjecttName), 10, 1)
            , ( N'Procedure schema name = ' + QUOTENAME(@procSchemaName), 10, 1)
        ;
       
        INSERT INTO @messages([Message], Severity, [State])
        SELECT 'Compatibility Level = ' + CAST([compatibility_level] AS nvarchar(10)), 10, 1 FROM sys.databases WHERE database_id = DB_ID()
        ;

        ----------------------------------------------------------------------------------------------------
        -- Check SQL Server Requierements
        ----------------------------------------------------------------------------------------------------
        
        -- Check min required version
        INSERT INTO @messages([Message], Severity, [State])
        SELECT N'Current SQL Server version is: ' + @productVersion + N'. Only version ' + @minProductVersion + + N' or higher is supported.', 16, 1 WHERE @version < @minVersion;

        -- check system datbabases
        INSERT INTO @messages([Message], Severity, [State])
        SELECT 'This procedure nust be installed and executed on a UiPath Orchestrator database. A system database is currently used:' + QUOTENAME(DB_NAME(DB_ID())), 16, 1
        FROM sys.databases WHERE database_id = DB_ID() AND (name IN(N'master', N'msdb', N'model', N'tempdb') OR is_distributor = 1);

        -- Check Database Compatibility Level
        INSERT INTO @messages([Message], Severity, [State])
        SELECT 'Database ' + QUOTENAME(DB_NAME(DB_ID())) + ' Compatibility Level is set to: '+ CAST([compatibility_level] AS nvarchar(10)) + '. Compatibility level 110 or higher is requiered.', 16, 1 FROM sys.databases WHERE database_id = DB_ID() AND [compatibility_level] < 110;

        -- Check opened transation(s)
        INSERT INTO @messages ([Message], Severity, [State])
        SELECT 'The transaction count is not 0.', 16, 1 WHERE @@TRANCOUNT <> 0;

        -- Check uses_ansi_nulls
        INSERT INTO @messages ([Message], Severity, [State])
        SELECT 'ANSI_NULLS must be set to ON for this Stored Procedure', 16, 1
        FROM sys.sql_modules WHERE [object_id] = @@PROCID AND uses_ansi_nulls <> 1;

        -- Check uses_quoted_identifier
        INSERT INTO @messages ([Message], Severity, [State])
        SELECT 'QUOTED_IDENTIFIER must be set to ON for this Stored Procedure', 16, 1
        FROM sys.sql_modules WHERE [object_id] = @@PROCID AND uses_quoted_identifier <> 1;

        ----------------------------------------------------------------------------------------------------
        -- Check Parameters
        ----------------------------------------------------------------------------------------------------

        SET @levelVerbose = @VerboseBelowLevel;

        -- Convert Yes / No varations to bit
        INSERT INTO @paramsYesNo([parameter], [value]) VALUES(N'NO', 0), (N'N', 0), (N'0', 0), (N'YES', 1), (N'Y', 1), (N'1', 1);

        -- Output Parameters
        INSERT INTO @messages([Message], Severity, [State])
        VALUES 
            (@lineBreak, 10, 1)
            , ( @lineSeparator, 10, 1)
            , ( N'Parameters', 10, 1)
            , ( @lineSeparator, 10, 1)
            , ( N'@RowsDeletedForEachLoop: ' + ISNULL(CAST(@RowsDeletedForEachLoop AS nvarchar(10)), N'NULL (=> default = ' + CAST(@deleteTopRows AS nvarchar(10)) + ')'), 10, 1)
            , ( N'@HoursToKeep: ' + ISNULL(CAST(@HoursToKeep AS nvarchar(10)), N'NULL'), 10, 1)
            , ( N'@CleanupBeforeDate: ' + ISNULL(CONVERT(nvarchar(20), @CleanupBeforeDate, 121), N'NULL'), 10, 1)
            , ( N'@MaxRunMinutes: ' + ISNULL(CAST(@MaxRunMinutes AS nvarchar(10)), N'NULL'), 10, 1)

            , ( N'@DryRunOnly: ' + ISNULL(@DryRunOnly, N'NULL (=> default = Yes)'), 10, 1)

            , ( N'@OnErrorRetry: ' + ISNULL(CAST(@OnErrorRetry AS nvarchar(10)), N'NULL'), 10, 1)
            , ( N'@OnErrorWaitMillisecond: ' + ISNULL(CAST(@OnErrorWaitMillisecond AS nvarchar(10)), N'NULL'), 10, 1)

            , ( N'@DisableIndex: ' + ISNULL(@DisableIndex, N'NULL (=> default = No)'), 10, 1)
            , ( N'@RebuildIndex: ' + ISNULL(@RebuildIndex, N'NULL (=> default = No)'), 10, 1)
            , ( N'@RebuildIndexOnline: ' + ISNULL(@RebuildIndexOnline, N'NULL (=> default = Yes on Enterprise Edition)'), 10, 1)

            , ( N'@SaveMessagesToTable: ' + ISNULL(@SaveMessagesToTable, N'NULL (=> default = Yes)'), 10, 1)
            , ( N'@SavedMessagesRetentionDays: ' + ISNULL(CAST(@SavedMessagesRetentionDays AS nvarchar(10)), N'NULL (=> default = 30)'), 10, 1)
            , ( N'@OutputMessagesToDataset: ' + ISNULL(@OutputMessagesToDataset, N'NULL (=> default = No)'), 10, 1)

            , ( N'@verbose: ' + ISNULL(@Verbose, N'NULL (=> default = No)'), 10, 1)
            , (@lineBreak, 10, 1)

        -- Check @RowsDeletedForEachLoop
        SET @deleteTopRows = ISNULL(NULLIF(@RowsDeletedForEachLoop, 0), @deleteTopRows);

        IF @deleteTopRows < @minDeleteRows OR @deleteTopRows > @maxDeleteRows 
        BEGIN
            INSERT INTO @messages ([Message], Severity, [State]) VALUES
                ( 'Parameter @RowsDeletedForEachLoop is invalid: ' + LTRIM(RTRIM(CAST(@RowsDeletedForEachLoop AS nvarchar(10)))), 10, 1)
                , ('USAGE: use a value between ' + FORMAT(@minDeleteRows,'#,0') + N' and ' + FORMAT(@maxDeleteRows,'#,0') +  N'', 10, 1)
                , ('Parameter @RowsDeletedForEachLoop is invalid', 16, 1);
        END

        -- Check @HoursToKeep
        IF @HoursToKeep IS NOT NULL AND @CleanupBeforeDate IS NOT NULL
        BEGIN 
            INSERT INTO @messages ([Message], Severity, [State]) VALUES ( N'@HoursToKeep OR @CleanupBeforeDate cannot be used simultaneously. Use either @HoursToKeep OR @CleanupBeforeDate', 16, 1);
        END 
        IF @HoursToKeep IS  NULL AND @CleanupBeforeDate IS  NULL
        BEGIN 
            INSERT INTO @messages ([Message], Severity, [State]) VALUES ( N'@HoursToKeep OR @CleanupBeforeDate cannot be both NULL. Use either @HoursToKeep OR @CleanupBeforeDate', 16, 1);
        END 
        IF @HoursToKeep < 1
        BEGIN
            INSERT INTO @messages ([Message], Severity, [State]) VALUES ( N'@HoursToKeep must be bigger than 0', 16, 1);
        END
        IF @CleanupBeforeDate > @startTime
        BEGIN
            INSERT INTO @messages ([Message], Severity, [State]) VALUES ( N'@CleanupBeforeDate must be a past date and time', 16, 1);
        END

        IF (@HoursToKeep > 1 AND @CleanupBeforeDate IS NULL) OR (@CleanupBeforeDate < @startTime AND @HoursToKeep IS NULL)
        BEGIN
            SET @maxCreationTime = CASE WHEN @HoursToKeep IS NULL THEN @CleanupBeforeDate ELSE DATEADD(hour, -ABS(@HoursToKeep), @startTime) END;
        END
        ELSE INSERT INTO @messages ([Message], Severity, [State]) VALUES ( N'Cleanup Date upper boundary cannot be set with the currently set values for @HoursToKeep and @CleanupBeforeDate', 16, 1);

        -- Check @DisableIndex / @RebuildIndex / @RebuildIndexOnline
        SELECT @indexDisable = [value] FROM @paramsYesNo WHERE [parameter] = NULLIF(LTRIM(RTRIM(@DisableIndex)), N'');
        SELECT @indexRebuild = [value] FROM @paramsYesNo WHERE [parameter] = NULLIF(LTRIM(RTRIM(@RebuildIndex)), N'');
        SELECT @indexRebuildOnline = [value] FROM @paramsYesNo WHERE [parameter] = NULLIF(LTRIM(RTRIM(@RebuildIndexOnline)), N'');

        IF @indexRebuild = 1 AND @indexRebuildOnline = 1 AND @engineEdition NOT IN (3,5,8)
        BEGIN
            INSERT INTO @messages ([Message], Severity, [State]) VALUES 
                (N'@RebuildIndexOnline is set to ' + @RebuildIndexOnline, 10, 1)
                , (N'This SQL Server Edition doesn''t support rebuilding index Online: '  +  CAST(SERVERPROPERTY('Edition') AS nvarchar(128)), 16, 1);
        END

        -- Check ALTER Permission on [dbo].[Logs]
        INSERT INTO @messages ([Message], Severity, [State])
        SELECT 'Permission not effectively granted: ' + UPPER(p.permission_name), 10, 1
        FROM (VALUES(N'', N'ALTER')) AS p (subentity_name, permission_name)
        LEFT JOIN sys.fn_my_permissions(N'[dbo].[Logs]', N'OBJECT') eff ON eff.subentity_name = p.subentity_name AND eff.permission_name = p.permission_name
        WHERE eff.permission_name IS NULL AND (@indexDisable = 1 OR @indexRebuild = 1)
        ORDER BY p.permission_name;

        IF @@ROWCOUNT > 0 
        BEGIN
            INSERT INTO @messages ([Message], Severity, [State]) VALUES
                (N'Error: missing permission', 10, 1)
                , (N'When set to Y, @DisableIndex or @RebuildIndex require ALTER permission on [dbo].[Logs] table', 16, 1);
        END

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
                , ('Parameter @@SaveMessagesToTable is invalid: ' + LTRIM(RTRIM(@SaveMessagesToTable)), 16, 1);
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
                    , (N'WHEN @SaveMessagesToTable is set to Y or YES, INSERT and DELETE permissions are required on [Maintenance].[Runs] and [Mainteance].[Messages] tables', 16, 1);
            END
        END

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
        -- Check Error(s) count
        ----------------------------------------------------------------------------------------------------

        SELECT @errorCount = COUNT(*) FROM @messages WHERE severity >= 16;
        -- IF @errorCount > 0 INSERT INTO @messages ([Message], Severity, [State]) SELECT N'End, see previous Error(s): ' + CAST(@errorCount AS nvarchar(10)) + N' found', 16, 1;

        ----------------------------------------------------------------------------------------------------
        -- Settings
        ----------------------------------------------------------------------------------------------------

        IF @errorCount = 0
        BEGIN
            INSERT INTO @messages([Message], Severity, [State])
            VALUES 
                ( @lineSeparator, 10, 1)
                , ( N'Run settings', 10, 1)
                , ( @lineSeparator, 10, 1)

            -- Check @OnErrorRetry
            SET @maxErrorRetry = @OnErrorRetry;
            IF @maxErrorRetry IS NULL 
            BEGIN
                SET @maxErrorRetry = 5;
                INSERT INTO @messages ([Message], Severity, [State]) VALUES 
                    (N'@OnErrorRetry is NULL. Default value will be used (5 times).', 10, 1);
            END
            IF @maxErrorRetry > 20
            BEGIN
                SET @maxErrorRetry = 20;
                INSERT INTO @messages ([Message], Severity, [State]) VALUES 
                    (N'@OnErrorRetry is bigger than 20. Max value will be used (20 times)', 10, 1);
            END

            -- Check @OnErrorWaitMillisecond
            IF @OnErrorWaitMillisecond IS NULL 
            BEGIN
                INSERT INTO @messages ([Message], Severity, [State]) VALUES 
                    (N'@OnErrorWaitMillisecond is NULL. Default value will be used (1000 ms).', 10, 1);
            END
            SELECT @errorDelay = ISNULL(ABS(@OnErrorWaitMillisecond), 1000);
            SELECT @errorWait = DATEADD(MILLISECOND, @errorDelay, 0);

            INSERT INTO @messages ([Message], Severity, [State]) VALUES 
				(N'@@LOCK_TIMEOUT = ' + CAST(@@LOCK_TIMEOUT AS nvarchar(20)), 10 , 1);

            -- Check Dry Run
            SELECT @dryRun = [value] FROM @paramsYesNo WHERE [parameter] = LTRIM(RTRIM(@DryRunOnly));
            IF @dryRun IS NULL 
            BEGIN
                SET @dryRun = 1;
                INSERT INTO @messages ([Message], Severity, [State]) VALUES 
                    (N'@DryRunOnly is NULL. Default value will be used (Yes).', 10, 1);
            END

            -- Update Missing @RebuildIndex
            IF @indexDisable IS NULL
            BEGIN
                SET @indexDisable = 0;
                INSERT INTO @messages ([Message], Severity, [State]) VALUES 
                    (N'@DisableIndex is NULL. Default value will be used (No).', 10, 1);
            END

            -- Update Missing @RebuildIndex
            IF @indexRebuild IS NULL
            BEGIN
                SET @indexRebuild = 0;
                INSERT INTO @messages ([Message], Severity, [State]) VALUES 
                    (N'@RebuildIndex is NULL. Default value will be used (No).', 10, 1);
            END

            -- Update missing @RebuildIndexOnline
            IF SERVERPROPERTY('EngineEdition') IN (3,5,8) AND @indexRebuild = 1 AND @indexRebuildOnline IS NULL
            BEGIN
                SET @indexRebuildOnline = 1;
                INSERT INTO @messages ([Message], Severity, [State]) VALUES 
                    (N'@RebuildIndexOnline is NULL. Default value will be used (Yes).', 10, 1);
            END

            -- Verbose Level
            SELECT @levelVerbose = CASE WHEN [value] = 1 THEN 0 ELSE @verboseBelowLevel END FROM @paramsYesNo WHERE [parameter] = LTRIM(RTRIM(@Verbose));
            IF @levelVerbose IS NULL 
            BEGIN
                SET @levelVerbose = @verboseBelowLevel;
                INSERT INTO @messages ([Message], Severity, [State]) VALUES 
                    (N'@Verbose is NULL. Default value will be used (No).', 10, 1);
            END

            -- Cleanup settings
            INSERT INTO @messages ([Message], Severity, [State]) SELECT N'Keep past hours: ' + COALESCE(CAST(@HoursToKeep AS nvarchar(10)), '-'), 10, 1 WHERE @HoursToKeep IS NOT NULL;
            INSERT INTO @messages ([Message], Severity, [State]) VALUES (N'Cleanup data before: ' + COALESCE(CONVERT(nvarchar(20), @maxCreationTime, 121), '-'), 10, 1);

            IF @CleanupBelowId IS NOT NULL
            BEGIN 
                SET @maxId = @CleanupBelowId
        
                INSERT INTO @messages ([Message], Severity, [State])
                SELECT 'Use Provided MAX Id: ' + COALESCE(CAST(@maxId AS nvarchar(10)), '-'), 10, 1;
            END
            ELSE
            BEGIN
                IF (SELECT COUNT(*) FROM [dbo].[Logs]) > 2*1000*1000
                BEGIN
                    BEGIN TRY
                        SELECT @maxId = MAX(Id) FROM [dbo].[Logs] WITH(INDEX([IX_Machine])) WHERE TimeStamp < @maxCreationTime;

                        INSERT INTO @messages ([Message], Severity, [State])
                        SELECT 'Get MAX Id from Index [IX_Machine]: ' + COALESCE(CAST(@maxId AS nvarchar(10)), '-'), 10, 1;
                    END TRY
                    BEGIN CATCH
                        INSERT INTO @messages ([Message], Severity, [State])
                        SELECT 'ERROR fechting MAX Id from Index [IX_Machine]', 10, 1;
                    END CATCH

                    IF @maxId IS NULL
                    BEGIN
                        BEGIN TRY
                            SELECT @maxId = MAX(id) FROM (SELECT id = MAX(Id), MachineId FROM [dbo].[Logs] WHERE TimeStamp < @maxCreationTime GROUP BY MachineId ) m;

                            INSERT INTO @messages ([Message], Severity, [State])
                            SELECT 'Get MAX Id from Machine Ids: ' + COALESCE(CAST(@maxId AS nvarchar(10)), '-'), 10, 1;
                        END TRY
                        BEGIN CATCH
                            INSERT INTO @messages ([Message], Severity, [State])
                            SELECT 'ERROR fechting MAX Id from Machine Ids', 10, 1;
                        END CATCH
                    END
                END
                
                IF @maxId IS NULL
                BEGIN
                    SELECT @maxId = MAX(Id) FROM [dbo].[Logs] WITH (READPAST) WHERE TimeStamp < @maxCreationTime;

                    INSERT INTO @messages ([Message], Severity, [State])
                    SELECT 'Get MAX Id from Table Clustered Index: ' + COALESCE(CAST(@maxId AS nvarchar(10)), '-'), 10, 1;
                END
            END

            INSERT INTO @messages ([Message], Severity, [State])
            SELECT CASE WHEN @maxId IS NULL THEN N'Nothing to clean up before ' + CONVERT(nvarchar(20), @maxCreationTime, 121)  ELSE N'Cleanup Row(s) below Id: ' + CAST(@maxId AS nvarchar(10)) END, 10, 1;

            INSERT INTO @messages ([Message], Severity, [State]) VALUES
                (N'Run until = ' + ISNULL(CONVERT(nvarchar(max), @maxRunDateTime, 121), N'unlimited'), 10, 1 )
        END

        ----------------------------------------------------------------------------------------------------
        -- Create new Run Id
        ----------------------------------------------------------------------------------------------------
        IF @logToTable = 1 
        BEGIN
            INSERT INTO [Maintenance].[Runs]([Type], [Info], [StartTime]) SELECT N'Cleanup Logs', N'PROCEDURE ' + @procName, @startTime;
            SELECT @runId = @@IDENTITY, @SavedToRunId = @@IDENTITY;
            INSERT INTO @messages ([Message], Severity, [State]) VALUES 
                (N'Messages saved to Run Id: ' + CONVERT(nvarchar(10), @runId), 10, 1);
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
        IF @errorCount > 0
        BEGIN
            SET @returnValue = 3;
            SET @message = N'Incorrect Parameters, see previous Error(s): ' + CAST(@errorCount AS nvarchar(10)) + N' found';
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 16, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = 0, @LogsStack = @logsStack OUTPUT;
            -- RETURN 123; => catch
        END
        ----------------------------------------------------------------------------------------------------
        -- End Run on Dry Run
        ----------------------------------------------------------------------------------------------------
        IF @dryRun <> 0 
        BEGIN
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineBreak, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;

            INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
            EXEC sp_executesql @stmt = @stmtEmptyLogsStack, @params = @paramsEmptyLogsStack, @LogsStack = @logsStack OUTPUT;

            SET @returnValue = 1;

            SET @message = 'DRY RUN ONLY (check output and parameters and set @DryRunOnly to No when ready)';
            EXEC [Maintenance].[AddRunMessage] @RunId = @RunId, @Procedure = @procName, @Message = @message, @Severity = 11, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = 0, @LogsStack = @logsStack OUTPUT;
            -- RETURN 1; => catch
        END
        ----------------------------------------------------------------------------------------------------
        -- Remove Saved messages
        ----------------------------------------------------------------------------------------------------
        --DELETE FROM @messages;

        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineBreak, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;

        ----------------------------------------------------------------------------------------------------
        -- Disable Indexes
        ----------------------------------------------------------------------------------------------------
        IF @indexDisable = 1
        BEGIN 
            -- NOT IMPLEMENTED: disable indexes
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = N'Disable Indexes', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = N'### NOT IMPLEMENTED ###: Disable Indexes', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineBreak, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;
            -- NOT IMPLEMENTED: disable indexes

            INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
            EXEC sp_executesql @stmt = @stmtEmptyLogsStack, @params = @paramsEmptyLogsStack, @LogsStack = @logsStack OUTPUT;
        END

        ----------------------------------------------------------------------------------------------------
        -- Cleanup
        ----------------------------------------------------------------------------------------------------
        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;
        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = 'Start Cleanup', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;
        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;
		IF @levelVerbose >= @VerboseBelowLevel 
        BEGIN
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = N'Cleanup in progress... (Verbose not set)', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = 0, @LogsStack = @logsStack OUTPUT;
        END

        INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
        EXEC sp_executesql @stmt = @stmtEmptyLogsStack, @params = @paramsEmptyLogsStack, @LogsStack = @logsStack OUTPUT;

        SELECT @totalIds = 0;
		SET @MinId = 0;
		SET @curIdStart = 0;
		SET @curIdEnd = 0;
		SET @countErrorRetry = 0;
		SET @errorCount = 0;    
		SET @loopStart = SYSDATETIME();

        WHILE 0 = 0
        BEGIN
            BEGIN TRY
                SET @countIds = 0;
                -- Get Ids

                BEGIN TRY
					IF @minId >= @curIdEnd SELECT @curIdStart = MIN(id), @curIdEnd = MAX(id) FROM (SELECT TOP(@deleteTopRows) id = id FROM [dbo].[Logs] WITH (READPAST) WHERE Id >= @minId AND Id <= @maxId AND TimeStamp < @maxCreationTime ORDER BY Id ASC) l(id);
                END TRY
                BEGIN CATCH
	                SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
                    IF @@TRANCOUNT > 0
                    BEGIN
                        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = '   => WARNING (Get Ids): Rollback transaction before retry', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = 0, @LogsStack = @logsStack OUTPUT;
                        ROLLBACK TRAN;
                        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = '   => WARNING (Get Ids): Rollback transaction before retry', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;
                    END

					SET @message = N' ~ Ids [ >=' + CAST(@minId AS nvarchar(10)) + ' ]';
		            IF @countErrorRetry = 0 EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 5, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;

                    SET @message = N'   => WARNING (Get Ids): '+ @ERROR_MESSAGE;
                    EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = @ERROR_STATE, @Number = @ERROR_NUMBER, @Line = @ERROR_LINE, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;

                    THROW
                END CATCH

                -- Cleanup
                BEGIN TRY
                    BEGIN TRAN;

                    WITH del AS (
                        SELECT TOP(@deleteTopRows) Id
                        FROM [dbo].[Logs] WITH (READPAST)
                        WHERE Id >= @curIdStart AND Id <= @maxId AND TimeStamp < @MaxCreationTime 
						ORDER BY Id ASC
                    )
                    DELETE FROM del

                    SELECT @countIds = @@ROWCOUNT;
                    IF @@TRANCOUNT > 0 COMMIT TRAN;
                    --IF @@TRANCOUNT > 0 ROLLBACK TRAN;

                END TRY
                BEGIN CATCH
	                SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
                    IF @@TRANCOUNT > 0
                    BEGIN
                        --RAISERROR('   => WARNING (Delete): Rollback transaction before retry', 10, 1) WITH NOWAIT;
                        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = '   => WARNING (Delete): Rollback transaction before retry', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = 0, @LogsStack = @logsStack OUTPUT;
                        ROLLBACK TRAN;
                        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = '   => WARNING (Delete): Rollback transaction before retry', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;
                    END

					SET @message = N' ~ Ids [ ' + CAST(@curIdStart AS nvarchar(10)) + N'-' + CAST(@curIdEnd AS nvarchar(10)) + ' ]';
		            IF @countErrorRetry = 0 EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 5, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;

                    SET @message = N'   => WARNING (Delete): '+ @ERROR_MESSAGE;
                    EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = @ERROR_STATE, @Number = @ERROR_NUMBER, @Line = @ERROR_LINE, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;

                    THROW;
                END CATCH

            END TRY
            BEGIN CATCH
                SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
                IF @@TRANCOUNT > 0 
                BEGIN 
                    EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = '   => Rollback transaction before retry', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = 0, @LogsStack = @logsStack OUTPUT;
                    ROLLBACK TRAN;
                    EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = '   => Rollback transaction before retry', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;
                END

                IF @countErrorRetry < @maxErrorRetry 
                BEGIN
                    SET @countErrorRetry = @countErrorRetry + 1;
                    SELECT @message = N'   ! Wait before retry: ' + CAST(@errorDelay AS nvarchar(10)) + N'ms [' + CAST(@countErrorRetry AS nvarchar(10)) + '/' +  CAST(@maxErrorRetry AS nvarchar(10)) + ']';
                    EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;
                    WAITFOR DELAY @errorWait;
                    SET @message = N'   + Retry [' + CAST(@countErrorRetry AS nvarchar(10)) + N'/' + CAST(@maxErrorRetry AS nvarchar(10)) + ']';
                    EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;
                    CONTINUE;
                END
                ELSE
                BEGIN;
                    SELECT @message = N'ERROR: Too many retries (' + CAST(@maxErrorRetry AS nvarchar(10)) + ')';
                    EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;
                    THROW;
                END
            END CATCH

            IF @countIds = 0
            BEGIN
                SET @message = N'nothing left to cleanup...';
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;
                BREAK;
            END

            SET @totalIds = @totalIds + @countIds;

            SET @message = N' - Ids [ ' + CAST(@curIdStart AS nvarchar(10)) + N'-' + CAST(@curIdEnd AS nvarchar(10)) + N' ]: deleted (count = ' + CAST(@countIds AS nvarchar(20)) + ', total deleted = ' + FORMAT(@totalIds,'#,0') + N', ' + CAST(DATEDIFF(MILLISECOND, @loopStart, SYSDATETIME()) AS nvarchar(20)) + 'ms)';
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 5, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;

            IF SYSDATETIME() > @maxRunDateTime 
            BEGIN
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineBreak, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;
                SELECT @message = N'TIME OUT:' + (SELECT CAST(DATEDIFF(MINUTE, @startTime, SYSDATETIME()) AS nvarchar(20)) ) + N' min (@MaxRunMinutes = ' + ISNULL(CAST(@MaxRunMinutes AS nvarchar(20)), N'' ) + N')';
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;
                BREAK;
            END 

            SET @minId = @curIdEnd+1;
            SET @countErrorRetry = 0;
			SET @loopStart = SYSDATETIME();

            INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
            EXEC sp_executesql @stmt = @stmtEmptyLogsStack, @params = @paramsEmptyLogsStack, @LogsStack = @logsStack OUTPUT;

        END

        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineBreak, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;
        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;
        SET @message = N'Cleanup finished'
        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;
        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;
        SET @message = N'Rows deleted: ' + ISNULL(FORMAT(@totalIds,'#,0'), '0');
        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;

        SET @message = N'Last Id deleted: ' + ISNULL(CAST(NULLIF(@minId-1, -1) AS nvarchar(max)), '-');
        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;    
        SET @message = N'Elapsed time : ' + CAST(CAST(DATEADD(SECOND, DATEDIFF(SECOND, @startTime, SYSDATETIME()), 0) AS time) AS nvarchar(20));
        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;

        INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
        EXEC sp_executesql @stmt = @stmtEmptyLogsStack, @params = @paramsEmptyLogsStack, @LogsStack = @logsStack OUTPUT;
        ----------------------------------------------------------------------------------------------------
        -- Rebuild Indexes
        ----------------------------------------------------------------------------------------------------
        IF @indexRebuild = 1 
        BEGIN 
            -- NOT IMPLEMENTED: rebuild index
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = N'Rebuild Indexes', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = N'### NOT IMPLEMENTED ###: Rebuild Indexes', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineBreak, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;
            -- NOT IMPLEMENTED: rebuild index
            INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
            EXEC sp_executesql @stmt = @stmtEmptyLogsStack, @params = @paramsEmptyLogsStack, @LogsStack = @logsStack OUTPUT;
        END

        ----------------------------------------------------------------------------------------------------
        -- Runs Cleanup
        ----------------------------------------------------------------------------------------------------
        IF @logToTable = 1 
        BEGIN
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineBreak, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = 'Old Runs cleanup', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;

            EXEC [Maintenance].[DeleteRuns] @CleanupAfterDays = @SavedMessagesRetentionDays, @RunId = @runId, @Procedure = @procName, @LogsStack = @logsStack OUTPUT;
        END

		EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineBreak, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;
		EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;
		EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = 'END (SUCCESS)', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;

        INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
        EXEC sp_executesql @stmt = @stmtEmptyLogsStack, @params = @paramsEmptyLogsStack, @LogsStack = @logsStack OUTPUT;

        SET @returnValue = 0; -- Success
    END TRY
    BEGIN CATCH
        SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();

        -- Record error message
        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @ERROR_MESSAGE, @Severity = @ERROR_SEVERITY, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @RaiseError = 0, @LogsStack = @logsStack OUTPUT;

        IF @@TRANCOUNT > 0 ROLLBACK TRAN;

        INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
        EXEC sp_executesql @stmt = @stmtEmptyLogsStack, @params = @paramsEmptyLogsStack, @LogsStack = @logsStack OUTPUT;

        IF @errorCount = 0
        BEGIN
            IF @dryRun <> 0 
            BEGIN 
                -- Output Message result set
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = 'END (DRY RUN)', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;
            END
            ELSE 
            BEGIN
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineBreak, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;

                SET @message = N'Execution finished with error(s)'
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;

                SET @message = N'Rows deleted: ' + ISNULL(FORMAT(@totalIds,'#,0'), '0');
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;

                SET @message = N'Last Id deleted: ' + CAST(ISNULL(@curIdEnd, N'-') AS nvarchar(max));
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;    

                SET @message = N'Elapsed time : ' + CAST(CAST(DATEADD(SECOND, DATEDIFF(SECOND, @startTime, SYSDATETIME()), 0) AS time) AS nvarchar(20));
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;

                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineBreak, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = 'END (FAIL)', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;

                SET @returnValue = 4
            END
        END
        ELSE
        BEGIN
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = 'END (INCORRECT PARAMETERS)', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @LogsStack = @logsStack OUTPUT;
        END

        RAISERROR(@ERROR_MESSAGE, @ERROR_SEVERITY, @ERROR_STATE);

        SET @returnValue = ISNULL(@returnValue, 255);
    END CATCH

    ----------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------
    INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
    EXEC sp_executesql @stmt = @stmtEmptyLogsStack, @params = @paramsEmptyLogsStack, @LogsStack = @logsStack OUTPUT;

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

