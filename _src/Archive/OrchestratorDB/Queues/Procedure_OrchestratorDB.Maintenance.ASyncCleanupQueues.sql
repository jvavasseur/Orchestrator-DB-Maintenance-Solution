SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- DROP PROCEDURE [Maintenance].[ASyncCleanupQueues]
----------------------------------------------------------------------------------------------------

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[ASyncCleanupQueues]') AND type in (N'P'))
BEGIN
    EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [Maintenance].[ASyncCleanupQueues] AS'
    PRINT '  + CREATE PROCEDURE: [Maintenance].[ASyncCleanupQueues]';
END
ELSE PRINT '  = PROCEDURE [Maintenance].[ASyncCleanupQueues] already exists' 
GO

PRINT '  ~ UPDATE PROCEDURE: [Maintenance].[ASyncCleanupQueues]'
GO  

ALTER PROCEDURE [Maintenance].[ASyncCleanupQueues]
----------------------------------------------------------------------------------------------------
-- ### [Object]: PROCEDURE [Maintenance].[ASyncCleanupQueues]
-- ### [Version]: 2020-10-01 00:00:00                                                         
-- ### [Source]: ??????
-- ### [Hash]: ??????
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
        SELECT N'ERROR: Current SQL Server version is ' + @productVersion + N'. Only version ' + @minProductVersion + + N' or higher is supported.', 16, 1 WHERE @version < @minVersion AND ServerProperty('EngineEdition') NOT IN (5, 8, 9)
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
        -- Check SELECT & DELETE permission on [dbo].[QueueItems]
        INSERT INTO @messages ([Message], Severity, [State])
        SELECT 'Permission not effectively granted: ' + UPPER(p.permission_name), 10, 1
        FROM (VALUES(N'', N'SELECT'), (N'', N'DELETE')) AS p (subentity_name, permission_name)
        LEFT JOIN sys.fn_my_permissions(N'[dbo].[QueueItems]', N'OBJECT') eff ON eff.subentity_name = p.subentity_name AND eff.permission_name = p.permission_name
        WHERE eff.permission_name IS NULL
        ORDER BY p.permission_name;

        IF @@ROWCOUNT > 0 
        BEGIN
            INSERT INTO @messages ([Message], Severity, [State]) VALUES
                (N'Error: missing permission', 10, 1)
                , (N'SELECT and DELETE permissions are required on [dbo].[QueueItems] table', 16, 1);
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
            INSERT INTO [Maintenance].[Runs]([Type], [Info], [StartTime]) SELECT N'Add Archive Queues Trigger', N'PROCEDURE ' + @procName, @startTime;
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
                EXEC [Maintenance].[ValidateASyncArchiveObjectsQueues] @Messages = @synonymMessages OUTPUT, @IsValid = @synonymIsValid OUTPUT;

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
        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = 'Start ASync Queues Cleanup', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

        INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
        EXEC sp_executesql @stmt = @stmtEmptyMessagesStack, @params = @paramsEmptyMessagesStack, @MessagesStack = @MessagesStack OUTPUT;

        INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
        EXEC sp_executesql @stmt = @stmtEmptyMessagesStack, @params = @paramsEmptyMessagesStack, @MessagesStack = @MessagesStack OUTPUT;

        BEGIN TRY
            INSERT INTO @tempListSync([Id], [DeleteAfterDatetime], [FirstASyncId], [LastAsyncId], [CountASyncIds], [IsDeleted], [IsSynced])
            SELECT Id, DeleteAfterDatetime, FirstASyncId, LastASyncId, CountASyncIds, IsDeleted, IsSynced
            FROM [Maintenance].[Synonym_ArchiveSync_Queues] 
            WHERE ( CountASyncIds > 0 AND IsArchived = 1 AND IsDeleted <> 1 AND DeleteAfterDatetime < @startTime )
            UNION
            SELECT syn.Id, syn.DeleteAfterDatetime, syn.FirstASyncId, syn.LastASyncId, syn.CountASyncIds, syn.IsDeleted, syn.IsSynced
            FROM [Maintenance].[ASyncStatus_Queues] sts 
            INNER JOIN [Maintenance].[Synonym_ArchiveSync_Queues] syn ON syn.Id = sts.SyncId
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
                IF EXISTS(SELECT 1 FROM [Maintenance].[ASyncStatus_Queues] WHERE [SyncId] = @cursorSyncId AND [IsDeleted] = 1)
                BEGIN
                    SELECT @message = @message + IIF(@cursorIsSynced = 1, N' remove (already deleted and synced)', N'skip (deleted but not synced yet)')
                    EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                    DELETE FROM [Maintenance].[ASyncStatus_Queues] WHERE [SyncId] = @cursorSyncId AND [IsDeleted] = 1 AND @cursorIsSynced = 1;
                    FETCH CursorSyncIds INTO @cursorSyncId, @cursorDeleteAfterDatetime, @cursorFirstASyncId, @cursorLastAsyncId, @cursorCountASyncIds, @cursorIsDeleted, @cursorIsSynced;
                    CONTINUE;
                END

                SELECT @message = @message + N'Start';
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                INSERT INTO [Maintenance].[ASyncStatus_Queues]([SyncId], [FirstASyncId]) SELECT @cursorSyncId, @cursorFirstASyncId WHERE NOT EXISTS(SELECT 1 FROM [Maintenance].[ASyncStatus_Queues] WHERE [SyncId] = @cursorSyncId)
                SELECT @firstId = COALESCE([LastASyncId] + 1, [FirstASyncId], @cursorFirstASyncId) FROM [Maintenance].[ASyncStatus_Queues] WHERE [SyncId] = @cursorSyncId
                UPDATE [Maintenance].[ASyncStatus_Queues] SET [FirstASyncId] = @firstId WHERE [SyncId] = @cursorSyncId

                SET @totalIds = 0;
                WHILE 0 >= 0
                BEGIN
                    INSERT INTO #tempIds(Id)
                    SELECT TOP(@maxLoopDeleteRows) Id FROM [Maintenance].[Synonym_ASyncDelete_Queues] WHERE [SyncId] = @cursorSyncId AND Id >= @firstId ORDER BY Id ASC;
                    IF @@ROWCOUNT = 0
                    BEGIN
                        SET @message = SPACE(@tab * 1) + 'Cleanup finished (total = ' + CAST(@totalIds AS nvarchar(100)) + N')';
                        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                        UPDATE stt SET [IsDeleted] = 1, [DeletedOnDate] = SYSDATETIME() FROM [Maintenance].[ASyncStatus_Queues] stt WHERE [SyncId] = @cursorSyncId;
                        BREAK;
                    END
                    SELECT @firstId = MIN(Id), @lastId = MAX(Id), @countIds = COUNT(*) FROM #tempIds;

                    BEGIN TRAN
                    DELETE ent FROM [dbo].[QueueItemComments] src INNER JOIN #tempIds ids ON ids.Id = src.QueueItemId;                    
                    DELETE ent FROM [dbo].[QueueItemEvents] src INNER JOIN #tempIds ids ON ids.Id = src.QueueItemId;                    
                    DELETE lgs FROM [dbo].[QueueItems] src INNER JOIN #tempIds ids ON ids.Id = src.Id;                    
                    UPDATE stt SET [LastAsyncId] = @lastId FROM [Maintenance].[ASyncStatus_Queues] stt WHERE [SyncId] = @cursorSyncId;
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
