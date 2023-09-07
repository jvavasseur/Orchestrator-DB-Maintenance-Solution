SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- DROP PROCEDURE [Maintenance].[ArchiveAuditLogs]
----------------------------------------------------------------------------------------------------

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[ArchiveAuditLogs]') AND type in (N'P'))
BEGIN
    EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [Maintenance].[ArchiveAuditLogs] AS'
    PRINT '  + CREATE PROCEDURE: [Maintenance].[ArchiveAuditLogs]';
END
ELSE PRINT '  = PROCEDURE [Maintenance].[ArchiveAuditLogs] already exists' 
GO

PRINT '  ~ UPDATE PROCEDURE: [Maintenance].[ArchiveAuditLogs]'
GO  

ALTER PROCEDURE [Maintenance].[ArchiveAuditLogs]
----------------------------------------------------------------------------------------------------
-- ### [Object]: PROCEDURE [Maintenance].[ArchiveAuditLogs]
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
    , @MaxConcurrentFilters int = NULL -- NULL or 0 = default value => number of filter in chronoligical order processed by a single batch
    /* Loop Limits */
    , @MaxRunMinutes int = NULL -- NULL or 0 = unlimited
    , @MaxBatchesLoops int = NULL -- NULL or 0 - unlimited
    /* Dry Run */
--    , @DryRunOnly nvarchar(MAX) = NULL -- Y{es} or N{o} => Only Check Parameters (default if NULL = Y)
    /* Delete settings */
	, @SynchronousDeleteIfNoDelay bit = 0
	, @IgnoreDeleteDelay bit = 0

    /* Archive table(s) settings */
    , @CreateArchiveTable nvarchar(MAX) = NULL -- Y{es} or N{o} => Archive table refered by Synonym is create when missing (default if NULL or empty = N)
    , @UpdateArchiveTable nvarchar(MAX) = NULL -- Y{es} or N{o} => Archive table refered by Synonym is update when column(s) are missing (default if NULL or empty = N)
    , @ExcludeColumns nvarchar(MAX) = NULL -- JSON string with array of string with column name(s)
    , @ExcludeEntitiesColumns nvarchar(MAX) = NULL -- JSON string with array of string with column name(s)
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
        DECLARE @maxCreationTime datetime;
        DECLARE @MaxErrorRetry tinyint;
        DECLARE @errorDelay smallint;
        DECLARE @errorWait datetime;
        DECLARE @returnValue int = 1;
        ----------------------------------------------------------------------------------------------------
        -- Filters
        ----------------------------------------------------------------------------------------------------
--        DECLARE @listFilters TABLE (OrderId smallint, ArchiveId bigint, SyncId bigint, CurrentId bigint, TargetId bigint, LastId bigint, TargetTimestamp datetime, PreviousTimestamp datetime , TenantId int, LevelId int, DeleteOnly bit, NoDelay bit, PRIMARY KEY(TenantId, LevelId));
        DECLARE @countFilterIds int;
        DECLARE @countArchiveIds int;
        DECLARE @targetTimestamp datetime;
        ----------------------------------------------------------------------------------------------------
        -- Archive settings
        ----------------------------------------------------------------------------------------------------
        DECLARE @topLoopFilters smallint;
        DECLARE @maxBatches smallint;
        ----------------------------------------------------------------------------------------------------
        -- Archive loop
        ----------------------------------------------------------------------------------------------------
        DECLARE @minId bigint = 0;
        DECLARE @maxId bigint;
        DECLARE @currentId bigint
        DECLARE @currentLoopId bigint;
        DECLARE @maxLoopDeleteRows int;
        DECLARE @loopCount int = 0;
        DECLARE @batchCount int = 0;
        ----------------------------------------------------------------------------------------------------
        -- Archive query
        ----------------------------------------------------------------------------------------------------
        DECLARE @archivedColumns nvarchar(MAX);
        DECLARE @sqlArchive nvarchar(MAX);
        DECLARE @archivedEntitiesColumns nvarchar(MAX);
        DECLARE @sqlArchiveEntities nvarchar(MAX);
        ----------------------------------------------------------------------------------------------------
        -- Delete 
        ----------------------------------------------------------------------------------------------------
        DECLARE @deleteIfNoDelay bit = 0
        DECLARE @ignoreDelay bit = 0
        ----------------------------------------------------------------------------------------------------
        -- Count row Ids
        ----------------------------------------------------------------------------------------------------
        DECLARE @countRowIds bigint;
        DECLARE @totalRowIds bigint;
        DECLARE @globalRowIds bigint;
        DECLARE @countDeleteOnlyIds int;
        DECLARE @totalDeleteOnlyIds int;
        DECLARE @globalDeleteOnlyIds bigint;
        DECLARE @countDeleted int;
        DECLARE @totalDeleted int;
        DECLARE @globalDeleted int;
        DECLARE @filtersArchived bigint;
        DECLARE @globalFiltersArchived bigint;
        DECLARE @syncArchived BIGINT
        DECLARE @triggerArchived bigint;
        DECLARE @globalTriggerArchived bigint;
        ----------------------------------------------------------------------------------------------------
        -- Archive Repeat
        ----------------------------------------------------------------------------------------------------
        DECLARE @cursorId bigint;
        DECLARE @cursorName nvarchar(100);
        DECLARE @cursorPreviousRunIds nvarchar(MAX);
        DECLARE @cursorDefinition nvarchar(max);
        DECLARE @cursorArchiveTriggerTime datetime;
        DECLARE @cursorArchiveAfterHours smallint;
        DECLARE @cursorDeleteDelayHours smallint;
        DECLARE @cursorRepeatArchive bit;
        DECLARE @cursorRepeatOffsetHours smallint;
        DECLARE @cursorRepeatUntil datetime;
        DECLARE @cursorCountValidFilters int;
        DECLARE @countRepeat int;

        ----------------------------------------------------------------------------------------------------
        -- Constant / Default value
        ----------------------------------------------------------------------------------------------------
        DECLARE @maxDeleteRows int = 100*1000; -- Raise an error if @RowsDeletedForEachLoop is bigger than this value
        DECLARE @minDeleteRows int = 1*1000; -- Raise an error if @RowsDeletedForEachLoop is smaller than this value
        DECLARE @defaultDeleteRows int = 10*1000;
        DECLARE @verboseBelowLevel int = 10; -- don't print message with Severity < 10 unless Verbose is set to Y

        DECLARE @maxCountFilters int = 10*1000 -- Raise an error if @MaxConcurrentFilters is bigger than this value
        DECLARE @minCountFilters int = 1 -- Raise an error if @MaxConcurrentFilters is bigger than this value
        DECLARE @defaultCountFilters int = 1000 -- used when @MaxConcurrentFilters is null

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
        DECLARE @sourceColumns nvarchar(MAX);
        DECLARE @sourceEntitiesColumns nvarchar(MAX);
        DECLARE @synonymIsValid bit;
        DECLARE @synonymCreateTable bit;
        DECLARE @synonymUpdateTable bit;
        DECLARE @synonymExcludeColumns nvarchar(MAX) 
        DECLARE @synonymExcludeEntitiesColumns nvarchar(MAX) 
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

        -- Check @MaxConcurrentFilters
        SET @topLoopFilters = ISNULL(NULLIF(@MaxConcurrentFilters, 0), @defaultCountFilters);
        IF @topLoopFilters < @minCountFilters OR @topLoopFilters > @maxCountFilters 
        BEGIN
            INSERT INTO @messages ([Message], Severity, [State]) VALUES
                ( 'Parameter @MaxConcurrentFilters is invalid: ' + LTRIM(RTRIM(CAST(@MaxConcurrentFilters AS nvarchar(MAX)))), 10, 1)
                , ('USAGE: use a value between ' + CAST(@minCountFilters AS nvarchar(MAX)) + N' and ' + CAST(@defaultCountFilters AS nvarchar(MAX)) +  N'', 10, 1)
                , ('Parameter @RowsDeletedForEachLoop is invalid', 16, 1);
        END

        -- Check @MaxBatchesLoops
        SET @maxBatches = NULLIF(@MaxBatchesLoops, 0);

        -- Check @SynchronousDeleteIfNoDelay
        SELECT @deleteIfNoDelay = [value] FROM @paramsYesNo WHERE [parameter] = LTRIM(RTRIM(@SynchronousDeleteIfNoDelay));
        INSERT INTO @messages ([Message], Severity, [State]) SELECT N'@SynchronousDeleteIfNoDelay is NULL or empty. Default value will be used (No)', 10, 1 WHERE @deleteIfNoDelay IS NULL;
        SET @deleteIfNoDelay = ISNULL(@deleteIfNoDelay, 0);

        -- Check @IgnoreDeleteDelay
        SELECT @ignoreDelay = [value] FROM @paramsYesNo WHERE [parameter] = LTRIM(RTRIM(@IgnoreDeleteDelay));
        INSERT INTO @messages ([Message], Severity, [State]) SELECT N'@IgnoreDeleteDelay is NULL or empty. Default value will be used (No).', 10, 1 WHERE @ignoreDelay IS NULL;
        INSERT INTO @messages ([Message], Severity, [State]) SELECT N'@SynchronousDeleteIfNoDelay must be set if @IgnoreDeleteDelay is set', 16, 1 WHERE @ignoreDelay = 1 AND @deleteIfNoDelay <> 1;
        SET @ignoreDelay = ISNULL(@ignoreDelay, 0);

        -- Check Create Archive Table
        SELECT @synonymCreateTable = [value] FROM @paramsYesNo WHERE [parameter] = LTRIM(RTRIM(@CreateArchiveTable));
        INSERT INTO @messages ([Message], Severity, [State]) SELECT N'@CreateArchiveTable is NULL or empty. Default value will be used (No).', 10, 1 WHERE @synonymCreateTable IS NULL;
        SET @synonymCreateTable = ISNULL(@synonymCreateTable, 0);

        -- Check Update Archive Table
        SELECT @synonymUpdateTable = [value] FROM @paramsYesNo WHERE [parameter] = LTRIM(RTRIM(@UpdateArchiveTable));
        INSERT INTO @messages ([Message], Severity, [State]) SELECT N'@UpdateArchiveTable is NULL or empty. Default value will be used (No).', 10, 1 WHERE @synonymUpdateTable IS NULL;
        SET @synonymUpdateTable = ISNULL(@synonymUpdateTable, 0);

        -- Check Exclude columns
        SELECT @synonymExcludeColumns = NULLIF(LTRIM(RTRIM(@ExcludeColumns)), N'');
        IF ISJSON(@synonymExcludeColumns) = 0 
        INSERT INTO @messages ([Message], Severity, [State]) SELECT N'ERROR: @synonymExcludeColumns is not a valid JSON string with an array of string(s) ["col1", "col2", ...]', 16, 1 WHERE ISJSON(@synonymExcludeColumns) = 0;

        -- Check Exclude Entities columns
        SELECT @synonymExcludeEntitiesColumns = NULLIF(LTRIM(RTRIM(@ExcludeEntitiesColumns)), N'');
        IF ISJSON(@synonymExcludeEntitiesColumns) = 0 
        INSERT INTO @messages ([Message], Severity, [State]) SELECT N'ERROR: @synonymExcludeEntitiesColumns is not a valid JSON string with an array of string(s) ["col1", "col2", ...]', 16, 1 WHERE ISJSON(@synonymExcludeEntitiesColumns) = 0;

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
/*        -- Check SELECT & DELETE permission on [dbo].[AuditLogs]
        INSERT INTO @messages ([Message], Severity, [State])
        SELECT 'Permission not effectively granted: ' + UPPER(p.permission_name), 10, 1
        FROM (VALUES(N'', N'SELECT'), (N'', N'DELETE')) AS p (subentity_name, permission_name)
        LEFT JOIN sys.fn_my_permissions(N'[dbo].[AuditLogs]', N'OBJECT') eff ON eff.subentity_name = p.subentity_name AND eff.permission_name = p.permission_name
        WHERE eff.permission_name IS NULL
        ORDER BY p.permission_name;
*/
        IF @@ROWCOUNT > 0 
        BEGIN
            INSERT INTO @messages ([Message], Severity, [State]) VALUES
                (N'Error: missing permission', 10, 1)
                , (N'SELECT and DELETE permissions are required on [dbo].[AuditLogs] table', 16, 1);
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
            INSERT INTO @messages ([Message], Severity, [State]) VALUES
                (N'Max batches = ' + ISNULL(CONVERT(nvarchar(MAX), @maxBatches, 121), N'unlimited'), 10, 1 )
		END

        ----------------------------------------------------------------------------------------------------
        -- Create new Run Id
        ----------------------------------------------------------------------------------------------------
        INSERT INTO @messages ([Message], Severity, [State]) 
        SELECT N'Parameter Run Id [' + CAST(@SavedToRunId AS nvarchar(100)) + N'] is already ended.' , 10, 1 FROM [Maintenance].[Runs] WHERE Id = @SavedToRunId AND EndDate IS NOT NULL
        UNION ALL SELECT N'Parameter Run Id [' + CAST(@SavedToRunId AS nvarchar(100)) + N'] not found.', 10, 1 WHERE @SavedToRunId IS NOT NULL AND NOT EXISTS (SELECT 1 FROM [Maintenance].[Runs] WHERE Id = @SavedToRunId)
        
        IF @SavedToRunId IS NULL OR NOT EXISTS(SELECT 1 FROM [Maintenance].[Runs] WHERE Id = @SavedToRunId AND EndDate IS NULL)
        BEGIN
            INSERT INTO [Maintenance].[Runs]([Type], [Info], [StartTime]) SELECT N'Add Archive AuditLogs Trigger', N'PROCEDURE ' + @procName, @startTime;
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
                EXEC [Maintenance].[ValidateArchiveObjectsAuditLogs] @Messages = @synonymMessages OUTPUT, @IsValid = @synonymIsValid OUTPUT, @CreateTable = @synonymCreateTable, @UpdateTable = @synonymUpdateTable, @SourceColumns = @sourceColumns OUTPUT, @ExcludeColumns = @synonymExcludeColumns, @sourceEntitiesColumns = @sourceEntitiesColumns OUTPUT, @synonymExcludeEntitiesColumns = @synonymExcludeEntitiesColumns OUTPUT;

                INSERT INTO @messages ([Procedure], [Message], Severity, [State])
                SELECT [Procedure], [Message], [Severity], [State] FROM OPENJSON(@synonymMessages, N'$') WITH ([Procedure] nvarchar(MAX), [Message] nvarchar(MAX), [Severity] tinyint, [State] tinyint);

                INSERT INTO @messages ([Message], Severity, [State])
                SELECT 'ERORR: Synonyms and archive/source tables checks failed, see previous errors', 16, 1 WHERE ISNULL(@synonymIsValid, 0) = 0
            END TRY
            BEGIN CATCH
                -- Get Unknown error
                SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();

                INSERT INTO @messages ([Procedure], [Message], Severity, [State])
                SELECT [Procedure], [Message], [Severity], [State] FROM OPENJSON(@synonymMessages, N'$') WITH ([Procedure] nvarchar(MAX), [Message] nvarchar(MAX), [Severity] tinyint, [State] tinyint);

                INSERT INTO @messages ([Message], Severity, [State]) VALUES 
                    (N'ERORR: error(s) occured while checking synonyms and archive/source tables', 16, 1);
                THROW;
            END CATCH
        END
        -- Prepare columns and archive query
        IF NOT EXISTS(SELECT 1 FROM @messages WHERE severity > 10)
        BEGIN
            BEGIN TRY
                SELECT @archivedColumns = NULL;
                SELECT @archivedColumns = COALESCE(@archivedColumns + N', ' + QUOTENAME([value]), QUOTENAME([value])) FROM OPENJSON(@sourceColumns)
                SELECT @sqlArchive = N'
                    INSERT INTO [Maintenance].[Synonym_Archive_AuditLogs](' + @archivedColumns + N')
                    SELECT ' + @archivedColumns + N' 
                    FROM #tempListIds ids
                    INNER JOIN [Maintenance].[Synonym_Source_AuditLogs] src ON ids.tempId = src.Id
                    WHERE tempDeleteOnly = 0 AND NOT EXISTS(SELECT 1 FROM [Maintenance].[Synonym_Archive_AuditLogs] WHERE Id = ids.tempId)';
                INSERT INTO @messages ([Message], Severity, [State]) VALUES 
                (N'Archive query: ' + ISNULL(@sqlArchive, N'-'), 10, 1);
            END TRY
            BEGIN CATCH
                -- Get Unknown error
                SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
                -- Save Unknwon Errror
                INSERT INTO @messages ([Message], Severity, [State]) VALUES 
                    (N'ERORR: error(s) occured while preparing archive SQL query', 16, 1);
                THROW;
            END CATCH
        END
        -- Prepare Entities columns and archive query
        IF NOT EXISTS(SELECT 1 FROM @messages WHERE severity > 10)
        BEGIN
            BEGIN TRY
                SELECT @archivedEntitiesColumns = NULL;
                SELECT @archivedEntitiesColumns = COALESCE(@archivedEntitiesColumns + N', ' + QUOTENAME([value]), QUOTENAME([value])) FROM OPENJSON(@sourceEntitiesColumns)
                SELECT @sqlArchive = N'
                    INSERT INTO [Maintenance].[Synonym_Archive_AuditLogsEntities](' + @archivedEntitiesColumns + N')
                    SELECT ' + @archivedEntitiesColumns + N' 
                    FROM #tempListIds ids
                    INNER JOIN [Maintenance].[Synonym_Source_AuditLogsEntities] src ON ids.tempId = src.AuditLogId
                    WHERE tempDeleteOnly = 0 AND NOT EXISTS(SELECT 1 FROM [Maintenance].[Synonym_Archive_AuditLogsEntities] WHERE Id = ids.tempId)';
                INSERT INTO @messages ([Message], Severity, [State]) VALUES 
                (N'Archive Entities query: ' + ISNULL(@sqlArchive, N'-'), 10, 1);
            END TRY
            BEGIN CATCH
                -- Get Unknown error
                SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
                -- Save Unknwon Errror
                INSERT INTO @messages ([Message], Severity, [State]) VALUES 
                    (N'ERORR: error(s) occured while preparing archive Entities SQL query', 16, 1);
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
        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = 'Start Archive', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

        INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
        EXEC sp_executesql @stmt = @stmtEmptyMessagesStack, @params = @paramsEmptyMessagesStack, @MessagesStack = @MessagesStack OUTPUT;

        INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
        EXEC sp_executesql @stmt = @stmtEmptyMessagesStack, @params = @paramsEmptyMessagesStack, @MessagesStack = @MessagesStack OUTPUT;

        -- Create temp table (Filter List / Repeat Triggers)
        DROP TABLE IF EXISTS #tempListFilters;
        CREATE TABLE #tempListFilters(OrderId smallint, ArchiveId bigint, SyncId bigint, CurrentId bigint, TargetId bigint, LastId bigint, TargetTimestamp datetime, PreviousTimestamp datetime , TenantId int, DeleteOnly bit, NoDelay bit, PRIMARY KEY(TenantId, SyncId));
        DROP TABLE IF EXISTS #tempRepeatTriggersTable;
        CREATE TABLE #tempRepeatTriggersTable(Id bigint, [Name] nvarchar(100), /*[PreviousRunIds] bigint,*/ [Definition] nvarchar(MAX)/*, TargetTimestamp*/, ArchiveTriggerTime datetime, ArchiveAfterHours smallint, DeleteDelayHours smallint, RepeatArchive bit, RepeatOffsetHours smallint, RepeatUntil datetime, CountValidFilters int)

        -- Batch Loop 
        WHILE 0 >= 0
        BEGIN
            SET @batchCount = @batchCount + 1;
            SET @message = N'Start new filter batch [' + CAST(@batchCount AS nvarchar(100)) + N']';
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = 0, @MessagesStack = @MessagesStack OUTPUT;
            BEGIN TRY
                -- Init #tempListFilters
                TRUNCATE TABLE #tempListFilters;
                SELECT @totalRowIds = 0, @totalDeleteOnlyIds = 0, @totalDeleted = 0

                --  Get Filters
                INSERT INTO #tempListFilters(ArchiveId, SyncId, CurrentId, TargetId, TargetTimestamp, PreviousTimestamp, TenantId, DeleteOnly, NoDelay, OrderId)
                SELECT TOP(@topLoopFilters) snc.ArchiveId, flt.SyncId, CurrentId = ISNULL(flt.CurrentId, 0), flt.TargetId, flt.TargetTimeStamp, flt.PreviousTimestamp , flt.TenantId, flt.DeleteOnly
                    , IIF( @deleteIfNoDelay = 1 AND (snc.DeleteAfterDatetime = flt.TargetTimestamp OR @ignoreDelay = 1), 1, 0)
                    , OrderId = ROW_NUMBER() OVER(PARTITION BY flt.TenantId ORDER BY flt.TargetTimeStamp ASC)
                FROM [Maintenance].[Filter_AuditLogs] flt 
                INNER JOIN [Maintenance].[Sync_AuditLogs] snc ON snc.Id = flt.SyncId
                INNER JOIN [Maintenance].[Archive_AuditLogs] arc ON arc.Id = snc.ArchiveId
                WHERE flt.IsArchived = 0 AND snc.IsArchived = 0 AND (flt.TargetId IS NULL OR flt.CurrentId IS NULL OR flt.CurrentId < flt.TargetId) AND arc.ToDo = 1 AND arc.ArchiveTriggerTime < @StartTime
                ORDER BY flt.PreviousTimestamp ASC, TargetTimestamp ASC;

                SELECT @countFilterIds = ISNULL(COUNT(*), 0), @countArchiveIds = ISNULL(COUNT(DISTINCT ArchiveId), 0) FROM #tempListFilters;
                SELECT @targetTimestamp = MAX(TargetTimeStamp) FROM #tempListFilters WHERE TargetTimeStamp IS NOT NULL;

                SELECT @maxId = MAX(Id) FROM [Maintenance].[Synonym_Source_AuditLogs] WITH(INDEX([IX_Machine])) WHERE TimeStamp <= @targetTimestamp;
                DECLARE @maxTargetId bigint;

                SELECT @maxTargetId = MAX(ISNULL(TargetId, 0)) FROM #tempListFilters;
                IF @maxId < @maxTargetId SET @maxId = @maxTargetId;
            END TRY
            BEGIN CATCH
                -- Get Unknown error
                SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
                -- Save Unknwon Errror
                SET @message = N'  Error(s) occured while retrieving filters';
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = @ERROR_STATE, @Number = @ERROR_NUMBER, @Line = @ERROR_LINE, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                THROW;
            END CATCH

            ----------------------------------------------------------------------------------------------------
            -- Add Repeat Trigger(s)
            ----------------------------------------------------------------------------------------------------
            BEGIN TRY
                -- Init #tempRepeatTriggersTable
                TRUNCATE TABLE #tempRepeatTriggersTable;

                INSERT INTO #tempRepeatTriggersTable(Id, [Name], [Definition], ArchiveTriggerTime, ArchiveAfterHours, DeleteDelayHours, RepeatArchive, RepeatOffsetHours, RepeatUntil, CountValidFilters)
                SELECT Id, [Name]/*, [PreviousRunIds]*/, [Definition]/*, TargetTimestamp*/, CAST( CAST(ArchiveTriggerTime AS float(53)) + ABS(@floatingHour * RepeatOffsetHours) AS datetime), ArchiveAfterHours, DeleteDelayHours, RepeatArchive, RepeatOffsetHours, RepeatUntil, CountValidFilters
                FROM [Maintenance].[Archive_AuditLogs] arc 
                WHERE arc.[ToDo] = 1 AND arc.RepeatArchive = 1 AND (RepeatUntil IS NULL OR RepeatUntil >= CAST( CAST(ArchiveTriggerTime AS float(53)) + ABS(@floatingHour * RepeatOffsetHours) AS datetime))
                    AND NOT EXISTS (SELECT  1 FROM [Maintenance].[Archive_AuditLogs] WHERE Id > arc.Id AND ParentArchiveId = arc.Id) 
                    AND ( (arc.CountValidFilters = 0 AND arc.[TargetTimestamp] <= ISNULL(@targetTimestamp, SYSDATETIME())) OR EXISTS(SELECT 1 FROM #tempListFilters WHERE ArchiveId = arc.Id) ) 
                ORDER BY Id ASC;

                SELECT @countRepeat = COUNT(*) FROM #tempRepeatTriggersTable;
                IF @countRepeat > 0 
                BEGIN
                    SET @message = SPACE(@tab * 1) + N'Repeat Archive Trigger(s)... [' + CAST(@countRepeat AS nvarchar(100)) + N']';
                    EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                    UPDATE arc SET PreviousRunIds = (
                        SELECT [runid], [message], [timestamp] FROM (
                            SELECT [runid] = @runId, [message] = N'Repeat Archive Trigger', [timestamp] = SYSDATETIME()
                            UNION ALL SELECT [runid], [message], [timestamp] FROM OPENJSON(PreviousRunIds) WITH ([runid] nvarchar(MAX) N'$.runid', [message] nvarchar(MAX) N'$.message', [timestamp] datetime2 N'$.timestamp')
                        ) v FOR JSON PATH
                    )
                    FROM [Maintenance].[Archive_AuditLogs] arc 
                    INNER JOIN #tempRepeatTriggersTable tbl ON tbl.Id = arc.Id;

                    IF CURSOR_STATUS('local', 'CursorRepeatClose') >= 0 CLOSE CursorRepeatClose;
                    IF CURSOR_STATUS('local', 'CursorRepeatClose') >= -1 DEALLOCATE CursorRepeatClose;

                    DECLARE CursorRepeatClose CURSOR FAST_FORWARD LOCAL FOR 
                        SELECT Id, [Name]/*, [PreviousRunIds]*/, [Definition]/*, TargetTimestamp*/, ArchiveTriggerTime, ArchiveAfterHours, DeleteDelayHours, RepeatArchive, RepeatOffsetHours, RepeatUntil, CountValidFilters FROM #tempRepeatTriggersTable;

                    OPEN CursorRepeatClose;
                    FETCH CursorRepeatClose INTO @cursorId, @cursorName, @cursorDefinition, @cursorArchiveTriggerTime, @cursorArchiveAfterHours, @cursorDeleteDelayHours, @cursorRepeatArchive, @cursorRepeatOffsetHours, @cursorRepeatUntil, @cursorCountValidFilters

                    -- Add Archive Trigger(s) (REPEAT)
                    IF CURSOR_STATUS('local', 'CursorRepeatClose') = 1
                    BEGIN
                        WHILE @@FETCH_STATUS = 0
                        BEGIN;
                            IF @cursorRepeatArchive = 1
                            BEGIN
                                SET @message = SPACE(@tab * 2) + N'Repeat Archive Id [' + CAST(@cursorId AS nvarchar(100)) + N'] on ' + CONVERT(nvarchar(100), @cursorArchiveTriggerTime, 120);
                                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                                EXEC [Maintenance].[AddArchiveTriggerAuditLogs] @SavedTorunId = @runId, @DryRunOnly = 0, @Name = @cursorName, @ArchiveTriggerTime = @cursorArchiveTriggerTime, @ArchiveAfterHours = @cursorArchiveAfterHours, @DeleteDelayHours = @cursorDeleteDelayHours, @Filters = @cursorDefinition, @RepeatArchive = @cursorRepeatArchive, @RepeatOffsetHours = @cursorRepeatOffsetHours, @RepeatUntil = @cursorRepeatUntil, @ParentArchiveId = @cursorId
                            END
                            FETCH CursorRepeatClose INTO @cursorId, @cursorName, @cursorDefinition, @cursorArchiveTriggerTime, @cursorArchiveAfterHours, @cursorDeleteDelayHours, @cursorRepeatArchive, @cursorRepeatOffsetHours, @cursorRepeatUntil, @cursorCountValidFilters
                        END
                    END
                    ELSE 
                    BEGIN
                        SET @message = 'Execution has been canceled: Error Opening Messages Cursor (duplicate triggers)';
                        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                    END

                    IF CURSOR_STATUS('local', 'CursorRepeatClose') >= 0 CLOSE CursorRepeatClose;
                    IF CURSOR_STATUS('local', 'CursorRepeatClose') >= -1 DEALLOCATE CursorRepeatClose;
                END
            END TRY
            BEGIN CATCH
                -- Get Unknown error
                SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
                -- Save Unknwon Errror
                SET @message = N'  Error(s) occured while duplicating triggers';
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = @ERROR_STATE, @Number = @ERROR_NUMBER, @Line = @ERROR_LINE, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                THROW;
            END CATCH

            INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
            EXEC sp_executesql @stmt = @stmtEmptyMessagesStack, @params = @paramsEmptyMessagesStack, @MessagesStack = @MessagesStack OUTPUT;

            IF @countFilterIds < 1
            BEGIN
                SET @message = SPACE(@tab * 1) + N'No remaining filters found in [Maintenance].[Filter_AuditLogs]'
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
            END
            ELSE
            BEGIN
                -- Update status for Archive with no valid filter
                IF EXISTS (SELECT 1 FROM [Maintenance].[Archive_AuditLogs] arc WHERE arc.[ToDo] = 1 AND arc.CountValidFilters = 0 AND arc.[TargetTimestamp] < @targetTimestamp)
                BEGIN
                    SELECT @message = NULL;
                    SELECT  @message = COALESCE(@message + ', ' + CAST(Id AS nvarchar(100)), CAST(Id AS nvarchar(100)) ) FROM [Maintenance].[Archive_AuditLogs] arc WHERE arc.[ToDo] = 1 AND arc.CountValidFilters = 0 AND arc.[TargetTimestamp] <= @targetTimestamp ORDER BY Id;
                    SET @message = SPACE(@tab * 1) + N'Close Archive Trigger(s) with no filter(s): ' + @message;
                    EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                    UPDATE arc SET IsFinished = 1, FinishedOnDate = SYSDATETIME(), CurrentRunId = @runid, [Message] = N'Archive Trigger finished (no filters)'
                        , PreviousRunIds = (
                        SELECT [runid], [message], [timestamp] FROM (
                            SELECT [runid] = @runId, [message] = N'Archive Trigger finished (no filters)', [timestamp] = SYSDATETIME()
                            UNION ALL SELECT [runid], [message], [timestamp] FROM OPENJSON(PreviousRunIds) WITH ([runid] nvarchar(MAX) N'$.runid', [message] nvarchar(MAX) N'$.message', [timestamp] datetime2 N'$.timestamp')
                        ) v FOR JSON PATH
                    )
                    FROM [Maintenance].[Archive_AuditLogs] arc WHERE arc.[ToDo] = 1 AND arc.CountValidFilters = 0 AND arc.[TargetTimestamp] <= @targetTimestamp;
                END    

                SET @message =  SPACE(@tab * 1) + N'Archive Filter(s) found: ' + ISNULL(CAST(@countFilterIds AS nvarchar(100)), N'-');
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                SET @message = NULL;
                SELECT  @message = COALESCE(@message + ', ' + [Archive], [Archive]) FROM (SELECT [ArchiveId], [Archive] = CAST(ArchiveId AS nvarchar(100)) + N'('+ CAST(COUNT(*) AS nvarchar(100)) + N')' FROM #tempListFilters GROUP BY [ArchiveId]) flt ORDER BY ArchiveId;
                SET @message =  SPACE(@tab * 2) + N'Processing Trigger Archive Id(s): ' + @message;
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                SET @message =  SPACE(@tab * 2) + N'Target timestamp: ' + ISNULL(CONVERT(nvarchar(100), @targetTimestamp, 120), N'-');
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                SET @message =  SPACE(@tab * 2) + N'Target Id: ' + ISNULL(CAST(@maxId AS nvarchar(100)), N'-');
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                -- Update TargetId 
                UPDATE #tempListFilters SET TargetId = @maxId; -- IIF(@maxId > TargetId OR TargetId IS NULL, @maxId, TargetId);

                BEGIN TRY
                    ---------------------------------------------------------------------------------------------------+
                    -- START TRANSACTION BLOCK                                                                         |
                    ---------------------------------------------------------------------------------------------------+
                    BEGIN TRAN;                                                                                      --|
                                                                                                                     --|
                    UPDATE flt SET TargetId = lst.TargetId                                                           --|
                    FROM [Maintenance].[Filter_AuditLogs] flt                                                        --|
                    INNER JOIN #tempListFilters lst ON lst.SyncId = flt.SyncId AND lst.TenantId = flt.TenantId;      --|
                                                                                                                     --|
                    -- Add Last Current Id to each 1st filter per Tenant from most recent Archiving trigger(s) per Tenant
                    UPDATE lst SET LastId = oap.LastId FROM #tempListFilters lst                                     --|
                    CROSS APPLY (                                                                                    --|
                        SELECT LastId = MAX(TargetId)                                                                --|
                        FROM [Maintenance].[Filter_AuditLogs] flt                                                    --|
                        WHERE flt.TenantId = lst.TenantId AND flt.IsArchived = 1 AND flt.CurrentId = flt.TargetId    --|
                    ) oap                                                                                            --|
                    WHERE lst.OrderId = 1;                                                                           --|
                                                                                                                     --|
                    UPDATE arc SET PreviousRunIds = (                                                                --|
                        SELECT [runid], [message], [timestamp] FROM (                                                --|
                            SELECT [runid] = @runId, [message] = N'Process Filter(s) [' + CAST((SELECT COUNT(*) FROM #tempListFilters WHERE ArchiveId = arc.Id) AS nvarchar(100)) + N']', [timestamp] = SYSDATETIME()
                            UNION ALL SELECT [runid], [message], [timestamp] FROM OPENJSON(PreviousRunIds) WITH ([runid] nvarchar(MAX) N'$.runid', [message] nvarchar(MAX) N'$.message', [timestamp] datetime2 N'$.timestamp')
                        ) v FOR JSON PATH                                                                            --|
                    ), CurrentRunId = @runid                                                                         --|
                    FROM [Maintenance].[Archive_AuditLogs] arc                                                       --|
                    WHERE EXISTS (SELECT 1 FROM #tempListFilters WHERE ArchiveId = arc.Id);                          --|
                                                                                                                     --|
                    IF @@TRANCOUNT > 0 COMMIT;                                                                       --|
                    ---------------------------------------------------------------------------------------------------+
                    -- END TRANSACTION BLOCK                                                                           |
                    ---------------------------------------------------------------------------------------------------+
                END TRY
                BEGIN CATCH
                    -- Get Unknown error
                    SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();

                    IF @@TRANCOUNT > 0 ROLLBACK;
                    -- Save Unknwon Errror
                    SET @message = N'  Error(s) occured while updating filters';
                    EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = @ERROR_STATE, @Number = @ERROR_NUMBER, @Line = @ERROR_LINE, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                    THROW;
                END CATCH

                -- Create temp table for Ids
                DROP TABLE IF EXISTS #tempListIds;
                CREATE TABLE #tempListIds(tempId bigint PRIMARY KEY CLUSTERED, tempSyncId bigint, tempDeleteOnly bit, tempNoDelay bit, INDEX [IX_DeleteOnly] NONCLUSTERED (tempId) WHERE tempDeleteOnly = 0);

                -- Init Counts
                SELECT @currentId = 0;
                SELECT @currentId = MIN(flt.CurrentId) FROM #tempListFilters flt;

                SET @message = SPACE(@tab * 1) + N'Start Archiving';
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                WHILE 1 = 1 -- Loop Archinving
                BEGIN
                    BEGIN TRY
                        TRUNCATE TABLE #tempListIds;

                        -- Get Ids from source table
                        INSERT INTO #tempListIds(tempId, tempSyncId, tempDeleteOnly, tempNoDelay)
                        SELECT TOP(@maxLoopDeleteRows) src.Id, flt.SyncId, flt.DeleteOnly, flt.NoDelay
                        FROM #tempListFilters flt
                        INNER JOIN [Maintenance].[Synonym_Source_AuditLogs] src ON src.TenantId = flt.TenantId
                        WHERE ( (src.TimeStamp > flt.PreviousTimestamp AND src.TimeStamp <= flt.TargetTimestamp) OR (flt.LastId IS NOT NULL AND src.TimeStamp <= flt.PreviousTimestamp AND src.Id > flt.LastId ) ) AND src.Id >= flt.CurrentId AND src.Id <= @maxId AND src.Id >= @currentId
                        ORDER BY src.Id ASC;
 
                        SELECT @countRowIds = @@ROWCOUNT;
                        SELECT @currentLoopId = MAX(tempId) + 1 FROM #tempListIds;

                        IF NOT @countRowIds > 0 
                        BEGIN
                            SET @message = SPACE(@tab * 1) + N'Nothing left to Archive in current batch...';
                            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                            -- If nothing left, save status...
                            -- Update IsArchived when current Id(s) reach Target Id
                            UPDATE flt SET CurrentId = @maxId, IsArchived = 1
                            FROM [Maintenance].[Filter_AuditLogs] flt
                            INNER JOIN #tempListFilters lst ON lst.TenantId = flt.TenantId AND lst.SyncId = flt.SyncId --AND flt.CurrentId >= flt.TargetId

                            SELECT @filtersArchived = @@ROWCOUNT, @globalFiltersArchived = ISNULL(@globalFiltersArchived, 0) + @@ROWCOUNT;
                            BREAK;
                        END
                        -----------------------------------------------------------------------------------------------------------------------+
                        -- START TRANSACTION BLOCK                                                                                             |
                        -----------------------------------------------------------------------------------------------------------------------+
                        BEGIN TRAN                                                                                                           --|
                        EXEC sp_executesql @stmt = @sqlArchive;                                                                              --|
                        EXEC sp_executesql @stmt = @sqlArchiveEntities;                                                                     --|
                                                                                                                                             --|
                        -- Add Delete info                                                                                                   --|
                        INSERT INTO [Maintenance].[Delete_AuditLogs](SyncId, Id)                                                             --|
                        SELECT tempSyncId, tempId FROM #tempListIds ids                                                                      --|
                        WHERE NOT EXISTS(SELECT 1 FROM [Maintenance].[Delete_AuditLogs] WHERE Id = ids.tempId) AND tempNoDelay = 0;          --|
                                                                                                                                             --|
                        DELETE src FROM #tempListIds ids INNER JOIN [Maintenance].[Synonym_Source_AuditLogs] src ON src.Id = ids.tempId WHERE @ignoreDelay = 1 OR ids.tempNoDelay = 1;
                                                                                                                                             --|
                        -- Update current Id(s)                                                                                              --|
                        UPDATE flt SET CurrentId = @currentLoopId                                                                            --|
                        FROM [Maintenance].[Filter_AuditLogs] flt                                                                            --|
                        INNER JOIN #tempListFilters lst ON lst.TenantId = flt.TenantId AND lst.SyncId = flt.SyncId                           --|
                        WHERE @currentLoopId IS NOT NULL;                                                                                    --|
                                                                                                                                             --|
                        IF @@TRANCOUNT > 0 COMMIT;                                                                                           --|
                        -----------------------------------------------------------------------------------------------------------------------+
                        -- START TRANSACTION BLOCK                                                                                             |
                        -----------------------------------------------------------------------------------------------------------------------+

                        --SELECT @minId = @currentId, @currentId = MAX(tempId) + 1, @countRowIds = COUNT(*), @totalRowIds = @totalRowIds + COUNT(*), @countDeleteOnlyIds = ISNULL(SUM(CAST(tempDeleteOnly AS tinyint)), 0), @totalDeleteOnlyIds = @totalDeleteOnlyIds + ISNULL(SUM(CAST(tempDeleteOnly AS tinyint)), 0) FROM #tempListIds;
                        SELECT @countDeleteOnlyIds = @countRowIds - COUNT(*) FROM #tempListIds WHERE tempDeleteOnly = 0
                        SELECT @countDeleted = COUNT(*) FROM #tempListIds WHERE tempNoDelay = 1
                        SELECT @minId = @currentId, @currentId = @currentLoopId
                        SELECT @totalRowIds = @totalRowIds + ISNULL(@countRowIds, 0), @totalDeleteOnlyIds = @totalDeleteOnlyIds + ISNULL(@countDeleteOnlyIds, 0), @totalDeleted = @totalDeleted + ISNULL(@countDeleted, 0), @loopCount = @loopCount + 1;

                        SET @message = SPACE(@tab * 2) + CAST(@loopCount AS nvarchar(100)) + N'- Ids ' + CAST(@minId AS nvarchar(100)) + N' to ' + CAST(@currentId - 1 AS nvarchar(100)) + N': ' + CAST(@countRowIds AS nvarchar(100)) + N' (archive = '+ CAST(@countRowIds - @countDeleteOnlyIds AS nvarchar(100)) + N', delete only = ' + CAST(@countDeleteOnlyIds AS nvarchar(100)) + N') / delete (no delay = ' + CAST(@countDeleted AS nvarchar(100)) + N', async = ' + CAST(@countRowIds - @countDeleted AS nvarchar(100)) + N')';
                        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                        -- Exit when @MaxRunMinutes is exceeded
                        IF SYSDATETIME() > @maxRunDateTime 
                        BEGIN
                            SET @isTimeOut = 1;
                            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineBreak, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                            SELECT @message = N'TIME OUT:' + (SELECT CAST(DATEDIFF(MINUTE, @startTime, SYSDATETIME()) AS nvarchar(MAX)) ) + N' min (@MaxRunMinutes = ' + ISNULL(CAST(@MaxRunMinutes AS nvarchar(MAX)), N'' ) + N')';
                            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                            BREAK;
                        END 

                    END TRY
                    BEGIN CATCH
                        -- Get Unknown error
                        SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();

                        IF @@TRANCOUNT > 0 ROLLBACK;
                        -- Save Unknwon Errror
                        SET @message = N'  Error(s) occured while updating filters';
                        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = @ERROR_STATE, @Number = @ERROR_NUMBER, @Line = @ERROR_LINE, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                        THROW;
                    END CATCH
                END --WHILE Archive

                INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
                EXEC sp_executesql @stmt = @stmtEmptyMessagesStack, @params = @paramsEmptyMessagesStack, @MessagesStack = @MessagesStack OUTPUT;

                SELECT @message = SPACE(@tab * 1) + N'Total: ' + CAST(@totalRowIds AS nvarchar(100)) + N' (archive = '+ CAST(@totalRowIds - @totalDeleteOnlyIds AS nvarchar(100)) + N', delete only = ' + CAST(@totalDeleteOnlyIds AS nvarchar(100)) + N') / delete (no delay = ' + CAST(@totalDeleted AS nvarchar(100)) + N', async = ' + CAST(@totalRowIds - @totalDeleted AS nvarchar(100)) + N')';;
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                -- Update Sync status when all filters are Archived
                UPDATE snc SET IsArchived = 1
                    , IsDeleted = IIF( @deleteIfNoDelay = 1 AND NOT EXISTS(SELECT 1 FROM [Maintenance].[Delete_AuditLogs] WHERE syncId = snc.Id), 1, 0)
                    , DeletedOnDate = IIF( @deleteIfNoDelay = 1 AND NOT EXISTS(SELECT 1 FROM [Maintenance].[Delete_AuditLogs] WHERE syncId = snc.Id), SYSDATETIME(), NULL)
                    , IsSynced = IIF( @deleteIfNoDelay = 1 AND NOT EXISTS(SELECT 1 FROM [Maintenance].[Delete_AuditLogs] WHERE syncId = snc.Id), 1, 0)
                    , SyncedOnDate = IIF( @deleteIfNoDelay = 1 AND NOT EXISTS(SELECT 1 FROM [Maintenance].[Delete_AuditLogs] WHERE syncId = snc.Id), SYSDATETIME(), NULL)
                    , FirstASyncId = (SELECT MIN(Id) FROM [Maintenance].[Delete_AuditLogs] WHERE syncId = snc.Id)
                    , LastASyncId = (SELECT MAX(Id) FROM [Maintenance].[Delete_AuditLogs] WHERE syncId = snc.Id)
                    , CountASyncIds = (SELECT COUNT(*) FROM [Maintenance].[Delete_AuditLogs] WHERE syncId = snc.Id)
                FROM [Maintenance].[Sync_AuditLogs] snc
                INNER JOIN [Maintenance].[Archive_AuditLogs] arc ON arc.Id = snc.ArchiveId
                WHERE EXISTS(SELECT 1 FROM #tempListFilters WHERE SyncId = snc.Id) AND
                    NOT EXISTS (SELECT 1 FROM [Maintenance].[Filter_AuditLogs] WHERE SyncId = snc.Id AND IsArchived = 0);
                SELECT @syncArchived = @@ROWCOUNT;

                -- Update Archive status when all sync and filters are archived
                UPDATE arc SET IsArchived = 1, ArchivedOnDate = SYSDATETIME()
                    , IsDeleted = IIF(NOT EXISTS (SELECT 1 FROM [Maintenance].[Sync_AuditLogs] WHERE ArchiveId = lst.ArchiveId AND IsSynced = 0), 1, 0)
                    , DeletedOnDate = IIF(NOT EXISTS (SELECT 1 FROM [Maintenance].[Sync_AuditLogs] WHERE ArchiveId = lst.ArchiveId AND IsSynced = 0), SYSDATETIME(), NULL)
                    , IsFinished = IIF(NOT EXISTS (SELECT 1 FROM [Maintenance].[Sync_AuditLogs] WHERE ArchiveId = lst.ArchiveId AND IsSynced = 0), 1, 0)
                    , FinishedOnDate = IIF(NOT EXISTS (SELECT 1 FROM [Maintenance].[Sync_AuditLogs] WHERE ArchiveId = lst.ArchiveId AND IsSynced = 0), SYSDATETIME(), NULL)
                    , IsSuccess = IIF(NOT EXISTS (SELECT 1 FROM [Maintenance].[Sync_AuditLogs] WHERE ArchiveId = lst.ArchiveId AND IsSynced = 0), 1, 0)
                    , [Message] = IIF(NOT EXISTS (SELECT 1 FROM [Maintenance].[Sync_AuditLogs] WHERE ArchiveId = lst.ArchiveId AND IsSynced = 0), N'Cleanup finished', 'Archiving finished')
                    , PreviousRunIds = (
                        SELECT [runid], [message], [timestamp] FROM (
                            SELECT [runid] = @runId, [message] = N'Cleanup finished', [timestamp] = SYSDATETIME() WHERE NOT EXISTS (SELECT 1 FROM [Maintenance].[Sync_AuditLogs] WHERE ArchiveId = lst.ArchiveId AND IsSynced = 0)
                            UNION ALL SELECT [runid] = @runId, [message] = N'All Archive Filter(s) finished]', [timestamp] = SYSDATETIME()
                            UNION ALL SELECT [runid], [message], [timestamp] FROM OPENJSON(PreviousRunIds) WITH ([runid] nvarchar(MAX) N'$.runid', [message] nvarchar(MAX) N'$.message', [timestamp] datetime2 N'$.timestamp')
                        ) v FOR JSON PATH
                    )
                FROM [Maintenance].[Archive_AuditLogs] arc
                INNER JOIN #tempListFilters lst ON lst.ArchiveId = arc.Id
                WHERE NOT EXISTS (SELECT 1 FROM [Maintenance].[Sync_AuditLogs] WHERE ArchiveId = lst.ArchiveId AND IsArchived = 0);

                SELECT @triggerArchived = @@ROWCOUNT, @globalTriggerArchived = ISNULL(@globalTriggerArchived, 0) + @@ROWCOUNT;

                SELECT @message = SPACE(@tab * 2) + N'Archive Filter(s) finished: ' + CAST(ISNULL(@filtersArchived, 0) AS nvarchar(100)) + N' / ' + CAST(ISNULL(@countFilterIds, 0) AS nvarchar(100)) + N' (remaining filters = '+ CAST(ISNULL(COUNT(*), 0) AS nvarchar(100)) + N')' FROM [Maintenance].[Filter_AuditLogs] WHERE IsArchived <> 1;
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                SELECT @message = SPACE(@tab * 2) + N'Archive Sync(s) finished: ' + ISNULL(CAST(@syncArchived AS nvarchar(100)), N'-');
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                SELECT @message = SPACE(@tab * 1) + N'Archive(s) finished: ' + ISNULL(CAST(@triggerArchived AS nvarchar(100)), N'-') + N' (in progress = ' + CAST(@countArchiveIds - ISNULL(@triggerArchived, 0) AS nvarchar(100)) + N', to do = '+ CAST(ISNULL(COUNT(*), 0) AS nvarchar(100)) + N')' FROM [Maintenance].[Archive_AuditLogs] WHERE [Todo] = 1 AND ArchiveTriggerTime < @StartTime;
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
            END

            SELECT @message = SPACE(@tab * 0) + N'Filter(s) batch finished [' + cAST(@batchCount AS nvarchar(100)) + N']';
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

            -- Save grant total
            SELECT @globalRowIds = ISNULL(@globalRowIds, 0) + (@totalRowIds), @globalDeleteOnlyIds = ISNULL(@globalDeleteOnlyIds, 0) + @totalDeleteOnlyIds, @globalDeleted = ISNULL(@globalDeleted, 0) + @totalDeleted;

            IF  (@countFilterIds < 1 AND @countRepeat < 1) OR  @batchCount >= @maxBatches OR SYSDATETIME() > @maxRunDateTime
            BEGIN
                SELECT @message = N'Archiving is finished ' + CASE 
                WHEN (@countFilterIds < 1 AND @countRepeat < 1) THEN N'(no remaining filters)'
                WHEN SYSDATETIME() > @maxRunDateTime THEN N'(time out)' 
                WHEN  @batchCount >= @maxBatches THEN N'(@MaxBatchesLoops limit reached [@MaxBatchesLoops = ' + CAST(@MaxBatchesLoops AS nvarchar(100)) + N'])'
                ELSE N'xxx' END;
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                BREAK
            END

        END --WHILE Batch

        SELECT @message = SPACE(@tab * 1) + N'Rows processed: ' + CAST(ISNULL(@globalRowIds, 0) AS nvarchar(100)) + N' (archived = ' + CAST(ISNULL(@globalRowIds - @globalDeleteOnlyIds, 0) AS nvarchar(100)) + N', delete only '+ CAST(ISNULL(@globalDeleteOnlyIds, 0) AS nvarchar(100)) + N')';
        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

        SELECT @message = SPACE(@tab * 2) + N'Deleted:  no delay = ' + CAST(ISNULL(@globalDeleted, 0) AS nvarchar(100)) + N', async =' + CAST(ISNULL(@globalRowIds - @globalDeleted, 0) AS nvarchar(100)) + N'';
        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

        SELECT @message = SPACE(@tab * 1) + 'Remaining Archive Trigger(s): ' + CAST(COUNT(*) AS nvarchar(100)) + IIF(COUNT(*) > 0, N' (outstanding = '+ CAST(SUM(IIF(arc.ArchiveTriggerTime < @startTime, 1, 0)) AS nvarchar(100)) + N', upcoming = ' + CAST(SUM(IIF(arc.ArchiveTriggerTime >= @startTime, 1, 0)) AS nvarchar(100)) + N')', '')
        FROM [Maintenance].[Archive_AuditLogs] arc
        WHERE arc.ToDo = 1
        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

        SELECT @message = SPACE(@tab * 1) + 'Remaining Filter(s): ' + CAST(COUNT(*) AS nvarchar(100)) + IIF(COUNT(*) > 0, N' (outstanding = '+ CAST(SUM(IIF(arc.ArchiveTriggerTime < @startTime, 1, 0)) AS nvarchar(100)) + N', upcoming = ' + CAST(SUM(IIF(arc.ArchiveTriggerTime >= @startTime, 1, 0)) AS nvarchar(100)) + N')', '')
        FROM [Maintenance].[Filter_AuditLogs] flt 
        INNER JOIN [Maintenance].[Sync_AuditLogs] snc ON snc.Id = flt.SyncId
        INNER JOIN [Maintenance].[Archive_AuditLogs] arc ON arc.Id = snc.ArchiveId 
        WHERE flt.IsArchived = 0 AND snc.IsArchived = 0 AND (flt.TargetId IS NULL OR flt.CurrentId IS NULL OR flt.CurrentId < flt.TargetId) AND arc.ToDo = 1 --AND arc.ArchiveTriggerTime < @StartTime
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

                SET @message = N'Rows deleted: ' + ISNULL(CAST(@totalRowIds AS nvarchar(MAX)), '0');
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
