SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- DROP PROCEDURE [Maintenance].[AddArchiveTriggerRobotLicenseLogs]
----------------------------------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[AddArchiveTriggerRobotLicenseLogs]') AND type in (N'P'))
BEGIN
    EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [Maintenance].[AddArchiveTriggerRobotLicenseLogs] AS'
    PRINT '  + CREATE PROCEDURE: [Maintenance].[AddArchiveTriggerRobotLicenseLogs]';
END
ELSE PRINT '  = PROCEDURE [Maintenance].[AddArchiveTriggerRobotLicenseLogs] already exists' 
GO

PRINT '  ~ UPDATE PROCEDURE: [Maintenance].[AddArchiveTriggerRobotLicenseLogs]'
GO

ALTER PROCEDURE [Maintenance].[AddArchiveTriggerRobotLicenseLogs]
----------------------------------------------------------------------------------------------------
-- ### [Object]: PROCEDURE [Maintenance].[AddArchiveTriggerRobotLicenseLogs]
-- ### [Version]: 2020-10-01 00:00:00                                                         
-- ### [Source]: ??????
-- ### [Hash]: ??????
-- ### [Docs]: https://???.???
-- !!! ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!!!
-- !!! ~~~~~~~~~ NOT OFFICIALLY SUPPORTED BY UIPATH 
-- !!! ~~~~~~~~~ SQL Server >= 2016 SP1
----------------------------------------------------------------------------------------------------
	@Name nvarchar(100) = NULL
    , @ArchiveTriggerTime datetime = NULL -- Replace by current date/time when NULL
    , @ArchiveAfterHours int = NULL -- Override missing/empty 'after_hours' value in JSON filters
	, @DeleteDelayHours int = 0 -- Override missing/empty delete_delay_hours value in JSON filters'
	, @Filters nvarchar(MAX)
    , @RepeatArchive [bit] = 0
    , @RepeatOffsetHours [smallint] = NULL
    , @RepeatUntil [datetime] = NULL
    , @ParentArchiveId [bigint] = NULL
    , @SynchronousDelete bit = 0 
    , @ForceSynchronousDelete bit = 0
    , @DryRunOnly nvarchar(MAX) = NULL -- Y{es} or N{o} => Only Check Parameters (default if NULL = Y)
    /* Messge Logging */
    , @SaveMessagesToTable nvarchar(MAX) = 'Y' -- Y{es} or N{o} => Save to [maintenance].[messages] table (default if NULL = Y)
    , @SavedToRunId int = NULL OUTPUT
--    , @OutputMessagesToDataset nvarchar(MAX) = 'N' -- Y{es} or N{o} => Output Messages Result set
--    , @Verbose nvarchar(MAX) = NULL -- Y{es} = Print all messages < 10
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

        DECLARE @startTime datetime = SYSDATETIME();
        DECLARE @startTimeFloat float(53);
        SELECT @startTimeFloat = CAST(@startTime AS float(53));

        DECLARE @tenantsSynonymIsValid bit;
        DECLARE @tenantsSynonymMessages nvarchar(MAX);        

        DECLARE @triggerTime datetime;
        DECLARE @triggerFloatingTime float(53);
        DECLARE @globalDeleteDelay int = 0;
        DECLARE @globalAfterHours int = NULL;

        DECLARE @logToTable bit;
        DECLARE @returnValue int = 1;

        ----------------------------------------------------------------------------------------------------
        -- JSON Validation
        ----------------------------------------------------------------------------------------------------
        DECLARE @json_IsValid bit = 0
        DECLARE @json_filters nvarchar(MAX);
        DECLARE @json_errors nvarchar(MAX);
        DECLARE @json_settings nvarchar(MAX);

        ----------------------------------------------------------------------------------------------------
        -- Archive / Filters
        ----------------------------------------------------------------------------------------------------
        DECLARE @Ids TABLE(Id bigint);
        DECLARE @listFilters TABLE([syncId] bigint, [tenants] int, [deleteOnly] bit, [archiveDate] datetime, [deleteDate] datetime, [next] datetime)
        DECLARE @countValidFilters int, @countDuplicateFilters int;
        DECLARE @targetTimestamp datetime;
        DECLARE @archiveId bigint;
        ----------------------------------------------------------------------------------------------------
        -- Constant / Default value
        ----------------------------------------------------------------------------------------------------
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
        -- Message / Error Handling
        ----------------------------------------------------------------------------------------------------
        DECLARE @lineSeparator nvarchar(MAX) = N'----------------------------------------------------------------------------------------------------';
        DECLARE @lineBreak nvarchar(MAX) = N'';
        DECLARE @message nvarchar(MAX);
        DECLARE @string nvarchar(MAX);
        DECLARE @space tinyint = 2 ;
        DECLARE @tab tinyint = 0 ;
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
        /* Messages */
        DECLARE @levelVerbose int;    
        DECLARE @outputDataset bit;
        DECLARE @messages TABLE(id int IDENTITY(0, 1) PRIMARY KEY, [date] datetime2 DEFAULT SYSDATETIME(), [procedure] nvarchar(MAX) NOT NULL DEFAULT QUOTENAME(COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?')) + N'.' + QUOTENAME(COALESCE(OBJECT_NAME(@@PROCID), N'?')), [message] nvarchar(MAX) NOT NULL, severity tinyint NOT NULL, state tinyint NOT NULL, [number] int, [line] int);
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
        IF @ParentArchiveId IS NOT NULL SET @tab = 4 ELSE SET @tab = 0;

        ----------------------------------------------------------------------------------------------------
        -- Gather General & Server Info
        ----------------------------------------------------------------------------------------------------

        -- Get Proc Version
/*
        EXEC sp_executesql @stmt = @stmtGetProcInfo, @params = @paramsGetProcInfo, @procid = @@PROCID, @info = N'Version', @output = @versionDatetime OUTPUT;
        INSERT INTO [Maintenance].[Runs]([Type], [Info], [StartTime]) SELECT N'Add Archive RobotLicenseLogs', N'PROCEDURE ' + @procName, @startTime;
        INSERT INTO @messages ([Message], Severity, [State])   
        SELECT 'Add Archive RobotLicenseLogs...' , 10, 1;
*/
        ----------------------------------------------------------------------------------------------------
        -- Create new Run Id
        ----------------------------------------------------------------------------------------------------
        INSERT INTO @messages ([Message], Severity, [State]) 
        SELECT SPACE(@tab+ @space * 0) + N'Add Archive Trigger' , 10, 1
        UNION ALL SELECT SPACE(@tab+ @space * 1) + N'Parameter Run Id [' + CAST(@SavedToRunId AS nvarchar(100)) + N'] is already ended.' , 10, 1 FROM [Maintenance].[Runs] WHERE Id = @SavedToRunId AND EndDate IS NOT NULL
        UNION ALL SELECT SPACE(@tab+ @space * 1) + N'Parameter Run Id [' + CAST(@SavedToRunId AS nvarchar(100)) + N'] not found.', 10, 1 WHERE @SavedToRunId IS NOT NULL AND NOT EXISTS (SELECT 1 FROM [Maintenance].[Runs] WHERE Id = @SavedToRunId)
        
        IF @SavedToRunId IS NULL OR NOT EXISTS(SELECT 1 FROM [Maintenance].[Runs] WHERE Id = @SavedToRunId AND EndDate IS NULL)
        BEGIN
            INSERT INTO [Maintenance].[Runs]([Type], [Info], [StartTime]) SELECT N'Add Archive RobotLicenseLogs Trigger', N'PROCEDURE ' + @procName, @startTime;
            SELECT @runId = @@IDENTITY, @SavedToRunId = @@IDENTITY;
            INSERT INTO @messages ([Message], Severity, [State]) VALUES 
                (SPACE(@tab+ @space * 1) + N'Messages saved to new Run Id: ' + CONVERT(nvarchar(MAX), @runId), 10, 1);
        END
        ELSE SELECT @runId = @SavedToRunId;

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

        BEGIN TRY
            EXEC [Maintenance].[SetSourceTableTenants] @IsValid = @tenantsSynonymIsValid OUTPUT, @Messages = @tenantsSynonymMessages OUTPUT;

            INSERT INTO @messages([message], [severity], [state])
            SELECT [Message], [Severity], 0 FROM OPENJSON(@tenantsSynonymMessages, N'$') WITH ([Message] nvarchar(MAX), [Severity] tinyint)
            UNION ALL SELECT 'Tenants synonym must be set using [Maintenance].[SetSourceTableTenants]', 16, 0 WHERE @tenantsSynonymIsValid = 0;
        END TRY
        BEGIN CATCH
            SET @message = N'ERROR: error(s) occurcered while validating Tenants synonym with with [Maintenance].[SetSourceTableTenants]'
            INSERT INTO @messages ([Message], Severity, [State]) VALUES
                    (ERROR_MESSAGE(), 10, 1)
                    , (@message, 16, 1)
        END CATCH

        ----------------------------------------------------------------------------------------------------
        -- Parameters
        ----------------------------------------------------------------------------------------------------
        -- Convert Yes / No varations to bit
        INSERT INTO @paramsYesNo([parameter], [value]) VALUES(N'NO', 0), (N'N', 0), (N'0', 0), (N'YES', 1), (N'Y', 1), (N'1', 1);

        -- Check Dry Run
        SELECT @dryRun = [value] FROM @paramsYesNo WHERE [parameter] = LTRIM(RTRIM(@DryRunOnly));
        IF @dryRun IS NULL 
        BEGIN
            SET @dryRun = 1;
            INSERT INTO @messages ([Message], Severity, [State]) VALUES (N'@DryRunOnly is NULL. Default value will be used (Yes).', 10, 1);
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

        SET @levelVerbose = 10;

        ----------------------------------------------------------------------------------------------------
        -- Parameters' Validation
        ----------------------------------------------------------------------------------------------------
        -- Check @@ArchiveTriggerTime
        SELECT @triggerTime = ISNULL(@ArchiveTriggerTime, SYSDATETIME());
        SELECT @triggerFloatingTime = CAST(@triggerTime AS float(53));

        -- Check Parameters
        IF EXISTS(SELECT 1 FROM @messages WHERE severity >= 16) 
        BEGIN 
            SELECT @message = N'Invalid configuration' WHERE @message IS NULL;
        END 
        ELSE
        BEGIN
            SELECT @json_filters = LTRIM(RTRIM(@Filters));

            -- Check @ArchiveAfterHours / @DeleteDelayHours
            SELECT @globalDeleteDelay = ISNULL(@DeleteDelayHours, @constDefaultDeleteDelay);
            SELECT @globalAfterHours = ISNULL(@ArchiveAfterHours, @constDefaultAfterHours);

            -- Output errors and warning
            INSERT INTO @messages ([Message], Severity, [State]) 
            SELECT SPACE(@tab+ @space * 1) + N'@ArchiveTriggerTime is NULL. Current Date & Time will be used', 10, 1 WHERE @ArchiveTriggerTime IS NULL
            UNION ALL SELECT SPACE(@tab+ @space * 1) + N'@ArchiveAfterHours is NULL. "after_hours" value(s) from JSON string will be used', 10, 1 WHERE @ArchiveAfterHours IS NULL AND @json_filters NOT IN (N'ALL', N'ACTIVE_TENANTS', N'DELETED_TENANTS')
            UNION ALL SELECT N'ERROR: @ArchiveAfterHours must be provided when keyword "' + @json_filters + N'" is used', 16, 1 WHERE @ArchiveAfterHours IS NULL AND @json_filters IN (N'ALL', N'ACTIVE_TENANTS', N'DELETED_TENANTS')
            UNION ALL SELECT SPACE(@tab+ @space * 1) + N'@DeleteDelayHours is NULL. "delete_delay_hours" value(s) from JSON string will be used when available or default value otherwise (' + CAST(@constDefaultDeleteDelay AS nvarchar(100)) N')', 10, 1 WHERE @DeleteDelayHours IS NULL
            UNION ALL SELECT N'ERROR: @ArchiveAfterHours must be at least 0 (>= 0)', 16, 1 WHERE @ArchiveAfterHours < 0
            UNION ALL SELECT N'@RepeatOffsetHours must be greater than 1 when @RepeatArchive is set (@RepeatOffsetHours = ' + ISNULL(CAST(@RepeatOffsetHours AS nvarchar(100)), N'NULL') + ')', 16, 1 WHERE (@RepeatOffsetHours IS NULL OR @RepeatOffsetHours < 1) AND @RepeatArchive = 1
            UNION ALL SELECT N'@RepeatUntil must be greater than the current date and time', 16, 1 WHERE (@RepeatUntil <= @triggerTime) AND @RepeatArchive = 1
            ;

        END

        -- Check Filters Definition
        IF EXISTS(SELECT 1 FROM @messages WHERE severity >= 16) 
        BEGIN 
            SELECT @message =  N'Invalid parameters' WHERE @message IS NULL;
        END 
        ELSE
        BEGIN
            IF @json_filters IN (N'ALL', N'ACTIVE_TENANTS', N'DELETED_TENANTS', N'#ALL#', N'#ACTIVE_TENANTS#', N'#DELETED_TENANTS#')
            BEGIN
                SELECT @json_filters = UPPER(@json_filters);
                INSERT INTO @messages ([Message], Severity, [State]) SELECT SPACE(@tab+ @space * 1) + N'@Filters keyword used: ' + @json_filters, 10, 1
                INSERT INTO @messages ([Message], Severity, [State]) VALUES (SPACE(@tab+ @space * 1) + N'Create JSON string and parameters...', 10, 1)
                SELECT @json_filters = ( SELECT [tenants] = N'#' + [tenants] + N'#' FROM (VALUES(N'ACTIVE_TENANTS'), (N'DELETED_TENANTS')) t([tenants]) WHERE @json_filters = N'ALL' OR @json_filters = N'#ALL#' OR @json_filters = [tenants] OR @json_filters = N'#'+ [tenants] + N'#' FOR JSON PATH )

                INSERT INTO @messages ([Message], Severity, [State]) SELECT SPACE(@tab+ @space * 1) + N'Resulting JSON string: ' + @json_filters, 10, 1
            END
            ELSE
            BEGIN
                INSERT INTO @messages ([Message], Severity, [State]) VALUES (SPACE(@tab+ @space * 1) + N'@Filters not a keyword ("ALL", "ACTIVE_TENANTS" or "DELETED_TENANTS"), valid JSON string expected', 10, 1)
            END

            BEGIN TRY            
                INSERT INTO @messages ([Message], Severity, [State]) VALUES (SPACE(@tab+ @space * 1) + N'Checking @Filters...', 10, 1)
                EXEC [Maintenance].[ParseJsonArchiveRobotLicenseLogs] @Filters = @json_filters, @Settings = @json_settings OUTPUT, @Messages = @json_errors OUTPUT, @IsValid = @json_IsValid OUTPUT, @AfterHours = @globalAfterHours, @DeleteDelayHhours = @globalDeleteDelay;
            END TRY
            BEGIN CATCH
                SET @message = N'ERROR: error(s) occurcered while validating settings with [Maintenance].[ParseJsonArchiveRobotLicenseLogs]'
                INSERT INTO @messages ([Message], Severity, [State]) VALUES
                        (ERROR_MESSAGE(), 10, 1)
                        , (@message, 16, 1)
                SET @json_IsValid = 0;
            END CATCH

            IF @json_IsValid = 1
            BEGIN
                INSERT INTO @messages ([Message], Severity, [State]) VALUES (SPACE(@tab+ @space * 1) + N'JSON string and parameters are valid', 10, 1);
                IF @dryRun = 1 SET @message = N'Dry Run' ELSE SET @message = N'Add Trigger';
            END
            ELSE 
            BEGIN;
                INSERT INTO @messages([procedure], [message], [severity], [state])
                SELECT [Procedure], [Message], [Severity], [State] FROM OPENJSON(@json_errors, N'$')
                    WITH ([Procedure] nvarchar(MAX) '$.Procedure', [Message] nvarchar(MAX) '$.Message', [Severity] smallint '$.Severity', [State] smallint '$.State');

                INSERT INTO @messages ([Message], Severity, [State]) VALUES (N'ERROR: JSON string and parameters are invalid, review previous error(s)', 16, 1);
                SET @message = N'Invalid JSON string and parameters';
            END 
        END

        SELECT @errorCount = COUNT(*) FROM @messages WHERE severity >= 16;
        ----------------------------------------------------------------------------------------------------
        -- Add new Archive RobotLicenseLogs
        ----------------------------------------------------------------------------------------------------
        BEGIN TRY
            INSERT INTO [Maintenance].[Archive_RobotLicenseLogs]([ParentArchiveId], [CurrentRunId], [PreviousRunIds], [Name], [Definition], [ArchiveTriggerTime], [ArchiveAfterHours], [DeleteDelayHours], [TargetId], [TargetTimestamp], [RepeatArchive], [RepeatOffsetHours], [RepeatUntil]
                -- , [AddNextArchive], [NextOffsetHours]
                , [IsDryRun], [IsSuccess], [IsError], [IsCanceled], [Message], [IsFinished], [FinishedOnDate]
                , [CountValidFilters], [CountDuplicateFilters] )
            SELECT @ParentArchiveId, @runId, (SELECT [runid] = @runId, [message] = @message, [timestamp] = SYSDATETIME() FOR JSON PATH), @Name, @Filters, @triggerTime, @ArchiveAfterHours, @DeleteDelayHours, NULL, @triggerTime, @RepeatArchive, @RepeatOffsetHours, @RepeatUntil
                , @dryRun, 0, IIF(@errorCount = 0 AND @json_IsValid = 1, 0, 1), 0, @message
                , @dryRun, IIF(@dryRun = 1, SYSDATETIME(), NULL)
                , 0, 0
            SET @archiveId = @@IDENTITY;
            
            INSERT INTO @messages ([Message], Severity, [State]) VALUES (SPACE(@tab+ @space * 1) + N'Archive RobotLicenseLogs Trigger created (Id = ' + CAST(@archiveId AS nvarchar(100)) + N')' + IIF(@errorCount > 0, N' with error(s)', N'') + N'.', 10, 1);
        END TRY
        BEGIN CATCH
            SET @message = N'ERROR: error(s) occurcered while adding Archive RobotLicenseLogs trigger to [Maintenance].[Archive_RobotLicenseLogs]'
            INSERT INTO @messages ([Message], Severity, [State]) VALUES
                    (ERROR_MESSAGE(), 10, 1)
                    , (@message, 16, 1)
        END CATCH 
        ----------------------------------------------------------------------------------------------------
        -- Check Error(s) count
        ----------------------------------------------------------------------------------------------------

        IF NOT EXISTS(SELECT 1 FROM @messages WHERE severity >= 16) AND @dryRun = 0
        BEGIN

            BEGIN TRY
                -- retrieve parsed filters and update target dates
                INSERT @listFilters ([tenants], [deleteOnly], [archiveDate], [deleteDate])
                SELECT [tenants] = [t], [deleteOnly] = [o]
                    , [TargetTimestamp] = CAST(@triggerTime -ABS(@floatingHour * [h]) AS datetime) 
                    , [DeleteAfterDatetime] = CAST(@triggerTime - ABS(@floatingHour * [h]) + ABS(@floatingHour * [d]) AS datetime) 
                FROM OPENJSON(@json_settings) WITH ([t] int, [l] int, [o] int, [h] int, [d] int) jsn

                SELECT @countValidFilters = COUNT(*) FROM @listFilters lst WHERE NOT EXISTS (SELECT 1 FROM [Maintenance].[Filter_RobotLicenseLogs] flt WHERE flt.TenantId = lst.tenants AND flt.TargetTimestamp >= lst.archiveDate);
                SELECT @countDuplicateFilters = COUNT(*) - @countValidFilters, @targetTimestamp = MAX(archiveDate) FROM @listFilters;

                IF @countValidFilters > 0
                BEGIN 
                    BEGIN TRAN

                    -- insert archive sync by target Delete date
                    INSERT INTO [Maintenance].[Sync_RobotLicenseLogs](ArchiveId, DeleteAfterDatetime)
                    OUTPUT inserted.Id INTO @Ids(Id)
                    SELECT DISTINCT @archiveId, deleteDate FROM @listFilters ORDER BY deleteDate DESC

                    -- match filters with inserted Sync Id
                    UPDATE lst SET syncId = ids.Id
                    FROM @Ids ids 
                    INNER JOIN [Maintenance].[Sync_RobotLicenseLogs] snc ON snc.Id = ids.Id
                    INNER JOIN @listFilters lst ON lst.deleteDate = snc.DeleteAfterDatetime

                    -- insert valid filter(s)
                    INSERT INTO [Maintenance].[Filter_RobotLicenseLogs]([SyncId],[TenantId], [DeleteOnly], [TargetTimestamp], [PreviousTimestamp])
                    SELECT lst.syncId, lst.tenants, lst.deleteOnly, lst.archiveDate, ISNULL(last.TargetTimestamp, 0) -- 0 => 19010101
                    FROM @listFilters lst
                    OUTER APPLY (SELECT MAX(TargetTimestamp) FROM [Maintenance].[Filter_RobotLicenseLogs] flt WHERE flt.TenantId = lst.tenants AND flt.TargetTimestamp < lst.archiveDate) last(TargetTimestamp) -- retrieve previous target date
                    WHERE NOT EXISTS (SELECT 1 FROM [Maintenance].[Filter_RobotLicenseLogs] flt WHERE flt.TenantId = lst.tenants AND flt.TargetTimestamp >= lst.archiveDate) -- remove existing filter(s) with a newer date

                    SET @message = SPACE(@tab+ @space * 1) + N'Valid Filter(s) added: ' + CAST(@countValidFilters AS nvarchar(100)) + N' , duplicate(s) found (' + CAST(@countDuplicateFilters AS nvarchar(100)) + N')';
                    INSERT INTO @messages ([Message], Severity, [State]) VALUES (@message, 10, 1);

                    IF @@TRANCOUNT > 0 COMMIT
                END
                ELSE
                BEGIN
                    SET @message = SPACE(@tab+ @space * 1) + N'No Filters added, only duplicate found (' + CAST(@countDuplicateFilters AS nvarchar(100)) + N')';
                    INSERT INTO @messages ([Message], Severity, [State]) VALUES (@message, 10, 1)
                END
                -- update valid and duplicate filter(s) count
                UPDATE arc SET [CountValidFilters] = @countValidFilters, [CountDuplicateFilters] = @countDuplicateFilters, [TargetTimestamp] = @targetTimestamp FROM [Maintenance].[Archive_RobotLicenseLogs] arc WHERE arc.Id = @archiveId;

                INSERT INTO @messages ([Message], Severity, [State])
                SELECT SPACE(@tab+ @space * 1) + N'Repeat enabled every [' + CAST(@RepeatOffsetHours AS nvarchar(100)) + N'] hour(s)' + IIF(@RepeatUntil IS NOT NULL, N' until [' + CONVERT(nvarchar(100), @RepeatUntil, 120) + N']', N''), 10, 1 WHERE @RepeatArchive = 1

            END TRY
            BEGIN CATCH
                SET @message = N'ERROR: error(s) occurcered while adding Sync and Filters'
                INSERT INTO @messages ([Message], Severity, [State]) VALUES
                        (ERROR_MESSAGE(), 10, 1)
                        , (@message, 16, 1)
                IF @@TRANCOUNT > 0 ROLLBACK
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
    SELECT @errorCount = COUNT(*) FROM @messages WHERE severity >= 16;

    OPEN CursorMessages;
    FETCH CursorMessages INTO @cursorDate, @cursorProcedure, @cursorMessage, @cursorSeverity, @cursorState, @cursorNumber, @cursorLine;

    IF CURSOR_STATUS('local', 'CursorMessages') = 1
    BEGIN
        WHILE @@FETCH_STATUS = 0
        BEGIN;
            IF @logToTable = 0 OR @errorCount > 0 OR @cursorSeverity >= @levelVerbose OR @cursorSeverity > 10 RAISERROR('%s', @cursorSeverity, @cursorState, @cursorMessage) WITH NOWAIT;
            --IF @cursorSeverity >= 16 RAISERROR('', 10, 1) WITH NOWAIT;
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

----------------------------------------------------------------------------------------------------
--
----------------------------------------------------------------------------------------------------
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
            SET @message = N'Incorrect Parameters, see previous Error(s): ' + CAST(@errorCount AS nvarchar(MAX)) + N' found';
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 16, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = 0, @MessagesStack = @MessagesStack OUTPUT;
            -- RETURN 123; => catch
        END
        ----------------------------------------------------------------------------------------------------
        -- End Run on Dry Run
        ----------------------------------------------------------------------------------------------------
        IF @dryRun <> 0 
        BEGIN
            INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
            EXEC sp_executesql @stmt = @stmtEmptyMessagesStack, @params = @paramsEmptyMessagesStack, @MessagesStack = @MessagesStack OUTPUT;

            SET @returnValue = 1;

            SET @message = '@DryRunOnly is enabled (check output and parameters and set @DryRunOnly to No when ready)';
            EXEC [Maintenance].[AddRunMessage] @RunId = @RunId, @Procedure = @procName, @Message = @message, @Severity = 11, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = 0, @MessagesStack = @MessagesStack OUTPUT;
            -- RETURN 1; => catch
        END

        --DELETE FROM @messages;
        SELECT @Message = SPACE(@tab+ @space * 0) + 'Valid Archive RobotLicenseLogs Trigger added (SUCCESS)';
		EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @Message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

        INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
        EXEC sp_executesql @stmt = @stmtEmptyMessagesStack, @params = @paramsEmptyMessagesStack, @MessagesStack = @MessagesStack OUTPUT;

        SET @returnValue = 0; -- Success
    END TRY
    BEGIN CATCH
        SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
        SELECT @errorCount = COUNT(*) FROM @messages WHERE severity >= 16;

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
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = 'Test Archive RobotLicenseLogs Trigger added (DRY RUN)', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
            END
            ELSE 
            BEGIN
                SET @message = N'Execution finished with error(s)'
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = 'Invalid Archive RobotLicenseLogs Trigger added (FAIL)', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                SET @errorCount = @errorCount + 1;
                SET @returnValue = 4;
            END
        END
        ELSE
        BEGIN
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = 'Invalid Archive RobotLicenseLogs Trigger added (INCORRECT PARAMETERS)', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
        END

            UPDATE arc SET [CountValidFilters] = 0, [CountDuplicateFilters] = 0, [IsDryRun] = @dryRun, [IsError] = @errorCount, [IsFinished] = 1, [FinishedOnDate] = SYSDATETIME()
            FROM [Maintenance].[Archive_RobotLicenseLogs] arc WHERE arc.Id = @archiveId;
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
    IF @runId IS NOT NULL AND (@SavedToRunId IS NULL OR @SavedToRunId <> @runId) UPDATE [Maintenance].[Runs] SET [EndDate] = SYSDATETIME(), [ErrorStatus] = @returnValue WHERE Id = @runId;

    ----------------------------------------------------------------------------------------------------
    -- End
    ----------------------------------------------------------------------------------------------------
    RETURN @returnValue;
END
GO
