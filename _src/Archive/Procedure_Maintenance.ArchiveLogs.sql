SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- PROCEDURE [Maintenance].[ArchiveLogs]
----------------------------------------------------------------------------------------------------

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[ArchiveLogs]') AND type in (N'P'))
BEGIN
    EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [Maintenance].[ArchiveLogs] AS'
    PRINT '  + PROCEDURE CREATED: [Maintenance].[ArchiveLogs]';
END
ELSE PRINT '  = PROCEDURE [Maintenance].[ArchiveLogs] already exists' 
GO

ALTER PROCEDURE [Maintenance].[ArchiveLogs]
----------------------------------------------------------------------------------------------------
-- ### [Object]: PROCEDURE [Maintenance].[ArchiveLogs]
-- ### [Version]: 2020-10-01 00:00:00                                                         
-- ### [Source]: ??????
-- ### [Hash]: ??????
-- ### [Docs]: https://???.???
----------------------------------------------------------------------------------------------------
    @RowsDeletedForEachLoop int = 10000 -- Don't go above 50.000 (min = 1000, max = 100.000)
    , @HoursToKeep int = NULL -- i.e. 168h = 7*24h = 7 days => value can't be NULL and must be bigger than 0 if @ArchiveBeforeDate is not set
    , @ArchiveBeforeDate datetime = NULL -- Use either @ArchiveBeforeDate or @HoursToKeep BUT not both
    , @KeepBeforeDate datetime = NULL -- Don't archive data before this date
    , @MaxRunMinutes int = NULL -- NULL or 0 = unlimited
    -- 
    , @DryRunOnly sysname = NULL -- Y{es} or N{o} => Only Check Parameters (default if NULL = Y)
    , @CleanupAfterArchiving sysname = NULL -- Y{es} or N{o} => delete from Logs table after archiving (default if NULL = Y)
    -- source & destination
    , @DestinationTable sysname -- Name of destination table (database.schema.table or schema.table)
    , @ArchiveIdentityColumn sysname = N'N' -- Y{es} or N{o} (default if NULL = No)
    , @DestinationColumnsMapping nvarchar(max) = NULL
    /* Error Handling */
    , @OnErrorRetry tinyint = NULL -- between 0 and 20 => retry up to 20 times (default if NULL = 10)
    , @OnErrorWaitMillisecond smallint = 1000 -- wait for milliseconds between each Retry (default if NULL = 1000ms)
    /* Index handling */
    , @DisableIndex sysname = NULL -- Y{es} or N{o} => Disables all non clustered indexes (default if NULL = N)
    , @RebuildIndex sysname = NULL -- Y{es} or N{o} => Rebuild all indexes (default if NULL = N)
    , @RebuildIndexOnline sysname = NULL -- Y{es} or N{o} => Rebuild Online on Enterprise edition Only
    /* Messge Logging */
    , @SaveMessagesToTable sysname = 'Y' -- Y{es} or N{o} => Save to [maintenance].[messages] table (default if NULL = Y)
    , @SavedMessagesRetentionDays smallint = 30
    , @Verbose sysname = NULL -- Y{es} = Print all messages < 10
AS
BEGIN
    BEGIN TRY 
        ----------------------------------------------------------------------------------------------------
        -- Local Run Variables
        ----------------------------------------------------------------------------------------------------
        DECLARE @runId int;  
        DECLARE @startTime datetime2 = SYSDATETIME();
        DECLARE @maxRunDateTime datetime2;
        DECLARE @logToTable bit;
        DECLARE @copyIdentity bit;
        DECLARE @mapping nvarchar(max);
        DECLARE @deleteTopRows int;
        DECLARE @maxCreationTime datetime;
        DECLARE @minCreationTime datetime;
        DECLARE @maxErrorRetry tinyint;
        DECLARE @errorDelay smallint;
        DECLARE @errorWait datetime;
        DECLARE @indexDisable bit;
        DECLARE @indexRebuild bit;
        DECLARE @indexRebuildOnline bit;
        ----------------------------------------------------------------------------------------------------
        -- Archive & Cleanup
        ----------------------------------------------------------------------------------------------------
        DECLARE @stmtArchive nvarchar(max);
        DECLARE @paramsArchive nvarchar(max) = N'@deleteTopRows bigint, @minId bigint, @maxId bigint, @minCreationTime datetime, @maxCreationTime datetime';
        DECLARE @tableArchive sysname;
        DECLARE @deleteRows bit;
        DECLARE @dryRun bit;
        DECLARE @ids TABLE (Id bigint PRIMARY KEY CLUSTERED);
        DECLARE @minId bigint = 0;
        DECLARE @maxId bigint;
        DECLARE @totalIds bigint;
        DECLARE @countIds bigint;
        DECLARE @currentId bigint;
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
        DECLARE @message nvarchar(4000);
        --DECLARE @sql nvarchar(max);
        DECLARE @string nvarchar(max);
        DECLARE @paramsYesNo TABLE ([id] tinyint IDENTITY(0, 1) PRIMARY KEY CLUSTERED, [parameter] sysname, [value] int)
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
        -- Source & Destination (mapping)
        ----------------------------------------------------------------------------------------------------
        DECLARE @delimiter char(1) = ',';
        DECLARE @redirection char(1) = '=';
        DECLARE @mappingList TABLE (item nvarchar(max), source sysname, destination sysname);
        DECLARE @sourceCols nvarchar(max)
        DECLARE @destCols nvarchar(max)
        ----------------------------------------------------------------------------------------------------
        -- Proc Info
        ----------------------------------------------------------------------------------------------------
        DECLARE @paramsGetProcInfo nvarchar(max) = N'@procid int, @info sysname, @output nvarchar(max) OUTPUT'
        DECLARE @stmtGetProcInfo nvarchar(max) = N'
            DECLARE @definition nvarchar(max) = OBJECT_DEFINITION(@procid), @keyword sysname = REPLICATE(''-'', 2) + SPACE(1) + REPLICATE(''#'', 3) + SPACE(1) + QUOTENAME(LTRIM(RTRIM(@info))) + '':'';
            SET @output = ''=''+ LTRIM(RTRIM( SUBSTRING(@definition, NULLIF(CHARINDEX(@keyword, @definition), 0 ) + LEN(@keyword), CHARINDEX( CHAR(13) , @definition, CHARINDEX(@keyword, @definition) + LEN(@keyword) + 1) - CHARINDEX(@keyword, @definition) - LEN(@keyword) ))) + ''='';
        ';
        DECLARE @procSchemaName sysname = COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?');
        DECLARE @procObjecttName sysname = COALESCE(OBJECT_NAME(@@PROCID), N'?');
        DECLARE @procName sysname = QUOTENAME(COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?')) + N'.' + QUOTENAME(COALESCE(OBJECT_NAME(@@PROCID), N'?')); 
        DECLARE @versionDatetime nvarchar(max);
        ----------------------------------------------------------------------------------------------------      
        -- Message / Error Handling
        ----------------------------------------------------------------------------------------------------
        DECLARE @stmtLogMessage nvarchar(max) = N'
            IF @LogToTable = 1 
            BEGIN
                INSERT INTO [Maintenance].[Messages](RunId, [Procedure], [Message], [Severity], [State])
                    SELECT @RunId, @Procedure, @Message, @Severity, @state WHERE @RunId IS NOT NULL;
            END
            IF @Severity >= @VerboseLevel 
            BEGIN
                IF @Severity < 10 SET @Severity = 10;
                RAISERROR(@Message, @Severity, @State);
            END
        ';
        DECLARE @paramsLogMessage nvarchar(max) = N'@RunId int, @Procedure sysname, @Message nvarchar(max), @Severity tinyint, @State tinyint, @VerboseLevel tinyint, @LogToTable bit';
        DECLARE @ERROR_NUMBER INT, @ERROR_SEVERITY INT, @ERROR_STATE INT, @ERROR_PROCEDURE NVARCHAR(126), @ERROR_LINE INT, @ERROR_MESSAGE NVARCHAR(2048) ;
        DECLARE @errorCount int = 0;
        DECLARE @messages TABLE(id int IDENTITY(0, 1) PRIMARY KEY, [date] datetime2 DEFAULT SYSDATETIME(), [procedure] sysname NOT NULL DEFAULT QUOTENAME(COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?')) + N'.' + QUOTENAME(COALESCE(OBJECT_NAME(@@PROCID), N'?')), [message] nvarchar(max) NOT NULL, severity tinyint NOT NULL, state tinyint NOT NULL, [number] int, [line] int);
        DECLARE CursorMessages CURSOR FAST_FORWARD LOCAL FOR SELECT [Date], [Procedure], [Message], [Severity], [State], [Number], [Line] FROM @messages ORDER BY [ID] ASC;
        DECLARE @cursorDate datetime2;
        DECLARE @cursorProcedure sysname;
        DECLARE @cursorMessage nvarchar(max);
        DECLARE @cursorSeverity tinyint;
        DECLARE @cursorState tinyint;
        DECLARE @cursorNumber int;
        DECLARE @cursorLine int;    
        DECLARE @levelVerbose int;    

        ----------------------------------------------------------------------------------------------------
        -- START
        ----------------------------------------------------------------------------------------------------
        SET ARITHABORT ON;
        SET NOCOUNT ON;
        SET NUMERIC_ROUNDABORT OFF;

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
            , ( N'@ArchiveBeforeDate: ' + ISNULL(CONVERT(nvarchar(20), @ArchiveBeforeDate, 121), N'NULL'), 10, 1)
            , ( N'@KeepBeforeDate: ' + ISNULL(CONVERT(nvarchar(20), @KeepBeforeDate, 121 ), N'NULL'), 10, 1)
            , ( N'@MaxRunMinutes: ' + ISNULL(CAST(@MaxRunMinutes AS nvarchar(10)), N'NULL'), 10, 1)
            , ( N'@DestinationTable: ' + ISNULL(@DestinationTable, N'NULL'), 10, 1)

            , ( N'@DryRunOnly: ' + ISNULL(@DryRunOnly, N'NULL (=> default = Yes)'), 10, 1)
            , ( N'@CleanupAfterArchiving: ' + ISNULL(@CleanupAfterArchiving, N'NULL (=> default = Yes)'), 10, 1)

            , ( N'@ArchiveIdentityColumn: ' + ISNULL(@ArchiveIdentityColumn, N'NULL (=> default = No)'), 10, 1)
            , ( N'@DestinationColumnsMapping: ' + ISNULL( REPLACE(REPLACE(REPLACE(@DestinationColumnsMapping, CHAR(9), N''), CHAR(10), N''), CHAR(13), N''), N'NULL'), 10, 1)

            , ( N'@OnErrorRetry: ' + ISNULL(CAST(@OnErrorRetry AS nvarchar(10)), N'NULL'), 10, 1)
            , ( N'@OnErrorWaitMillisecond: ' + ISNULL(CAST(@OnErrorWaitMillisecond AS nvarchar(10)), N'NULL'), 10, 1)

            , ( N'@DisableIndex: ' + ISNULL(@DisableIndex, N'NULL (=> default = No)'), 10, 1)
            , ( N'@RebuildIndex: ' + ISNULL(@RebuildIndex, N'NULL (=> default = No)'), 10, 1)
            , ( N'@RebuildIndexOnline: ' + ISNULL(@RebuildIndexOnline, N'NULL (=> default = Yes on Enterprise Edition)'), 10, 1)

            , ( N'@SaveMessagesToTable: ' + ISNULL(@SaveMessagesToTable, N'NULL (=> default = Yes)'), 10, 1)
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
        IF @HoursToKeep IS NOT NULL AND @ArchiveBeforeDate IS NOT NULL
        BEGIN 
            INSERT INTO @messages ([Message], Severity, [State]) VALUES ( N'@HoursToKeep OR @ArchiveBeforeDate cannot be used simultaneously. Use either @HoursToKeep OR @ArchiveBeforeDate', 16, 1);
        END 
        IF @HoursToKeep IS  NULL AND @ArchiveBeforeDate IS  NULL
        BEGIN 
            INSERT INTO @messages ([Message], Severity, [State]) VALUES ( N'@HoursToKeep OR @ArchiveBeforeDate cannot be both NULL. Use either @HoursToKeep OR @ArchiveBeforeDate', 16, 1);
        END 
        IF @HoursToKeep < 1
        BEGIN
            INSERT INTO @messages ([Message], Severity, [State]) VALUES ( N'@HoursToKeep must be bigger than 0', 16, 1);
        END
        IF @ArchiveBeforeDate > @startTime
        BEGIN
            INSERT INTO @messages ([Message], Severity, [State]) VALUES ( N'@ArchiveBeforeDate must be a past date and time', 16, 1);
        END

        IF (@HoursToKeep > 1 AND @ArchiveBeforeDate IS NULL) OR (@ArchiveBeforeDate < @startTime AND @HoursToKeep IS NULL)
        BEGIN
            SET @maxCreationTime = CASE WHEN @HoursToKeep IS NULL THEN @ArchiveBeforeDate ELSE DATEADD(hour, -ABS(@HoursToKeep), @startTime) END;
        END
        ELSE INSERT INTO @messages ([Message], Severity, [State]) VALUES ( N'Archive Date upper boundary cannot be set with the currently set values for @HoursToKeep and @ArchiveBeforeDate', 16, 1);

        -- Check @KeepBeforeDate
        IF @KeepBeforeDate > @startTime
        BEGIN
            INSERT INTO @messages ([Message], Severity, [State]) VALUES ( N'@KeepBeforeDate must be a past date and time', 16, 1);
        END
        ELSE IF @KeepBeforeDate > @maxCreationTime
        BEGIN
            INSERT INTO @messages ([Message], Severity, [State]) VALUES ( N'@KeepBeforeDate must be a date before: ' + CONVERT(nvarchar(20), @maxCreationTime, 121 ), 16, 1);
        END
        ELSE SET @minCreationTime = @KeepBeforeDate;

        -- Check @ArchiveIdentityColumn (NULL => NO)
        SELECT @copyIdentity = [value] FROM @paramsYesNo WHERE [parameter] = ISNULL(NULLIF(LTRIM(RTRIM(@ArchiveIdentityColumn)), N''), N'N');
        IF @copyIdentity IS NULL 
        BEGIN
            INSERT INTO @messages ([Message], Severity, [State]) VALUES
                ( 'Parameter @ArchiveIdentityColumn is invalid: ' + LTRIM(RTRIM(@ArchiveIdentityColumn)), 10, 1)
                , ('USE: Y or N', 10, 1)
                , ('Parameter @ArchiveIdentityColumn is invalid: ' + LTRIM(RTRIM(@ArchiveIdentityColumn)), 16, 1);
        END

        -- Clean Column Mapping parameter 
        SET @mapping = @DestinationColumnsMapping
        -- Remove special characters: end of line, return, space and double delimiter
        SET @mapping = REPLACE(@mapping, CHAR(13), N'');
        SET @mapping = REPLACE(@mapping, CHAR(10), N'');
        SET @mapping = REPLACE(@mapping, CHAR(9), N'');
        --SET @mapping = REPLACE(@mapping, N'*', N'%');
        SET @mapping = LTRIM(RTRIM(@mapping));
        WHILE CHARINDEX(@delimiter+' ', @mapping) > 0 SET @mapping = REPLACE(@mapping, @delimiter+' ', @delimiter);
        WHILE CHARINDEX(' '+@delimiter, @mapping) > 0 SET @mapping = REPLACE(@mapping,' '+@delimiter, @delimiter);
        WHILE CHARINDEX(@delimiter+@delimiter, @mapping) > 0 SET @mapping = REPLACE(@mapping,@delimiter+@delimiter, @delimiter);
        WHILE CHARINDEX(@redirection+' ', @mapping) > 0 SET @mapping = REPLACE(@mapping, @redirection+' ', @redirection);
        WHILE CHARINDEX(' '+@redirection, @mapping) > 0 SET @mapping = REPLACE(@mapping,' '+@redirection, @redirection);
        WHILE CHARINDEX(@redirection+@redirection, @mapping) > 0 SET @mapping = REPLACE(@mapping,@redirection+@redirection, @redirection);
        --WHILE CHARINDEX(@delimiter+'[', @mapping) > 0 SET @mapping = REPLACE(@mapping, @delimiter+'[', @delimiter);
        --WHILE CHARINDEX(']'+@delimiter, @mapping) > 0 SET @mapping = REPLACE(@mapping, ']'+@delimiter, @delimiter);
        --WHILE CHARINDEX(@redirection+'[', @mapping) > 0 SET @mapping = REPLACE(@mapping, @redirection+'[', @redirection);
        --WHILE CHARINDEX(']'+@redirection, @mapping) > 0 SET @mapping = REPLACE(@mapping, ']'+@redirection, @redirection);
    	--WHILE CHARINDEX('[', @mapping) = 1 SET @mapping = RIGHT(@mapping, LEN(@mapping)-1);
	    --SET @mapping = REVERSE(LTRIM(RTRIM(@mapping)));
    	--WHILE CHARINDEX(']', @mapping) = 1 SET @mapping = RIGHT(@mapping, LEN(@mapping)-1);
	    --SET @mapping = REVERSE(LTRIM(RTRIM(@mapping)));*/

        -- Extract Columns' mapping from @mapping and return a list of source and destination columns
        WITH Split (StartPosition, EndPosition, Item) AS
        (
            SELECT StartPosition = 1
                , EndPosition = COALESCE(NULLIF(CHARINDEX(',', @mapping, 1), 0), LEN(@mapping) + 1)
                , Item = SUBSTRING(@mapping, 1, COALESCE(NULLIF(CHARINDEX(@Delimiter, @mapping, 1), 0), LEN(@mapping) + 1) - 1)
            WHERE @mapping IS NOT NULL
            UNION ALL
            SELECT StartPosition = CAST(EndPosition AS int) + 1
                , EndPosition = COALESCE(NULLIF(CHARINDEX(',', @mapping, EndPosition + 1), 0), LEN(@mapping) + 1)
                , Item = LTRIM(RTRIM( SUBSTRING(@mapping, EndPosition + 1, COALESCE(NULLIF(CHARINDEX(@Delimiter, @mapping, EndPosition + 1), 0), LEN(@mapping) + 1) - EndPosition - 1) ))
            FROM Split
            WHERE EndPosition < LEN(@mapping) + 1
        )
        INSERT INTO @mappingList(item, source, destination)
        SELECT DISTINCT Item
            , CASE WHEN CHARINDEX('[', [Source]) = 1 AND CHARINDEX(']', REVERSE(Source)) = 1 THEN SUBSTRING(Source, 2, LEN(Source) - 1 ) ELSE Source END
            , CASE WHEN CHARINDEX('[', [Destination]) = 1 AND CHARINDEX(']', REVERSE(Destination)) = 1 THEN SUBSTRING(Destination, 2, LEN(Destination) - 2 ) ELSE Destination END
        FROM (
            SELECT Item
                , [Source] = CAST( LEFT(Item, COALESCE(NULLIF(CHARINDEX(@redirection, Item, 1), 0), 1) -1) AS sysname )
                , [Destination] = CAST( RIGHT(Item, LEN(Item) - CHARINDEX(@redirection, Item, 1)) AS sysname )
            FROM Split
            WHERE Item <> N''
        ) i
        OPTION (MAXRECURSION 0);

        -- Check missing source column name(s)
        SET @string = NULL;
        SELECT TOP(10) @string = COALESCE(@string + ', ', '') + ISNULL(item, '') FROM @mappingList map WHERE source = N'';
        IF @string IS NOT NULL
        BEGIN
            INSERT INTO @messages ([Message], Severity, [State]) VALUES 
                (N'Invalid mapping: missing source column(s).', 10, 1)
                , (N'Error(s): ' + @string + N'.', 10, 1)
                , (N'Usage: <source-column1> = <destination-column1>, <source-column2> = <destination-column2>, ... = ...', 10, 1)
                , (N'Invalid mapping [missing source column(s)].', 16, 1);
        END

        -- Check duplicate destination column name(s)
        SET @string = NULL;
        SELECT TOP(10) @string = COALESCE(@string + ', ', '') + ISNULL(destination, '') FROM @mappingList map WHERE destination <> N'' GROUP BY destination HAVING COUNT(*) > 1;

        IF @string IS NOT NULL
        BEGIN       
            INSERT INTO @messages ([Message], Severity, [State]) VALUES 
                (N'Invalid mapping: duplicate destination column(s).', 10, 1)
                , (N'Error(s): ' + @string + N'.', 10, 1)
                , (N'Usage: <source-column1> = <destination-column1>, <source-column2> = <destination-column2>, ... = ...', 10, 1)
                , (N'Invalid mapping [duplicate destination column(s)].', 16, 1);
        END;

        -- Check column(s) both mapped and removed
        SET @string = NULL;
        SELECT TOP(10) @string = COALESCE(@string + ', ', '') + ISNULL(item, '') FROM @mappingList map
        WHERE (map.destination = N'' AND EXISTS(SELECT 1 FROM @mappingList WHERE source = map.source AND destination <> N''))
            OR (map.destination <> N'' AND EXISTS(SELECT 1 FROM @mappingList WHERE source = map.source AND destination = N''))
        ORDER BY map.item;

        IF @string IS NOT NULL
        BEGIN
            INSERT INTO @messages ([Message], Severity, [State]) VALUES 
                (N'Invalid mapping: source column(s) mapped to both empty and destination columns.', 10, 1)
                , (N'Error(s): ' + @string + N'.', 10, 1)
                , (N'Invalid mapping (source column(s) mapped to both empty and destination columns).', 16, 1)
        END;

        -- Check Indetity column mapping
        INSERT INTO @messages ([Message], Severity, [State])
        SELECT TOP(1) N'Invalid mapping [identity column]: ' + map.item + N'. Use @ArchiveIdentityColumn=Y{es} or remove Identity Column from mapping.', 16, 1
        FROM sys.tables tbl 
        INNER JOIN sys.columns col ON col.object_id = tbl.object_id
        INNER JOIN @mappingList map ON map.source = col.name
        WHERE tbl.object_id = OBJECT_ID(N'[dbo].[logs]') AND type_desc = N'USER_TABLE'
            AND col.is_identity = 1 AND @copyIdentity <> 1 AND map.destination <> N'' ;

        -- Check invalid source column(s)
        SET @string = NULL;
        SELECT TOP(10) @string = COALESCE(@string + ', ', '') + ISNULL(source, '') 
        FROM @mappingList map
        WHERE NOT EXISTS (
            SELECT 1 FROM sys.tables tbl 
            INNER JOIN sys.columns col ON col.object_id = tbl.object_id
            WHERE tbl.object_id = OBJECT_ID(N'[dbo].[logs]') AND type_desc = N'USER_TABLE' AND map.source = col.name
        );

        IF ISNULL(@string, N'') <> N'' 
        BEGIN 
            INSERT INTO @messages ([Message], Severity, [State]) VALUES 
                (N'Invalid mapping: source column(s) not found.', 10, 1)
                , (N'Error(s): ' + @string + N'.', 10, 1)
                , (N'Invalid mapping [source column(s) not found].', 16, 1);
        END;

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

        -- Check ALTER Permission on [dbo].[logs]
        INSERT INTO @messages ([Message], Severity, [State])
        SELECT 'Permission not effectively granted: ' + UPPER(p.permission_name), 10, 1
        FROM (VALUES(N'', N'ALTER')) AS p (subentity_name, permission_name)
        LEFT JOIN sys.fn_my_permissions(N'[dbo].[logs]', N'OBJECT') eff ON eff.subentity_name = p.subentity_name AND eff.permission_name = p.permission_name
        WHERE eff.permission_name IS NULL AND (@indexDisable = 1 OR @indexRebuild = 1)
        ORDER BY p.permission_name;

        IF @@ROWCOUNT > 0 
        BEGIN
            INSERT INTO @messages ([Message], Severity, [State]) VALUES
                (N'Error: missing permission', 10, 1)
                , (N'When set to Y, @DisableIndex or @RebuildIndex require ALTER permission on [dbo].[logs] table', 16, 1);
        END

        -- Check SELECT & DELETE permission on [dbo].[logs]
        INSERT INTO @messages ([Message], Severity, [State])
        SELECT 'Permission not effectively granted: ' + UPPER(p.permission_name), 10, 1
        FROM (VALUES(N'', N'SELECT'), (N'', N'DELETE')) AS p (subentity_name, permission_name)
        LEFT JOIN sys.fn_my_permissions(N'[dbo].[logs]', N'OBJECT') eff ON eff.subentity_name = p.subentity_name AND eff.permission_name = p.permission_name
        WHERE eff.permission_name IS NULL
        ORDER BY p.permission_name;

        IF @@ROWCOUNT > 0 
        BEGIN
            INSERT INTO @messages ([Message], Severity, [State]) VALUES
                (N'Error: missing permission', 10, 1)
                , (N'SELECT and DELETE permissions are required on [dbo].[logs] table', 16, 1);
        END

        -- Check if Source and Destination are the same table
        IF OBJECT_ID(@DestinationTable) = OBJECT_ID(N'[dbo].[logs]')
        BEGIN
            INSERT INTO @messages ([Message], Severity, [State]) VALUES
                (N'Error: invalid destination table', 10, 1)
                , (N'@DestinationTable cannot be the same as source table ([dbo].[logs])', 16, 1);
        END

        -- Check Invalid or missing Destination Table
        IF OBJECT_ID(@DestinationTable) IS NULL 
        BEGIN
            INSERT INTO @messages ([Message], Severity, [State]) VALUES
                (N'Error: missing or invalid destination table', 10, 1)
                , (N'@DestinationTable must an existing and valid table: ' + @DestinationTable, 16, 1);
        END
        ELSE
        BEGIN
            -- Check INSERT permission on Destination table
            INSERT INTO @messages ([Message], Severity, [State])
            SELECT 'Permission not effectively granted: ' + UPPER(p.permission_name), 10, 1
            FROM (VALUES(N'', N'INSERT')) AS p (subentity_name, permission_name)
            LEFT JOIN sys.fn_my_permissions(@DestinationTable, N'OBJECT') eff ON eff.subentity_name = p.subentity_name AND eff.permission_name = p.permission_name
            WHERE eff.permission_name IS NULL
            ORDER BY p.permission_name;

            IF @@ROWCOUNT > 0 
            BEGIN
                INSERT INTO @messages ([Message], Severity, [State]) VALUES
                    (N'Error: missing permission or invalid destination table', 10, 1)
                    , (N'INSERT permission is required on Destnation table AND @DestinationTable must an existing table' + @DestinationTable, 16, 1);
            END
            ELSE SET @tableArchive = @DestinationTable;
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
            -- Check missing permissions on Runs and Maintenance Table
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

        ----------------------------------------------------------------------------------------------------
        -- Check Error(s) count
        ----------------------------------------------------------------------------------------------------

        SELECT @errorCount = COUNT(*) FROM @messages WHERE severity >= 16;
        IF @errorCount > 0 INSERT INTO @messages ([Message], Severity, [State]) SELECT N'End, see previous Error(s): ' + CAST(@errorCount AS nvarchar(10)) + N' found', 16, 1;

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

            -- Check Dry Run
            SELECT @dryRun = [value] FROM @paramsYesNo WHERE [parameter] = LTRIM(RTRIM(@DryRunOnly));
            IF @dryRun IS NULL 
            BEGIN
                SET @dryRun = 1;
                INSERT INTO @messages ([Message], Severity, [State]) VALUES 
                    (N'@DryRunOnly is NULL. Default value will be used (Yes).', 10, 1);
            END

            -- Check 
            SELECT @deleteRows = [value] FROM @paramsYesNo WHERE [parameter] = LTRIM(RTRIM(@CleanupAfterArchiving));
            IF @deleteRows IS NULL 
            BEGIN
                SET @deleteRows = 1;
                INSERT INTO @messages ([Message], Severity, [State]) VALUES 
                    (N'@CleanupAfterArchiving is NULL. Default value will be used (Yes).', 10, 1);
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

            -- Archive settings
            INSERT INTO @messages ([Message], Severity, [State]) SELECT N'Keep past hours: ' + CAST(@HoursToKeep AS nvarchar(10)), 10, 1 WHERE @HoursToKeep IS NOT NULL;
            INSERT INTO @messages ([Message], Severity, [State]) VALUES (N'Archive data before: ' + CONVERT(nvarchar(20), @maxCreationTime, 121), 10, 1);
            INSERT INTO @messages ([Message], Severity, [State]) SELECT N'Keep data before: ' + CAST(@minCreationTime AS nvarchar(10)), 10, 1 WHERE @minCreationTime IS NOT NULL;

            SELECT @maxId = MAX(Id) FROM dbo.Logs WITH (READPAST) WHERE TimeStamp < @maxCreationTime;

            INSERT INTO @messages ([Message], Severity, [State])
            SELECT CASE WHEN @maxId IS NULL THEN N'Nothing to archive before ' + CONVERT(nvarchar(20), @maxCreationTime, 121)  ELSE N'Archive Row(s) below Id: ' + CAST(@maxId AS nvarchar(10)) END, 10, 1;

            INSERT INTO @messages ([Message], Severity, [State]) VALUES
                (N'Run until = ' + ISNULL(CONVERT(nvarchar(max), @maxRunDateTime, 121), N'unlimited'), 10, 1 )
        END

        ----------------------------------------------------------------------------------------------------
        -- Create new Run Id
        ----------------------------------------------------------------------------------------------------
        IF @logToTable = 1 
        BEGIN
            INSERT INTO [Maintenance].[Runs]([Type], [Info], [StartTime]) SELECT N'Archive Logs', N'PROCEDURE ' + @procName, @startTime;
            SELECT @runId = @@IDENTITY;
            INSERT INTO @messages ([Message], Severity, [State]) VALUES 
                (N'Messages saved to Run Id: ' + CONVERT(nvarchar(10), @runId), 10, 1);
        END

        ----------------------------------------------------------------------------------------------------
        -- Output Column Mapping
        ----------------------------------------------------------------------------------------------------
        If @errorCount = 0 
        BEGIN
            INSERT INTO @messages ([Message], Severity, [State]) VALUES 
                (@lineBreak, 10, 1)
                , (@lineSeparator, 10, 1)
                , (N'Columns'' Mapping', 10, 1)
                , (@lineSeparator, 10, 1)

            INSERT INTO @messages ([Message], Severity, [State])
            SELECT CASE WHEN col.is_identity = 1 AND @copyIdentity = 0 THEN N'  ### ' + QUOTENAME(col.name) + N' <=> # (Identity Column, not archived)'
                        WHEN map.destination IS NULL OR col.name = map.destination THEN N'  ==> ' + QUOTENAME(col.name) + N' <=> ' + QUOTENAME(col.name) + N' (archived)'
                        WHEN map.destination <> N'' THEN N'  ~~> ' + QUOTENAME(col.name) +  N' <=> ' + QUOTENAME(map.destination) + N' (remapped)'
                        ELSE  N'  ### ' + QUOTENAME(col.name) + N' <=> # (not archived)'
                    END
                , 10, 1
            FROM sys.tables tbl 
            INNER JOIN sys.columns col ON col.object_id = tbl.object_id
            LEFT JOIN @mappingList map ON map.source = col.name
            WHERE tbl.object_id = OBJECT_ID(N'[dbo].[logs]') AND type_desc = N'USER_TABLE'
            ORDER BY col.column_id ASC, map.destination ASC
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
            IF @runId IS NOT NULL UPDATE [Maintenance].[Runs] SET [EndDate] = SYSDATETIME(), [ErrorStatus] = 2 WHERE Id = @runId;
            RETURN;
        END

        ----------------------------------------------------------------------------------------------------
        -- End Run on Dry Run
        ----------------------------------------------------------------------------------------------------
        IF @dryRun <> 0 
        BEGIN
            IF @runId IS NOT NULL UPDATE [Maintenance].[Runs] SET [EndDate] = SYSDATETIME(), [ErrorStatus] = 1 WHERE Id = @runId;

            EXEC sp_executesql @stmt = @stmtLogMessage, @params = @paramsLogMessage, @RunId = @runId, @Procedure = @procName, @Message = @lineBreak, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;
            EXEC sp_executesql @stmt = @stmtLogMessage, @params = @paramsLogMessage, @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;
            SET @message = 'DRY RUN ONLY (check parameters and set @DryRunOnly to No when ready)';
            EXEC sp_executesql @stmt = @stmtLogMessage, @params = @paramsLogMessage, @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 11, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;
            RETURN;
        END
        ----------------------------------------------------------------------------------------------------
        -- Remove Saved messages
        ----------------------------------------------------------------------------------------------------
        DELETE FROM @messages;

        EXEC sp_executesql @stmt = @stmtLogMessage, @params = @paramsLogMessage, @RunId = @runId, @Procedure = @procName, @Message = @lineBreak, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;

        ----------------------------------------------------------------------------------------------------
        -- Disable Indexes
        ----------------------------------------------------------------------------------------------------
        IF @indexDisable = 1
        BEGIN 
            -- NOT IMPLEMENTED: disable indexes
            EXEC sp_executesql @stmt = @stmtLogMessage, @params = @paramsLogMessage, @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;
            EXEC sp_executesql @stmt = @stmtLogMessage, @params = @paramsLogMessage, @RunId = @runId, @Procedure = @procName, @Message = N'Disable Indexes', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;
            EXEC sp_executesql @stmt = @stmtLogMessage, @params = @paramsLogMessage, @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;
            EXEC sp_executesql @stmt = @stmtLogMessage, @params = @paramsLogMessage, @RunId = @runId, @Procedure = @procName, @Message = N'### NOT IMPLEMENTED ###: Disable Indexes', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;
            EXEC sp_executesql @stmt = @stmtLogMessage, @params = @paramsLogMessage, @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;
            EXEC sp_executesql @stmt = @stmtLogMessage, @params = @paramsLogMessage, @RunId = @runId, @Procedure = @procName, @Message = @lineBreak, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;
            -- NOT IMPLEMENTED: disable indexes
        END

        ----------------------------------------------------------------------------------------------------
        -- Archive
        ----------------------------------------------------------------------------------------------------
        EXEC sp_executesql @stmt = @stmtLogMessage, @params = @paramsLogMessage, @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;
        EXEC sp_executesql @stmt = @stmtLogMessage, @params = @paramsLogMessage, @RunId = @runId, @Procedure = @procName, @Message = 'Start archive', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;
        EXEC sp_executesql @stmt = @stmtLogMessage, @params = @paramsLogMessage, @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;
        EXEC sp_executesql @stmt = @stmtLogMessage, @params = @paramsLogMessage, @RunId = @runId, @Procedure = @procName, @Message = N'Archiving in progress... (Verbose not set)', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = 0;

        -- Get source & destination columns
        SELECT @sourceCols = COALESCE(@sourceCols + ', ', '') + 'lgs.'+QUOTENAME(col.name)
            , @destCols = COALESCE(@destCols + ', ', '') + QUOTENAME(COALESCE(NULLIF(map.destination, col.name), col.name))
        FROM sys.tables tbl 
        INNER JOIN sys.columns col ON col.object_id = tbl.object_id
        LEFT JOIN @mappingList map ON map.source = col.name
        WHERE tbl.object_id = OBJECT_ID(N'[dbo].[logs]') AND type_desc = N'USER_TABLE' 
            AND (col.is_identity = 0 OR (col.is_identity = 1 AND @copyIdentity = 1) ) AND (map.destination <> N'' OR map.destination IS NULL)
        ORDER BY col.column_id ASC, map.destination ASC;


        SET @stmtArchive = N'
            DECLARE @ids TABLE (Id bigint PRIMARY KEY CLUSTERED);

            INSERT INTO @ids(id) SELECT TOP(@deleteTopRows) id FROM [dbo].[Logs] WITH (READPAST) WHERE Id >= @minId AND Id <= @maxId AND (@minCreationTime IS NULL OR TimeStamp >= @minCreationTime) AND TimeStamp < @maxCreationTime ORDER BY Id ASC;

            INSERT INTO ' + @tableArchive + N'(' + @destCols + N')
            SELECT ' + @sourceCols + N'
            FROM [dbo].[Logs] lgs WITH (READPAST)
            INNER JOIN @ids ids ON ids.Id = lgs.Id
            ORDER BY Id ASC;

            SELECT Id FROM @Ids;
        '

        SELECT @totalIds = 0, @MinId = 0, @countErrorRetry = 0, @errorCount = 0;    

        WHILE 0 = 0
        BEGIN
            BEGIN TRY
                BEGIN TRAN

                -- Archive
                BEGIN TRY
                    INSERT INTO @ids(Id)
                    EXEC sp_executesql @stmt = @stmtArchive, @params = @paramsArchive, @deleteTopRows = @deleteTopRows, @minId = @minId, @maxId = @maxId, @minCreationTime = @minCreationTime, @maxCreationTime = @maxCreationTime;

                    SELECT @countIds = @@ROWCOUNT;
                END TRY
                BEGIN CATCH
                    IF @@TRANCOUNT > 0 ROLLBACK TRAN;
                    SET @message = N'ERROR (Archive): '+ ERROR_MESSAGE();
                    EXEC sp_executesql @stmt = @stmtLogMessage, @params = @paramsLogMessage, @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;
                    THROW
                END CATCH

                -- Cleanup
                IF @deleteRows = 1 AND 1=0
                BEGIN
                    BEGIN TRY
                        DELETE lgs 
                        FROM [dbo].[Logs] lgs
                        INNER JOIN @ids ids ON ids.Id = lgs.Id;
                    END TRY
                    BEGIN CATCH
                        IF @@TRANCOUNT > 0 ROLLBACK TRAN;
                        SET @message = N'ERROR (Delete): '+ ERROR_MESSAGE();
                        EXEC sp_executesql @stmt = @stmtLogMessage, @params = @paramsLogMessage, @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;
                        THROW
                    END CATCH
                END

                IF @@TRANCOUNT > 0 COMMIT TRAN;

            END TRY
            BEGIN CATCH
                SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
                IF @@TRANCOUNT > 0 ROLLBACK TRAN;

                IF @countErrorRetry < @maxErrorRetry 
                BEGIN
                    SET @countErrorRetry = @countErrorRetry + 1;
                    SELECT @message = N' - Wait before retry: ' + CAST(@errorDelay AS nvarchar(10)) + N'ms';
                    EXEC sp_executesql @stmt = @stmtLogMessage, @params = @paramsLogMessage, @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;
                    WAITFOR DELAY @errorWait;
                    SET @message = N' ~ Retry ' + CAST(@countErrorRetry AS nvarchar(10)) + N' / ' + CAST(@maxErrorRetry AS nvarchar(10));
                    EXEC sp_executesql @stmt = @stmtLogMessage, @params = @paramsLogMessage, @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;
                    CONTINUE;
                END
                ELSE
                BEGIN;
                    THROW;
                END
            END CATCH

            IF @countIds = 0
            BEGIN
                SET @message = N'nothing left to archive...';
                EXEC sp_executesql @stmt = @stmtLogMessage, @params = @paramsLogMessage, @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;
                BREAK;
            END

            SET @countErrorRetry = 0 
            SET @totalIds = @totalIds + @countIds;
            SELECT @currentId = MAX(Id), @minId = MIN(Id) FROM @ids;

            DELETE FROM @ids;

            SET @message = N' - Archived Ids: ' + CAST(@countIds AS nvarchar(10)) + N' (ID ' + CAST(@MinId AS nvarchar(10)) + N' to ' + CAST(@currentId AS nvarchar(10)) + N', total archived = ' + CAST(@totalIds AS nvarchar(10)) + N')';
            EXEC sp_executesql @stmt = @stmtLogMessage, @params = @paramsLogMessage, @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 5, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;

            SET @minId = @currentId +1;

            IF SYSDATETIME() > @maxRunDateTime 
            BEGIN
                EXEC sp_executesql @stmt = @stmtLogMessage, @params = @paramsLogMessage, @RunId = @runId, @Procedure = @procName, @Message = @lineBreak, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;
                SELECT @message = N'TIME OUT:' + (SELECT CAST(DATEDIFF(MINUTE, @startTime, SYSDATETIME()) AS nvarchar(20)) ) + N' minutes (@MaxRunMinutes = ' + ISNULL(CAST(@MaxRunMinutes AS nvarchar(20)), N'' ) + N')';
                EXEC sp_executesql @stmt = @stmtLogMessage, @params = @paramsLogMessage, @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;
                BREAK;
            END
        END

        EXEC sp_executesql @stmt = @stmtLogMessage, @params = @paramsLogMessage, @RunId = @runId, @Procedure = @procName, @Message = @lineBreak, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;
        EXEC sp_executesql @stmt = @stmtLogMessage, @params = @paramsLogMessage, @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;
        SET @message = N'Archiving is finished'
        EXEC sp_executesql @stmt = @stmtLogMessage, @params = @paramsLogMessage, @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;
        EXEC sp_executesql @stmt = @stmtLogMessage, @params = @paramsLogMessage, @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;
        SET @message = N'Rows archived: ' + CAST(@totalIds AS nvarchar(max));
        EXEC sp_executesql @stmt = @stmtLogMessage, @params = @paramsLogMessage, @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;
        SET @message = N'Last Id archived: ' + CAST(@currentId AS nvarchar(max));
        EXEC sp_executesql @stmt = @stmtLogMessage, @params = @paramsLogMessage, @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;    
        SET @message = N'Elapsed time (minutes): ' + CAST(DATEDIFF(MINUTE, @startTime, SYSDATETIME()) AS nvarchar(10));
        EXEC sp_executesql @stmt = @stmtLogMessage, @params = @paramsLogMessage, @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;

        ----------------------------------------------------------------------------------------------------
        -- Rebuild Indexes
        ----------------------------------------------------------------------------------------------------
        IF @indexRebuild = 1 
        BEGIN 
            -- NOT IMPLEMENTED: rebuild index
            EXEC sp_executesql @stmt = @stmtLogMessage, @params = @paramsLogMessage, @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;
            EXEC sp_executesql @stmt = @stmtLogMessage, @params = @paramsLogMessage, @RunId = @runId, @Procedure = @procName, @Message = N'Rebuild Indexes', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;
            EXEC sp_executesql @stmt = @stmtLogMessage, @params = @paramsLogMessage, @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;
            EXEC sp_executesql @stmt = @stmtLogMessage, @params = @paramsLogMessage, @RunId = @runId, @Procedure = @procName, @Message = N'### NOT IMPLEMENTED ###: Rebuild Indexes', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;
            EXEC sp_executesql @stmt = @stmtLogMessage, @params = @paramsLogMessage, @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;
            EXEC sp_executesql @stmt = @stmtLogMessage, @params = @paramsLogMessage, @RunId = @runId, @Procedure = @procName, @Message = @lineBreak, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;
            -- NOT IMPLEMENTED: rebuild index
        END

    END TRY
    BEGIN CATCH
        SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
        IF @@TRANCOUNT > 0 ROLLBACK TRAN;
        IF @dryRun <> 0 THROW;
        EXEC sp_executesql @stmt = @stmtLogMessage, @params = @paramsLogMessage, @RunId = @runId, @Procedure = @procName, @Message = @lineBreak, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;
        EXEC sp_executesql @stmt = @stmtLogMessage, @params = @paramsLogMessage, @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;
        SET @message = N'Archiving is finished with error(s)'
        EXEC sp_executesql @stmt = @stmtLogMessage, @params = @paramsLogMessage, @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;
        EXEC sp_executesql @stmt = @stmtLogMessage, @params = @paramsLogMessage, @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;
        SET @message = N'Rows archived: ' + ISNULL(CAST(@totalIds AS nvarchar(max)), 0);
        EXEC sp_executesql @stmt = @stmtLogMessage, @params = @paramsLogMessage, @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;
        SET @message = N'Last Id archived: ' + CAST(ISNULL(@currentId, N'-') AS nvarchar(max));
        EXEC sp_executesql @stmt = @stmtLogMessage, @params = @paramsLogMessage, @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;    
        SET @message = N'Elapsed time (minutes): ' + CAST(DATEDIFF(MINUTE, @startTime, SYSDATETIME()) AS nvarchar(10));
        EXEC sp_executesql @stmt = @stmtLogMessage, @params = @paramsLogMessage, @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable;
        THROW;
    END CATCH
    ----------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------
    RETURN;
END
GO
