SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- DROP PROCEDURE [Maintenance].[ValidateArchiveObjects]
----------------------------------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[ValidateArchiveObjects]') AND type in (N'P'))
BEGIN
    EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [Maintenance].[ValidateArchiveObjects] AS'
    PRINT '  + CREATE PROCEDURE: [Maintenance].[ValidateArchiveObjects]';
END
ELSE PRINT '  = PROCEDURE [Maintenance].[ValidateArchiveObjects] already exists' 
GO

PRINT '  ~ UPDATE PROCEDURE: [Maintenance].[ValidateArchiveObjects]'
GO

ALTER PROCEDURE [Maintenance].[ValidateArchiveObjects]
----------------------------------------------------------------------------------------------------
-- ### [Object]: PROCEDURE [Maintenance].[ValidateArchiveObjects]
-- ### [Version]: 2020-10-01 00:00:00                                                         
-- ### [Source]: ??????
-- ### [Hash]: ??????
-- ### [Docs]: https://???.???
-- !!! ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!!!
-- !!! ~~~~~~~~~ NOT OFFICIALLY SUPPORTED BY UIPATH 
-- !!! ~~~~~~~~~ SQL Server >= 2016 SP1
----------------------------------------------------------------------------------------------------
    @SynonymSourceName nvarchar(256)
    , @SynonymSourceSchema nvarchar(256)
    , @SynonymArchiveName nvarchar(256)
    , @SynonymArchiveSchema nvarchar(256)
    , @SynonymASyncStatusName nvarchar(256)
    , @SynonymASyncStatusSchema nvarchar(256)
    , @ClusteredName nvarchar(128)
	, @ArchiveTableFullParts nvarchar(250)
	, @SourceTableFullParts nvarchar(250)
    , @ExcludeColumns nvarchar(MAX) = NULL
    , @IgnoreMissingColumns bit = 0
    , @ASyncStatusTableFullParts nvarchar(256) = NULL
    , @ASyncStatusExpectedColumns nvarchar(MAX) = NULL
    , @CreateOrUpdateSynonyms bit = 0
    , @CreateTable bit = 0
    , @UpdateTable bit = 0
    , @SourceColumns nvarchar(MAX) OUTPUT
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
        -- Source Variables
        ----------------------------------------------------------------------------------------------------
        DECLARE @sourceIsValid bit = 0;
        DECLARE @sourceTable nvarchar(256);
        DECLARE @sourceTable4Parts nvarchar(256);
        DECLARE @paramsSourceTableChecks nvarchar(MAX) = N'@Message nvarchar(MAX) OUTPUT, @Columns nvarchar(MAX) OUTPUT';
        DECLARE @stmtSourceTableChecks nvarchar(MAX) = N'';
        DECLARE @sourceJsonColumns nvarchar(MAX);
        DECLARE @listSourceColumns TABLE(Id int, [name] nvarchar(128), [type] nvarchar(128), max_length smallint, precision tinyint, scale tinyint
            , [datatype] AS ( [type] + 
                    CASE WHEN type IN ('varchar', 'char', 'varbinary', 'binary', 'text') THEN '(' + CASE WHEN max_length = -1 THEN 'MAX' ELSE CAST(max_length AS VARCHAR(5)) END + ')'
                    WHEN type IN ('nvarchar', 'nchar', 'ntext') THEN '(' + CASE WHEN max_length = -1 THEN 'MAX' ELSE CAST(max_length / 2 AS VARCHAR(5)) END + ')'
                    WHEN type IN ('datetime2', 'time2', 'datetimeoffset') THEN '(' + CAST(scale AS VARCHAR(5)) + ')'
                    WHEN type IN ('decimal', 'numeric') THEN '(' + CAST([precision] AS VARCHAR(5)) + ',' + CAST(scale AS VARCHAR(5)) + ')'
                    WHEN type IN ('float') THEN '(' + CAST([precision] AS VARCHAR(5)) + ')'
                    ELSE '' END )
        );
        DECLARE @listExcludeColumns TABLE([key] nvarchar(4000), [value] nvarchar(MAX), [type] int);
        DECLARE @jsonExclude nvarchar(MAX);
        ----------------------------------------------------------------------------------------------------
        -- Archive variables
        ----------------------------------------------------------------------------------------------------
        DECLARE @archiveObjectId bigint;
        DECLARE @archiveTable nvarchar(256);
        DECLARE @archiveSchema nvarchar(128);
        DECLARE @archiveTable2Parts nvarchar(256);
        DECLARE @listArchiveColumns TABLE(Id int, [name] nvarchar(128), [type] nvarchar(128), max_length smallint, precision tinyint, scale tinyint
            , [datatype] AS ( [type] + 
                    CASE WHEN type IN ('varchar', 'char', 'varbinary', 'binary', 'text') THEN '(' + CASE WHEN max_length = -1 THEN 'MAX' ELSE CAST(max_length AS VARCHAR(5)) END + ')'
                    WHEN type IN ('nvarchar', 'nchar', 'ntext') THEN '(' + CASE WHEN max_length = -1 THEN 'MAX' ELSE CAST(max_length / 2 AS VARCHAR(5)) END + ')'
                    WHEN type IN ('datetime2', 'time2', 'datetimeoffset') THEN '(' + CAST(scale AS VARCHAR(5)) + ')'
                    WHEN type IN ('decimal', 'numeric') THEN '(' + CAST([precision] AS VARCHAR(5)) + ',' + CAST(scale AS VARCHAR(5)) + ')'
                    WHEN type IN ('float') THEN '(' + CAST([precision] AS VARCHAR(5)) + ')'
                    ELSE '' END )
        );
        ----------------------------------------------------------------------------------------------------
        -- Sync Status variables
        ----------------------------------------------------------------------------------------------------
        DECLARE @asyncStatusTable nvarchar(256);
        DECLARE @asyncStatusSchema nvarchar(128);
        DECLARE @asyncStatusTable4Parts nvarchar(256);
        DECLARE @paramsASyncStatusTableChecks nvarchar(MAX) = N'@Message nvarchar(MAX) OUTPUT, @Columns nvarchar(MAX) OUTPUT';
        DECLARE @stmtASyncStatusTableChecks nvarchar(MAX);
        DECLARE @asyncStatusJsonColumns nvarchar(MAX);
        DECLARE @expectedASyncStatusColumns nvarchar(MAX)
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
            -- Synonym for Source table
            SELECT @sourceTable = ISNULL(LTRIM(RTRIM(@SourceTableFullParts)), N'');
            SELECT @sourceTable4Parts = ISNULL(QUOTENAME(LTRIM(RTRIM(PARSENAME(@sourceTable, 4)))) + N'.', N'') + ISNULL(QUOTENAME(LTRIM(RTRIM(PARSENAME(@sourceTable, 3)))) + N'.', N'') + ISNULL(QUOTENAME(LTRIM(RTRIM(PARSENAME(@sourceTable, 2)))) + N'.', N'') + ISNULL(QUOTENAME(LTRIM(RTRIM(PARSENAME(@sourceTable, 1)))), N'');

            INSERT INTO @json_errors([id], [severity], [message]) 
            SELECT 0, 16, N'ERROR[SS1]: Synonym ' + QUOTENAME(@synonymSourceSchema) + N'.' + QUOTENAME(@synonymSourceName) + N' not found and @SourceTableFullParts not provided' WHERE NOT EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = @synonymSourceName AND schema_id = SCHEMA_ID(@synonymSourceSchema)) AND @sourceTable = N''
            UNION ALL SELECT 1, 16, N'ERROR[SS2]: Synonym ' + QUOTENAME(@synonymSourceSchema) + N'.' + QUOTENAME(@synonymSourceName) + N' refers to an invalid @SourceTableFullParts''s name' WHERE @sourceTable <> N'' AND @sourceTable4Parts = N''
            UNION ALL SELECT 2, 16, N'ERROR[SS3]: Synonym ' + QUOTENAME(@synonymSourceSchema) + N'.' + QUOTENAME(@synonymSourceName) + N' not found and @CreateSynonym not enabled' WHERE NOT EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = @synonymSourceName AND schema_id = SCHEMA_ID(@synonymSourceSchema)) AND @sourceTable <> N'' AND @sourceTable4Parts <> N'' AND @CreateOrUpdateSynonyms <> 1
            UNION ALL SELECT 3, 16, N'ERROR[SS4]: Synonym ' + QUOTENAME(@synonymSourceSchema) + N'.' + QUOTENAME(@synonymSourceName) + N' must refers to a 2 (or more) parts name with at least a schema and table name: [schema_name].[table_name]' WHERE @sourceTable <> N'' AND @sourceTable4Parts <> N'' AND PARSENAME(@sourceTable, 2) IS NULL
            ;
        END TRY
        BEGIN CATCH;
            IF @@TRANCOUNT > 0 ROLLBACK;
            SET @message = N'ERROR[SS0]: error(s) occured while checking Source synonym';
            THROW;
        END CATCH; 

        BEGIN TRY
            -- Synonym for Archive table
            SELECT @archiveTable = ISNULL(LTRIM(RTRIM(@archiveTableFullParts)), N'');
            SELECT @archiveTable2Parts = ISNULL(QUOTENAME(LTRIM(RTRIM(PARSENAME(@archiveTable, 2)))) + N'.', N'') + ISNULL(QUOTENAME(LTRIM(RTRIM(PARSENAME(@archiveTable, 1)))), N'');

            INSERT INTO @json_errors([id], [severity], [message])
            SELECT 11, 16, N'ERROR[AS1]: Synonym ' + QUOTENAME(@synonymArchiveSchema) + N'.' + QUOTENAME(@synonymArchiveName) + N' not found and @ArchiveTableFullParts not provided' WHERE NOT EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = @synonymArchiveName AND schema_id = SCHEMA_ID(@synonymArchiveSchema)) AND  @archiveTable = N''
            UNION ALL SELECT 12, 16, N'ERROR[AS2]: Synonym ' + QUOTENAME(@synonymArchiveSchema) + N'.' + QUOTENAME(@synonymArchiveName) + N' not found and @ArchiveTableFullParts''s name is invalid' WHERE @archiveTable <> N'' AND @archiveTable2Parts = N''
            UNION ALL SELECT 13, 16, N'ERROR[AS3]: Synonym ' + QUOTENAME(@synonymArchiveSchema) + N'.' + QUOTENAME(@synonymArchiveName) + N' not found and @CreateOrUpdateSynonyms not enabled' WHERE  NOT EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = @synonymArchiveName AND schema_id = SCHEMA_ID(@synonymArchiveSchema)) AND @archiveTable <> N'' AND @archiveTable2Parts <> N'' AND @CreateOrUpdateSynonyms <> 1
            UNION ALL SELECT 14, 16, N'ERROR[AS4]: Synonym ' + QUOTENAME(@synonymArchiveSchema) + N'.' + QUOTENAME(@synonymArchiveName) + N' must refers to a 2 parts name with schema and table on the current database: [schema_name].[table_name]' WHERE @archiveTable <> N'' AND @archiveTable2Parts <> N'' AND PARSENAME(@archiveTable, 3) IS NOT NULL
            UNION ALL SELECT 15, 16, N'ERROR[AS5]: Synonym ' + QUOTENAME(@synonymArchiveSchema) + N'.' + QUOTENAME(@synonymArchiveName) + N' must refers to a 2 parts name with schema and table on the current database: [schema_name].[table_name]' WHERE @archiveTable <> N'' AND @archiveTable2Parts <> N'' AND PARSENAME(@archiveTable, 2) IS NULL
            ;
        END TRY
        BEGIN CATCH;
            IF @@TRANCOUNT > 0 ROLLBACK;
            SET @message = N'ERROR[AS0]: error(s) occured while checking Archive synonyms';
            THROW;
        END CATCH; 

        BEGIN TRY
            -- Synonym for ASync Status table
            SELECT @asyncStatusTable = ISNULL(LTRIM(RTRIM(@asyncStatusTableFullParts)), N'');
            SELECT @asyncStatusTable4Parts = ISNULL(QUOTENAME(LTRIM(RTRIM(PARSENAME(@asyncStatusTable, 4)))) + N'.', N'') + ISNULL(QUOTENAME(LTRIM(RTRIM(PARSENAME(@asyncStatusTable, 3)))) + N'.', N'') + ISNULL(QUOTENAME(LTRIM(RTRIM(PARSENAME(@asyncStatusTable, 2)))) + N'.', N'') + ISNULL(QUOTENAME(LTRIM(RTRIM(PARSENAME(@asyncStatusTable, 1)))), N'');

            INSERT INTO @json_errors([id], [severity], [message])
            SELECT 11, 16, N'ERROR[ST1]: Synonym ' + QUOTENAME(@synonymASyncStatusSchema) + N'.' + QUOTENAME(@synonymASyncStatusName) + N' not found and @ASyncStatusTableFullParts not provided' WHERE NOT EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = @synonymASyncStatusName AND schema_id = SCHEMA_ID(@synonymASyncStatusSchema)) AND  @asyncStatusTable = N''
            UNION ALL SELECT 12, 16, N'ERROR[ST2]: Synonym ' + QUOTENAME(@synonymASyncStatusSchema) + N'.' + QUOTENAME(@synonymASyncStatusName) + N' not found and @ASyncStatusTableFullParts''s name is invalid' WHERE @asyncStatusTable <> N'' AND @asyncStatusTable4Parts = N''
            UNION ALL SELECT 13, 16, N'ERROR[ST3]: Synonym ' + QUOTENAME(@synonymASyncStatusSchema) + N'.' + QUOTENAME(@synonymASyncStatusName) + N' not found and @CreateOrUpdateSynonyms not enabled' WHERE  NOT EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = @synonymASyncStatusName AND schema_id = SCHEMA_ID(@synonymASyncStatusSchema)) AND @asyncStatusTable <> N'' AND @asyncStatusTable4Parts <> N'' AND @CreateOrUpdateSynonyms <> 1
            UNION ALL SELECT 3, 16, N'ERROR[ST4]: Synonym ' + QUOTENAME(@synonymASyncStatusSchema) + N'.' + QUOTENAME(@synonymASyncStatusName) + N' must refers to a 2 (or more) parts name with at least a schema and table name: [schema_name].[table_name]' WHERE @asyncStatusTable <> N'' AND @asyncStatusTable4Parts <> N'' AND PARSENAME(@asyncStatusTable, 2) IS NULL
            ;
        END TRY
        BEGIN CATCH;
            IF @@TRANCOUNT > 0 ROLLBACK;
            SET @message = N'ERROR[ST0]: error(s) occured while checking ASyncStatus synonyms';
            THROW;
        END CATCH; 

        -- Check Source Table
        IF NOT EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10)
        BEGIN
            BEGIN TRY
                SELECT @sourceTable4Parts = base_object_name FROM sys.synonyms WHERE [name] = @synonymSourceName AND schema_id = SCHEMA_ID(@synonymSourceSchema) AND (@sourceTable4Parts IS NULL OR @sourceTable4Parts = N'');

                SELECT @stmtSourceTableChecks = N'
                DROP TABLE IF EXISTS #tempSourceTable;
                BEGIN TRY
                    SELECT TOP(0) * INTO #tempSourceTable FROM ' + @sourceTable4Parts + N';

                    SELECT @Columns = (
                        SELECT col.column_id, [column] = col.name, [type] = tpe.name, col.max_length, col.precision, col.scale
                        FROM tempdb.sys.columns AS col
                        INNER JOIN tempdb.sys.types AS tpe ON col.system_type_id = tpe.system_type_id AND tpe.system_type_id = tpe.user_type_id
                        WHERE [object_id] = OBJECT_ID(N''tempdb.dbo.#tempSourceTable'')
                        FOR JSON PATH
                    );
                END TRY
                BEGIN CATCH
                    THROW;
                END CATCH
                ';
                EXEC sp_executesql @stmt = @stmtSourceTableChecks, @params = @paramsSourceTableChecks, @Message = NULL, @Columns = @sourceJsonColumns OUTPUT;

                INSERT INTO @listSourceColumns([id], [name], [type], [max_length], [precision], [scale])
                SELECT [id], [name], [type], [max_length], [precision], [scale] FROM OPENJSON(@sourceJsonColumns, N'$')
                WITH ([id] int N'$.column_id', [name] nvarchar(128) N'$.column', [type] nvarchar(128), max_length smallint, precision tinyint, scale tinyint)

                IF @@ROWCOUNT = 0 INSERT INTO @json_errors([id], [severity], [message]) SELECT 20, 16, N'ERROR[TS1]: No column retrieved from remote source table';
            END TRY
            BEGIN CATCH
                SET @message = N'ERROR[TS0]: error(s) occured while checking remote source table';
                THROW;
            END CATCH
        END

        -- Check Exclude list
        IF NOT EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10)
        BEGIN
            BEGIN TRY
                SELECT @jsonExclude = ISNULL(LTRIM(RTRIM(@ExcludeColumns)), N'')
                IF ISJSON(@jsonExclude) = 1 
                BEGIN
                    INSERT INTO @listExcludeColumns([key], [value], [type])
                    SELECT [key], LTRIM(RTRIM([value])), [type] FROM OPENJSON(@ExcludeColumns, N'$')
                END
                INSERT INTO @json_errors([id], [severity], [message])
                SELECT 21, 16, N'ERROR[EX1] @ExcludeColumns is not a valid JSON string, an array of string(s) is expected: ["col1", "col2", ...]' WHERE @jsonExclude IS NOT NULL AND @jsonExclude <> N'' AND ISJSON(@ExcludeColumns) = 0
                UNION ALL SELECT 21, 16, N'ERROR[EX2] @ExcludeColumns contains invalid type(s), only an array of string(s) is expected: ["col1", "col2", ...]' WHERE EXISTS(SELECT 1 FROM @listExcludeColumns WHERE [type] <> 1) 
                UNION ALL SELECT 21, 16, N'ERROR[EX3] column ' + QUOTENAME(@clusteredName) + N' cannot be excluded (Primary / Clustered Key)' WHERE EXISTS(SELECT 1 FROM @listExcludeColumns WHERE [value] = @clusteredName) 
                UNION ALL SELECT 21, 16, N'ERROR[EX4] column ' + QUOTENAME(exc.[value]) + N' not found in Source table' FROM @listExcludeColumns exc WHERE exc.[value] <> N'' AND exc.[value] IS NOT NULL AND exc.[type] = 1 AND NOT EXISTS(SELECT 1 FROM @listSourceColumns WHERE [name] = exc.[value]) 
            END TRY
            BEGIN CATCH
                SET @message = N'ERROR[EX0]: error(s) occured while checking exclude list';
                THROW;
            END CATCH
        END
        -- Check Archive Table
        IF NOT EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10)
        BEGIN
            BEGIN TRY
                SELECT @archiveTable2Parts = base_object_name FROM sys.synonyms WHERE [name] = @synonymArchiveName AND schema_id = SCHEMA_ID(@synonymArchiveSchema) AND (@archiveTable2Parts IS NULL OR @archiveTable2Parts = N'');
                SELECT @archiveSchema = PARSENAME(@archiveTable2Parts, 2), @archiveTable = PARSENAME(@archiveTable2Parts, 1), @archiveObjectId = OBJECT_ID(@archiveTable2Parts)

                INSERT INTO @listArchiveColumns([id], [name], [type], [max_length], [precision], [scale])
                SELECT col.column_id, [column] = col.name, [type] = tpe.name, col.max_length, col.precision, col.scale
                FROM sys.columns AS col
                INNER JOIN sys.types AS tpe ON col.system_type_id = tpe.system_type_id AND tpe.system_type_id = tpe.user_type_id
                WHERE [object_id] = @archiveObjectId;

                INSERT INTO @json_errors([id], [severity], [message])
                SELECT 31, 16, N'ERROR[TA1]: Synonym ' + QUOTENAME(@synonymArchiveSchema) + N'.' + QUOTENAME(@synonymArchiveName) + N' refers to missing schema: ' + QUOTENAME(@archiveSchema) WHERE NOT EXISTS (SELECT 1 FROM sys.schemas WHERE [schema_id] = SCHEMA_ID(@archiveSchema))
                UNION ALL SELECT 32, 16, N'ERROR[TA2]: @CreateTable is not enabled and Synonym ' + QUOTENAME(@synonymArchiveSchema) + N'.' + QUOTENAME(@synonymArchiveName) + N' refers to a missing table: ' + @archiveTable + N'' WHERE @archiveObjectId IS NULL AND @CreateTable <> 1
                UNION ALL SELECT 33, 16, N'ERROR[TA3]: date type mismatch between source and archive table on column ' + QUOTENAME(src.[name]) + N': ' + src.[datatype] + N' vs ' + arc.[datatype]  FROM @listSourceColumns src
                    INNER JOIN @listArchiveColumns arc ON arc.[name] = src.[name] AND (arc.[type] <> src.[type] OR arc.[max_length] <> src.[max_length] OR arc.[precision] <> src.[precision] OR arc.[scale] <> src.[scale])
                UNION ALL SELECT 34, 16, N'ERROR[TA4]: missing column on archive table (@UpdateTable not set): ' +  QUOTENAME(src.[name]) FROM @listSourceColumns src 
                    WHERE NOT EXISTS(SELECT 1 FROM @listArchiveColumns WHERE [name] = src.[name]) AND @UpdateTable <> 1 AND @archiveObjectId IS NOT NULL AND (@IgnoreMissingColumns IS NULL OR @IgnoreMissingColumns = 0)
                UNION ALL SELECT 35, 16, N'ERROR[TA5]: Synonym ' + QUOTENAME(@synonymArchiveSchema) + N'.' + QUOTENAME(@synonymArchiveName) + N' must refer to a user table: ' + QUOTENAME(@archiveSchema) + N'.' + QUOTENAME(@archiveTable) WHERE OBJECTPROPERTY(@archiveObjectId, 'IsTable') = 0
                UNION ALL SELECT 36, 16, N'ERROR[TA6]: Synonym ' + QUOTENAME(@synonymArchiveSchema) + N'.' + QUOTENAME(@synonymArchiveName) + N' must refer to a user table with no IDENTITY column: ' + QUOTENAME(@archiveSchema) + N'.' + QUOTENAME(@archiveTable) WHERE OBJECTPROPERTY(@archiveObjectId, 'TableHasIdentity') = 1
                UNION ALL SELECT 37, 16, N'ERROR[TA7]: column is missing from source table:' + @clusteredName WHERE NOT EXISTS(SELECT 1 FROM @listSourceColumns WHERE [name] = @clusteredName)
                ;
            END TRY
            BEGIN CATCH
                IF @@TRANCOUNT > 0 ROLLBACK;
                SET @message = N'ERROR[TA0]: error(s) occured while checking archive table';
                THROW;
            END CATCH
        END
        -- Check ASyncStatus Table
        IF NOT EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10)
        BEGIN
            BEGIN TRY
                SELECT @asyncStatusTable4Parts = base_object_name FROM sys.synonyms WHERE [name] = @synonymASyncStatusName AND schema_id = SCHEMA_ID(@synonymASyncStatusSchema) AND (@asyncStatusTable4Parts IS NULL OR @asyncStatusTable4Parts = N'');

                SELECT @stmtASyncStatusTableChecks = N'
                DROP TABLE IF EXISTS #tempASyncStatusTable;
                BEGIN TRY
                    SELECT TOP(0) * INTO #tempASyncStatusTable FROM ' + @asyncStatusTable4Parts + N';

                    SELECT @Columns = (
                        SELECT col.column_id, [column] = col.name, [type] = tpe.name, col.max_length, col.precision, col.scale
                        FROM tempdb.sys.columns AS col
                        INNER JOIN tempdb.sys.types AS tpe ON col.system_type_id = tpe.system_type_id AND tpe.system_type_id = tpe.user_type_id
                        WHERE [object_id] = OBJECT_ID(N''tempdb.dbo.#tempASyncStatusTable'')
                        FOR JSON PATH
                    );
                END TRY
                BEGIN CATCH
                    THROW;
                END CATCH
                ';
                -- retrieve ASyncStatus columns
                EXEC sp_executesql @stmt = @stmtASyncStatusTableChecks, @params = @paramsASyncStatusTableChecks, @Message = NULL, @Columns = @asyncStatusJsonColumns OUTPUT;

                -- Set default columns if not provided
                SELECT @expectedASyncStatusColumns = ISNULL(LTRIM(RTRIM(@ASyncStatusExpectedColumns)), N'[{"column":"SyncId","type":"bigint"},{"column":"IsDeleted","type":"bit"},{"column":"DeletedOnDate","type":"datetime"},{"column":"FirstASyncId","type":"bigint"},{"column":"LastASyncId","type":"bigint"}]' );
                -- check columns
                WITH exp([name], [type], [max_length]) AS (
                    SELECT [name], [type], [max_length]/*, [precision], [scale]*/ FROM OPENJSON(@expectedASyncStatusColumns, N'$')
                    WITH ([name] nvarchar(128) N'$.column', [type] nvarchar(128), max_length smallint/*, precision tinyint, scale tinyint*/)                
                ), col([name], [type], [max_length]) AS(
                    SELECT [name], [type], [max_length] FROM OPENJSON(@asyncStatusJsonColumns, N'$')
                    WITH ([name] nvarchar(128) N'$.column', [type] nvarchar(128), max_length smallint)
                )
                INSERT INTO @json_errors([id], [severity], [message])
                SELECT 20, 16, N'ERROR[TS1]: No column retrieved from remote ASyncStatus table ' + @asyncStatusTable4Parts + N' refered by synonym ' + QUOTENAME(@synonymASyncStatusSchema) + N'.' + QUOTENAME(@synonymASyncStatusName) WHERE NOT EXISTS(SELECT 1 FROM col)
                UNION ALL SELECT 20, 16, N'ERROR[TS2]: Expected column ' + QUOTENAME(x.[name]) + N' not found in remote ASyncStatus table ' + @asyncStatusTable4Parts + N' refered by synonym ' + QUOTENAME(@synonymASyncStatusSchema) + N'.' + QUOTENAME(@synonymASyncStatusName) FROM exp x WHERE NOT EXISTS(SELECT 1 FROM col WHERE [name] = x.[name])
                UNION ALL SELECT 20, 16, N'ERROR[TS3]: Invalid type '+ QUOTENAME(c.[type]) + N' for column ' + QUOTENAME(x.[name]) + N' in remote ASyncStatus table ' + @asyncStatusTable4Parts + N' refered by synonym ' + QUOTENAME(@synonymASyncStatusSchema) + N'.' + QUOTENAME(@synonymASyncStatusName) + N' (' + QUOTENAME(x.[type]) + N' expected)' FROM exp x INNER JOIN col c ON x.[name] = c.[name] AND x.[type] <> c.[type]
                ;
            END TRY
            BEGIN CATCH
                SET @message = N'ERROR[TS0]: error(s) occured while checking remote ASyncStatus table';
                THROW;
            END CATCH
        END        
        ----------------------------------------------------------------------------------------------------      
        -- Create or Update Synonym(s) / Table / Columns
        ----------------------------------------------------------------------------------------------------
        IF NOT EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10)
        BEGIN
            -- Get list of Columns from source table
            SELECT @SourceColumns = NULL;
            DELETE col FROM @listSourceColumns col WHERE EXISTS(SELECT 1 FROM @listExcludeColumns WHERE [value] = col.[name])
            SELECT @SourceColumns = COALESCE(@SourceColumns + N', "' + [name] + N'"', N'"' + [name] + N'"') FROM @listSourceColumns col ORDER BY Id;
            SELECT @SourceColumns = N'['+ @SourceColumns + N']';

            -- Start Transaction for all upcoming schema changes
            BEGIN TRAN
            -- Create Source synonym if missing or outdated
            BEGIN TRY
                IF NOT EXISTS (SELECT 1 FROM sys.synonyms WHERE [name] = @synonymSourceName AND schema_id = SCHEMA_ID(@synonymSourceSchema) AND base_object_name = @sourceTable4Parts) AND @sourceTable4Parts <> N'' AND @CreateOrUpdateSynonyms = 1
                BEGIN
                    SET @sql = NULL;
                    INSERT INTO @json_errors([id], [severity], [message]) SELECT 0, 10, N'Create or alter Source Synonym '+ QUOTENAME(@synonymSourceSchema) + N'.' + QUOTENAME(@synonymSourceName) + ' with base object ' + @sourceTable4Parts;
                    SELECT @sql = N'DROP SYNONYM ' + QUOTENAME(@synonymSourceSchema) + N'.' + QUOTENAME(@synonymSourceName) + N';' FROM sys.synonyms WHERE [name] = @synonymSourceName AND schema_id = SCHEMA_ID(@synonymSourceSchema);
                    SELECT @sql = ISNULL(@sql, N'') + N'CREATE SYNONYM ' + QUOTENAME(@synonymSourceSchema) + N'.' + QUOTENAME(@synonymSourceName) + N' FOR ' + @sourceTable4Parts + N';';
                    EXEC sp_executesql @statement = @sql;
                END
            END TRY
            BEGIN CATCH
                SET @message = N'ERROR[SU0]: error(s) occured while creating or updating Source synomym';
                THROW;
            END CATCH
            -- Create Archive synonym if missing or outdated
            BEGIN TRY
                IF NOT EXISTS (SELECT 1 FROM sys.synonyms WHERE [name] = @synonymArchiveName AND schema_id = SCHEMA_ID(@synonymArchiveSchema) AND base_object_name = @archiveTable2Parts) AND @archiveTable2Parts <> N'' AND @CreateOrUpdateSynonyms = 1
                BEGIN
                    SET @sql = NULL;
                    INSERT INTO @json_errors([id], [severity], [message]) SELECT 10, 10, N'Create or alter Archive Synonym '+ QUOTENAME(@synonymArchiveSchema) + N'.' + QUOTENAME(@synonymArchiveName) + ' with base object: ' + @archiveTable2Parts;
                    SELECT @sql = N'DROP SYNONYM ' + QUOTENAME(@synonymArchiveSchema) + N'.' + QUOTENAME(@synonymArchiveName) + N';' FROM sys.synonyms WHERE [name] = @synonymArchiveName AND schema_id = SCHEMA_ID(@synonymArchiveSchema);
                    SELECT @sql = ISNULL(@sql, N'') + N'CREATE SYNONYM ' + QUOTENAME(@synonymArchiveSchema) + N'.' + QUOTENAME(@synonymArchiveName) + N' FOR ' + @archiveTable2Parts + N';';
                    EXEC sp_executesql @statement = @sql;
                END
            END TRY
            BEGIN CATCH
                IF @@TRANCOUNT > 0 ROLLBACK;
                SET @message = N'ERROR[SU1]: error(s) occured while creating or updating Archive synomym';
                THROW;
            END CATCH
            -- Create ASync Status synonym if missing or outdated
            BEGIN TRY
                IF NOT EXISTS (SELECT 1 FROM sys.synonyms WHERE [name] = @SynonymASyncStatusName AND schema_id = SCHEMA_ID(@synonymASyncStatusSchema) AND base_object_name = @asyncStatusTable4Parts) AND @asyncStatusTable4Parts  <> N'' AND @CreateOrUpdateSynonyms = 1
                BEGIN
                    SET @sql = NULL;
                    INSERT INTO @json_errors([id], [severity], [message]) SELECT 10, 10, N'Create or alter ASync Status Synonym '+ QUOTENAME(@synonymASyncStatusSchema) + N'.' + QUOTENAME(@synonymASyncStatusName) + ' with base object: ' + @asyncStatusTable4Parts;
                    SELECT @sql = N'DROP SYNONYM ' + QUOTENAME(@synonymASyncStatusSchema) + N'.' + QUOTENAME(@synonymASyncStatusName) + N';' FROM sys.synonyms WHERE [name] = @synonymASyncStatusName AND schema_id = SCHEMA_ID(@synonymASyncStatusSchema);
                    SELECT @sql = ISNULL(@sql, N'') + N'CREATE SYNONYM ' + QUOTENAME(@synonymASyncStatusSchema) + N'.' + QUOTENAME(@synonymASyncStatusName) + N' FOR ' + @asyncStatusTable4Parts + N';';
                    EXEC sp_executesql @statement = @sql;
                END
            END TRY
            BEGIN CATCH
                IF @@TRANCOUNT > 0 ROLLBACK;
                SET @message = N'ERROR[SU1]: error(s) occured while creating or updating ASync Status synomym';
                THROW;
            END CATCH
            -- Create Archive base table or Add missing column(s)
            BEGIN TRY
                IF ( (@archiveObjectId IS NULL AND @CreateTable = 1) OR @UpdateTable = 1) AND EXISTS( SELECT 1 FROM @listSourceColumns src WHERE [name] <> @clusteredName AND NOT EXISTS(SELECT 1 FROM @listArchiveColumns WHERE [name] = src.[name]) )
                BEGIN
                    -- Prepare CREATE/ALTER statement
                    SELECT @sql = NULL;
                    SELECT @sql = COALESCE(@sql + N', ' + q.[query], q.[query])
                    FROM (
                        SELECT TOP(1024) [query] = src.[name] + N' ' + src.[datatype] + N' NULL' + CHAR(13) + CHAR(10) 
                        FROM @listSourceColumns src WHERE [name] <> @clusteredName AND NOT EXISTS(SELECT 1 FROM @listArchiveColumns WHERE [name] = src.[name]) --AND @UpdateTable <> 1 AND @archiveObjectId IS NOT NULL
                        ORDER BY Id ASC
                    ) q([query]);
                    SELECT @message = COALESCE(@message + N', ' + src.[name], src.[name]) FROM @listSourceColumns src WHERE NOT EXISTS(SELECT 1 FROM @listArchiveColumns WHERE [name] = src.[name]) ORDER BY Id ASC;

                    IF @archiveObjectId IS NULL --> CREATE
                    BEGIN
                        INSERT INTO @json_errors([id], [severity], [message])
                        SELECT 30, 10, N'Create table refered to by archive synonym: ' + QUOTENAME(@synonymArchiveSchema) + N'.' + QUOTENAME(@synonymArchiveName) + N' => '+ QUOTENAME(@archiveSchema) + N'.' + QUOTENAME(@archiveTable) + N' (' + @message + N')';
                        SELECT @sql = N'CREATE TABLE ' + QUOTENAME(@archiveSchema) + '.' + QUOTENAME(@archiveTable) + '(' + QUOTENAME([name]) + ' ['+ datatype + '] NOT NULL CONSTRAINT [PK_' + @archiveSchema + '.'+ @archiveTable + '] PRIMARY KEY CLUSTERED ([Id] ASC)' + CHAR(13) + CHAR(10) + N', ' + @sql + N');'
                        FROM @listSourceColumns WHERE [name] = @clusteredName;
                    END
                    ELSE --> ALTER
                    BEGIN
                        INSERT INTO @json_errors([id], [severity], [message])
                        SELECT 30, 10, N'Add missing column(s) to table refered to by archive synonym: ' + QUOTENAME(@synonymArchiveSchema) + N'.' + QUOTENAME(@synonymArchiveName) + N' => '+ QUOTENAME(@archiveSchema) + N'.' + QUOTENAME(@archiveTable) + N' (' + @message + N')';
                        SELECT @sql = N'ALTER TABLE '+ QUOTENAME(@archiveSchema) + '.' + QUOTENAME(@archiveTable) + ' ADD ' + @sql + N';';
                    END

                    -- Execute create/alter table
                    EXEC sp_executesql @stmt = @sql;
                END
            END TRY
            BEGIN CATCH
                IF @@TRANCOUNT > 0 ROLLBACK;
                SET @message = N'ERROR[SU2]: error(s) occured while creating or updating source table';
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
