SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- DROP PROCEDURE [Maintenance].[SetArchiveTable]
----------------------------------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[SetArchiveTable]') AND type in (N'P'))
BEGIN
    EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [Maintenance].[SetArchiveTable] AS'
    PRINT '  + CREATE PROCEDURE: [Maintenance].[SetArchiveTable]';
END
ELSE PRINT '  = PROCEDURE [Maintenance].[SetArchiveTable] already exists' 
GO

PRINT '  ~ UPDATE PROCEDURE: [Maintenance].[SetArchiveTable]'
GO

ALTER PROCEDURE [Maintenance].[SetArchiveTable]
----------------------------------------------------------------------------------------------------
-- ### [Object]: PROCEDURE [Maintenance].[SetArchiveTable]
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
	, @ArchiveTableName nvarchar(250) = NULL
	, @ArchiveTableSchema nvarchar(250) = NULL
    , @ClusteredName nvarchar(128)
    , @ExcludeColumns nvarchar(MAX) = NULL
    , @IgnoreMissingColumns bit = 0
    , @CreateOrUpdateSynonym bit = 0
    , @CreateTable bit = 1
    , @UpdateTable bit = 1
    , @RemoveIdentity bit = 0
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
        DECLARE @sourceMessages nvarchar(MAX);
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
            EXEC [Maintenance].[SetSourceTable] @SynonymName = @SynonymSourceName, @SynonymSchema = @SynonymSourceSchema
                , @Columns = @sourceJsonColumns OUTPUT
                , @IsValid = @sourceIsValid OUTPUT
                , @Messages = @sourceMessages OUTPUT
            ;
            INSERT INTO @json_errors([id], [severity], [message])
            SELECT 0, [Severity], [Message] FROM OPENJSON(@sourceMessages, N'$') WITH ([Message] nvarchar(MAX), [Severity] tinyint);
        END TRY
        BEGIN CATCH;
            IF @@TRANCOUNT > 0 ROLLBACK;
            SET @message = N'ERROR[SS0]: error(s) occured while checking Source synonym and table';
            THROW;
        END CATCH; 

        BEGIN TRY
            -- Synonym for Archive table
            SELECT @archiveTable = LTRIM(RTRIM(@ArchiveTableName)), @archiveSchema = LTRIM(RTRIM(@ArchiveTableSchema));
            SELECT @archiveTable2Parts = ISNULL(QUOTENAME(@archiveSchema) + N'.' + QUOTENAME(@archiveTable), N'');
            SELECT @archiveTable = ISNULL(@archiveTable, N''), @archiveSchema = ISNULL(@archiveSchema, N'');

            INSERT INTO @json_errors([id], [severity], [message])
            SELECT 11, 16, N'ERROR[AS1]: Synonym ' + QUOTENAME(@SynonymArchiveSchema) + N'.' + QUOTENAME(@SynonymArchiveName) + N' not found and @ArchiveTableName not provided' WHERE NOT EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = @SynonymArchiveName AND schema_id = SCHEMA_ID(@SynonymArchiveSchema)) AND  @archiveTable = N''
            UNION ALL SELECT 11, 16, N'ERROR[AS1]: Synonym ' + QUOTENAME(@SynonymArchiveSchema) + N'.' + QUOTENAME(@SynonymArchiveName) + N' not found and @ArchiveTableSchema not provided' WHERE NOT EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = @SynonymArchiveName AND schema_id = SCHEMA_ID(@SynonymArchiveSchema)) AND  @archiveSchema = N''
            UNION ALL SELECT 13, 16, N'ERROR[AS3]: Synonym ' + QUOTENAME(@SynonymArchiveSchema) + N'.' + QUOTENAME(@SynonymArchiveName) + N' not found and @CreateOrUpdateSynonym not enabled' WHERE  NOT EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = @SynonymArchiveName AND schema_id = SCHEMA_ID(@SynonymArchiveSchema)) AND @archiveTable2Parts <> N'' /*AND @archiveTable2Parts <> N''*/ AND @CreateOrUpdateSynonym <> 1
            ;
        END TRY
        BEGIN CATCH;
            IF @@TRANCOUNT > 0 ROLLBACK;
            SET @message = N'ERROR[AS0]: error(s) occured while checking Archive synonyms';
            THROW;
        END CATCH; 

        -- Check Source Table
        IF NOT EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10)
        BEGIN
            BEGIN TRY
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
                SELECT @archiveTable2Parts = base_object_name FROM sys.synonyms WHERE [name] = @SynonymArchiveName AND schema_id = SCHEMA_ID(@SynonymArchiveSchema) AND (@archiveTable2Parts IS NULL OR @archiveTable2Parts = N'');
                SELECT @archiveSchema = ISNULL(LTRIM(RTRIM(@ArchiveTableSchema)), PARSENAME(@archiveTable2Parts, 2)), @archiveTable = ISNULL(LTRIM(RTRIM(@ArchiveTableName)), PARSENAME(@archiveTable2Parts, 1)), @archiveObjectId = OBJECT_ID(@archiveTable2Parts)

                INSERT INTO @listArchiveColumns([id], [name], [type], [max_length], [precision], [scale])
                SELECT col.column_id, [column] = col.name, [type] = tpe.name, col.max_length, col.precision, col.scale
                FROM sys.columns AS col
                INNER JOIN sys.types AS tpe ON col.system_type_id = tpe.system_type_id AND tpe.system_type_id = tpe.user_type_id
                WHERE [object_id] = @archiveObjectId;

                INSERT INTO @json_errors([id], [severity], [message])
                SELECT 31, 16, N'ERROR[TA1]: Synonym ' + QUOTENAME(@SynonymArchiveSchema) + N'.' + QUOTENAME(@SynonymArchiveName) + N' refers to a non existing schema: ' + QUOTENAME(@archiveSchema) WHERE NOT EXISTS (SELECT 1 FROM sys.schemas WHERE [schema_id] = SCHEMA_ID(@archiveSchema))
                UNION ALL SELECT 32, 16, N'ERROR[TA2]: @CreateTable is not enabled and Synonym ' + QUOTENAME(@SynonymArchiveSchema) + N'.' + QUOTENAME(@SynonymArchiveName) + N' refers to a non existing table: ' + @archiveTable + N'' WHERE @archiveObjectId IS NULL AND @CreateTable <> 1
                UNION ALL SELECT 33, 16, N'ERROR[TA3]: datatype mismatch between source and archive table on column ' + QUOTENAME(src.[name]) + N': ' + src.[datatype] + N' vs ' + arc.[datatype]  FROM @listSourceColumns src
                    INNER JOIN @listArchiveColumns arc ON arc.[name] = src.[name] AND (arc.[type] <> src.[type] OR arc.[max_length] <> src.[max_length] OR arc.[precision] <> src.[precision] OR arc.[scale] <> src.[scale])
                UNION ALL SELECT 34, 16, N'ERROR[TA4]: missing column on archive table (@UpdateTable not set): ' +  QUOTENAME(src.[name]) FROM @listSourceColumns src 
                    WHERE NOT EXISTS(SELECT 1 FROM @listArchiveColumns WHERE [name] = src.[name]) AND @UpdateTable <> 1 AND @archiveObjectId IS NOT NULL AND (@IgnoreMissingColumns IS NULL OR @IgnoreMissingColumns = 0)
                UNION ALL SELECT 35, 16, N'ERROR[TA5]: Synonym ' + QUOTENAME(@SynonymArchiveSchema) + N'.' + QUOTENAME(@SynonymArchiveName) + N' must refer to a user table: ' + QUOTENAME(@archiveSchema) + N'.' + QUOTENAME(@archiveTable) WHERE OBJECTPROPERTY(@archiveObjectId, 'IsTable') = 0
                UNION ALL SELECT 36, 16, N'ERROR[TA6]: Synonym ' + QUOTENAME(@SynonymArchiveSchema) + N'.' + QUOTENAME(@SynonymArchiveName) + N' must refer to a user table with no IDENTITY column: ' + QUOTENAME(@archiveSchema) + N'.' + QUOTENAME(@archiveTable) WHERE OBJECTPROPERTY(@archiveObjectId, 'TableHasIdentity') = 1
                UNION ALL SELECT 37, 16, N'ERROR[TA7]: column is missing from source table:' + @clusteredName WHERE NOT EXISTS(SELECT 1 FROM @listSourceColumns WHERE [name] = @clusteredName)
                ;
            END TRY
            BEGIN CATCH
                IF @@TRANCOUNT > 0 ROLLBACK;
                SET @message = N'ERROR[TA0]: error(s) occured while checking archive table';
                THROW;
            END CATCH
        END   

        ----------------------------------------------------------------------------------------------------      
        -- Create or Update Synonym / Table / Column
        ----------------------------------------------------------------------------------------------------
        IF NOT EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10)
        BEGIN
            -- Get list of Columns from source table
            SELECT @SourceColumns = NULL;
            DELETE col FROM @listSourceColumns col WHERE EXISTS(SELECT 1 FROM @listExcludeColumns WHERE [value] = col.[name])
            SELECT @SourceColumns = COALESCE(@SourceColumns + N', "' + QUOTENAME([name]) + N'"', N'"' + QUOTENAME([name]) + N'"') FROM @listSourceColumns col ORDER BY Id;
            SELECT @SourceColumns = N'['+ @SourceColumns + N']';

            -- Start Transaction for all upcoming schema changes
            BEGIN TRAN
            -- Create Archive synonym if missing or outdated
            BEGIN TRY
                IF NOT EXISTS (SELECT 1 FROM sys.synonyms WHERE [name] = @SynonymArchiveName AND schema_id = SCHEMA_ID(@SynonymArchiveSchema) AND base_object_name = @archiveTable2Parts) AND @archiveTable2Parts <> N'' AND @CreateOrUpdateSynonym = 1
                BEGIN
                    SET @sql = NULL;
                    INSERT INTO @json_errors([id], [severity], [message]) SELECT 10, 10, N'Create or alter Archive Synonym '+ QUOTENAME(@SynonymArchiveSchema) + N'.' + QUOTENAME(@SynonymArchiveName) + ' with base object: ' + @archiveTable2Parts;
                    SELECT @sql = N'DROP SYNONYM ' + QUOTENAME(@SynonymArchiveSchema) + N'.' + QUOTENAME(@SynonymArchiveName) + N';' FROM sys.synonyms WHERE [name] = @SynonymArchiveName AND schema_id = SCHEMA_ID(@SynonymArchiveSchema);
                    SELECT @sql = ISNULL(@sql, N'') + N'CREATE SYNONYM ' + QUOTENAME(@SynonymArchiveSchema) + N'.' + QUOTENAME(@SynonymArchiveName) + N' FOR ' + @archiveTable2Parts + N';';
                    EXEC sp_executesql @statement = @sql;
                END
            END TRY
            BEGIN CATCH
                IF @@TRANCOUNT > 0 ROLLBACK;
                SET @message = N'ERROR[SU1]: error(s) occured while creating or updating Archive synomym';
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
                        SELECT TOP(1024) [query] = QUOTENAME(src.[name]) + N' ' + src.[datatype] + N' NULL' + CHAR(13) + CHAR(10) 
                        FROM @listSourceColumns src WHERE [name] <> @clusteredName AND NOT EXISTS(SELECT 1 FROM @listArchiveColumns WHERE [name] = src.[name]) AND src.[datatype] <> N'timestamp' --AND @UpdateTable <> 1 AND @archiveObjectId IS NOT NULL
                        ORDER BY Id ASC
                    ) q([query]);
                    SELECT @message = COALESCE(@message + N', ' + src.[name], src.[name]) FROM @listSourceColumns src WHERE NOT EXISTS(SELECT 1 FROM @listArchiveColumns WHERE [name] = src.[name]) ORDER BY Id ASC;

                    IF @archiveObjectId IS NULL --> CREATE
                    BEGIN
                        INSERT INTO @json_errors([id], [severity], [message])
                        SELECT 30, 10, N'Create table refered to by archive synonym: ' + QUOTENAME(@SynonymArchiveSchema) + N'.' + QUOTENAME(@SynonymArchiveName) + N' => '+ QUOTENAME(@archiveSchema) + N'.' + QUOTENAME(@archiveTable) + N' (' + @message + N')';
                        SELECT @sql = N'CREATE TABLE ' + QUOTENAME(@archiveSchema) + '.' + QUOTENAME(@archiveTable) + '(' + QUOTENAME([name]) + ' ['+ datatype + '] NOT NULL CONSTRAINT [PK_' + @archiveSchema + '.'+ @archiveTable + '] PRIMARY KEY CLUSTERED ([Id] ASC)' + CHAR(13) + CHAR(10) + N', ' + @sql + N');'
                        FROM @listSourceColumns WHERE [name] = @clusteredName;
                    END
                    ELSE --> ALTER
                    BEGIN
                        INSERT INTO @json_errors([id], [severity], [message])
                        SELECT 30, 10, N'Add missing column(s) to table refered to by archive synonym: ' + QUOTENAME(@SynonymArchiveSchema) + N'.' + QUOTENAME(@SynonymArchiveName) + N' => '+ QUOTENAME(@archiveSchema) + N'.' + QUOTENAME(@archiveTable) + N' (' + @message + N')';
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
    INSERT INTO @json_errors([id], [severity], [message]) SELECT 100, 10, N'Archive Synonym is valid: '+ QUOTENAME(@SynonymArchiveSchema) + N'.' + QUOTENAME(@SynonymArchiveName) + ' => ' + @archiveTable2Parts WHERE @IsValid = 1;
    
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
