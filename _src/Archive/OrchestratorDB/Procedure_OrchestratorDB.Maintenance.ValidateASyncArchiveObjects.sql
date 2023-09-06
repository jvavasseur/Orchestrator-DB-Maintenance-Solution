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
-- ### [Version]: 2020-10-01 00:00:00                                                         
-- ### [Source]: ??????
-- ### [Hash]: ??????
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
