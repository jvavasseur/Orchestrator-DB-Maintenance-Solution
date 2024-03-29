SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- DROP PROCEDURE [Maintenance].[SetOrchestratorArchiveTables]
----------------------------------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[SetOrchestratorArchiveTables]') AND type in (N'P'))
BEGIN
    EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [Maintenance].[SetOrchestratorArchiveTables] AS'
    PRINT '  + CREATE PROCEDURE: [Maintenance].[SetOrchestratorArchiveTables]';
END
ELSE PRINT '  = PROCEDURE [Maintenance].[SetOrchestratorArchiveTables] already exists' 
GO

PRINT '  ~ UPDATE PROCEDURE: [Maintenance].[SetOrchestratorArchiveTables]'
GO

ALTER PROCEDURE [Maintenance].[SetOrchestratorArchiveTables]
----------------------------------------------------------------------------------------------------
-- ### [Object]: PROCEDURE [Maintenance].[SetOrchestratorArchiveTables]
-- ### [Version]: 2020-10-01 00:00:00                                                         
-- ### [Source]: ??????
-- ### [Hash]: ??????
-- ### [Docs]: https://???.???
-- !!! ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!!!
-- !!! ~~~~~~~~~ NOT OFFICIALLY SUPPORTED BY UIPATH 
-- !!! ~~~~~~~~~ SQL Server >= 2016 SP1
----------------------------------------------------------------------------------------------------
    @ArchiveTableNamePattern nvarchar(128)
    , @ArchiveSchema nvarchar(128)
    , @CreateTable bit = 1
    , @UpdateTable bit = 1
    , @RemoveIdentity bit = 0
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
        DECLARE @externalListName nvarchar(128) = N'ArchivingListOrchestratorDBTables';
        --DECLARE @externalListSchema nvarchar(128) = N'Maintenance';
        --DECLARE @listTableColuns nvarchar(MAX);
        DECLARE @synonymSourceName nvarchar(256);
        DECLARE @synonymSourceSchema nvarchar(256) = N'Maintenance';
        DECLARE @synonymArchiveName nvarchar(256);
        DECLARE @synonymArchiveSchema nvarchar(256) = N'Maintenance';
        DECLARE @clusteredName nvarchar(128) = N'Id';
        DECLARE @archiveTableName nvarchar(128)
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
        DECLARE @cursorCluster nvarchar(128);
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
        SELECT @ArchiveTableNamePattern = ISNULL(LTRIM(RTRIM(@ArchiveTableNamePattern)), N'');
        INSERT INTO @json_errors([severity], [message]) SELECT 16, N'@ArchiveTableNamePattern is missing' WHERE @ArchiveTableNamePattern = N''
        UNION ALL SELECT 16, N'Table name expression #NAME# not found in @ArchiveTableNamePattern: ' + @ArchiveTableNamePattern WHERE CHARINDEX(N'#name#', @ArchiveTableNamePattern) = 0

        IF NOT EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10)
        BEGIN
            BEGIN TRY
                DECLARE CursorTables CURSOR FAST_FORWARD LOCAL FOR SELECT [group], [schema], [table], [cluster], [exists], [isvalid], [columns] FROM [Maintenance].[Synonym_ArchivingListOrchestratorDBTables] WHERE [isarchived] >= 1;
                OPEN CursorTables;
                FETCH CursorTables INTO @cursorGroup, @cursorSchema, @cursorTable, @cursorCluster, @cursorExists, @cursorIsValid, @cursorColumns;

                IF CURSOR_STATUS('local', 'CursorTables') = 1
                BEGIN
                    WHILE @@FETCH_STATUS = 0
                    BEGIN;
                        IF @cursorIsValid = 0 
                        BEGIN
                            INSERT INTO @json_errors([severity], [message]) SELECT 10, N'Missing or invalid source table: Group = ' + ISNULL(@cursorGroup, '-') + N', Table = ' + @cursorSchema + N'.' + @cursorTable + N' ';
                            INSERT INTO @json_errors([severity], [message]) SELECT 10,  @cursorCluster;
                            --CONTINUE;
                        END
                        ELSE
                        BEGIN 
                            IF NOT EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10)
                            BEGIN
                                BEGIN TRY
                                    SELECT @synonymName = N'Synonym_Source_' + @cursorTable, @sourceTableFullParts =  QUOTENAME(@cursorSchema) + N'.' + QUOTENAME(@cursorTable)

                                    SELECT @archiveTableName = REPLACE(@ArchiveTableNamePattern, N'#NAME#', @cursorTable)
                                    SELECT @synonymSourceName = N'Synonym_Source_' + @cursorTable, @synonymArchiveName = N'Synonym_Archive_' + @cursorTable;
                                    IF @archiveTableName = @cursorTable AND @ArchiveSchema = @cursorSchema
                                    BEGIN
                                        INSERT INTO @json_errors([severity], [message]) SELECT 16, N'Archive Table name cannot be an existing Orchestrator table name: ' + @ArchiveTableNamePattern + N' =>  ' + QUOTENAME(@cursorSchema) + N'.' + QUOTENAME(@cursorTable);
                                        CONTINUE;
                                    END
                                    ELSE
                                    BEGIN
                                        EXEC [Maintenance].[SetArchiveTable]
                                            @SynonymSourceName = @synonymSourceName, @SynonymSourceSchema = @synonymSourceSchema
                                            , @SynonymArchiveName = @synonymArchiveName, @SynonymArchiveSchema = @synonymArchiveSchema
                                            , @ArchiveTableName = @archiveTableName, @ArchiveTableSchema = @ArchiveSchema
                                            , @ClusteredName = @cursorCluster
                                            --, @ExcludeColumns = @ExcludeColumns
                                            --, @IgnoreMissingColumns = @IgnoreMissingColumns
                                            , @CreateOrUpdateSynonym = 1
                                            , @CreateTable = @CreateTable
                                            , @UpdateTable = @UpdateTable
                                            , @RemoveIdentity = @RemoveIdentity
                                            , @SourceColumns = NULL--@SourceColumns OUTPUT
                                            , @IsValid = @outputIsValid OUTPUT
                                            , @Messages = @outputMessages OUTPUT
                                        ;
                                    END
                                END TRY
                                BEGIN CATCH
                                    INSERT INTO @json_errors([severity], [message]) SELECT 16, ERROR_MESSAGE()
                                    INSERT INTO @json_errors([severity], [message]) SELECT 16, N'ERROR[SY0]: error(s) occured while checking synonym for table: ' + QUOTENAME(@cursorSchema) + N'.' + QUOTENAME(@cursorTable);
                                END CATCH
                            END
                            INSERT INTO @json_errors([procedure], [severity], [message]) SELECT N'[Maintenance].[SetArchiveTable]', [severity], [message] FROM OPENJSON(@outputMessages) WITH([Message] nvarchar(MAX), [Severity] int)
                        END
                        FETCH CursorTables INTO @cursorGroup, @cursorSchema, @cursorTable, @cursorCluster, @cursorExists, @cursorIsValid, @cursorColumns;
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
