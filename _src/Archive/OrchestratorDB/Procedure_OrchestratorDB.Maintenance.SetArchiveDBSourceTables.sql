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
-- ### [Version]: 2020-10-01 00:00:00                                                         
-- ### [Source]: ??????
-- ### [Hash]: ??????
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
