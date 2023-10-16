SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- DROP PROCEDURE [Maintenance].[CreateArchivingExternalTable]
----------------------------------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[CreateArchivingExternalTable]') AND type in (N'P'))
BEGIN
    EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [Maintenance].[CreateArchivingExternalTable] AS'
    PRINT '  + CREATE PROCEDURE: [Maintenance].[CreateArchivingExternalTable]';
END
ELSE PRINT '  = PROCEDURE [Maintenance].[CreateArchivingExternalTable] already exists' 
GO

PRINT '  ~ UPDATE PROCEDURE: [Maintenance].[CreateArchivingExternalTable]'
GO

ALTER PROCEDURE [Maintenance].[CreateArchivingExternalTable]
----------------------------------------------------------------------------------------------------
-- ### [Object]: PROCEDURE [Maintenance].[CreateArchivingExternalTable]
-- ### [Version]: 2020-10-01 00:00:00                                                         
-- ### [Source]: ??????
-- ### [Hash]: ??????
-- ### [Docs]: https://???.???
-- !!! ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!!!
-- !!! ~~~~~~~~~ NOT OFFICIALLY SUPPORTED BY UIPATH 
-- !!! ~~~~~~~~~ SQL Server >= 2016 SP1
----------------------------------------------------------------------------------------------------
    @ExternalDataSource nvarchar(128)
    , @ExternalName nvarchar(128)
    , @ExternalSchema nvarchar(128)
    , @Columns nvarchar(MAX)
    , @ShowMessages bit = 1
    , @ThrowError bit = 1
--    , @Columns nvarchar(MAX) = NULL OUTPUT
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
        -- Table Variables
        ----------------------------------------------------------------------------------------------------
        DECLARE @datasourceId int;
        DECLARE @tableId int
        DECLARE @replaceTable bit = 0;
        DECLARE @listExisting TABLE([column] nvarchar(128), [datatype] nvarchar(128))
        DECLARE @listColumns TABLE([id] int IDENTITY(0, 1), [column] nvarchar(128), [datatype] nvarchar(128))
        ----------------------------------------------------------------------------------------------------
        -- Misc      
        ----------------------------------------------------------------------------------------------------
        DECLARE @sql nvarchar(max);
        DECLARE @sqlVars nvarchar(MAX);
        DECLARE @sqlValues nvarchar(MAX);
        DECLARE @sqlCols nvarchar(MAX);
        ----------------------------------------------------------------------------------------------------      
        -- Message / Error Handling
        ----------------------------------------------------------------------------------------------------
        DECLARE @message nvarchar(MAX);
        DECLARE @ERROR_NUMBER INT, @ERROR_SEVERITY INT, @ERROR_STATE INT, @ERROR_PROCEDURE NVARCHAR(126), @ERROR_LINE INT, @ERROR_MESSAGE NVARCHAR(2048);
        DECLARE @space tinyint = 2;
        DECLARE @tab tinyint = 0;
        DECLARE @json_errors TABLE([id] tinyint NOT NULL, [severity] int NOT NULL, [message] nvarchar(MAX) NOT NULL);

        ----------------------------------------------------------------------------------------------------      
        -- Checks Data Source and External Table
        ----------------------------------------------------------------------------------------------------
        SELECT @datasourceId = data_source_id FROM sys.external_data_sources WHERE [name] = @ExternalDataSource;
        SELECT @tableId = object_id, @replaceTable = @replaceTable | IIF([data_source_id] <> @datasourceId, 1, 0) FROM sys.external_tables WHERE [name] = @ExternalName AND [schema_id] = SCHEMA_ID(@ExternalSchema);

        INSERT INTO @listExisting([column], [datatype])
        SELECT TOP(1000) col.[name] 
            , [datatype] =   tpe.[name] + 
                CASE WHEN tpe.[name] IN (N'varchar', N'char', N'varbinary', N'binary', N'text') THEN '(' + CASE WHEN col.max_length = -1 THEN 'MAX' ELSE CAST(col.max_length AS VARCHAR(5)) END + ')'
                WHEN tpe.[name] IN (N'nvarchar', N'nchar', N'ntext') THEN '(' + CASE WHEN col.max_length = -1 THEN 'MAX' ELSE CAST(col.max_length / 2 AS VARCHAR(5)) END + ')'
                WHEN tpe.[name] IN (N'datetime2', N'time2', N'datetimeoffset') THEN '(' + CAST(col.scale AS VARCHAR(5)) + ')'
                WHEN tpe.[name] IN (N'decimal', N'numeric') THEN '(' + CAST(col.[precision] AS VARCHAR(5)) + ',' + CAST(col.scale AS VARCHAR(5)) + ')'
                WHEN tpe.[name] IN (N'float') THEN '(' + CAST(col.[precision] AS VARCHAR(5)) + ')'
                ELSE '' END
        FROM sys.columns AS col
        INNER JOIN sys.types AS tpe ON col.system_type_id = tpe.system_type_id AND tpe.system_type_id = tpe.user_type_id
        WHERE col.object_id = @tableId AND tpe.[name] <> N'timestamp'

        IF ISJSON(@Columns) = 1
        BEGIN
            INSERT INTO @listColumns([column], [datatype])
            SELECT[column], [datatype]
            FROM OPENJSON(@Columns) WITH([column] nvarchar(128), [datatype] nvarchar(128))
            WHERE [column] IS NOT NULL AND [datatype] IS NOT NULL;
        END

        SELECT @replaceTable = 1 WHERE EXISTS(SELECT [column], [datatype] FROM @listExisting EXCEPT SELECT [column], [datatype] FROM @listColumns);
        SELECT @replaceTable = 1 WHERE EXISTS(SELECT [column], [datatype] FROM @listColumns EXCEPT SELECT [column], [datatype] FROM @listExisting);

        INSERT INTO @json_errors([id], [severity], [message]) 
        SELECT 0, 16, N'Data Source does not exists: ' + @ExternalDataSource WHERE @datasourceId IS NULL
        UNION ALL SELECT 0, 1, N'Data Source exists: ' + @ExternalDataSource WHERE @datasourceId IS NOT NULL
        UNION ALL SELECT 1, 16, N'@Columns is not a valid JSON string' WHERE ISJSON(@Columns) = 0
        UNION ALL SELECT 1, 16, N'No column name found in @Columns' WHERE NOT EXISTS(SELECT 1 FROM @listColumns)
        UNION ALL SELECT 2, 16, N'External table not found but an object with this name already exists and must be replaced: ' +  QUOTENAME(@ExternalSchema) + N'.' + QUOTENAME(@ExternalName) FROM sys.objects WHERE [name] = @ExternalName AND [schema_id] = SCHEMA_ID(@ExternalSchema) AND @tableId IS NULL
        UNION ALL SELECT 2, 1, N'External table already exists: ' +  QUOTENAME(@ExternalSchema) + N'.' + QUOTENAME(@ExternalName) WHERE @tableId IS NOT NULL AND @replaceTable = 0
        ;

        SELECT @message = N'ERROR[PR0]: error(s) occured while checking parameters' WHERE EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10);
        
        IF NOT EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10) AND @replaceTable = 1 AND @tableId IS NOT NULL
        BEGIN
            BEGIN TRY
                INSERT INTO @json_errors([id], [severity], [message]) SELECT 10, 10, N'Drop external table: ' +  QUOTENAME(@ExternalSchema) + N'.' + QUOTENAME(@ExternalName)
                UNION ALL SELECT 2, 10, N'External Table exists but will be replaced: ' + QUOTENAME(@ExternalSchema) + N'.' + QUOTENAME(@ExternalName) WHERE @datasourceId IS NOT NULL AND @tableId IS NOT NULL AND @replaceTable = 1;
                SELECT @sql = N'DROP EXTERNAL TABLE '+ QUOTENAME(@ExternalSchema) + N'.' + QUOTENAME(@ExternalName) + N';';
                EXEC sp_executesql @stmt = @sql;
            END TRY
            BEGIN CATCH
                SET @message = N'ERROR[DT0]: error(s) occured while removing external table';
                THROW;
            END CATCH
        END

        -- Prepare columns Name/Type string
        SELECT @sqlVars = NULL;
        SELECT @sqlVars = COALESCE(@sqlVars + N', @c' + CAST([id] AS nvarchar(10)) + N' ' + [datatype], N'DECLARE @c' + CAST([id] AS nvarchar(10)) + N' ' + [datatype]) 
            , @sqlValues = COALESCE(@sqlValues + N', @c' + CAST([id] AS nvarchar(10)) + N' = ' + QUOTENAME([column]), N'@c' + CAST([id] AS nvarchar(10)) + N' = ' + QUOTENAME([column])) 
            , @sqlCols = COALESCE(@sqlCols + N', ' + QUOTENAME([column]) + N' ' + [datatype], N'' + QUOTENAME([column])  + N' ' + [datatype]) 
        FROM @listColumns;

        -- Create External Table
        IF NOT EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10) AND (@tableId IS NULL OR @replaceTable = 1)
        BEGIN
            BEGIN TRY
                INSERT INTO @json_errors([id], [severity], [message]) SELECT 11, 10, N'Create external table: ' +  QUOTENAME(@ExternalSchema) + N'.' + QUOTENAME(@ExternalName)
                SELECT @sql = N'CREATE EXTERNAL TABLE '+ QUOTENAME(@ExternalSchema) + N'.' + QUOTENAME(@ExternalName) + N'(' + @sqlCols + N') WITH ( DATA_SOURCE = ' + QUOTENAME(@ExternalDataSource) + N');';

                EXEC sp_executesql @stmt = @sql;
            END TRY
            BEGIN CATCH
                SET @message = N'ERROR[XT0]: error(s) occured while creating external table';
                THROW;
            END CATCH
        END

        -- Test External Table
        IF NOT EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10)
        BEGIN
            BEGIN TRY
--                INSERT INTO @json_errors([id], [severity], [message]) SELECT 21, 10, N'Test external table: ' +  QUOTENAME(@ExternalSchema) + N'.' + QUOTENAME(@ExternalName)
                SELECT @sql = N'
                    BEGIN TRY
                        ' + @sqlVars + N';
                        SELECT TOP(1) ' + @sqlValues + N' FROM ' + QUOTENAME(@ExternalSchema) + N'.' + QUOTENAME(@ExternalName) + N';
                        SELECT @error_message = NULL
                    END TRY
                    BEGIN CATCH
                        SELECT @error_message = ERROR_MESSAGE();
                        THROW;
                    END CATCH
                ';
                -- Test external table
                EXEC sp_executesql @stmt = @sql, @params = N'@error_message nvarchar(2048) OUTPUT', @error_message = @ERROR_MESSAGE OUTPUT;
                INSERT INTO @json_errors([id], [severity], [message]) SELECT 22, 10, N'External table is valid: ' +  QUOTENAME(@ExternalSchema) + N'.' + QUOTENAME(@ExternalName) WHERE @ERROR_MESSAGE IS NULL
                INSERT INTO @json_errors([id], [severity], [message]) SELECT 20, 16, 'y'+ @ERROR_MESSAGE WHERE @ERROR_MESSAGE IS NOT NULL;
            END TRY
            BEGIN CATCH
                SET @message = N'ERROR[TS0]: error(s) occured while testing external table';
                THROW;
            END CATCH
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
                SELECT TOP(100) [Message] = LEFT([message], 4000), [Severity] = [severity], [State] = 1 FROM @json_errors WHERE [severity] >= 10 OR @IsValid = 0 OR @ShowMessages = 1 ORDER BY [id] ASC
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

/*
EXEC [Maintenance].[CreateArchivingExternalTable] @ExternalDataSource = 'ArchivingDatasourceForOrchestratorDB', @ExternalName = N'ArchivingListOrchestratorDBTables', @ExternalSchema = N'Maintenance'
, @Columns = N'[{"column":"group","datatype":"nvarchar(128)"},{"column":"schema","datatype":"nvarchar(128)"},{"column":"table","datatype":"nvarchar(128)"},{"column":"exists","datatype":"bit"},{"column":"isvalid","datatype":"bit"},{"column":"columns","datatype":"nvarchar(MAX)"}]'
, @ShowMessages = 1
, @ThrowError = 0;
GO
*/

/*
drop external data source [ArchivingDatasourceForOrchestratorDB]
drop external table [Maintenance].[ArchivingListOrchestratorDBTables]

SELECT * FROM sys.external_data_sources WHERE [name] = @ExternalDataSource
SELECT * FROM sys.external_tables 
SELECT 'drop external table ' +  quotename(OBJECT_SCHEMA_NAME(object_id)) + '.'+ quotename(name), * FROM sys.external_tables 

SELECT * FROM [Maintenance].[ArchivingOrchestratorDBTables]
SELECT * FROM [Maintenance].[ArchivingOrchestratorDBTablesx]
*/

