SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- DROP PROCEDURE [Maintenance].[ValidateASyncArchiveObjectsLogs]
----------------------------------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[ValidateASyncArchiveObjectsLogs]') AND type in (N'P'))
BEGIN
    EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [Maintenance].[ValidateASyncArchiveObjectsLogs] AS'
    PRINT '  + CREATE PROCEDURE: [Maintenance].[ValidateASyncArchiveObjectsLogs]';
END
ELSE PRINT '  = PROCEDURE [Maintenance].[ValidateASyncArchiveObjectsLogs] already exists' 
GO

PRINT '  ~ UPDATE PROCEDURE: [Maintenance].[ValidateASyncArchiveObjectsLogs]'
GO

ALTER PROCEDURE [Maintenance].[ValidateASyncArchiveObjectsLogs]
----------------------------------------------------------------------------------------------------
-- ### [Object]: PROCEDURE [Maintenance].[ValidateASyncArchiveObjectsLogs]
-- ### [Version]: 2020-10-01 00:00:00                                                         
-- ### [Source]: ??????
-- ### [Hash]: ??????
-- ### [Docs]: https://???.???
-- !!! ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!!!
-- !!! ~~~~~~~~~ NOT OFFICIALLY SUPPORTED BY UIPATH 
-- !!! ~~~~~~~~~ SQL Server >= 2016 SP1
----------------------------------------------------------------------------------------------------
	@ASyncDeleteTableFullParts nvarchar(256) = NULL
	, @ArchiveSyncTableFullParts nvarchar(256) = NULL
    , @CreateOrUpdateSynonyms bit = 0
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
        -- Settings
        ----------------------------------------------------------------------------------------------------
        DECLARE @synonymASyncDeleteName nvarchar(256) = N'Synonym_Archive_Delete_Logs';
        DECLARE @synonymASyncDeleteSchema nvarchar(256) = N'Maintenance';
        DECLARE @synonymArchiveSyncName nvarchar(256) = N'Synonym_Archive_Sync_Logs';
        DECLARE @synonymArchiveSyncSchema nvarchar(256) = N'Maintenance';
--        DECLARE @synonymASyncStatusName nvarchar(256) = N'Synonym_Archive_Status_Logs';
--        DECLARE @synonymASyncStatusSchema nvarchar(256) = N'Maintenance';
        DECLARE @clusteredName nvarchar(128) = N'Id';
        ----------------------------------------------------------------------------------------------------      
        -- Message / Error Handling
        ----------------------------------------------------------------------------------------------------
        DECLARE @message nvarchar(MAX);
        DECLARE @ERROR_NUMBER INT, @ERROR_SEVERITY INT, @ERROR_STATE INT, @ERROR_PROCEDURE NVARCHAR(126), @ERROR_LINE INT, @ERROR_MESSAGE NVARCHAR(2048) ;

        ----------------------------------------------------------------------------------------------------
        -- Call Main Checks Procedure      
        ----------------------------------------------------------------------------------------------------
        EXEC [Maintenance].[ValidateASyncArchiveObjects]
            @SynonymASyncDeleteName = @synonymASyncDeleteName
            , @SynonymASyncDeleteSchema = @synonymASyncDeleteSchema
            , @SynonymArchiveSyncName = @synonymArchiveSyncName
            , @SynonymArchiveSyncSchema = @synonymArchiveSyncSchema
            , @ASyncDeleteTableFullParts = @ASyncDeleteTableFullParts
            , @ArchiveSyncTableFullParts = @ArchiveSyncTableFullParts
            , @CreateOrUpdateSynonyms = @CreateOrUpdateSynonyms
            , @IsValid = @IsValid OUTPUT
            , @Messages = @Messages OUTPUT
        ;
    END TRY
    BEGIN CATCH
        SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
        SET @message = N'ERROR[CH0]: error(s) occured while checking ASync and Delete objects';
        SET @Messages = 
        ( 
            SELECT [Procedure] = QUOTENAME(COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?')) + N'.' + QUOTENAME(COALESCE(OBJECT_NAME(@@PROCID), N'?')), [Message], [Severity], [State] 
            FROM (
                SELECT [Message]  = N'ERROR: ' + @ERROR_MESSAGE, [Severity] = 16, [State] = 1 WHERE @ERROR_MESSAGE IS NOT NULL
                UNION ALL SELECT @message, 16, 1 WHERE @message IS NOT NULL
            ) jsn
            FOR JSON PATH
        );
        SET @IsValid = 0;
    END CATCH
    RETURN 0;
END
GO
