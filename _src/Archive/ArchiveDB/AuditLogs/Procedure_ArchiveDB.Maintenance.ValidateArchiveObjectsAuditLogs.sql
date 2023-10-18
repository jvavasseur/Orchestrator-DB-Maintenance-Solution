SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- DROP PROCEDURE [Maintenance].[ValidateArchiveObjectsAuditLogs]
----------------------------------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[ValidateArchiveObjectsAuditLogs]') AND type in (N'P'))
BEGIN
    EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [Maintenance].[ValidateArchiveObjectsAuditLogs] AS'
    PRINT '  + CREATE PROCEDURE: [Maintenance].[ValidateArchiveObjectsAuditLogs]';
END
ELSE PRINT '  = PROCEDURE [Maintenance].[ValidateArchiveObjectsAuditLogs] already exists' 
GO

PRINT '  ~ UPDATE PROCEDURE: [Maintenance].[ValidateArchiveObjectsAuditLogs]'
GO

ALTER PROCEDURE [Maintenance].[ValidateArchiveObjectsAuditLogs]
----------------------------------------------------------------------------------------------------
-- ### [Object]: PROCEDURE [Maintenance].[ValidateArchiveObjectsAuditLogs]
-- ### [Version]: 2020-10-01 00:00:00                                                         
-- ### [Source]: ??????
-- ### [Hash]: ??????
-- ### [Docs]: https://???.???
-- !!! ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!!!
-- !!! ~~~~~~~~~ NOT OFFICIALLY SUPPORTED BY UIPATH 
-- !!! ~~~~~~~~~ SQL Server >= 2016 SP1
----------------------------------------------------------------------------------------------------
	@ArchiveTableFullParts nvarchar(256) = NULL
	, @SourceEntitiesTableFullParts nvarchar(256) = NULL
	, @ArchiveEntitiesTableFullParts nvarchar(256) = NULL
	, @SourceTableFullParts nvarchar(256) = NULL
    , @ASyncStatusTableFullParts nvarchar(256) = NULL
    , @ExcludeColumns nvarchar(MAX) = NULL
    , @ExcludeEntitiesColumns nvarchar(MAX) = NULL
    , @IgnoreMissingColumns nvarchar(MAX) = NULL
    , @CreateOrUpdateSynonyms bit = 0
    , @CreateTable bit = 0
    , @UpdateTable bit = 0
    , @SourceColumns nvarchar(MAX) = NULL OUTPUT
    , @SourceEntitiesColumns nvarchar(MAX) = NULL OUTPUT
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
        DECLARE @synonymSourceName nvarchar(256) = N'Synonym_Source_AuditLogs';
        DECLARE @synonymSourceSchema nvarchar(256) = N'Maintenance';
        DECLARE @synonymArchiveName nvarchar(256) = N'Synonym_Archive_AuditLogs';
        DECLARE @synonymArchiveSchema nvarchar(256) = N'Maintenance';
        DECLARE @synonymSourceEntitiesName nvarchar(256) = N'Synonym_Source_AuditLogEntities';
        DECLARE @synonymSourceEntitiesSchema nvarchar(256) = N'Maintenance';
        DECLARE @synonymArchiveEntitiesName nvarchar(256) = N'Synonym_Archive_AuditLogsEntities';
        DECLARE @synonymArchiveEntitiesSchema nvarchar(256) = N'Maintenance';
        DECLARE @synonymASyncStatusName nvarchar(256) = N'Synonym_Source_ASyncStatus_AuditLogs';
        DECLARE @synonymASyncStatusSchema nvarchar(256) = N'Maintenance';
        DECLARE @clusteredName nvarchar(128) = N'Id';
        DECLARE @auditLogsValid bit = 0;
        DECLARE @auditLogsEntitiesValid bit = 0;
        DECLARE @auditLogsMessages nvarchar(MAX);
        DECLARE @auditLogsEntitiesMessages nvarchar(MAX);
        ----------------------------------------------------------------------------------------------------      
        -- Message / Error Handling
        ----------------------------------------------------------------------------------------------------
        DECLARE @message nvarchar(MAX);
        DECLARE @ERROR_NUMBER INT, @ERROR_SEVERITY INT, @ERROR_STATE INT, @ERROR_PROCEDURE NVARCHAR(126), @ERROR_LINE INT, @ERROR_MESSAGE NVARCHAR(2048) ;

        ----------------------------------------------------------------------------------------------------
        -- Call Main Checks Procedure      
        ----------------------------------------------------------------------------------------------------
        EXEC [Maintenance].[ValidateArchiveObjects]
            @SynonymSourceName = @synonymSourceName
            , @SynonymSourceSchema = @synonymSourceSchema
            , @SynonymArchiveName = @synonymArchiveName
            , @SynonymArchiveSchema = @synonymArchiveSchema
            , @synonymASyncStatusName = @synonymASyncStatusName
            , @synonymASyncStatusSchema = @synonymASyncStatusSchema
            , @ClusteredName = @clusteredName
            , @ArchiveTableFullParts = @ArchiveTableFullParts
            , @SourceTableFullParts = @SourceTableFullParts
            , @ExcludeColumns = @ExcludeColumns
            , @IgnoreMissingColumns = @IgnoreMissingColumns
            , @ASyncStatusTableFullParts = @ASyncStatusTableFullParts
            , @CreateOrUpdateSynonyms = @CreateOrUpdateSynonyms
            , @CreateTable = @CreateTable
            , @UpdateTable = @UpdateTable
            , @SourceColumns = @SourceColumns OUTPUT
            , @IsValid = @auditLogsValid OUTPUT
            , @Messages = @auditLogsMessages  OUTPUT
        ;
        EXEC [Maintenance].[ValidateArchiveObjects]
            @SynonymSourceName = @synonymSourceEntitiesName
            , @SynonymSourceSchema = @synonymSourceEntitiesSchema
            , @SynonymArchiveName = @synonymArchiveEntitiesName
            , @SynonymArchiveSchema = @synonymArchiveEntitiesSchema
            , @synonymASyncStatusName = @synonymASyncStatusName
            , @synonymASyncStatusSchema = @synonymASyncStatusSchema
            , @ClusteredName = @clusteredName
            , @ArchiveTableFullParts = @ArchiveEntitiesTableFullParts
            , @SourceTableFullParts = @SourceEntitiesTableFullParts
            , @ExcludeColumns = @ExcludeEntitiesColumns
            , @IgnoreMissingColumns = @IgnoreMissingColumns
            , @ASyncStatusTableFullParts = NULL
            , @CreateOrUpdateSynonyms = @CreateOrUpdateSynonyms
            , @CreateTable = @CreateTable
            , @UpdateTable = @UpdateTable
            , @SourceColumns = @SourceEntitiesColumns OUTPUT
            , @IsValid = @auditLogsEntitiesValid OUTPUT
            , @Messages = @auditLogsEntitiesMessages OUTPUT
        ;
        SELECT @IsValid = IIF(@auditLogsValid = 1 AND @auditLogsEntitiesValid = 1, 1, 0);

    SET @Messages = --ISNULL(
    ( 
        SELECT [Procedure] = QUOTENAME(COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?')) + N'.' + QUOTENAME(COALESCE(OBJECT_NAME(@@PROCID), N'?')), [Message], [Severity], [State] 
        FROM (
            SELECT [Procedure], [Message], [Severity], [State] FROM OPENJSON(@auditLogsMessages, N'$') WITH ([Procedure] nvarchar(128) N'$.Procedure', [Message] nvarchar(128) N'$.Message', [Severity] int, [State] smallint)
            UNION
            SELECT [Procedure], [Message], [Severity], [State] FROM OPENJSON(@auditLogsEntitiesMessages, N'$') WITH ([Procedure] nvarchar(128) N'$.Procedure', [Message] nvarchar(128) N'$.Message', [Severity] int, [State] smallint)
        ) jsn
        FOR JSON PATH
    );


    END TRY
    BEGIN CATCH
        SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();    
        SET @message = N'ERROR[CH0]: error(s) occured while checking source and archive Audit Logs objects';
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

