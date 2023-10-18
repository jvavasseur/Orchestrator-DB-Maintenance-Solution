SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- DROP PROCEDURE [Maintenance].[ValidateArchiveObjectsQueues]
----------------------------------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[ValidateArchiveObjectsQueues]') AND type in (N'P'))
BEGIN
    EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [Maintenance].[ValidateArchiveObjectsQueues] AS'
    PRINT '  + CREATE PROCEDURE: [Maintenance].[ValidateArchiveObjectsQueues]';
END
ELSE PRINT '  = PROCEDURE [Maintenance].[ValidateArchiveObjectsQueues] already exists' 
GO

PRINT '  ~ UPDATE PROCEDURE: [Maintenance].[ValidateArchiveObjectsQueues]'
GO

ALTER PROCEDURE [Maintenance].[ValidateArchiveObjectsQueues]
----------------------------------------------------------------------------------------------------
-- ### [Object]: PROCEDURE [Maintenance].[ValidateArchiveObjectsQueues]
-- ### [Version]: 2020-10-01 00:00:00                                                         
-- ### [Source]: ??????
-- ### [Hash]: ??????
-- ### [Docs]: https://???.???
-- !!! ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!!!
-- !!! ~~~~~~~~~ NOT OFFICIALLY SUPPORTED BY UIPATH 
-- !!! ~~~~~~~~~ SQL Server >= 2016 SP1
----------------------------------------------------------------------------------------------------
	@ArchiveTableFullParts nvarchar(256) = NULL
   	, @SourceTableFullParts nvarchar(256) = NULL
	, @SourceItemCommentsTableFullParts nvarchar(256) = NULL
	, @ArchiveItemCommentsTableFullParts nvarchar(256) = NULL
	, @SourceItemEventsTableFullParts nvarchar(256) = NULL
	, @ArchiveItemEventsTableFullParts nvarchar(256) = NULL
    , @ASyncStatusTableFullParts nvarchar(256) = NULL
    , @ExcludeColumns nvarchar(MAX) = NULL
    , @ExcludeItemCommentsColumns nvarchar(MAX) = NULL
    , @ExcludeItemEventsColumns nvarchar(MAX) = NULL
    , @IgnoreMissingColumns nvarchar(MAX) = NULL
    , @CreateOrUpdateSynonyms bit = 0
    , @CreateTable bit = 0
    , @UpdateTable bit = 0
    , @SourceColumns nvarchar(MAX) = NULL OUTPUT
    , @SourceItemCommentsColumns nvarchar(MAX) = NULL OUTPUT
    , @SourceItemEventsColumns nvarchar(MAX) = NULL OUTPUT
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
        DECLARE @synonymSourceName nvarchar(256) = N'Synonym_Source_QueueItems';
        DECLARE @synonymSourceSchema nvarchar(256) = N'Maintenance';
        DECLARE @synonymArchiveName nvarchar(256) = N'Synonym_Archive_QueueItems';
        DECLARE @synonymArchiveSchema nvarchar(256) = N'Maintenance';
        DECLARE @synonymSourceItemCommentsName nvarchar(256) = N'Synonym_Source_QueueItemComments';
        DECLARE @synonymSourceItemCommentsSchema nvarchar(256) = N'Maintenance';
        DECLARE @synonymArchiveItemCommentsName nvarchar(256) = N'Synonym_Archive_QueueItemComments';
        DECLARE @synonymArchiveItemCommentsSchema nvarchar(256) = N'Maintenance';
        DECLARE @synonymSourceItemEventsName nvarchar(256) = N'Synonym_Source_QueueItemEvents';
        DECLARE @synonymSourceItemEventsSchema nvarchar(256) = N'Maintenance';
        DECLARE @synonymArchiveItemEventsName nvarchar(256) = N'Synonym_Archive_QueueItemEvents';
        DECLARE @synonymArchiveItemEventsSchema nvarchar(256) = N'Maintenance';
        DECLARE @synonymASyncStatusName nvarchar(256) = N'Synonym_Source_ASyncStatus_Queues';
        DECLARE @synonymASyncStatusSchema nvarchar(256) = N'Maintenance';
        DECLARE @clusteredName nvarchar(128) = N'Id';
        DECLARE @queueItemsValid bit = 0;
        DECLARE @queueItemCommentsValid bit = 0;
        DECLARE @queueItemEventsValid bit = 0;
        DECLARE @queueItemsMessages nvarchar(MAX);
        DECLARE @queueItemCommentsMessages nvarchar(MAX);
        DECLARE @queueItemEventsMessages nvarchar(MAX);
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
            , @IsValid = @queueItemsValid OUTPUT
            , @Messages = @queueItemsMessages  OUTPUT
        ;
        EXEC [Maintenance].[ValidateArchiveObjects]
            @SynonymSourceName = @synonymSourceItemCommentsName
            , @SynonymSourceSchema = @synonymSourceItemCommentsSchema
            , @SynonymArchiveName = @synonymArchiveItemCommentsName
            , @SynonymArchiveSchema = @synonymArchiveItemCommentsSchema
            , @synonymASyncStatusName = @synonymASyncStatusName
            , @synonymASyncStatusSchema = @synonymASyncStatusSchema
            , @ClusteredName = @clusteredName
            , @ArchiveTableFullParts = @ArchiveItemCommentsTableFullParts
            , @SourceTableFullParts = @SourceItemCommentsTableFullParts
            , @ExcludeColumns = @ExcludeItemCommentsColumns
            , @IgnoreMissingColumns = @IgnoreMissingColumns
            , @ASyncStatusTableFullParts = NULL
            , @CreateOrUpdateSynonyms = @CreateOrUpdateSynonyms
            , @CreateTable = @CreateTable
            , @UpdateTable = @UpdateTable
            , @SourceColumns = @SourceItemCommentsColumns OUTPUT
            , @IsValid = @QueueItemCommentsValid OUTPUT
            , @Messages = @QueueItemCommentsMessages OUTPUT
        ;
        EXEC [Maintenance].[ValidateArchiveObjects]
            @SynonymSourceName = @synonymSourceItemEventsName
            , @SynonymSourceSchema = @synonymSourceItemEventsSchema
            , @SynonymArchiveName = @synonymArchiveItemEventsName
            , @SynonymArchiveSchema = @synonymArchiveItemEventsSchema
            , @synonymASyncStatusName = @synonymASyncStatusName
            , @synonymASyncStatusSchema = @synonymASyncStatusSchema
            , @ClusteredName = @clusteredName
            , @ArchiveTableFullParts = @ArchiveItemEventsTableFullParts
            , @SourceTableFullParts = @SourceItemEventsTableFullParts
            , @ExcludeColumns = @ExcludeItemEventsColumns
            , @IgnoreMissingColumns = @IgnoreMissingColumns
            , @ASyncStatusTableFullParts = NULL
            , @CreateOrUpdateSynonyms = @CreateOrUpdateSynonyms
            , @CreateTable = @CreateTable
            , @UpdateTable = @UpdateTable
            , @SourceColumns = @SourceItemEventsColumns OUTPUT
            , @IsValid = @QueueItemEventsValid OUTPUT
            , @Messages = @QueueItemEventsMessages OUTPUT
        ;

        SELECT @IsValid = IIF(@queueItemsValid = 1 AND @QueueItemCommentsValid = 1 AND @QueueItemEventsValid = 1, 1, 0);

    SET @Messages = --ISNULL(
    ( 
        SELECT [Procedure] = QUOTENAME(COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?')) + N'.' + QUOTENAME(COALESCE(OBJECT_NAME(@@PROCID), N'?')), [Message], [Severity], [State] 
        FROM (
            SELECT [Procedure], [Message], [Severity], [State] FROM OPENJSON(@queueItemsMessages, N'$') WITH ([Procedure] nvarchar(128) N'$.Procedure', [Message] nvarchar(128) N'$.Message', [Severity] int, [State] smallint)
            UNION
            SELECT [Procedure], [Message], [Severity], [State] FROM OPENJSON(@QueueItemCommentsMessages, N'$') WITH ([Procedure] nvarchar(128) N'$.Procedure', [Message] nvarchar(128) N'$.Message', [Severity] int, [State] smallint)
            UNION
            SELECT [Procedure], [Message], [Severity], [State] FROM OPENJSON(@QueueItemEventsMessages, N'$') WITH ([Procedure] nvarchar(128) N'$.Procedure', [Message] nvarchar(128) N'$.Message', [Severity] int, [State] smallint)
        ) jsn
        FOR JSON PATH
    );


    END TRY
    BEGIN CATCH
        SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();    
        SET @message = N'ERROR[CH0]: error(s) occured while checking source and archive Queues objects';
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

