SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- DROP PROCEDURE [Maintenance].[SimpleArchivingJobs]
----------------------------------------------------------------------------------------------------

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[SimpleArchivingJobs]') AND type in (N'P'))
BEGIN
    EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [Maintenance].[SimpleArchivingJobs] AS'
    PRINT '  + CREATE PROCEDURE: [Maintenance].[SimpleArchivingJobs]';
END
ELSE PRINT '  = PROCEDURE [Maintenance].[SimpleArchivingJobs] already exists' 
GO

PRINT '  ~ UPDATE PROCEDURE: [Maintenance].[SimpleArchivingJobs]'
GO  

ALTER PROCEDURE [Maintenance].[SimpleArchivingJobs]
----------------------------------------------------------------------------------------------------
-- ### [Object]: PROCEDURE [Maintenance].[SimpleArchivingJobs]
-- ### [Version]: 2020-10-01 00:00:00                                                         
-- ### [Source]: ??????
-- ### [Hash]: ??????
-- ### [Docs]: https://???.???
-- !!! ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!!!
-- !!! ~~~~~~~~~ NOT OFFICIALLY SUPPORTED BY UIPATH 
-- !!! ~~~~~~~~~ SQL Server >= 2016 SP1
----------------------------------------------------------------------------------------------------
    @RetentionDate datetime = NULL
    , @RetentionDays int = NULL
    , @RetentionHours int = NULL
    , @RoundUpPreviousDay bit = 1
    /* Row count settings */
    , @RowsDeletedForEachLoop int = 10000 -- Don't go above 50.000 (min = 1000, Max = 100.000)
    , @MaxConcurrentFilters int = NULL -- NULL or 0 = default value => number of filter in chronoligical order processed by a single batch
    /* Loop Limits */
    , @MaxRunMinutes int = NULL -- NULL or 0 = unlimited
    , @MaxBatchesLoops int = NULL -- NULL or 0 - unlimited
    /* Dry Run */
--    , @DryRunOnly nvarchar(MAX) = NULL -- Y{es} or N{o} => Only Check Parameters (default if NULL = Y)
    /* Delete settings */
	, @SynchronousDeleteIfNoDelay bit = 1
	, @IgnoreDeleteDelay bit = 1

    /* Archive table(s) settings */
    , @CreateArchiveTable nvarchar(MAX) = 1 -- Y{es} or N{o} => Archive table refered by Synonym is create when missing (default if NULL or empty = N)
    , @UpdateArchiveTable nvarchar(MAX) = 1 -- Y{es} or N{o} => Archive table refered by Synonym is update when column(s) are missing (default if NULL or empty = N)
    , @ExcludeColumns nvarchar(MAX) = NULL -- JSON string with array of string with column name(s)
    /* Error Handling */
    , @OnErrorRetry tinyint = NULL -- between 0 and 20 => retry up to 20 times (default if NULL = 10)
    , @OnErrorWaitMillisecond smallint = 1000 -- wait for milliseconds between each Retry (default if NULL = 1000ms)
    /* Messge Logging */
    , @SaveMessagesToTable nvarchar(MAX) = 'Y' -- Y{es} or N{o} => Save to [maintenance].[messages] table (default if NULL = Y)
    , @SavedMessagesRetentionDays smallint = 30
    , @SavedToRunId int = NULL OUTPUT
    , @OutputMessagesToDataset nvarchar(MAX) = 'N' -- Y{es} or N{o} => Output Messages Result set
    , @Verbose nvarchar(MAX) = NULL -- Y{es} = Print all messages < 10
AS
BEGIN
    BEGIN TRY 
        SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
		SET LOCK_TIMEOUT 5000;
        SET ARITHABORT ON;
        SET NOCOUNT ON;
        SET NUMERIC_ROUNDABORT OFF;

        ----------------------------------------------------------------------------------------------------
        -- Local Run Variables
        ----------------------------------------------------------------------------------------------------
        DECLARE @returnValue int = 1;
        DECLARE @triggerDate datetime;
        DECLARE @triggerFloatingDate float(53);
        DECLARE @floatingDay float(53) = 1;
        DECLARE @floatingHour float(53) = @floatingDay / 24;
        ----------------------------------------------------------------------------------------------------
        --
        ----------------------------------------------------------------------------------------------------
        IF @RetentionDate IS NULL AND @RetentionDays IS NULL AND @RetentionHours IS NULL RAISERROR(N'@RetentionDate or @RetentionDays/@RetentionHours must be provided', 16, 1);
        IF @RetentionDate IS NOT NULL AND (@RetentionDays IS NOT NULL OR @RetentionHours IS NOT NULL) RAISERROR(N'@RetentionDate AND @RetentionDays/@RetentionHours cannot be used together', 16, 1);
        IF @RetentionDate > SYSDATETIME() RAISERROR(N'@RetentionDate must be a past date', 16, 1);

        -- Use current Date if @RetentionDate is missing
        SELECT @triggerDate = ISNULL(@RetentionDate, SYSDATETIME());

        -- Remove time part (=> begining of day)
        IF @RoundUpPreviousDay = 1 SELECT @triggerDate = CAST(@triggerDate AS DATE);
        SELECT @triggerFloatingDate = CAST(@triggerDate AS float(53));
        -- Remove retention Days/Hours
        SELECT @triggerDate = CAST(@triggerFloatingDate -ISNULL(ABS(@floatingDay  * @RetentionDays), 0) -ISNULL(ABS(@floatingHour * @RetentionHours), 0) AS datetime) 

        -- Add Trigger
        EXEC [Maintenance].[AddArchiveTriggerJobs] @ArchiveTriggerTime = @triggerDate, @Filters = N'ALL', @ArchiveAfterHours = 0, @DeleteDelayHours = 0, @DryRunOnly = 0;
        -- Start Archive
        EXEC [Maintenance].[ArchiveJobs] @RowsDeletedForEachLoop = @RowsDeletedForEachLoop, @MaxConcurrentFilters = @MaxConcurrentFilters, @MaxRunMinutes = @MaxRunMinutes, @MaxBatchesLoops = @MaxBatchesLoops
            , @SynchronousDeleteIfNoDelay = @SynchronousDeleteIfNoDelay, @IgnoreDeleteDelay = @IgnoreDeleteDelay
            , @CreateArchiveTable = @CreateArchiveTable, @UpdateArchiveTable = @UpdateArchiveTable, @ExcludeColumns = @ExcludeColumns

        SET @returnValue = 0; -- Success
    END TRY
    BEGIN CATCH
        THROW;
    END CATCH

    SET @returnValue = ISNULL(@returnValue, 255);
    --IF @runId IS NOT NULL UPDATE [Maintenance].[Runs] SET [EndDate] = SYSDATETIME(), [ErrorStatus] = @returnValue WHERE Id = @runId;

    ----------------------------------------------------------------------------------------------------
    -- End
    ----------------------------------------------------------------------------------------------------
    RETURN @returnValue;
END
GO
