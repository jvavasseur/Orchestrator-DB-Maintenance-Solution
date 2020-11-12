SET NOCOUNT ON
SET XACT_ABORT ON;
GO

PRINT 'CREATE PROCDURE';
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Cleanup].[ValidatePurgeDate]') AND type = N'P')
BEGIN
    PRINT ' + Create Procedure [Cleanup].[ValidatePurgeDate]'
    EXEC sp_executesql N'CREATE PROCEDURE [Cleanup].[ValidatePurgeDate] AS SELECT 1'
END
ELSE PRINT ' = Procedure [Cleanup].[ValidatePurgeDate] already exists';
GO

PRINT ' ~ Update Procedure [Cleanup].[ValidatePurgeDate]';
GO

----------------------------------------------------------------------------------------------------
-- Validate purge date parameters and return a valid purge date limit
-- INTPUT @KeepLastDays = number of days to be kept
--        @KeepAfterDate = date limit
--        @ForceDeleteRecentPast = special parameter when date is within 30 days or below
-- Output @PurgeDateLimit = valid purge date
/*
EXEC [Cleanup].[ValidatePurgeDate] ...;
GO
*/
----------------------------------------------------------------------------------------------------
ALTER PROCEDURE [Cleanup].[ValidatePurgeDate]
    @StartDateTime datetime2
	, @KeepLastDays int = 90
	, @KeepAfterDate datetime2 = NULL
	, @ForceDeleteRecentPast nvarchar(max) = NULL
    , @PurgeDateLimit datetime2 OUTPUT
AS 
BEGIN
    SET NOCOUNT ON;
    SET ARITHABORT ON;
    SET ARITHABORT OFF;

    BEGIN TRY
        -- Output
        DECLARE @Message nvarchar(max);
        DECLARE @ErrorMessage nvarchar(max);
        -- Date validation
        DECLARE @DeleteDays int;
        DECLARE @ForceRecentPastKeyword nvarchar(max);
        DECLARE @ForcePastDaysKeywords TABLE(PastDays tinyint PRIMARY KEY CLUSTERED, Keyword nvarchar(max));
        INSERT INTO @ForcePastDaysKeywords(PastDays, Keyword) VALUES(30, N'force-month'), (7, N'force-week'), (1, N'force-day'), (0, N'force-remove-all-days');

 		SET @ErrorMessage = N'Missing Keep Last Days or After Date value. Use either @KeepLastDays or @KeppAfterDate';
		IF @KeepLastDays IS NULL AND @KeepAfterDate IS NULL THROW 70003, @ErrorMessage, 1;

		-- Find most recent date between @KeepLastDays and @KeepAfterDate if both provided
		SELECT @PurgeDateLimit = CASE WHEN @KeepLastDays IS NULL THEN @KeepAfterDate
								  WHEN @KeepAfterDate IS NULL THEN LastDaysDate
								  WHEN @KeepAfterDate > LastDaysDate THEN @KeepAfterDate
								  ELSE LastDaysDate END
		FROM (SELECT DATEADD(dd, - ABS(@KeepLastDays), @StartDateTime)) AS KeepLastDays(LastDaysDate);

		SET @ErrorMessage = N'Date in the future is invalid. @KeepAfterDate=' + CONVERT(nvarchar, @PurgeDateLimit, 120);
		IF @PurgeDateLimit > @StartDateTime THROW 70004, @ErrorMessage, 1;

		-- Get number of days kept
		SET @DeleteDays = DATEDIFF(day, @PurgeDateLimit, @StartDateTime);

		-- Check if Force keyword is required for days in recent past
		SELECT @ForceRecentPastKeyword = kwd.Keyword FROM @ForcePastDaysKeywords kwd
		OUTER APPLY (SELECT TOP(1) PastDays FROM @ForcePastDaysKeywords WHERE PastDays < kwd.PastDays ORDER BY PastDays DESC) prc
		WHERE @DeleteDays >= COALESCE(prc.PastDays+1, kwd.PastDays) AND @DeleteDays <= kwd.PastDays;

		SET @ErrorMessage = N'Delete date limit is in recent past. @ForceDeleteRecentPast must be used with a valid keyword. @ForceDeleteRecentPast=' + ISNULL(''''+@ForceDeleteRecentPast+'''', N'NULL') + N' [Date='+ CONVERT(nvarchar, @PurgeDateLimit, 120) + N']';
		IF @ForceRecentPastKeyword IS NOT NULL AND @ForceRecentPastKeyword <> @ForceDeleteRecentPast  THROW 70005, @ErrorMessage, 1;

		-- Output validated force recent past value
		SET @Message = N'[PARAMETER] Force Delete Recent Past = "' + @ForceDeleteRecentPast + N'" [Delete date limit=''' + CAST(@PurgeDateLimit AS nvarchar(max)) + ''']';
		IF @ForceRecentPastKeyword IS NOT NULL RAISERROR('%s', 10 ,1 , @Message) WITH NOWAIT;

		-- Output validated delete date limit
		SET @Message = N'[PARAMETER] Delete before date = ' + CONVERT(nvarchar, @PurgeDateLimit, 120) + N' [@KeepLastDays=' + ISNULL(CONVERT(nvarchar, @KeepLastDays, 120), N'NULL')  + N' / @KeepAfterDate=' + ISNULL(CONVERT(nvarchar, @KeepAfterDate, 120), N'NULL') + N']';
		RAISERROR('%s', 10 ,1 , @Message) WITH NOWAIT;
	END TRY
	BEGIN CATCH
        IF @@trancount > 0 ROLLBACK TRANSACTION
        THROW;
        RETURN 1;
	END CATCH
END;
GO
