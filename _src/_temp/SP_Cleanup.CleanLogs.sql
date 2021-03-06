SET NOCOUNT ON;
GO

-- Create an empty procedure if it doesn'texist yet...
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Cleanup].[CleanLogs]') AND type in (N'P'))
BEGIN
	EXEC('CREATE PROCEDURE [Cleanup].[CleanLogs] AS SELECT 1')
END

PRINT N' ~ ALTER PROCEDURE [Cleanup].[CleanLogs]'
GO

----------------------------------------------------------------------------------------------------
-- 
-- 
----------------------------------------------------------------------------------------------------
--DROP PROCEDURE [Cleanup].[CleanLogs]
ALTER PROCEDURE [Cleanup].[CleanLogs]
	@TenantList nvarchar(max) = N''
	, @MaxLevelToDelete nvarchar(20) = N'warn'

	, @KeepLastDays int = 90
	, @KeepAfterDate datetime2 = NULL
	, @ForceDeleteRecentPast nvarchar(max) = NULL

	, @RowsDeletedByIteration Int = 10000

	, @StopAfterRunTimeInMinutes int = 120
	, @StopAfterDateTime datetime2 = NULL

	, @TLogMaxUsage nvarchar(max) = N'50%'
	, @TLogForceAllowGrowth nvarchar(max) = N'N'
	, @TLogThresholdAction nvarchar(max) = N''
	, @TlogWaitMaxMinutes tinyint = 0

--	, @loopDelCount int = 10000
--	, @DaysToKeep int NOT NULL = 180
--	, @logPercentThreshold tinyint = 25, @logWaitMinutes int = 1, @logWaitMax int = 60
AS
BEGIN
    SET NOCOUNT ON;
    SET ARITHABORT ON;
    SET NUMERIC_ROUNDABORT OFF;

	-- Init
	DECLARE @StartDateTime datetime2 = SYSDATETIME();
	-- Output
    DECLARE @Message nvarchar(max);
    DECLARE @ErrorMessage nvarchar(max);
	-- Parameters
	DECLARE @Parameters nvarchar(max);
	-- Date limit
	DECLARE @PurgeDateLimit datetime2;
	-- Log level validation
	DECLARE @MaxLevelId int;
	-- Tenants validation
	DECLARE @TenantsNotFound nvarchar(max);
	DECLARE @TenantsNames nvarchar(max);
	DECLARE @tenants TABLE(/*Unifier int IDENTITY(0, 1) PRIMARY KEY NONCLUSTERED,*/ Id int NULL INDEX ix CLUSTERED, [Name] nvarchar(128));
	DECLARE @TenantMachines TABLE(Id bigint PRIMARY KEY CLUSTERED); --, TenantId int NOT NULL, UNIQUE NONCLUSTERED(TenantId, MachineId));
	-- RowsDeletedByIteration
	DECLARE @RowsDeletedMin int = 100;
	DECLARE @RowsDeletedMax int = 500000;
	DECLARE @RowsDeletedDefault int = 50000;
	-- TLog
	DECLARE @TLogDefaultAction nvarchar(max) = N'STOP'
	DECLARE @TLogStopActions TABLE(Action nvarchar(max));
	INSERT INTO @TLogListActions(Action) VALUES(N'STOP'), (N'CHECKPOINT'), (N'BACKUP'), (N'WAIT')

	BEGIN TRY
	    ----------------------------------------------------------------------------------------------------
    	-- Parameter' List
		----------------------------------------------------------------------------------------------------
		SET @Parameters = N'@TenantList = ' + ISNULL(N'''' + REPLACE(@TenantList, N'''' ,N'''''') + N'''', N'NULL');
		SET @Parameters = @Parameters + ', @MaxLevelToDelete = ' + ISNULL('''' + REPLACE(@MaxLevelToDelete, N'''', N'''''') + '''', N'NULL');
		SET @Parameters = @Parameters + ', @KeepLastDays = ' + ISNULL(CAST(@KeepLastDays AS nvarchar(20)), N'NULL');
		SET @Parameters = @Parameters + ', @KeepAfterDate = ' + ISNULL(CONVERT(nvarchar, @KeepAfterDate, 120), N'NULL');
		SET @Parameters = @Parameters + ', @ForceDeleteRecentPast = ' + ISNULL('''' + REPLACE(@ForceDeleteRecentPast, N'''', N'''''') + '''', N'NULL');
		SET @Parameters = @Parameters + ', @RowsDeletedByIteration = ' + ISNULL(CAST(@RowsDeletedByIteration AS nvarchar(20)), N'NULL');
		SET @Parameters = @Parameters + ', @StopAfterRunTimeInMinutes = ' + ISNULL(CAST(@StopAfterRunTimeInMinutes AS nvarchar(20)), N'NULL');
		SET @Parameters = @Parameters + ', @StopAfterDateTime = ' + ISNULL(CONVERT(nvarchar, @StopAfterDateTime, 120), N'NULL');
		SET @Parameters = @Parameters + ', @TLogMaxUsagePercent = ' + ISNULL(CAST(@TLogMaxUsagePercent AS nvarchar(20)), N'NULL');
		SET @Parameters = @Parameters + ', @TlogWaitMaxMinutes = ' + ISNULL(CAST(@TlogWaitMaxMinutes AS nvarchar(20)), N'NULL');

		RAISERROR('%s', 10 ,1 , @Parameters) WITH NOWAIT;

		/***************************************************************************************************
    	** Check Parameters
		***************************************************************************************************/

		----------------------------------------------------------------------------------------------------
		-- Validate Max Level to delete 
		----------------------------------------------------------------------------------------------------
		ALTER PROCEDURE [Cleanup].[ValidateMaxLogLevel] @MaxLevelToDelete = @MaxLevelToDelete, @MaxLevelId = @MaxLevelId OUTPUT;

		----------------------------------------------------------------------------------------------------
		-- Validate Max Dates to delete
		----------------------------------------------------------------------------------------------------
		EXEC [Cleanup].[ValidatePurgeDate] @StartDateTime = @StartDateTime
										, @KeepLastDays = @KeepLastDays
										, @KeepAfterDate = @KeepAfterDate
										, @ForceDeleteRecentPast = @ForceDeleteRecentPast
										, @PurgeDateLimit = @PurgeDateLimit OUTPUT;

		----------------------------------------------------------------------------------------------------
		-- Check Tenants
		----------------------------------------------------------------------------------------------------
		INSERT INTO @tenants(Id, [Name])
		EXEC [Maintenance].[SplitListTenants] @TenantList = @TenantList, @Delimiter = ',', @DiscardDelimiter = '-';

		SELECT @TenantsNotFound = COALESCE(@TenantsNotFound + N', ' + [Name], [Name]) FROM @tenants WHERE Id IS NULL;
		SET @Message = N'[WARNING] Tenant(s) not found:';
		IF @TenantsNotFound IS NOT NULL RAISERROR('%s %s', 10 ,1 , @Message, @TenantsNotFound) WITH NOWAIT;

		IF NOT EXISTS(SELECT 1 FROM @tenants WHERE Id IS NOT NULL) THROW 70006, N'No valid tenants found. Check previous warning.', 1;

		SELECT @TenantsNames = COALESCE(@TenantsNames + N', ' + [Name], [Name]) FROM @tenants WHERE Id IS NOT NULL;
		SET @Message = N'[PARAMETER] Tenants =';
		RAISERROR('%s %s', 10 ,1 , @Message, @TenantsNames) WITH NOWAIT;

		INSERT INTO @TenantMachines(Id)
		SELECT mch.Id FROM @tenants tnt
		INNER JOIN dbo.Machines mch ON mch.TenantId = tnt.Id

		----------------------------------------------------------------------------------------------------
		-- Check @RowsDeletedByIteration
		----------------------------------------------------------------------------------------------------
		IF @RowsDeletedByIteration IS NULL 
		BEGIN
			SET @RowsDeletedByIteration = @RowsDeletedDefault;
			SET @Message = N'[WARNING] @RowsDeletedByIteration IS NULL and parameter has been replaced by default value';
			RAISERROR('%s', 10 ,1 , @Message) WITH NOWAIT;
		END 
		ELSE IF @RowsDeletedByIteration < @RowsDeletedMin 
		BEGIN 
			SET @Message = N'[WARNING] @RowsDeletedByIteration parameter is below lower default value and has been replaced: @RowsDeletedByIteration=';
			RAISERROR('%s%d', 10 ,1 , @Message, @RowsDeletedByIteration) WITH NOWAIT;
			SET @RowsDeletedByIteration = @RowsDeletedMin;
		END
		ELSE IF @RowsDeletedByIteration > @RowsDeletedMax 
		BEGIN
			SET @Message = N'[WARNING] @RowsDeletedByIteration parameter is above upper default value and has been replaced: @RowsDeletedByIteration=';
			RAISERROR('%s%d', 10 ,1 , @Message, @RowsDeletedByIteration) WITH NOWAIT;
			SET @RowsDeletedByIteration = @RowsDeletedMax;
		END
		SET @Message = N'[PARAMETER] Rows deleted by iteration =';
		RAISERROR('%s %d', 10 ,1 , @Message, @RowsDeletedByIteration) WITH NOWAIT;
/*
		SELECT 'mch1',  COUNT(*), MIN(lgs.Id), MAX(lgs.Id), MIN(lgs.[TimeStamp]), MAX(lgs.[TimeStamp]) 
		FROM dbo.logs2 lgs
		INNER JOIN @machines mch ON lgs.MachineId = mch.Id
		WHERE lgs.[TimeStamp] < @date*/

/*
	SELECT @DaysToKeep = ISNULL(@DaysToKeep, 730)
		, @loopDelCount = ISNULL(@loopDelCount, 10000)
		, @logPercentThreshold = ISNULL(@logPercentThreshold, 25), @logWaitMinutes = ISNULL(@logWaitMinutes, 1), @logWaitMax = ISNULL(@logWaitMax, 60)
	

	BEGIN TRY
		SELECT @logWaitDelay = CAST(CAST(DATEADD(MINUTE, @logWaitMinutes, '') AS time) as varchar(8));
		SET @totalUserBillingItems = 0;
		
		SET @msg = N'Start ' + @TableName + N' Cleanup';
		PRINT @msg;
		INSERT INTO [Cleanup].[CleanupHistory]([TableNames], [Message], [DeletedCount], [Status])
		SELECT @TableName, @msg, 0, 1;		
		
		SET @msg = N'PARAMS: @DaysToKeep int = ' + CAST(@DaysToKeep as nvarchar(20)) 
			+ N', @loopDelCount int = ' + CAST(@loopDelCount as nvarchar(20))
			+ N', @logPercentThreshold tinyint = ' + CAST(@logPercentThreshold as nvarchar(20))
			+ N', @logWaitMinutes int = ' + CAST(@logWaitMinutes as nvarchar(20))
			+ N', @logWaitMax int = ' + CAST(@logWaitMax as nvarchar(20))
		;
		PRINT @msg;
		INSERT INTO [Cleanup].[CleanupHistory]([TableNames], [Message], [DeletedCount], [Status])
		SELECT @TableName, @msg, 0, 1;		
		
		SELECT @MinDateToKeep = DATEADD(day, -@DaysToKeep, SYSUTCDATETIME());
		SET @msg = N'Min Date to Keep = ' + CAST(@MinDateToKeep as nvarchar(20));
		PRINT @msg;
		INSERT INTO [Cleanup].[CleanupHistory]([TableNames], [Message], [DeletedCount], [Status])
		SELECT @TableName, @msg, 0, 1;

		SELECT	@UserBillingMaxId =	MAX(ID)
		FROM	dbo.UserBilling		AS	ub
		WHERE	ub.ConnectDateTime	<	@MinDateToKeep

		SET @msg = N'Max UserBilling ID to Keep = ' + CAST(@UserBillingMaxId as nvarchar(20));
		PRINT @msg;
		INSERT INTO [Cleanup].[CleanupHistory]([TableNames], [Message], [DeletedCount], [Status])
		SELECT @TableName, @msg, 0, 1;

		-- Loop through User Billing until there is nothing left older than @DaysToKeep
		WHILE @UserBillingMaxId > 0
		BEGIN
			SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; 
			BEGIN TRAN
			
			----------------------------------------------------------------------------------------------------
			-- Start Cleanup
			----------------------------------------------------------------------------------------------------

			---------------------------------------------
			--UserServicePlatform
			DELETE	usp
			FROM dbo.UserServicePlatform AS usp
			INNER JOIN dbo.UserService AS us ON us.ID  = usp.UserServiceID
			INNER JOIN (
				SELECT TOP(@loopDelCount) ID
				FROM dbo.UserBilling
				WHERE ID < @UserBillingMaxId
				ORDER BY ID
			) i ON us.UserBillingID = i.ID;

			SET @nbUserServicePlatformItems = @@ROWCOUNT;
			
			---------------------------------------------
			--UserService
			DELETE us
			FROM	dbo.UserService AS	us
			INNER JOIN	(
				SELECT TOP(@loopDelCount) ID
				FROM dbo.UserBilling
				WHERE ID < @UserBillingMaxId
				ORDER BY ID
			) AS ub ON ub.ID = us.UserBillingID
			;
			
			SET @nbUserServiceItems = @@ROWCOUNT;
		
			---------------------------------------------
			--ServiceQuality
			DELETE sq
			FROM	dbo.ServiceQuality AS	sq
			INNER JOIN	(
				SELECT TOP(@loopDelCount) ID
				FROM dbo.UserBilling
				WHERE ID < @UserBillingMaxId
				ORDER BY ID
			) AS ub ON ub.ID = sq.UserBillingID
			;
					
			SET @nbServiceQualityItems = @@ROWCOUNT;

			---------------------------------------------
			--UserBilling
			WITH i AS (
				SELECT TOP(@loopDelCount) ID
				FROM dbo.UserBilling
				WHERE ID < @UserBillingMaxId
				ORDER BY ID
			)
			DELETE FROM i;
						
			SET @nbUserBillingItems = @@ROWCOUNT;
			
			SET @totalUserBillingItems = @totalUserBillingItems + @nbUserBillingItems;
			
			IF @@TRANCOUNT > 0 COMMIT TRAN;
			SET TRANSACTION ISOLATION LEVEL READ COMMITTED; 
			
			--ROLLBACK TRAN; 
			
			SET @msg = '  - Cleaned : ' 
				+ CAST(@nbUserServicePlatformItems AS nvarchar(10)) + ' USP items' 
				+ ', ' + CAST(@nbUserServiceItems AS nvarchar(10)) + ' US items'
				+ ', ' + CAST(@nbServiceQualityItems AS nvarchar(10)) + ' SQ items'
				+ ', ' + CAST(@nbUserBillingItems AS nvarchar(10)) + ' UB items'
				+ ', ' + CAST(@totalUserBillingItems AS nvarchar(10)) + ' TOTAL UB items';
			PRINT @msg;
			INSERT INTO [Cleanup].[CleanupHistory]([TableNames], [Message], [DeletedCount], [Status])
			SELECT @TableName, @msg, @nbUserBillingItems, 1;		

			IF @nbUserBillingItems = 0 BREAK;

			SET @logWaitTotal = 0;

			WHILE 1 = 1
			BEGIN
				DELETE FROM @space;

				INSERT INTO @space([Database Name], [Log Size (MB)], [Log Space Used (%)], [Status])
				EXEC (N'DBCC SQLPERF(logspace) WITH NO_INFOMSGS');
				
				IF @logWaitTotal >= @logWaitMax
				BEGIN
					RAISERROR('Logs usage over thresold (%d) for over %d minutes', 16, 1, @logPercentThreshold, @logWaitMax)
				END

				IF EXISTS(SELECT 1 FROM @space WHERE [Database Name] = DB_NAME() AND [Log Space Used (%)] > @logPercentThreshold)
				BEGIN
					SET @msg = N'*** LOGs usage over threshold [' + CAST((SELECT [Log Space Used (%)] FROM @space WHERE [Database Name] = DB_NAME()) AS nvarchar(10)) + N'% used'
						+ N', threshold =' + CAST(@logPercentThreshold AS nvarchar(10)) + N'%]';
					PRINT @msg;
					INSERT INTO [Cleanup].[CleanupHistory]([TableNames], [Message], [DeletedCount], [Status])
					SELECT @TableName, @msg, 0, 1;		

					SET @msg = N'*** Wait ' + CAST(@logWaitMinutes AS varchar(5)) + ' minutes (total = ' + CAST(@logWaitTotal AS varchar(5)) + ')';
					PRINT @msg;
					INSERT INTO [Cleanup].[CleanupHistory]([TableNames], [Message], [DeletedCount], [Status])
					SELECT @TableName, @msg, 0, 1;		

					WAITFOR DELAY @logWaitDelay
					SET @logWaitTotal = @logWaitTotal + @logWaitMinutes
				END
				ELSE BREAK;
			END
			----------------------------------------------------------------------------------------------------
			-- End Cleanup
			----------------------------------------------------------------------------------------------------
		END

		SET @msg = N'End Cleanup';
		PRINT @msg;
		INSERT INTO [Cleanup].[CleanupHistory]([TableNames], [Message], [DeletedCount], [Status])
		SELECT @TableName, @msg, @totalUserBillingItems, 1;		
	*/	
	END TRY
	BEGIN CATCH
		DECLARE @ERROR_NUMBER INT, @ERROR_SEVERITY INT, @ERROR_STATE INT, @ERROR_PROCEDURE NVARCHAR(126), @ERROR_LINE INT, @ERROR_MESSAGE NVARCHAR(2048) 
		
		SELECT    @ERROR_NUMBER = ERROR_NUMBER()
				, @ERROR_SEVERITY = ERROR_SEVERITY()
				, @ERROR_STATE = ERROR_STATE()
				, @ERROR_PROCEDURE = ERROR_PROCEDURE()
				, @ERROR_LINE = ERROR_LINE()
				, @ERROR_MESSAGE = ERROR_MESSAGE()

		IF @@TRANCOUNT > 0 ROLLBACK TRAN;

/*		SET @msg = CAST(
			 N'@ERROR_NUMBER = ' + CAST(ERROR_NUMBER() AS nvarchar(20)) 
			+ N', @ERROR_SEVERITY = ' + CAST(ERROR_SEVERITY() AS nvarchar(20)) 
			+ N' , @ERROR_STATE = ' + CAST(ERROR_STATE() AS nvarchar(20)) 
			+ N' , @ERROR_PROCEDURE = '+ ERROR_PROCEDURE()
			+ N' , @ERROR_LINE = ' + CAST(ERROR_LINE() AS nvarchar(20)) 
			+ N' , @ERROR_MESSAGE = ' + ERROR_MESSAGE()
			AS nvarchar(4000))
		;
		
		PRINT @msg;
		INSERT INTO [Cleanup].[CleanupHistory]([TableNames], [Message], [DeletedCount], [Status])
		SELECT @TableName, @msg, 0, 0;		

		RAISERROR('ErrorNumber: %d, ErrorMessage: %s, ErrorSeverity: %d, ErrorState: %d, ErrorProcedure: %s, ErrorLine: %d', 16, 1, @ERROR_NUMBER, @ERROR_MESSAGE, @ERROR_SEVERITY, @ERROR_STATE, @ERROR_PROCEDURE, @ERROR_LINE);
*/
		THROW;
	END CATCH

END
GO

/*
=
	@TenantList = N'default,  , -p% , - , , #INACTIVE_TENANTS# , #ACTIVE_TENANTS#'
	, @MaxLevelToDelete = N'warn'
	, @KeepLastDays = 90, @KeepAfterDate = NULL
	, @RowsDeletedByIteration = 10000, @StopAfterRunTimeInMinutes = 120, @StopAfterDateTime = NULL
	, @TLogMaxUsagePercent = 60, @TlogWaitMaxMinutes = 0
*/