SET NOCOUNT ON
GO

PRINT 'CREATE PROCDURE';
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Cleanup].[ValidateMaxLogLevel]') AND type = N'P')
BEGIN
    PRINT ' + Create Procedure [Cleanup].[ValidateMaxLogLevel]'
    EXEC sp_executesql N'CREATE PROCEDURE [Cleanup].[ValidateMaxLogLevel] AS SELECT 1'
END
ELSE PRINT ' = Procedure [Cleanup].[ValidateMaxLogLevel] already exists';
GO

PRINT ' ~ Update Procedure [Cleanup].[ValidateMaxLogLevel]';
GO

----------------------------------------------------------------------------------------------------
-- Validate Max log level to be deleted
-- INTPUT @KeepLastDays = number of days to be kept
--        @KeepAfterDate = date limit
--        @ForceDeleteRecentPast = special parameter when date is within 30 days or below
-- Output = Table list of 
--            - Tenant Id and Name when item in @TenantList match to an existing 
--            - Item name when item in @TenantList doesn't match to an existing tenant
/*
EXEC [Cleanup].[ValidateMaxLogLevel] N'default,  , -p%, testxxx , #INACTIVE_TENANTS# , #DELETED_TENANTS#';
GO
*/
----------------------------------------------------------------------------------------------------
ALTER PROCEDURE [Cleanup].[ValidateMaxLogLevel]
	@MaxLevelToDelete nvarchar(20) = N'warn'
	, @MaxLevelId int OUTPUT
AS 
BEGIN
    SET NOCOUNT ON;

	-- Output
    DECLARE @Message nvarchar(max);
    DECLARE @ErrorMessage nvarchar(max);
	-- Log level validation
	DECLARE @ListLevels nvarchar(2048);

    BEGIN TRY
		-- Get valid Log level Id and Name
		SELECT @ListLevels = COALESCE(@ListLevels + N', ' + level, level) FROM (SELECT CAST(id AS nvarchar(5)) + N' or ' + level FROM [Maintenance].[LogLevels]()) AS levels(level);

		SET @ErrorMessage = N'Max Level can''t be NULL. Use: ' + @ListLevels;
	    IF @MaxLevelToDelete IS NULL THROW 70001, @ErrorMessage, 1;

		SELECT @MaxLevelToDelete = LTRIM(RTRIM(@MaxLevelToDelete));
		SET @ErrorMessage = N'Max Level is missing. Use: ' + @ListLevels;
    	IF @MaxLevelToDelete = '' THROW 70002, @ErrorMessage, 1;

		-- Validate level and get Id
		SELECT @MaxLevelId = [Maintenance].[GetLogLevelId](@MaxLevelToDelete);

		SET @ErrorMessage = N'Max Level is invalid: '+ @MaxLevelToDelete + N'. Use: ' + @ListLevels;
	    IF @MaxLevelId IS NULL THROW 70003, @ErrorMessage, 1;

		-- Output validated Level Id value
		SET @Message = N'[PARAMETER] Max level = ' + CAST(@MaxLevelId AS nvarchar(10)) + N' [@MaxLevelToDelete=''' + CAST(@MaxLevelToDelete AS nvarchar(max)) + ''']';
		RAISERROR('%s', 10 ,1 , @Message) WITH NOWAIT;
	END TRY
	BEGIN CATCH
        THROW;
	END CATCH
END;
GO
