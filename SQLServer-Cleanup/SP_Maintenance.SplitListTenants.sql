SET NOCOUNT ON
GO

PRINT 'CREATE SCHEMA';
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Maintenance')
BEGIN 
	PRINT ' + Create Schema [Maintenance]';
	EXEC sp_executesql N'CREATE SCHEMA [Maintenance]';
END
ELSE PRINT ' = Schema already exists: [Maintenance]';
GO
PRINT ''

PRINT 'CREATE PROCDURE';
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[SplitListTenants]') AND type = N'P')
BEGIN
    PRINT ' + Create Procedure [Maintenance].[SplitListTenants]'
    EXEC sp_executesql N'CREATE PROCEDURE [Maintenance].[SplitListTenants] AS SELECT 1'
END
ELSE PRINT ' = Procedure [Maintenance].[SplitListTenants] already exists';
GO

PRINT ' ~ Update Procedure [Maintenance].[SplitListTenants]';
GO

----------------------------------------------------------------------------------------------------
-- Split tenant' list and return a table will valid tenants
-- INTPUT @TenantList = comma separated list of tenant
--                      dash sign disards a tenant
--                      % is used for wildcard selection
--                      Special values: #ALL_TENANTS#, #ACTIVE_TENANTS#, #INACTIVE_TENANTS#, #DELETED_TENANTS#
-- Output = Table list of 
--            - Tenant' Id and Name when item in @TenantList match to an existing 
--            - Item name when item in @TenantList doesn't match to an existing tenant
/*
EXEC [Maintenance].[SplitListTenants] N'default,  , -p%, testxxx , #INACTIVE_TENANTS# , #DELETED_TENANTS#';
GO
*/
----------------------------------------------------------------------------------------------------
ALTER PROCEDURE [Maintenance].[SplitListTenants]
    @TenantList nvarchar(max)
    , @Delimiter char(1) = ',', @DiscardDelimiter char(1) = '-'
AS 
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @message nvarchar(2048);

    DECLARE @keywords TABLE(keyword nvarchar(20));
    INSERT INTO @keywords(keyword) VALUES(N'#ALL_TENANTS#'), (N'#ACTIVE_TENANTS#'), (N'#INACTIVE_TENANTS#'), (N'#DELETED_TENANTS#');
    DECLARE @invalidKeywords nvarchar(max);
    DECLARE @itemsList TABLE(Id int, Item nvarchar(max), Tenant nvarchar(128), isDiscarded bit);
    
    BEGIN TRY
        -- Remove special characters: end of line, return, space and double delimiter
        SET @TenantList = REPLACE(@TenantList, CHAR(10), '');
        SET @TenantList = REPLACE(@TenantList, CHAR(13), '');
        SET @TenantList = LTRIM(RTRIM(@TenantList));
        WHILE CHARINDEX(@Delimiter+' ', @TenantList) > 0 SET @TenantList = REPLACE(@TenantList, @Delimiter+' ', @Delimiter);
        WHILE CHARINDEX(' '+@Delimiter, @TenantList) > 0 SET @TenantList = REPLACE(@TenantList,' '+@Delimiter, @Delimiter);
        WHILE CHARINDEX(@Delimiter+@Delimiter, @TenantList) > 0 SET @TenantList = REPLACE(@TenantList,@Delimiter+@Delimiter, @Delimiter);

        -- Check empty list
        IF @TenantList IS NULL THROW 60001, N'@TenantList can''t be NULL', 1;
        IF @TenantList = '' THROW 60002, N'@TenantList can''t be empty', 1;
        IF @TenantList = @Delimiter THROW 60003, N'@TenantList can''t be empty', 1;

        -- Extract items from @TenantList and return a list of matching id and tenants' names or unmatched items
        WITH Split (StartPosition, EndPosition, Item) AS
        (
            SELECT StartPosition = 1
                , EndPosition = COALESCE(NULLIF(CHARINDEX(',', @TenantList, 1), 0), LEN(@TenantList) + 1)
                , Item = SUBSTRING(@TenantList, 1, COALESCE(NULLIF(CHARINDEX(@Delimiter, @TenantList, 1), 0), LEN(@TenantList) + 1) - 1)
            WHERE @TenantList IS NOT NULL
            UNION ALL
            SELECT StartPosition = CAST(EndPosition AS int) + 1
                , EndPosition = COALESCE(NULLIF(CHARINDEX(',', @TenantList, EndPosition + 1), 0), LEN(@TenantList) + 1)
                , Item = SUBSTRING(@TenantList, EndPosition + 1, COALESCE(NULLIF(CHARINDEX(@Delimiter, @TenantList, EndPosition + 1), 0), LEN(@TenantList) + 1) - EndPosition - 1)
            FROM Split
            WHERE EndPosition < LEN(@TenantList) + 1
        )
        , Items(Item, isDiscarded) AS(
            SELECT Item = CASE WHEN LEFT(Item, 1) <> @DiscardDelimiter THEN Item ELSE RIGHT(Item, LEN(Item)-1) END
                , isDiscarded = CASE WHEN LEFT(Item, 1) <> @DiscardDelimiter THEN 0 ELSE 1 END
            FROM Split
        )
        INSERT INTO @itemsList(Id, Item, Tenant, isDiscarded)
        SELECT tnt.Id, itm.Item, tnt.Name, itm.isDiscarded
        FROM Items itm
        LEFT JOIN dbo.Tenants tnt ON tnt.Name LIKE itm.Item
            OR itm.Item = N'#ALL_TENANTS#'
            OR (itm.Item = N'#ACTIVE_TENANTS#' AND tnt.IsActive = 1) OR (itm.Item = N'#INACTIVE_TENANTS#' AND tnt.IsActive = 0) 
            OR (itm.Item = N'#DELETED_TENANTS#' AND tnt.IsDeleted = 1)  
        WHERE itm.item <> N''
        OPTION (MAXRECURSION 0);

        -- Check invalid keywords usage
        SELECT @invalidKeywords = NULL;
        SELECT @invalidKeywords = COALESCE(@invalidKeywords + N', ' + item, item) FROM (
            SELECT item FROM @itemsList WHERE item LIKE N'#%#' AND Id IS NULL
        ) AS kw;

        IF @invalidKeywords IS NOT NULL 
        BEGIN
            SET @message = NULL;
            SELECT @message = COALESCE(@message + N', ' + keyword, keyword) FROM @keywords;
            SET @message = N'Invalid keyword(s): ' + @invalidKeywords + N'. Use only valid keyword(s): ' + @message;
            THROW 60004, @message, 1;
        END;

        -- Get a unique list of Id/Tenants and unmatched item (if any)
        SELECT Id, COALESCE(Tenant, Item) FROM @itemsList WHERE isDiscarded = 0
        EXCEPT
        SELECT Id, Tenant FROM @itemsList WHERE isDiscarded = 1;

	END TRY
	BEGIN CATCH
        THROW;
	END CATCH
END;
GO
