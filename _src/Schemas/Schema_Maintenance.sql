SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- ### [Object]: Schema [Maintenance]
-- ### [Version]: 2020-10-01 00:00:00                                                         
-- ### [Hash]: XxXxXxX
-- ### [Docs]: https://XxXxXxX
----------------------------------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Maintenance')
BEGIN 
	PRINT ' + Create Schema [Maintenance]';
	EXEC sp_executesql N'CREATE SCHEMA [Maintenance]';
END
ELSE PRINT ' = Schema already exists: [Maintenance]';
GO
