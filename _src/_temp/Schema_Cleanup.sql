SET NOCOUNT ON
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Cleanup')
BEGIN 
	PRINT ' + Create Schema [Cleanup]';
	EXEC sp_executesql N'CREATE SCHEMA [Cleanup]';
END
ELSE PRINT ' = Schema already exists: [Cleanup]';
GO
