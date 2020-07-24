SET NOCOUNT ON
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Diagnostic')
BEGIN 
	PRINT ' + Create Schema [Diagnostic]';
	EXEC sp_executesql N'CREATE SCHEMA [Diagnostic]';
END
ELSE PRINT ' = Schema already exists: [Diagnostic]';
GO
