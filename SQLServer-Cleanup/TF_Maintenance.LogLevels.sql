SET NOCOUNT ON
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[LogLevels]') AND type = N'TF')
BEGIN
    PRINT ' + Create Function [Maintenance].[LogLevels]'
    EXEC sp_executesql N'CREATE FUNCTION [Maintenance].[LogLevels]() RETURNS @output TABLE(Id int) AS BEGIN RETURN; END';
END
ELSE PRINT ' = Function [Maintenance].[LogLevels] already exists';
GO

PRINT ' ~ Update Function [Maintenance].[LogLevels]';
GO

----------------------------------------------------------------------------------------------------
-- Returns a table with all valid log level' Ids and Names
--   Input = none
--   Output = Log Level table with Id and Level name
----------------------------------------------------------------------------------------------------
ALTER FUNCTION [Maintenance].[LogLevels]()
 RETURNS @Levels TABLE(Id int PRIMARY KEY, Level nvarchar(20))
AS 
BEGIN
    INSERT INTO @Levels(Id, Level)
    SELECT Id, Level
    FROM (VALUES(0, 'Trace'), (1, 'Debug'), (2, 'Info'), (3, 'Warn'), (4, 'Error'), (5, 'Fatal')) AS Levels(Id, Level);
    
    RETURN;
END
GO
