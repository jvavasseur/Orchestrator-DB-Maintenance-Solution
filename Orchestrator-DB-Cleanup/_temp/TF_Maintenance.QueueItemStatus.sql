SET NOCOUNT ON
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[QueueItemStatus]') AND type = N'TF')
BEGIN
    PRINT ' + Create Function [Maintenance].[QueueItemStatus]'
    EXEC sp_executesql N'CREATE FUNCTION [Maintenance].[QueueItemStatus]() RETURNS @output TABLE(Id int) AS BEGIN RETURN; END';
END
ELSE PRINT ' = Function [Maintenance].[QueueItemStatus] already exists';
GO

PRINT ' ~ Update Function [Maintenance].[QueueItemStatus]';
GO

----------------------------------------------------------------------------------------------------
-- Returns a table with all valid queue item' Ids and Names
--   Input = none
--   Output = Log Level table with Id and Status name
----------------------------------------------------------------------------------------------------
ALTER FUNCTION [Maintenance].[QueueItemStatus]()
 RETURNS @status TABLE(Id int PRIMARY KEY, Status nvarchar(20))
AS 
BEGIN
    INSERT INTO @status(Id, Status)
    SELECT Id, Status
    FROM (VALUES(0, 'Trace'), (1, 'Debug'), (2, 'Info'), (3, 'Warn'), (4, 'Error'), (5, 'Fatal')) AS Status(Id, Status);
    
    RETURN;
END
GO
