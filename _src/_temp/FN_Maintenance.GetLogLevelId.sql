SET NOCOUNT ON
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[GetLogLevelId]') AND type = N'FN')
BEGIN
    PRINT ' + Create Function [Maintenance].[GetLogLevelId]'
    EXEC sp_executesql N'CREATE FUNCTION [Maintenance].[GetLogLevelId]() RETURNS bit AS BEGIN RETURN (NULL) END';
END
ELSE PRINT ' = Function [Maintenance].[GetLogLevelId] already exists';
GO

PRINT ' ~ Update Function [Maintenance].[GetLogLevelId]';
GO

----------------------------------------------------------------------------------------------------
-- Returns the Id of the input level Id or Name or NULL if the value doesn't exists
--   Intput @level = text value with evel numeric id or level name
--   Output = level Id (int) 
----------------------------------------------------------------------------------------------------
ALTER FUNCTION [Maintenance].[GetLogLevelId](@level nvarchar(max)) RETURNS INT
AS 
BEGIN
    DECLARE @Id int;

    SELECT @Id = CAST(Id AS int)
    FROM [Maintenance].[LogLevels]()
    WHERE Id = TRY_PARSE(@level AS int) OR Level = LTRIM(RTRIM(@level));

    -- Return level Id when @level is matched with Id or Name and NULL otherwise
    RETURN(@Id);
END
GO
