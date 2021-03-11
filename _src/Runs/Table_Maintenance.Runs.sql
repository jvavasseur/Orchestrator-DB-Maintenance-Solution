SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- TABLE [Maintenance].[Runs]
----------------------------------------------------------------------------------------------------
IF NOT EXISTS(SELECT 1 FROM sys.tables WHERE name = N'Runs' AND SCHEMA_NAME(schema_id) = N'Maintenance')
BEGIN
    CREATE TABLE [Maintenance].[Runs]
    (
        Id int IDENTITY(0, 1) CONSTRAINT [PK_Maintenance.Run] PRIMARY KEY CLUSTERED(Id)
        , [Type] nvarchar(128) NOT NULL
        , [info] nvarchar(max)
        , [StartTime] datetime2 NOT NULL CONSTRAINT df_StartTime DEFAULT SYSDATETIME()
        , [EndDate] datetime2
        , [ErrorStatus] tinyint
    );
    PRINT '  + TABLE CREATED: [Maintenance].[Runs]';
END
ELSE
BEGIN
    PRINT '  = TABLE [Maintenance].[Runs] already exists' 

    IF EXISTS( SELECT col.name FROM sys.tables tbl 
        INNER JOIN sys.columns col ON tbl.object_id = col.object_id
        WHERE tbl.name = N'Runs' AND SCHEMA_NAME(tbl.schema_id) = N'Maintenance' AND col.name = N'Type' AND col.system_type_id = 231 AND col.user_type_id <> 231
    )
    BEGIN
        PRINT '  ~ UPDATE TABLE [Maintenance].[Runs] COLUMN: [Type] nvarchar(128) NOT NULL' 
        ALTER TABLE [Maintenance].[Runs] ALTER COLUMN [Type] nvarchar(128) NOT NULL;
    END

END
GO
