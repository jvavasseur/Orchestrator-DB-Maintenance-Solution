SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- ### [Object]: TABLE [Maintenance].[Messages]
-- ### [Version]: 2020-10-01 00:00:00                                                         
-- ### [Source]: ??????
-- ### [Hash]: ??????
-- ### [Docs]: https://???.???
----------------------------------------------------------------------------------------------------
IF NOT EXISTS(SELECT 1 FROM sys.tables WHERE name = N'Messages' AND SCHEMA_NAME(schema_id) = N'Maintenance')
BEGIN
    CREATE TABLE [Maintenance].[Messages]
    (
        [Id] bigint IDENTITY(0, 1) CONSTRAINT [PK_Maintenance.Messages] PRIMARY KEY CLUSTERED(Id)
        , [RunId] int NOT NULL CONSTRAINT [FK_RunId] FOREIGN KEY(RunId) REFERENCES [Maintenance].[Runs](Id) ON DELETE CASCADE
        , [Date] datetime2 NOT NULL CONSTRAINT CK_Date DEFAULT SYSDATETIME()
        , [Procedure] nvarchar(max) NOT NULL
        , [Message] nvarchar(max) NOT NULL
        , [Severity] tinyint NOT NULL
        , [State] tinyint NOT NULL
        , [Number] int
        , [Line] int
    );
    PRINT '  + TABLE CREATED: [Maintenance].[Messages]';
END
ELSE
BEGIN
    PRINT '  = TABLE [Maintenance].[Messages] already exists' 

    -- Update existing sysname column to nvarchar(max)
    IF EXISTS( SELECT col.name FROM sys.tables tbl 
        INNER JOIN sys.columns col ON tbl.object_id = col.object_id
        WHERE tbl.name = N'Messages' AND SCHEMA_NAME(tbl.schema_id) = N'Maintenance' AND col.name = N'Procedure' AND col.system_type_id = 231 AND col.max_length <> -1
    )
    BEGIN
        PRINT '  ~ UPDATE TABLE [Maintenance].[Messages] COLUMN: [Procedure] nvarchar(max) NOT NULL' 
        ALTER TABLE [Maintenance].[Messages] ALTER COLUMN [Procedure] nvarchar(max) NOT NULL;
    END
END
GO
