SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- ### [Object]: TABLE [Maintenance].[ASyncStatus_Jobs]
-- ### [Version]: 2023-07-01 00:00:00                                                         
-- ### [Source]: ??????
-- ### [Hash]: ??????
-- ### [Docs]: https://???.???
-- !!! ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!!!
-- !!! ~~~~~~~~~ NOT OFFICIALLY SUPPORTED BY UIPATH 
----------------------------------------------------------------------------------------------------
IF NOT EXISTS(SELECT 1 FROM sys.tables WHERE name = N'ASyncStatus_Jobs' AND SCHEMA_NAME(schema_id) = N'Maintenance')
BEGIN
    PRINT '  + CREATE TABLE: [Maintenance].[ASyncStatus_Jobs]';

	CREATE TABLE [Maintenance].[ASyncStatus_Jobs](
		[SyncId] [bigint] NOT NULL
		, [IsDeleted] [bit] NOT NULL CONSTRAINT [DF_Maintenance.ASyncStatus_Jobs.IsDeleted] DEFAULT 0
		, [DeletedOnDate] [datetime] NULL
		, [FirstASyncId] [bigint] NULL
		, [LastAsyncId] [bigint] NULL
		, CONSTRAINT [PK_Maintenance.ASyncStatus_Jobs] PRIMARY KEY CLUSTERED ([SyncId] ASC)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
	) ON [PRIMARY]
END
ELSE PRINT '  = Table already exists: [Maintenance].[ASyncStatus_Jobs]';