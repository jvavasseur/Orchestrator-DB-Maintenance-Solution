SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- ### [Object]: TABLE [Maintenance].[Archive_Logs]
-- ### [Version]: 2023-07-01 00:00:00                                                         
-- ### [Source]: ??????
-- ### [Hash]: ??????
-- ### [Docs]: https://???.???
-- !!! ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!!!
-- !!! ~~~~~~~~~ NOT OFFICIALLY SUPPORTED BY UIPATH 
----------------------------------------------------------------------------------------------------
IF NOT EXISTS(SELECT 1 FROM sys.tables WHERE name = N'Archive_Logs' AND SCHEMA_NAME(schema_id) = N'Maintenance')
BEGIN
    PRINT '  + CREATE TABLE: [Maintenance].[Archive_Logs]';
	CREATE TABLE [Maintenance].[Archive_Logs](
		[Id] [bigint] IDENTITY(0,1) NOT NULL
		, [ParentArchiveId] [bigint]
		, [CurrentRunId] [bigint] NULL
		, [PreviousRunIds] [nvarchar](MAX) NULL
		, [Name] [nvarchar](100) NULL
		-- Settings
		, [Definition] [nvarchar](max) NOT NULL
		, [ArchiveTriggerTime] [datetime] NOT NULL
	    , [ArchiveAfterHours] smallint NULL
		, [DeleteDelayHours] smallint NULL
		, [TargetId] [bigint] NULL
		, [TargetTimestamp] [datetime] NULL
		, [CurrentId] [bigint] NULL
		, [RepeatArchive] [bit] NOT NULL CONSTRAINT [DF_Maintenance.Archive_Logs.RepeatArchive] DEFAULT 0
		, [RepeatOffsetHours] [smallint] NULL CONSTRAINT [DF_Maintenance.Archive_Logs.RepeatOffsetHours] CHECK (RepeatOffsetHours IS NULL OR RepeatOffsetHours > 0)
		, [RepeatUntil] [datetime] NULL --CONSTRAINT [DF_Maintenance.Archive_Logs.AddNextArchives] DEFAULT 0
		-- Status
		, [CreationDate] [datetime] NOT NULL CONSTRAINT [DF_Maintenance.Archive_Logs.CreationDate] DEFAULT SYSDATETIME()
		, [IsDryRun] [bit] NOT NULL CONSTRAINT [DF_Maintenance.Archive_Logs.IsDryRun] DEFAULT 0
		, [IsSuccess] [bit] NOT NULL CONSTRAINT [DF_Maintenance.Archive_Logs.IsSuccess] DEFAULT 0
		, [IsError] [bit] NOT NULL CONSTRAINT [DF_Maintenance.Archive_Logs.IsError] DEFAULT 0
		, [IsCanceled] [bit] NOT NULL CONSTRAINT [DF_Maintenance.Archive_Logs.IsCanceled] DEFAULT 0
		, [Message] nvarchar(MAX) NULL
		, [CountValidFilters] int NULL
		, [CountDuplicateFilters] int NULL
		-- Execution
		, [IsArchived] [bit] NOT NULL CONSTRAINT [DF_Maintenance.Archive_Logs.IsArchived] DEFAULT 0
		, [ArchivedOnDate] [datetime] NULL
		, [IsDeleted] [bit] NOT NULL CONSTRAINT [DF_Maintenance.Archive_Logs.IsDeleted] DEFAULT 0
		, [DeletedOnDate] [datetime] NULL
		, [IsFinished] [bit] NOT NULL CONSTRAINT [DF_Maintenance.Archive_Logs.IsFinished] DEFAULT 0
		, [FinishedOnDate] [datetime] NULL
		, [ToDo] AS IIF(IsArchived <> 1 AND IsFinished <> 1 AND IsDryRun <> 1 AND IsError <> 1, 1, 0)
		, CONSTRAINT [PK_Maintenance.Archive_Logs] PRIMARY KEY CLUSTERED ([Id] ASC)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
	) ON [PRIMARY]
END
ELSE PRINT '  = Table already exists: [Maintenance].[Archive_Logs]';
GO
