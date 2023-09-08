SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- ### [Object]: TABLE [Maintenance].[Sync_AuditLogs]
-- ### [Version]: 2023-07-01 00:00:00                                                         
-- ### [Source]: ??????
-- ### [Hash]: ??????
-- ### [Docs]: https://???.???
-- !!! ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!!!
-- !!! ~~~~~~~~~ NOT OFFICIALLY SUPPORTED BY UIPATH 
----------------------------------------------------------------------------------------------------
IF NOT EXISTS(SELECT 1 FROM sys.tables WHERE name = N'Sync_AuditLogs' AND SCHEMA_NAME(schema_id) = N'Maintenance')
BEGIN
    PRINT '  + CREATE TABLE: [Maintenance].[Sync_AuditLogs]';

	CREATE TABLE [Maintenance].[Sync_AuditLogs](
		[Id] [bigint] IDENTITY(0,1) NOT NULL
		, [ArchiveId] [bigint] NOT NULL
		, [DeleteAfterDatetime] [datetime] NOT NULL
		, [IsArchived] [bit] NOT NULL CONSTRAINT [DF_Maintenance.Sync_AuditLogs.IsArchived] DEFAULT 0
		, [IsDeleted] [bit] NOT NULL CONSTRAINT [DF_Maintenance.Sync_AuditLogs.IsDeleted] DEFAULT 0
		, [DeletedOnDate] [datetime] NULL
		, [IsSynced] [bit] NOT NULL CONSTRAINT [DF_Maintenance.Sync_AuditLogs.IsSynced] DEFAULT 0
		, [SyncedOnDate] [datetime] NULL
		, [RowcountDeleted] [bigint] NOT NULL CONSTRAINT [DF_Maintenance.Sync_AuditLogs.RowcountDeleted] DEFAULT 0
		, [FirstASyncId] [bigint] NULL
		, [LastAsyncId] [bigint] NULL
		, [CountASyncIds] [bigint] NULL
		, CONSTRAINT [PK_Maintenance.Sync_AuditLogs] PRIMARY KEY CLUSTERED ([Id] ASC)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
		, INDEX [IX_Maintenance.Sync_AuditLogs.NotDeleted] (DeleteAfterDatetime) WHERE [Id] < 0
	) ON [PRIMARY]

	CREATE UNIQUE NONCLUSTERED INDEX [IX_Maintenance.Sync_AuditLogs.NotDeleted] ON [Maintenance].[Sync_AuditLogs] (DeleteAfterDatetime) INCLUDE ([Id], [FirstASyncId], [LastAsyncId],  [CountASyncIds], [IsDeleted], [IsSynced]) 
		WHERE CountASyncIds > 0 AND IsArchived = 1 AND [IsDeleted] <> 1 WITH ( DROP_EXISTING = ON );

	ALTER TABLE [Maintenance].[Sync_AuditLogs]  WITH CHECK ADD  CONSTRAINT [FK_Maintenance.Sync_AuditLogs.Archive_AuditLogs] FOREIGN KEY([ArchiveId])
	REFERENCES [Maintenance].[Archive_AuditLogs] ([Id])
	ON DELETE CASCADE

	ALTER TABLE [Maintenance].[Sync_AuditLogs] CHECK CONSTRAINT [FK_Maintenance.Sync_AuditLogs.Archive_AuditLogs]
END
ELSE PRINT '  = Table already exists: [Maintenance].[Sync_AuditLogs]';
GO
