SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- ### [Object]: TABLE [Maintenance].[Filter_AuditLogs]
-- ### [Version]: 2023-07-01 00:00:00                                                         
-- ### [Source]: ??????
-- ### [Hash]: ??????
-- ### [Docs]: https://???.???
-- !!! ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!!!
-- !!! ~~~~~~~~~ NOT OFFICIALLY SUPPORTED BY UIPATH 
----------------------------------------------------------------------------------------------------
IF NOT EXISTS(SELECT 1 FROM sys.tables WHERE name = N'Filter_AuditLogs' AND SCHEMA_NAME(schema_id) = N'Maintenance')
BEGIN
    PRINT '  + CREATE TABLE: [Maintenance].[Filter_AuditLogs]';
	CREATE TABLE [Maintenance].[Filter_AuditLogs](
		[SyncId] [bigint] NOT NULL,
		[TenantId] [int] NOT NULL,
		[DeleteOnly] [bit] NOT NULL,
		[TargetTimestamp] [datetime] NOT NULL,
		[PreviousTimestamp] [datetime] NOT NULL,
		[IsArchived] [bit] NOT NULL CONSTRAINT [DF_Maintenance.Filter_AuditLogs.IsArchived] DEFAULT 0,
		[CurrentId] [bigint] NULL,
		[TargetId] [bigint] NULL,
		CONSTRAINT [PK_Maintenance.Filter_AuditLogs] PRIMARY KEY CLUSTERED ([SyncId] ASC, [TenantId] ASC)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
		, INDEX [IX_Maintenance.Filter_AuditLogs.LastId] NONCLUSTERED (TenantId, TargetId) WHERE IsArchived = 1 AND [TargetId] IS NOT NULL AND [CurrentId] IS NOT NULL --AND [CurrentId] = [TargetId]
	) ON [PRIMARY]

	ALTER TABLE [Maintenance].[Filter_AuditLogs]  WITH CHECK ADD  CONSTRAINT [FK_Maintenance.Filter_AuditLogs.Sync_AuditLogs] FOREIGN KEY([SyncId])
	REFERENCES [Maintenance].[Sync_AuditLogs] ([Id])

	ALTER TABLE [Maintenance].[Filter_AuditLogs] CHECK CONSTRAINT [FK_Maintenance.Filter_AuditLogs.Sync_AuditLogs]
END
ELSE PRINT '  = Table already exists: [Maintenance].[Filter_AuditLogs]';
GO
