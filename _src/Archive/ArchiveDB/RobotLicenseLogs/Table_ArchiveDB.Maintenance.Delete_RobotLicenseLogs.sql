SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- ### [Object]: TABLE [Maintenance].[Delete_RobotLicenseLogs]
-- ### [Version]: 2023-07-01 00:00:00                                                         
-- ### [Source]: ??????
-- ### [Hash]: ??????
-- ### [Docs]: https://???.???
-- !!! ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!!!
-- !!! ~~~~~~~~~ NOT OFFICIALLY SUPPORTED BY UIPATH 
----------------------------------------------------------------------------------------------------
IF NOT EXISTS(SELECT 1 FROM sys.tables WHERE name = N'Delete_RobotLicenseLogs' AND SCHEMA_NAME(schema_id) = N'Maintenance')
BEGIN
    PRINT '  + CREATE TABLE: [Maintenance].[Delete_RobotLicenseLogs]';
	CREATE TABLE [Maintenance].[Delete_RobotLicenseLogs](
		[SyncId] [bigint] NOT NULL
		, [Id] [bigint] NOT NULL
		, CONSTRAINT [PK_Delete_RobotLicenseLogs] PRIMARY KEY CLUSTERED ([SyncId] ASC, [Id] ASC)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
		, INDEX [UX_Maintenance.Delete_RobotLicenseLogs.Id] UNIQUE NONCLUSTERED (Id)		
	) ON [PRIMARY]

	ALTER TABLE [Maintenance].[Delete_RobotLicenseLogs]  WITH CHECK ADD  CONSTRAINT [FK_Maintenance.Delete_RobotLicenseLogs.Sync_RobotLicenseLogs] FOREIGN KEY([SyncId])
	REFERENCES [Maintenance].[Sync_RobotLicenseLogs] ([Id])
	
	ALTER TABLE [Maintenance].[Delete_RobotLicenseLogs] CHECK CONSTRAINT [FK_Maintenance.Delete_RobotLicenseLogs.Sync_RobotLicenseLogs]
END
ELSE PRINT '  = Table already exists: [Maintenance].[Delete_RobotLicenseLogs]';
GO
