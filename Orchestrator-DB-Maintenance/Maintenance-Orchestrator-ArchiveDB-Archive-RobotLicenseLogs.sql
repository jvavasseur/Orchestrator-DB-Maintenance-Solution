SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- ### [Object]: Schema [Maintenance]
-- ### [Version]: 2022-11-29T17:14:37+01:00
-- ### [Source]: _src/Schemas/Schema_Maintenance.sql
-- ### [Hash]: b13f81b [SHA256-DDF08A6921E64960697C69ED645F0AF2E4CCE3595481E1BD606509755A18F0FE]
-- ### [Docs]: https://???.???
----------------------------------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Maintenance')
BEGIN 
	PRINT ' + Create Schema [Maintenance]';
	EXEC sp_executesql N'CREATE SCHEMA [Maintenance]';
END
ELSE PRINT ' = Schema already exists: [Maintenance]';
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- ### [Object]: TABLE [Maintenance].[Runs]
-- ### [Version]: 2022-02-11T14:24:45+01:00
-- ### [Source]: _src/Runs/Table_Maintenance.Runs.sql
-- ### [Hash]: 977a791 [SHA256-8F55CD3EB205659CB8F4A11E5D5ED5A12440E40D22A93B18E9289EC368FAF640]
-- ### [Docs]: https://???.???
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

    -- Update existing sysname column to nvarchar(max)
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

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- ### [Object]: TABLE [Maintenance].[Messages]
-- ### [Version]: 2022-02-11T14:24:45+01:00
-- ### [Source]: _src/Runs/Table_Maintenance.Messages.sql
-- ### [Hash]: 977a791 [SHA256-E8CFF249EA3841977F65C278341B910301AACCE921684E10FEDC2F3E740E90C9]
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

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- PROCEDURE [Maintenance].[AddRunMessage]
----------------------------------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[AddRunMessage]') AND type in (N'P'))
BEGIN
    EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [Maintenance].[AddRunMessage] AS'
    PRINT '  + PROCEDURE CREATED: [Maintenance].[AddRunMessage]';
END
ELSE PRINT '  = PROCEDURE [Maintenance].[AddRunMessage] already exists' 
GO

ALTER PROCEDURE [Maintenance].[AddRunMessage]
----------------------------------------------------------------------------------------------------
-- ### [Object]: PROCEDURE [Maintenance].[AddRunMessage]
-- ### [Version]: 2023-01-31T12:58:44+00:00
-- ### [Source]: _src/Runs/Procedure_Maintenance.AddRunMessage.sql
-- ### [Hash]: d0ca361 [SHA256-AB8A92454EE6D7FD1A73DF40513F33C0DD9DDE342C9D900F558E46A2E3D91BAB]
-- ### [Docs]: https://???.???
----------------------------------------------------------------------------------------------------
    @RunId int
	, @Procedure nvarchar(max)
	, @Message nvarchar(max)
	, @Severity tinyint
    , @State tinyint
	, @Number int = NULL
	, @Line int = NULL
    , @VerboseLevel tinyint
	, @LogToTable bit
    , @RaiseError bit = 1
    , @MessagesStack xml = NULL OUTPUT
AS
BEGIN
    SET ARITHABORT ON;
    SET NOCOUNT ON;
    SET NUMERIC_ROUNDABORT OFF;

    BEGIN TRY
        DECLARE @ERROR_NUMBER INT, @ERROR_SEVERITY INT, @ERROR_STATE INT, @ERROR_PROCEDURE NVARCHAR(126), @ERROR_LINE INT, @ERROR_MESSAGE NVARCHAR(2048) ;
        DECLARE @count int;
        DECLARE @date datetime;
        DECLARE @log xml;
        /*IF @@TRANCOUNT <> 0 
        BEGIN
            RAISERROR(N'Can''t run when the transaction count is bigger than 0.', 16, 1);
            SET @RunId = NULL;
        END*/
        SET @VerboseLevel = ISNULL(@VerboseLevel, 10);
        SET @Procedure = ISNULL(@Procedure, QUOTENAME(COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?')) + N'.' + QUOTENAME(COALESCE(OBJECT_NAME(@@PROCID), N'?')));
        SET @date = SYSDATETIME();
        SET @MessagesStack = COALESCE(@MessagesStack, N'<messages></messages>');
        SET @Message = COALESCE(@Message, N'');

        IF @LogToTable = 1 
        BEGIN
            INSERT INTO [Maintenance].[Messages](RunId, [Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
            SELECT @RunId, @date, @Procedure, @Message, @Severity, @state, @Number, @Line WHERE @RunId IS NOT NULL;
        END

        SET @log = (  SELECT * FROM (SELECT SYSDATETIME(), @Procedure, @Message, @Severity, @state, @Number, @Line) x([Date], [Procedure], [Message], [Severity], [State], [Number], [Line]) FOR XML PATH('message') )
        SET @MessagesStack.modify('insert sql:variable("@log") as last into (/messages)[1]')

        IF @Severity >= @VerboseLevel 
        BEGIN
            IF @Severity < 10 SET @Severity = 10;
            IF @Severity > 10
            BEGIN
                IF @RaiseError <> 1 SET @Severity = 10;
                --ELSE RAISERROR(@Message, 10, @State);
            END
            RAISERROR(@Message, @Severity, @State);
        END
    END TRY
    BEGIN CATCH
        SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
        --IF @@TRANCOUNT > 0 ROLLBACK;
        IF @Message = @ERROR_MESSAGE RAISERROR(@ERROR_MESSAGE, @ERROR_SEVERITY, @ERROR_STATE);
        ELSE THROW;
    END CATCH

    RETURN 0
END

GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- PROCEDURE [Maintenance].[DeleteRuns]
----------------------------------------------------------------------------------------------------

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[DeleteRuns]') AND type in (N'P'))
BEGIN
    EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [Maintenance].[DeleteRuns] AS'
    PRINT '  + PROCEDURE CREATED: [Maintenance].[DeleteRuns]';
END
ELSE PRINT '  = PROCEDURE [Maintenance].[DeleteRuns] already exists' 
GO

ALTER PROCEDURE [Maintenance].[DeleteRuns]
----------------------------------------------------------------------------------------------------
-- ### [Object]: PROCEDURE [Maintenance].[DeleteRuns]
-- ### [Version]: 2023-01-31T12:58:44+00:00
-- ### [Source]: _src/Runs/Procedure_Maintenance.DeleteRuns.sql
-- ### [Hash]: d0ca361 [SHA256-C80C680B2B24A52204AA835431EE11A3CBB83FB7C32FA62BFDA5E114FD1A4692]
-- ### [Docs]: https://???.???
----------------------------------------------------------------------------------------------------
    @CleanupAfterDays tinyint = 30
    , @RunId int = NULL
    , @Procedure nvarchar(max) = NULL
    , @MessagesStack xml = NULL OUTPUT
AS
BEGIN
    SET ARITHABORT ON;
    SET NOCOUNT ON;
    SET NUMERIC_ROUNDABORT OFF;

    BEGIN TRY
        DECLARE @ERROR_NUMBER INT, @ERROR_SEVERITY INT, @ERROR_STATE INT, @ERROR_PROCEDURE NVARCHAR(126), @ERROR_LINE INT, @ERROR_MESSAGE NVARCHAR(2048) ;
        DECLARE @count int;
    	DECLARE @localRunId int;
        DECLARE @message nvarchar(max);
    
        IF @@TRANCOUNT <> 0 
        BEGIN
            RAISERROR(N'Can''t run when the transaction count is bigger than 0.', 16, 1);
        END
        SET @CleanupAfterDays = ISNULL(ABS(@CleanupAfterDays), 30);
        SET @Procedure = ISNULL(@Procedure, QUOTENAME(COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?')) + N'.' + QUOTENAME(COALESCE(OBJECT_NAME(@@PROCID), N'?')));
        
        BEGIN TRY
			SET @localRunId = @RunId
            IF @localRunId IS NULL 
            BEGIN
                INSERT INTO [Maintenance].[Runs]([Type], [Info], [StartTime]) VALUES
                    (N'Cleanup Runs', N'PROCEDURE ' + @Procedure, SYSDATETIME());
                SELECT @localRunId = @@IDENTITY;
            END
/*
            INSERT INTO [Maintenance].[Messages](RunId, [Procedure], [Message], [Severity], [State])
                VALUES(@localRunId, @Procedure, N'Runs cleanup started', 10, 1)
                    , (@localRunId, @Procedure, N'Keep Runs from past days: ' + CAST(@CleanupAfterDays AS nvarchar(10)), 10, 1)
                    , (@localRunId, @Procedure, N'Delete Runs before: ' + CONVERT(nvarchar(20), DATEADD(DAY, - @CleanupAfterDays, SYSDATETIME()), 121), 10, 1)
            ;*/

            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @Procedure, @Message = N'Runs cleanup started', @Severity = 10, @State = 1, @VerboseLevel = 10, @LogToTable = 1, @MessagesStack = @MessagesStack OUTPUT;
            SET @message = N'Keep Runs from past days: ' + CAST(@CleanupAfterDays AS nvarchar(10));
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @Procedure, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = 10, @LogToTable = 1, @MessagesStack = @MessagesStack OUTPUT;
            SET @message =  N'Delete Runs before: ' + CONVERT(nvarchar(20), DATEADD(DAY, - @CleanupAfterDays, SYSDATETIME()), 121);
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @Procedure, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = 10, @LogToTable = 1, @MessagesStack = @MessagesStack OUTPUT;

            BEGIN TRAN

            DELETE FROM [Maintenance].[Runs] WHERE [StartTime] < DATEADD(DAY, - @CleanupAfterDays, SYSDATETIME()) AND [Id] <> @localRunId;
            SET @count = @@ROWCOUNT;

            IF @@TRANCOUNT > 0 COMMIT;
/*
            INSERT INTO [Maintenance].[Messages](RunId, [Procedure], [Message], [Severity], [State]) VALUES
                (@localRunId, @Procedure, N'Runs deleted: ' + CAST(@count AS nvarchar(10)), 10, 1)
                , (@localRunId, @Procedure, N'Runs Cleanup Finished', 10, 1);
*/
            SET @message = N'Runs deleted: ' + CAST(@count AS nvarchar(10));
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @Procedure, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = 10, @LogToTable = 1, @MessagesStack = @MessagesStack OUTPUT;
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @Procedure, @Message = N'Runs Cleanup Finished', @Severity = 10, @State = 1, @VerboseLevel = 10, @LogToTable = 1, @MessagesStack = @MessagesStack OUTPUT;

            UPDATE [Maintenance].[Runs] SET [EndDate] = SYSDATETIME(), [ErrorStatus] = 0 WHERE Id = @localRunId AND @RunId IS NULL;
        END TRY
        BEGIN CATCH
            SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
            IF @@TRANCOUNT > 0 ROLLBACK;

            IF @localRunId IS NOT NULL
            BEGIN
                SET @message = N'Error: ' + @ERROR_MESSAGE;
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @Procedure, @Message = @message, @Severity = @ERROR_SEVERITY, @State = @ERROR_STATE, @VerboseLevel = 10, @LogToTable = 1, @RaiseError = 0, @MessagesStack = @MessagesStack OUTPUT;
            END; 

            THROW;

        END CATCH
    END TRY
    BEGIN CATCH
        SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
        IF @@TRANCOUNT > 0 ROLLBACK;

        IF @localRunId IS NOT NULL 
        BEGIN
            -- Save Unknown Error
--            INSERT INTO [Maintenance].[Messages](RunId, [Procedure], [Message], [Severity], [State]) VALUES
  --              (@localRunId, @Procedure, N'Runs Cleanup Finished with error(s)', 16, 1);
                
            SET @message = N'Runs Cleanup Finished with error(s)';
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @Procedure, @Message = @message, @Severity = @ERROR_SEVERITY, @State = @ERROR_STATE, @VerboseLevel = 10, @LogToTable = 1, @RaiseError = 0, @MessagesStack = @MessagesStack OUTPUT;

            UPDATE [Maintenance].[Runs] SET [EndDate] = SYSDATETIME(), [ErrorStatus] = 4 WHERE Id = @localRunId;
        END;

        THROW;
    END CATCH
END

GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- ### [Object]: TABLE [Maintenance].[Archive_RobotLicenseLogs]
-- ### [Version]: 2023-09-08T11:08:50+02:00
-- ### [Source]: _src/Archive/ArchiveDB/RobotLicenseLogs/Table_ArchiveDB.Maintenance.Archive_RobotLicenseLogs.sql
-- ### [Hash]: c3792cf [SHA256-AFC27CD7E7C5B83677FBE008345209792BA2177E8258140B3068B12275F51F54]
-- ### [Docs]: https://???.???
-- !!! ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!!!
-- !!! ~~~~~~~~~ NOT OFFICIALLY SUPPORTED BY UIPATH 
----------------------------------------------------------------------------------------------------
IF NOT EXISTS(SELECT 1 FROM sys.tables WHERE name = N'Archive_RobotLicenseLogs' AND SCHEMA_NAME(schema_id) = N'Maintenance')
BEGIN
    PRINT '  + CREATE TABLE: [Maintenance].[Archive_RobotLicenseLogs]';
	CREATE TABLE [Maintenance].[Archive_RobotLicenseLogs](
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
		, [RepeatArchive] [bit] NOT NULL CONSTRAINT [DF_Maintenance.Archive_RobotLicenseLogs.RepeatArchive] DEFAULT 0
		, [RepeatOffsetHours] [smallint] NULL CONSTRAINT [DF_Maintenance.Archive_RobotLicenseLogs.RepeatOffsetHours] CHECK (RepeatOffsetHours IS NULL OR RepeatOffsetHours > 0)
		, [RepeatUntil] [datetime] NULL --CONSTRAINT [DF_Maintenance.Archive_RobotLicenseLogs.AddNextArchives] DEFAULT 0
		-- Status
		, [CreationDate] [datetime] NOT NULL CONSTRAINT [DF_Maintenance.Archive_RobotLicenseLogs.CreationDate] DEFAULT SYSDATETIME()
		, [IsDryRun] [bit] NOT NULL CONSTRAINT [DF_Maintenance.Archive_RobotLicenseLogs.IsDryRun] DEFAULT 0
		, [IsSuccess] [bit] NOT NULL CONSTRAINT [DF_Maintenance.Archive_RobotLicenseLogs.IsSuccess] DEFAULT 0
		, [IsError] [bit] NOT NULL CONSTRAINT [DF_Maintenance.Archive_RobotLicenseLogs.IsError] DEFAULT 0
		, [IsCanceled] [bit] NOT NULL CONSTRAINT [DF_Maintenance.Archive_RobotLicenseLogs.IsCanceled] DEFAULT 0
		, [Message] nvarchar(MAX) NULL
		, [CountValidFilters] int NULL
		, [CountDuplicateFilters] int NULL
		-- Execution
		, [IsArchived] [bit] NOT NULL CONSTRAINT [DF_Maintenance.Archive_RobotLicenseLogs.IsArchived] DEFAULT 0
		, [ArchivedOnDate] [datetime] NULL
		, [IsDeleted] [bit] NOT NULL CONSTRAINT [DF_Maintenance.Archive_RobotLicenseLogs.IsDeleted] DEFAULT 0
		, [DeletedOnDate] [datetime] NULL
		, [IsFinished] [bit] NOT NULL CONSTRAINT [DF_Maintenance.Archive_RobotLicenseLogs.IsFinished] DEFAULT 0
		, [FinishedOnDate] [datetime] NULL
		, [ToDo] AS IIF(IsArchived <> 1 AND IsFinished <> 1 AND IsDryRun <> 1 AND IsError <> 1, 1, 0)
		, CONSTRAINT [PK_Maintenance.Archive_RobotLicenseLogs] PRIMARY KEY CLUSTERED ([Id] ASC)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
	) ON [PRIMARY]
END
ELSE PRINT '  = Table already exists: [Maintenance].[Archive_RobotLicenseLogs]';
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- ### [Object]: TABLE [Maintenance].[Sync_RobotLicenseLogs]
-- ### [Version]: 2023-09-08T11:43:56+02:00
-- ### [Source]: _src/Archive/ArchiveDB/RobotLicenseLogs/Table_ArchiveDB.Maintenance.Sync_RobotLicenseLogs.sql
-- ### [Hash]: 82d9e7c [SHA256-0644E4F71D9A3517D02F8A30A7AD784A1EACBEFF9E9C1D45952E2A385AB30342]
-- ### [Docs]: https://???.???
-- !!! ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!!!
-- !!! ~~~~~~~~~ NOT OFFICIALLY SUPPORTED BY UIPATH 
----------------------------------------------------------------------------------------------------
IF NOT EXISTS(SELECT 1 FROM sys.tables WHERE name = N'Sync_RobotLicenseLogs' AND SCHEMA_NAME(schema_id) = N'Maintenance')
BEGIN
    PRINT '  + CREATE TABLE: [Maintenance].[Sync_RobotLicenseLogs]';

	CREATE TABLE [Maintenance].[Sync_RobotLicenseLogs](
		[Id] [bigint] IDENTITY(0,1) NOT NULL
		, [ArchiveId] [bigint] NOT NULL
		, [DeleteAfterDatetime] [datetime] NOT NULL
		, [IsArchived] [bit] NOT NULL CONSTRAINT [DF_Maintenance.Sync_RobotLicenseLogs.IsArchived] DEFAULT 0
		, [IsDeleted] [bit] NOT NULL CONSTRAINT [DF_Maintenance.Sync_RobotLicenseLogs.IsDeleted] DEFAULT 0
		, [DeletedOnDate] [datetime] NULL
		, [IsSynced] [bit] NOT NULL CONSTRAINT [DF_Maintenance.Sync_RobotLicenseLogs.IsSynced] DEFAULT 0
		, [SyncedOnDate] [datetime] NULL
		, [RowcountDeleted] [bigint] NOT NULL CONSTRAINT [DF_Maintenance.Sync_RobotLicenseLogs.RowcountDeleted] DEFAULT 0
		, [FirstASyncId] [bigint] NULL
		, [LastAsyncId] [bigint] NULL
		, [CountASyncIds] [bigint] NULL
		, CONSTRAINT [PK_Maintenance.Sync_RobotLicenseLogs] PRIMARY KEY CLUSTERED ([Id] ASC)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
--		, INDEX [IX_Maintenance.Sync_RobotLicenseLogs.NotSync] (DeleteAfterDatetime) WHERE [Id] < 0
		, INDEX [IX_Maintenance.Sync_RobotLicenseLogs.NotDeleted] (DeleteAfterDatetime) WHERE [Id] < 0
	) ON [PRIMARY]

	CREATE UNIQUE NONCLUSTERED INDEX [IX_Maintenance.Sync_RobotLicenseLogs.NotDeleted] ON [Maintenance].[Sync_RobotLicenseLogs] (DeleteAfterDatetime) INCLUDE ([Id], [FirstASyncId], [LastAsyncId],  [CountASyncIds], [IsDeleted], [IsSynced]) 
		WHERE CountASyncIds > 0 AND IsArchived = 1 AND [IsDeleted] <> 1 WITH ( DROP_EXISTING = ON );
--	CREATE UNIQUE NONCLUSTERED INDEX [IX_Maintenance.Sync_RobotLicenseLogs.NotSync] ON [Maintenance].[Sync_RobotLicenseLogs] (DeleteAfterDatetime) INCLUDE ([FirstASyncId], [LastAsyncId], [Id]) WHERE IsArchived = 1 AND [IsDeleted] = 1 AND IsSynced <> 1 WITH ( DROP_EXISTING = ON );

	ALTER TABLE [Maintenance].[Sync_RobotLicenseLogs]  WITH CHECK ADD  CONSTRAINT [FK_Maintenance.Sync_RobotLicenseLogs.Archive_RobotLicenseLogs] FOREIGN KEY([ArchiveId])
	REFERENCES [Maintenance].[Archive_RobotLicenseLogs] ([Id])
	ON DELETE CASCADE

	ALTER TABLE [Maintenance].[Sync_RobotLicenseLogs] CHECK CONSTRAINT [FK_Maintenance.Sync_RobotLicenseLogs.Archive_RobotLicenseLogs]
END
ELSE PRINT '  = Table already exists: [Maintenance].[Sync_RobotLicenseLogs]';
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- ### [Object]: TABLE [Maintenance].[Filter_RobotLicenseLogs]
-- ### [Version]: 2023-10-06T11:29:36+02:00
-- ### [Source]: _src/Archive/ArchiveDB/RobotLicenseLogs/Table_ArchiveDB.Maintenance.Filter_RobotLicenseLogs.sql
-- ### [Hash]: dc39c27 [SHA256-CF4D5A4FEF0499D2BE86A2B098B54723EA79593AAEC315EE498970FF27676ABE]
-- ### [Docs]: https://???.???
-- !!! ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!!!
-- !!! ~~~~~~~~~ NOT OFFICIALLY SUPPORTED BY UIPATH 
----------------------------------------------------------------------------------------------------
IF NOT EXISTS(SELECT 1 FROM sys.tables WHERE name = N'Filter_RobotLicenseLogs' AND SCHEMA_NAME(schema_id) = N'Maintenance')
BEGIN
    PRINT '  + CREATE TABLE: [Maintenance].[Filter_RobotLicenseLogs]';
	CREATE TABLE [Maintenance].[Filter_RobotLicenseLogs](
		[SyncId] [bigint] NOT NULL,
		[TenantId] [int] NOT NULL,
		[DeleteOnly] [bit] NOT NULL,
		[TargetTimestamp] [datetime] NOT NULL,
		[PreviousTimestamp] [datetime] NOT NULL,
		[IsArchived] [bit] NOT NULL CONSTRAINT [DF_Maintenance.Filter_RobotLicenseLogs.IsArchived] DEFAULT 0,
		[CurrentId] [bigint] NULL,
		[TargetId] [bigint] NULL,
		CONSTRAINT [PK_Maintenance.Filter_RobotLicenseLogs] PRIMARY KEY CLUSTERED ([SyncId] ASC, [TenantId] ASC)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
		, INDEX [IX_Maintenance.Filter_RobotLicenseLogs.LastId] NONCLUSTERED (TenantId, TargetId) WHERE IsArchived = 1 AND [TargetId] IS NOT NULL AND [CurrentId] IS NOT NULL --AND [CurrentId] = [TargetId]
	) ON [PRIMARY]

	ALTER TABLE [Maintenance].[Filter_RobotLicenseLogs]  WITH CHECK ADD  CONSTRAINT [FK_Maintenance.Filter_RobotLicenseLogs.Sync_RobotLicenseLogs] FOREIGN KEY([SyncId])
	REFERENCES [Maintenance].[Sync_RobotLicenseLogs] ([Id])

	ALTER TABLE [Maintenance].[Filter_RobotLicenseLogs] CHECK CONSTRAINT [FK_Maintenance.Filter_RobotLicenseLogs.Sync_RobotLicenseLogs]
END
ELSE PRINT '  = Table already exists: [Maintenance].[Filter_RobotLicenseLogs]';
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- ### [Object]: TABLE [Maintenance].[Delete_RobotLicenseLogs]
-- ### [Version]: 2023-09-08T11:43:56+02:00
-- ### [Source]: _src/Archive/ArchiveDB/RobotLicenseLogs/Table_ArchiveDB.Maintenance.Delete_RobotLicenseLogs.sql
-- ### [Hash]: 82d9e7c [SHA256-826C9393839CD55F293E737D706564DDAA0E1923F4B363C0CA5112D0EDD19176]
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

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- DROP PROCEDURE [Maintenance].[ValidateArchiveObjects]
----------------------------------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[ValidateArchiveObjects]') AND type in (N'P'))
BEGIN
    EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [Maintenance].[ValidateArchiveObjects] AS'
    PRINT '  + CREATE PROCEDURE: [Maintenance].[ValidateArchiveObjects]';
END
ELSE PRINT '  = PROCEDURE [Maintenance].[ValidateArchiveObjects] already exists' 
GO

PRINT '  ~ UPDATE PROCEDURE: [Maintenance].[ValidateArchiveObjects]'
GO

ALTER PROCEDURE [Maintenance].[ValidateArchiveObjects]
----------------------------------------------------------------------------------------------------
-- ### [Object]: PROCEDURE [Maintenance].[ValidateArchiveObjects]
-- ### [Version]: 2023-09-06T14:49:31+02:00
-- ### [Source]: _src/Archive/ArchiveDB/Procedure_ArchiveDB.Maintenance.ValidateArchiveObjects.sql
-- ### [Hash]: 3491f17 [SHA256-5A934DC744E52C6330BE0252700A1250B189727D8548E5C70FD0A71E4EB6ED40]
-- ### [Docs]: https://???.???
-- !!! ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!!!
-- !!! ~~~~~~~~~ NOT OFFICIALLY SUPPORTED BY UIPATH 
-- !!! ~~~~~~~~~ SQL Server >= 2016 SP1
----------------------------------------------------------------------------------------------------
    @SynonymSourceName nvarchar(256)
    , @SynonymSourceSchema nvarchar(256)
    , @SynonymArchiveName nvarchar(256)
    , @SynonymArchiveSchema nvarchar(256)
    , @SynonymASyncStatusName nvarchar(256)
    , @SynonymASyncStatusSchema nvarchar(256)
    , @ClusteredName nvarchar(128)
	, @ArchiveTableFullParts nvarchar(250)
	, @SourceTableFullParts nvarchar(250)
    , @ExcludeColumns nvarchar(MAX) = NULL
    , @IgnoreMissingColumns bit = 0
    , @ASyncStatusTableFullParts nvarchar(256) = NULL
    , @ASyncStatusExpectedColumns nvarchar(MAX) = NULL
    , @CreateOrUpdateSynonyms bit = 0
    , @CreateTable bit = 0
    , @UpdateTable bit = 0
    , @SourceColumns nvarchar(MAX) OUTPUT
    , @IsValid bit = 0 OUTPUT
    , @Messages nvarchar(MAX) OUTPUT
AS
BEGIN
    BEGIN TRY 
        SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
		SET LOCK_TIMEOUT 5000;
        SET ARITHABORT ON;
        SET NOCOUNT ON;
        SET NUMERIC_ROUNDABORT OFF;

        ----------------------------------------------------------------------------------------------------
        -- Source Variables
        ----------------------------------------------------------------------------------------------------
        DECLARE @sourceIsValid bit = 0;
        DECLARE @sourceTable nvarchar(256);
        DECLARE @sourceTable4Parts nvarchar(256);
        DECLARE @paramsSourceTableChecks nvarchar(MAX) = N'@Message nvarchar(MAX) OUTPUT, @Columns nvarchar(MAX) OUTPUT';
        DECLARE @stmtSourceTableChecks nvarchar(MAX) = N'';
        DECLARE @sourceJsonColumns nvarchar(MAX);
        DECLARE @listSourceColumns TABLE(Id int, [name] nvarchar(128), [type] nvarchar(128), max_length smallint, precision tinyint, scale tinyint
            , [datatype] AS ( [type] + 
                    CASE WHEN type IN ('varchar', 'char', 'varbinary', 'binary', 'text') THEN '(' + CASE WHEN max_length = -1 THEN 'MAX' ELSE CAST(max_length AS VARCHAR(5)) END + ')'
                    WHEN type IN ('nvarchar', 'nchar', 'ntext') THEN '(' + CASE WHEN max_length = -1 THEN 'MAX' ELSE CAST(max_length / 2 AS VARCHAR(5)) END + ')'
                    WHEN type IN ('datetime2', 'time2', 'datetimeoffset') THEN '(' + CAST(scale AS VARCHAR(5)) + ')'
                    WHEN type IN ('decimal', 'numeric') THEN '(' + CAST([precision] AS VARCHAR(5)) + ',' + CAST(scale AS VARCHAR(5)) + ')'
                    WHEN type IN ('float') THEN '(' + CAST([precision] AS VARCHAR(5)) + ')'
                    ELSE '' END )
        );
        DECLARE @listExcludeColumns TABLE([key] nvarchar(4000), [value] nvarchar(MAX), [type] int);
        DECLARE @jsonExclude nvarchar(MAX);
        ----------------------------------------------------------------------------------------------------
        -- Archive variables
        ----------------------------------------------------------------------------------------------------
        DECLARE @archiveObjectId bigint;
        DECLARE @archiveTable nvarchar(256);
        DECLARE @archiveSchema nvarchar(128);
        DECLARE @archiveTable2Parts nvarchar(256);
        DECLARE @listArchiveColumns TABLE(Id int, [name] nvarchar(128), [type] nvarchar(128), max_length smallint, precision tinyint, scale tinyint
            , [datatype] AS ( [type] + 
                    CASE WHEN type IN ('varchar', 'char', 'varbinary', 'binary', 'text') THEN '(' + CASE WHEN max_length = -1 THEN 'MAX' ELSE CAST(max_length AS VARCHAR(5)) END + ')'
                    WHEN type IN ('nvarchar', 'nchar', 'ntext') THEN '(' + CASE WHEN max_length = -1 THEN 'MAX' ELSE CAST(max_length / 2 AS VARCHAR(5)) END + ')'
                    WHEN type IN ('datetime2', 'time2', 'datetimeoffset') THEN '(' + CAST(scale AS VARCHAR(5)) + ')'
                    WHEN type IN ('decimal', 'numeric') THEN '(' + CAST([precision] AS VARCHAR(5)) + ',' + CAST(scale AS VARCHAR(5)) + ')'
                    WHEN type IN ('float') THEN '(' + CAST([precision] AS VARCHAR(5)) + ')'
                    ELSE '' END )
        );
        ----------------------------------------------------------------------------------------------------
        -- Sync Status variables
        ----------------------------------------------------------------------------------------------------
        DECLARE @asyncStatusTable nvarchar(256);
        DECLARE @asyncStatusSchema nvarchar(128);
        DECLARE @asyncStatusTable4Parts nvarchar(256);
        DECLARE @paramsASyncStatusTableChecks nvarchar(MAX) = N'@Message nvarchar(MAX) OUTPUT, @Columns nvarchar(MAX) OUTPUT';
        DECLARE @stmtASyncStatusTableChecks nvarchar(MAX);
        DECLARE @asyncStatusJsonColumns nvarchar(MAX);
        DECLARE @expectedASyncStatusColumns nvarchar(MAX)
        ----------------------------------------------------------------------------------------------------
        -- Misc      
        ----------------------------------------------------------------------------------------------------
        DECLARE @sql nvarchar(max);
        ----------------------------------------------------------------------------------------------------      
        -- Message / Error Handling
        ----------------------------------------------------------------------------------------------------
        DECLARE @message nvarchar(MAX);
        DECLARE @ERROR_NUMBER INT, @ERROR_SEVERITY INT, @ERROR_STATE INT, @ERROR_PROCEDURE NVARCHAR(126), @ERROR_LINE INT, @ERROR_MESSAGE NVARCHAR(2048);
        DECLARE @space tinyint = 2;
        DECLARE @tab tinyint = 0;
        DECLARE @json_errors TABLE([id] tinyint NOT NULL, [severity] int NOT NULL, [message] nvarchar(MAX) NOT NULL);

        ----------------------------------------------------------------------------------------------------      
        -- Checks Synonyms and Tables
        ----------------------------------------------------------------------------------------------------
        -- Check Missing Synonym
        BEGIN TRY
            -- Synonym for Source table
            SELECT @sourceTable = ISNULL(LTRIM(RTRIM(@SourceTableFullParts)), N'');
            SELECT @sourceTable4Parts = ISNULL(QUOTENAME(LTRIM(RTRIM(PARSENAME(@sourceTable, 4)))) + N'.', N'') + ISNULL(QUOTENAME(LTRIM(RTRIM(PARSENAME(@sourceTable, 3)))) + N'.', N'') + ISNULL(QUOTENAME(LTRIM(RTRIM(PARSENAME(@sourceTable, 2)))) + N'.', N'') + ISNULL(QUOTENAME(LTRIM(RTRIM(PARSENAME(@sourceTable, 1)))), N'');

            INSERT INTO @json_errors([id], [severity], [message]) 
            SELECT 0, 16, N'ERROR[SS1]: Synonym ' + QUOTENAME(@synonymSourceSchema) + N'.' + QUOTENAME(@synonymSourceName) + N' not found and @SourceTableFullParts not provided' WHERE NOT EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = @synonymSourceName AND schema_id = SCHEMA_ID(@synonymSourceSchema)) AND @sourceTable = N''
            UNION ALL SELECT 1, 16, N'ERROR[SS2]: Synonym ' + QUOTENAME(@synonymSourceSchema) + N'.' + QUOTENAME(@synonymSourceName) + N' refers to an invalid @SourceTableFullParts''s name' WHERE @sourceTable <> N'' AND @sourceTable4Parts = N''
            UNION ALL SELECT 2, 16, N'ERROR[SS3]: Synonym ' + QUOTENAME(@synonymSourceSchema) + N'.' + QUOTENAME(@synonymSourceName) + N' not found and @CreateSynonym not enabled' WHERE NOT EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = @synonymSourceName AND schema_id = SCHEMA_ID(@synonymSourceSchema)) AND @sourceTable <> N'' AND @sourceTable4Parts <> N'' AND @CreateOrUpdateSynonyms <> 1
            UNION ALL SELECT 3, 16, N'ERROR[SS4]: Synonym ' + QUOTENAME(@synonymSourceSchema) + N'.' + QUOTENAME(@synonymSourceName) + N' must refers to a 2 (or more) parts name with at least a schema and table name: [schema_name].[table_name]' WHERE @sourceTable <> N'' AND @sourceTable4Parts <> N'' AND PARSENAME(@sourceTable, 2) IS NULL
            ;
        END TRY
        BEGIN CATCH;
            IF @@TRANCOUNT > 0 ROLLBACK;
            SET @message = N'ERROR[SS0]: error(s) occured while checking Source synonym';
            THROW;
        END CATCH; 

        BEGIN TRY
            -- Synonym for Archive table
            SELECT @archiveTable = ISNULL(LTRIM(RTRIM(@archiveTableFullParts)), N'');
            SELECT @archiveTable2Parts = ISNULL(QUOTENAME(LTRIM(RTRIM(PARSENAME(@archiveTable, 2)))) + N'.', N'') + ISNULL(QUOTENAME(LTRIM(RTRIM(PARSENAME(@archiveTable, 1)))), N'');

            INSERT INTO @json_errors([id], [severity], [message])
            SELECT 11, 16, N'ERROR[AS1]: Synonym ' + QUOTENAME(@synonymArchiveSchema) + N'.' + QUOTENAME(@synonymArchiveName) + N' not found and @ArchiveTableFullParts not provided' WHERE NOT EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = @synonymArchiveName AND schema_id = SCHEMA_ID(@synonymArchiveSchema)) AND  @archiveTable = N''
            UNION ALL SELECT 12, 16, N'ERROR[AS2]: Synonym ' + QUOTENAME(@synonymArchiveSchema) + N'.' + QUOTENAME(@synonymArchiveName) + N' not found and @ArchiveTableFullParts''s name is invalid' WHERE @archiveTable <> N'' AND @archiveTable2Parts = N''
            UNION ALL SELECT 13, 16, N'ERROR[AS3]: Synonym ' + QUOTENAME(@synonymArchiveSchema) + N'.' + QUOTENAME(@synonymArchiveName) + N' not found and @CreateOrUpdateSynonyms not enabled' WHERE  NOT EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = @synonymArchiveName AND schema_id = SCHEMA_ID(@synonymArchiveSchema)) AND @archiveTable <> N'' AND @archiveTable2Parts <> N'' AND @CreateOrUpdateSynonyms <> 1
            UNION ALL SELECT 14, 16, N'ERROR[AS4]: Synonym ' + QUOTENAME(@synonymArchiveSchema) + N'.' + QUOTENAME(@synonymArchiveName) + N' must refers to a 2 parts name with schema and table on the current database: [schema_name].[table_name]' WHERE @archiveTable <> N'' AND @archiveTable2Parts <> N'' AND PARSENAME(@archiveTable, 3) IS NOT NULL
            UNION ALL SELECT 15, 16, N'ERROR[AS5]: Synonym ' + QUOTENAME(@synonymArchiveSchema) + N'.' + QUOTENAME(@synonymArchiveName) + N' must refers to a 2 parts name with schema and table on the current database: [schema_name].[table_name]' WHERE @archiveTable <> N'' AND @archiveTable2Parts <> N'' AND PARSENAME(@archiveTable, 2) IS NULL
            ;
        END TRY
        BEGIN CATCH;
            IF @@TRANCOUNT > 0 ROLLBACK;
            SET @message = N'ERROR[AS0]: error(s) occured while checking Archive synonyms';
            THROW;
        END CATCH; 

        BEGIN TRY
            -- Synonym for ASync Status table
            SELECT @asyncStatusTable = ISNULL(LTRIM(RTRIM(@asyncStatusTableFullParts)), N'');
            SELECT @asyncStatusTable4Parts = ISNULL(QUOTENAME(LTRIM(RTRIM(PARSENAME(@asyncStatusTable, 4)))) + N'.', N'') + ISNULL(QUOTENAME(LTRIM(RTRIM(PARSENAME(@asyncStatusTable, 3)))) + N'.', N'') + ISNULL(QUOTENAME(LTRIM(RTRIM(PARSENAME(@asyncStatusTable, 2)))) + N'.', N'') + ISNULL(QUOTENAME(LTRIM(RTRIM(PARSENAME(@asyncStatusTable, 1)))), N'');

            INSERT INTO @json_errors([id], [severity], [message])
            SELECT 11, 16, N'ERROR[ST1]: Synonym ' + QUOTENAME(@synonymASyncStatusSchema) + N'.' + QUOTENAME(@synonymASyncStatusName) + N' not found and @ASyncStatusTableFullParts not provided' WHERE NOT EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = @synonymASyncStatusName AND schema_id = SCHEMA_ID(@synonymASyncStatusSchema)) AND  @asyncStatusTable = N''
            UNION ALL SELECT 12, 16, N'ERROR[ST2]: Synonym ' + QUOTENAME(@synonymASyncStatusSchema) + N'.' + QUOTENAME(@synonymASyncStatusName) + N' not found and @ASyncStatusTableFullParts''s name is invalid' WHERE @asyncStatusTable <> N'' AND @asyncStatusTable4Parts = N''
            UNION ALL SELECT 13, 16, N'ERROR[ST3]: Synonym ' + QUOTENAME(@synonymASyncStatusSchema) + N'.' + QUOTENAME(@synonymASyncStatusName) + N' not found and @CreateOrUpdateSynonyms not enabled' WHERE  NOT EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = @synonymASyncStatusName AND schema_id = SCHEMA_ID(@synonymASyncStatusSchema)) AND @asyncStatusTable <> N'' AND @asyncStatusTable4Parts <> N'' AND @CreateOrUpdateSynonyms <> 1
            UNION ALL SELECT 3, 16, N'ERROR[ST4]: Synonym ' + QUOTENAME(@synonymASyncStatusSchema) + N'.' + QUOTENAME(@synonymASyncStatusName) + N' must refers to a 2 (or more) parts name with at least a schema and table name: [schema_name].[table_name]' WHERE @asyncStatusTable <> N'' AND @asyncStatusTable4Parts <> N'' AND PARSENAME(@asyncStatusTable, 2) IS NULL
            ;
        END TRY
        BEGIN CATCH;
            IF @@TRANCOUNT > 0 ROLLBACK;
            SET @message = N'ERROR[ST0]: error(s) occured while checking ASyncStatus synonyms';
            THROW;
        END CATCH; 

        -- Check Source Table
        IF NOT EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10)
        BEGIN
            BEGIN TRY
                SELECT @sourceTable4Parts = base_object_name FROM sys.synonyms WHERE [name] = @synonymSourceName AND schema_id = SCHEMA_ID(@synonymSourceSchema) AND (@sourceTable4Parts IS NULL OR @sourceTable4Parts = N'');

                SELECT @stmtSourceTableChecks = N'
                DROP TABLE IF EXISTS #tempSourceTable;
                BEGIN TRY
                    SELECT TOP(0) * INTO #tempSourceTable FROM ' + @sourceTable4Parts + N';

                    SELECT @Columns = (
                        SELECT col.column_id, [column] = col.name, [type] = tpe.name, col.max_length, col.precision, col.scale
                        FROM tempdb.sys.columns AS col
                        INNER JOIN tempdb.sys.types AS tpe ON col.system_type_id = tpe.system_type_id AND tpe.system_type_id = tpe.user_type_id
                        WHERE [object_id] = OBJECT_ID(N''tempdb.dbo.#tempSourceTable'')
                        FOR JSON PATH
                    );
                END TRY
                BEGIN CATCH
                    THROW;
                END CATCH
                ';
                EXEC sp_executesql @stmt = @stmtSourceTableChecks, @params = @paramsSourceTableChecks, @Message = NULL, @Columns = @sourceJsonColumns OUTPUT;

                INSERT INTO @listSourceColumns([id], [name], [type], [max_length], [precision], [scale])
                SELECT [id], [name], [type], [max_length], [precision], [scale] FROM OPENJSON(@sourceJsonColumns, N'$')
                WITH ([id] int N'$.column_id', [name] nvarchar(128) N'$.column', [type] nvarchar(128), max_length smallint, precision tinyint, scale tinyint)

                IF @@ROWCOUNT = 0 INSERT INTO @json_errors([id], [severity], [message]) SELECT 20, 16, N'ERROR[TS1]: No column retrieved from remote source table';
            END TRY
            BEGIN CATCH
                SET @message = N'ERROR[TS0]: error(s) occured while checking remote source table';
                THROW;
            END CATCH
        END

        -- Check Exclude list
        IF NOT EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10)
        BEGIN
            BEGIN TRY
                SELECT @jsonExclude = ISNULL(LTRIM(RTRIM(@ExcludeColumns)), N'')
                IF ISJSON(@jsonExclude) = 1 
                BEGIN
                    INSERT INTO @listExcludeColumns([key], [value], [type])
                    SELECT [key], LTRIM(RTRIM([value])), [type] FROM OPENJSON(@ExcludeColumns, N'$')
                END
                INSERT INTO @json_errors([id], [severity], [message])
                SELECT 21, 16, N'ERROR[EX1] @ExcludeColumns is not a valid JSON string, an array of string(s) is expected: ["col1", "col2", ...]' WHERE @jsonExclude IS NOT NULL AND @jsonExclude <> N'' AND ISJSON(@ExcludeColumns) = 0
                UNION ALL SELECT 21, 16, N'ERROR[EX2] @ExcludeColumns contains invalid type(s), only an array of string(s) is expected: ["col1", "col2", ...]' WHERE EXISTS(SELECT 1 FROM @listExcludeColumns WHERE [type] <> 1) 
                UNION ALL SELECT 21, 16, N'ERROR[EX3] column ' + QUOTENAME(@clusteredName) + N' cannot be excluded (Primary / Clustered Key)' WHERE EXISTS(SELECT 1 FROM @listExcludeColumns WHERE [value] = @clusteredName) 
                UNION ALL SELECT 21, 16, N'ERROR[EX4] column ' + QUOTENAME(exc.[value]) + N' not found in Source table' FROM @listExcludeColumns exc WHERE exc.[value] <> N'' AND exc.[value] IS NOT NULL AND exc.[type] = 1 AND NOT EXISTS(SELECT 1 FROM @listSourceColumns WHERE [name] = exc.[value]) 
            END TRY
            BEGIN CATCH
                SET @message = N'ERROR[EX0]: error(s) occured while checking exclude list';
                THROW;
            END CATCH
        END
        -- Check Archive Table
        IF NOT EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10)
        BEGIN
            BEGIN TRY
                SELECT @archiveTable2Parts = base_object_name FROM sys.synonyms WHERE [name] = @synonymArchiveName AND schema_id = SCHEMA_ID(@synonymArchiveSchema) AND (@archiveTable2Parts IS NULL OR @archiveTable2Parts = N'');
                SELECT @archiveSchema = PARSENAME(@archiveTable2Parts, 2), @archiveTable = PARSENAME(@archiveTable2Parts, 1), @archiveObjectId = OBJECT_ID(@archiveTable2Parts)

                INSERT INTO @listArchiveColumns([id], [name], [type], [max_length], [precision], [scale])
                SELECT col.column_id, [column] = col.name, [type] = tpe.name, col.max_length, col.precision, col.scale
                FROM sys.columns AS col
                INNER JOIN sys.types AS tpe ON col.system_type_id = tpe.system_type_id AND tpe.system_type_id = tpe.user_type_id
                WHERE [object_id] = @archiveObjectId;

                INSERT INTO @json_errors([id], [severity], [message])
                SELECT 31, 16, N'ERROR[TA1]: Synonym ' + QUOTENAME(@synonymArchiveSchema) + N'.' + QUOTENAME(@synonymArchiveName) + N' refers to missing schema: ' + QUOTENAME(@archiveSchema) WHERE NOT EXISTS (SELECT 1 FROM sys.schemas WHERE [schema_id] = SCHEMA_ID(@archiveSchema))
                UNION ALL SELECT 32, 16, N'ERROR[TA2]: @CreateTable is not enabled and Synonym ' + QUOTENAME(@synonymArchiveSchema) + N'.' + QUOTENAME(@synonymArchiveName) + N' refers to a missing table: ' + @archiveTable + N'' WHERE @archiveObjectId IS NULL AND @CreateTable <> 1
                UNION ALL SELECT 33, 16, N'ERROR[TA3]: date type mismatch between source and archive table on column ' + QUOTENAME(src.[name]) + N': ' + src.[datatype] + N' vs ' + arc.[datatype]  FROM @listSourceColumns src
                    INNER JOIN @listArchiveColumns arc ON arc.[name] = src.[name] AND (arc.[type] <> src.[type] OR arc.[max_length] <> src.[max_length] OR arc.[precision] <> src.[precision] OR arc.[scale] <> src.[scale])
                UNION ALL SELECT 34, 16, N'ERROR[TA4]: missing column on archive table (@UpdateTable not set): ' +  QUOTENAME(src.[name]) FROM @listSourceColumns src 
                    WHERE NOT EXISTS(SELECT 1 FROM @listArchiveColumns WHERE [name] = src.[name]) AND @UpdateTable <> 1 AND @archiveObjectId IS NOT NULL AND (@IgnoreMissingColumns IS NULL OR @IgnoreMissingColumns = 0)
                UNION ALL SELECT 35, 16, N'ERROR[TA5]: Synonym ' + QUOTENAME(@synonymArchiveSchema) + N'.' + QUOTENAME(@synonymArchiveName) + N' must refer to a user table: ' + QUOTENAME(@archiveSchema) + N'.' + QUOTENAME(@archiveTable) WHERE OBJECTPROPERTY(@archiveObjectId, 'IsTable') = 0
                UNION ALL SELECT 36, 16, N'ERROR[TA6]: Synonym ' + QUOTENAME(@synonymArchiveSchema) + N'.' + QUOTENAME(@synonymArchiveName) + N' must refer to a user table with no IDENTITY column: ' + QUOTENAME(@archiveSchema) + N'.' + QUOTENAME(@archiveTable) WHERE OBJECTPROPERTY(@archiveObjectId, 'TableHasIdentity') = 1
                UNION ALL SELECT 37, 16, N'ERROR[TA7]: column is missing from source table:' + @clusteredName WHERE NOT EXISTS(SELECT 1 FROM @listSourceColumns WHERE [name] = @clusteredName)
                ;
            END TRY
            BEGIN CATCH
                IF @@TRANCOUNT > 0 ROLLBACK;
                SET @message = N'ERROR[TA0]: error(s) occured while checking archive table';
                THROW;
            END CATCH
        END
        -- Check ASyncStatus Table
        IF NOT EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10)
        BEGIN
            BEGIN TRY
                SELECT @asyncStatusTable4Parts = base_object_name FROM sys.synonyms WHERE [name] = @synonymASyncStatusName AND schema_id = SCHEMA_ID(@synonymASyncStatusSchema) AND (@asyncStatusTable4Parts IS NULL OR @asyncStatusTable4Parts = N'');

                SELECT @stmtASyncStatusTableChecks = N'
                DROP TABLE IF EXISTS #tempASyncStatusTable;
                BEGIN TRY
                    SELECT TOP(0) * INTO #tempASyncStatusTable FROM ' + @asyncStatusTable4Parts + N';

                    SELECT @Columns = (
                        SELECT col.column_id, [column] = col.name, [type] = tpe.name, col.max_length, col.precision, col.scale
                        FROM tempdb.sys.columns AS col
                        INNER JOIN tempdb.sys.types AS tpe ON col.system_type_id = tpe.system_type_id AND tpe.system_type_id = tpe.user_type_id
                        WHERE [object_id] = OBJECT_ID(N''tempdb.dbo.#tempASyncStatusTable'')
                        FOR JSON PATH
                    );
                END TRY
                BEGIN CATCH
                    THROW;
                END CATCH
                ';
                -- retrieve ASyncStatus columns
                EXEC sp_executesql @stmt = @stmtASyncStatusTableChecks, @params = @paramsASyncStatusTableChecks, @Message = NULL, @Columns = @asyncStatusJsonColumns OUTPUT;

                -- Set default columns if not provided
                SELECT @expectedASyncStatusColumns = ISNULL(LTRIM(RTRIM(@ASyncStatusExpectedColumns)), N'[{"column":"SyncId","type":"bigint"},{"column":"IsDeleted","type":"bit"},{"column":"DeletedOnDate","type":"datetime"},{"column":"FirstASyncId","type":"bigint"},{"column":"LastASyncId","type":"bigint"}]' );
                -- check columns
                WITH exp([name], [type], [max_length]) AS (
                    SELECT [name], [type], [max_length]/*, [precision], [scale]*/ FROM OPENJSON(@expectedASyncStatusColumns, N'$')
                    WITH ([name] nvarchar(128) N'$.column', [type] nvarchar(128), max_length smallint/*, precision tinyint, scale tinyint*/)                
                ), col([name], [type], [max_length]) AS(
                    SELECT [name], [type], [max_length] FROM OPENJSON(@asyncStatusJsonColumns, N'$')
                    WITH ([name] nvarchar(128) N'$.column', [type] nvarchar(128), max_length smallint)
                )
                INSERT INTO @json_errors([id], [severity], [message])
                SELECT 20, 16, N'ERROR[TS1]: No column retrieved from remote ASyncStatus table ' + @asyncStatusTable4Parts + N' refered by synonym ' + QUOTENAME(@synonymASyncStatusSchema) + N'.' + QUOTENAME(@synonymASyncStatusName) WHERE NOT EXISTS(SELECT 1 FROM col)
                UNION ALL SELECT 20, 16, N'ERROR[TS2]: Expected column ' + QUOTENAME(x.[name]) + N' not found in remote ASyncStatus table ' + @asyncStatusTable4Parts + N' refered by synonym ' + QUOTENAME(@synonymASyncStatusSchema) + N'.' + QUOTENAME(@synonymASyncStatusName) FROM exp x WHERE NOT EXISTS(SELECT 1 FROM col WHERE [name] = x.[name])
                UNION ALL SELECT 20, 16, N'ERROR[TS3]: Invalid type '+ QUOTENAME(c.[type]) + N' for column ' + QUOTENAME(x.[name]) + N' in remote ASyncStatus table ' + @asyncStatusTable4Parts + N' refered by synonym ' + QUOTENAME(@synonymASyncStatusSchema) + N'.' + QUOTENAME(@synonymASyncStatusName) + N' (' + QUOTENAME(x.[type]) + N' expected)' FROM exp x INNER JOIN col c ON x.[name] = c.[name] AND x.[type] <> c.[type]
                ;
            END TRY
            BEGIN CATCH
                SET @message = N'ERROR[TS0]: error(s) occured while checking remote ASyncStatus table';
                THROW;
            END CATCH
        END        
        ----------------------------------------------------------------------------------------------------      
        -- Create or Update Synonym(s) / Table / Columns
        ----------------------------------------------------------------------------------------------------
        IF NOT EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10)
        BEGIN
            -- Get list of Columns from source table
            SELECT @SourceColumns = NULL;
            DELETE col FROM @listSourceColumns col WHERE EXISTS(SELECT 1 FROM @listExcludeColumns WHERE [value] = col.[name])
            SELECT @SourceColumns = COALESCE(@SourceColumns + N', "' + [name] + N'"', N'"' + [name] + N'"') FROM @listSourceColumns col ORDER BY Id;
            SELECT @SourceColumns = N'['+ @SourceColumns + N']';

            -- Start Transaction for all upcoming schema changes
            BEGIN TRAN
            -- Create Source synonym if missing or outdated
            BEGIN TRY
                IF NOT EXISTS (SELECT 1 FROM sys.synonyms WHERE [name] = @synonymSourceName AND schema_id = SCHEMA_ID(@synonymSourceSchema) AND base_object_name = @sourceTable4Parts) AND @sourceTable4Parts <> N'' AND @CreateOrUpdateSynonyms = 1
                BEGIN
                    SET @sql = NULL;
                    INSERT INTO @json_errors([id], [severity], [message]) SELECT 0, 10, N'Create or alter Source Synonym '+ QUOTENAME(@synonymSourceSchema) + N'.' + QUOTENAME(@synonymSourceName) + ' with base object ' + @sourceTable4Parts;
                    SELECT @sql = N'DROP SYNONYM ' + QUOTENAME(@synonymSourceSchema) + N'.' + QUOTENAME(@synonymSourceName) + N';' FROM sys.synonyms WHERE [name] = @synonymSourceName AND schema_id = SCHEMA_ID(@synonymSourceSchema);
                    SELECT @sql = ISNULL(@sql, N'') + N'CREATE SYNONYM ' + QUOTENAME(@synonymSourceSchema) + N'.' + QUOTENAME(@synonymSourceName) + N' FOR ' + @sourceTable4Parts + N';';
                    EXEC sp_executesql @statement = @sql;
                END
            END TRY
            BEGIN CATCH
                SET @message = N'ERROR[SU0]: error(s) occured while creating or updating Source synomym';
                THROW;
            END CATCH
            -- Create Archive synonym if missing or outdated
            BEGIN TRY
                IF NOT EXISTS (SELECT 1 FROM sys.synonyms WHERE [name] = @synonymArchiveName AND schema_id = SCHEMA_ID(@synonymArchiveSchema) AND base_object_name = @archiveTable2Parts) AND @archiveTable2Parts <> N'' AND @CreateOrUpdateSynonyms = 1
                BEGIN
                    SET @sql = NULL;
                    INSERT INTO @json_errors([id], [severity], [message]) SELECT 10, 10, N'Create or alter Archive Synonym '+ QUOTENAME(@synonymArchiveSchema) + N'.' + QUOTENAME(@synonymArchiveName) + ' with base object: ' + @archiveTable2Parts;
                    SELECT @sql = N'DROP SYNONYM ' + QUOTENAME(@synonymArchiveSchema) + N'.' + QUOTENAME(@synonymArchiveName) + N';' FROM sys.synonyms WHERE [name] = @synonymArchiveName AND schema_id = SCHEMA_ID(@synonymArchiveSchema);
                    SELECT @sql = ISNULL(@sql, N'') + N'CREATE SYNONYM ' + QUOTENAME(@synonymArchiveSchema) + N'.' + QUOTENAME(@synonymArchiveName) + N' FOR ' + @archiveTable2Parts + N';';
                    EXEC sp_executesql @statement = @sql;
                END
            END TRY
            BEGIN CATCH
                IF @@TRANCOUNT > 0 ROLLBACK;
                SET @message = N'ERROR[SU1]: error(s) occured while creating or updating Archive synomym';
                THROW;
            END CATCH
            -- Create ASync Status synonym if missing or outdated
            BEGIN TRY
                IF NOT EXISTS (SELECT 1 FROM sys.synonyms WHERE [name] = @SynonymASyncStatusName AND schema_id = SCHEMA_ID(@synonymASyncStatusSchema) AND base_object_name = @asyncStatusTable4Parts) AND @asyncStatusTable4Parts  <> N'' AND @CreateOrUpdateSynonyms = 1
                BEGIN
                    SET @sql = NULL;
                    INSERT INTO @json_errors([id], [severity], [message]) SELECT 10, 10, N'Create or alter ASync Status Synonym '+ QUOTENAME(@synonymASyncStatusSchema) + N'.' + QUOTENAME(@synonymASyncStatusName) + ' with base object: ' + @asyncStatusTable4Parts;
                    SELECT @sql = N'DROP SYNONYM ' + QUOTENAME(@synonymASyncStatusSchema) + N'.' + QUOTENAME(@synonymASyncStatusName) + N';' FROM sys.synonyms WHERE [name] = @synonymASyncStatusName AND schema_id = SCHEMA_ID(@synonymASyncStatusSchema);
                    SELECT @sql = ISNULL(@sql, N'') + N'CREATE SYNONYM ' + QUOTENAME(@synonymASyncStatusSchema) + N'.' + QUOTENAME(@synonymASyncStatusName) + N' FOR ' + @asyncStatusTable4Parts + N';';
                    EXEC sp_executesql @statement = @sql;
                END
            END TRY
            BEGIN CATCH
                IF @@TRANCOUNT > 0 ROLLBACK;
                SET @message = N'ERROR[SU1]: error(s) occured while creating or updating ASync Status synomym';
                THROW;
            END CATCH
            -- Create Archive base table or Add missing column(s)
            BEGIN TRY
                IF ( (@archiveObjectId IS NULL AND @CreateTable = 1) OR @UpdateTable = 1) AND EXISTS( SELECT 1 FROM @listSourceColumns src WHERE [name] <> @clusteredName AND NOT EXISTS(SELECT 1 FROM @listArchiveColumns WHERE [name] = src.[name]) )
                BEGIN
                    -- Prepare CREATE/ALTER statement
                    SELECT @sql = NULL;
                    SELECT @sql = COALESCE(@sql + N', ' + q.[query], q.[query])
                    FROM (
                        SELECT TOP(1024) [query] = src.[name] + N' ' + src.[datatype] + N' NULL' + CHAR(13) + CHAR(10) 
                        FROM @listSourceColumns src WHERE [name] <> @clusteredName AND NOT EXISTS(SELECT 1 FROM @listArchiveColumns WHERE [name] = src.[name]) --AND @UpdateTable <> 1 AND @archiveObjectId IS NOT NULL
                        ORDER BY Id ASC
                    ) q([query]);
                    SELECT @message = COALESCE(@message + N', ' + src.[name], src.[name]) FROM @listSourceColumns src WHERE NOT EXISTS(SELECT 1 FROM @listArchiveColumns WHERE [name] = src.[name]) ORDER BY Id ASC;

                    IF @archiveObjectId IS NULL --> CREATE
                    BEGIN
                        INSERT INTO @json_errors([id], [severity], [message])
                        SELECT 30, 10, N'Create table refered to by archive synonym: ' + QUOTENAME(@synonymArchiveSchema) + N'.' + QUOTENAME(@synonymArchiveName) + N' => '+ QUOTENAME(@archiveSchema) + N'.' + QUOTENAME(@archiveTable) + N' (' + @message + N')';
                        SELECT @sql = N'CREATE TABLE ' + QUOTENAME(@archiveSchema) + '.' + QUOTENAME(@archiveTable) + '(' + QUOTENAME([name]) + ' ['+ datatype + '] NOT NULL CONSTRAINT [PK_' + @archiveSchema + '.'+ @archiveTable + '] PRIMARY KEY CLUSTERED ([Id] ASC)' + CHAR(13) + CHAR(10) + N', ' + @sql + N');'
                        FROM @listSourceColumns WHERE [name] = @clusteredName;
                    END
                    ELSE --> ALTER
                    BEGIN
                        INSERT INTO @json_errors([id], [severity], [message])
                        SELECT 30, 10, N'Add missing column(s) to table refered to by archive synonym: ' + QUOTENAME(@synonymArchiveSchema) + N'.' + QUOTENAME(@synonymArchiveName) + N' => '+ QUOTENAME(@archiveSchema) + N'.' + QUOTENAME(@archiveTable) + N' (' + @message + N')';
                        SELECT @sql = N'ALTER TABLE '+ QUOTENAME(@archiveSchema) + '.' + QUOTENAME(@archiveTable) + ' ADD ' + @sql + N';';
                    END

                    -- Execute create/alter table
                    EXEC sp_executesql @stmt = @sql;
                END
            END TRY
            BEGIN CATCH
                IF @@TRANCOUNT > 0 ROLLBACK;
                SET @message = N'ERROR[SU2]: error(s) occured while creating or updating source table';
                THROW;
            END CATCH
            SET @message = NULL;
            IF @@TRANCOUNT > 0 COMMIT
        END
    END TRY
    BEGIN CATCH
        SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
        IF @@TRANCOUNT > 0 ROLLBACK;
    END CATCH

--    SELECT TOP(100) 'message'= 'output', [Message] = LEFT([message], 4000), [Severity] = [severity], [State] = 1 FROM @json_errors ORDER BY [id] ASC
    -- Check / Set @IsValid flag
    SET @IsValid = IIF(NOT EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10) AND @ERROR_NUMBER IS NULL AND @message IS NULL, 1, 0);

    SET @Messages = --ISNULL(
    ( 
        SELECT [Procedure] = QUOTENAME(COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?')) + N'.' + QUOTENAME(COALESCE(OBJECT_NAME(@@PROCID), N'?')), [Message], [Severity], [State] 
        FROM (
            SELECT [Message], [Severity], [State] FROM (
                SELECT TOP(100) [Message] = LEFT([message], 4000), [Severity] = [severity], [State] = 1 FROM @json_errors ORDER BY [id] ASC
            ) err
            UNION ALL SELECT N'ERROR: ' + @ERROR_MESSAGE, 16, 1 WHERE @ERROR_MESSAGE IS NOT NULL
            UNION ALL SELECT @message, 16, 1 WHERE @message IS NOT NULL
        ) jsn
        FOR JSON PATH)
    --    , N'[]')
    ;
    RETURN 0;
END
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- DROP PROCEDURE [Maintenance].[SetSourceTable]
----------------------------------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[SetSourceTable]') AND type in (N'P'))
BEGIN
    EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [Maintenance].[SetSourceTable] AS'
    PRINT '  + CREATE PROCEDURE: [Maintenance].[SetSourceTable]';
END
ELSE PRINT '  = PROCEDURE [Maintenance].[SetSourceTable] already exists' 
GO

PRINT '  ~ UPDATE PROCEDURE: [Maintenance].[SetSourceTable]'
GO

ALTER PROCEDURE [Maintenance].[SetSourceTable]
----------------------------------------------------------------------------------------------------
-- ### [Object]: PROCEDURE [Maintenance].[SetSourceTable]
-- ### [Version]: 2023-10-06T11:29:36+02:00
-- ### [Source]: _src/Archive/ArchiveDB/Procedure_ArchiveDB.Maintenance.SetSourceTable.sql
-- ### [Hash]: dc39c27 [SHA256-45AC573DF06F2BF3CECAB5846176D1BE55C217062136931AF84831AA441EE4EF]
-- ### [Docs]: https://???.???
-- !!! ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!!!
-- !!! ~~~~~~~~~ NOT OFFICIALLY SUPPORTED BY UIPATH 
-- !!! ~~~~~~~~~ SQL Server >= 2016 SP1
----------------------------------------------------------------------------------------------------
    @SynonymName nvarchar(256)
    , @SynonymSchema nvarchar(256)
	, @SourceTableFullParts nvarchar(250) = NULL
    , @SourceExpectedColumns nvarchar(MAX) = NULL
    , @CreateOrUpdateSynonym bit = 0
    , @Columns nvarchar(MAX) = NULL OUTPUT
    , @IsValid bit = 0 OUTPUT
    , @Messages nvarchar(MAX) OUTPUT
AS
BEGIN
    BEGIN TRY 
        SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
		SET LOCK_TIMEOUT 5000;
        SET ARITHABORT ON;
        SET NOCOUNT ON;
        SET NUMERIC_ROUNDABORT OFF;

        ----------------------------------------------------------------------------------------------------
        -- ASync Delete Variables
        ----------------------------------------------------------------------------------------------------
        DECLARE @sourceTable nvarchar(256);
        DECLARE @sourceTable4Parts nvarchar(256);
        DECLARE @paramsSourceTableChecks nvarchar(MAX) = N'@Message nvarchar(MAX) OUTPUT, @Columns nvarchar(MAX) OUTPUT';
        DECLARE @stmtSourceTableChecks nvarchar(MAX) = N'';
        DECLARE @sourceJsonColumns nvarchar(MAX);
        DECLARE @expectedSourceColumns nvarchar(MAX)
        ----------------------------------------------------------------------------------------------------
        -- Misc      
        ----------------------------------------------------------------------------------------------------
        DECLARE @sql nvarchar(max);
        ----------------------------------------------------------------------------------------------------      
        -- Message / Error Handling
        ----------------------------------------------------------------------------------------------------
        DECLARE @message nvarchar(MAX);
        DECLARE @ERROR_NUMBER INT, @ERROR_SEVERITY INT, @ERROR_STATE INT, @ERROR_PROCEDURE NVARCHAR(126), @ERROR_LINE INT, @ERROR_MESSAGE NVARCHAR(2048);
        DECLARE @space tinyint = 2;
        DECLARE @tab tinyint = 0;
        DECLARE @json_errors TABLE([id] tinyint NOT NULL, [severity] int NOT NULL, [message] nvarchar(MAX) NOT NULL);

        ----------------------------------------------------------------------------------------------------      
        -- Checks Synonyms and Tables
        ----------------------------------------------------------------------------------------------------
        -- Check Missing Synonym
        BEGIN TRY
            -- Synonym for Source table
            SELECT @sourceTable = ISNULL(LTRIM(RTRIM(@SourceTableFullParts)), N'');
            SELECT @sourceTable4Parts = ISNULL(QUOTENAME(LTRIM(RTRIM(PARSENAME(@sourceTable, 4)))) + N'.', N'') + ISNULL(QUOTENAME(LTRIM(RTRIM(PARSENAME(@sourceTable, 3)))) + N'.', N'') + ISNULL(QUOTENAME(LTRIM(RTRIM(PARSENAME(@sourceTable, 2)))) + N'.', N'') + ISNULL(QUOTENAME(LTRIM(RTRIM(PARSENAME(@sourceTable, 1)))), N'');

            INSERT INTO @json_errors([id], [severity], [message]) 
            SELECT 0, 16, N'ERROR[SS1]: Synonym ' + QUOTENAME(@synonymSchema) + N'.' + QUOTENAME(@synonymName) + N' not found and @SourceTableFullParts not provided' WHERE NOT EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = @synonymName AND schema_id = SCHEMA_ID(@synonymSchema)) AND @sourceTable = N''
            UNION ALL SELECT 1, 16, N'ERROR[SS2]: Synonym ' + QUOTENAME(@synonymSchema) + N'.' + QUOTENAME(@synonymName) + N' refers to an invalid @SourceTableFullParts''s name' WHERE @sourceTable <> N'' AND @sourceTable4Parts = N''
            UNION ALL SELECT 2, 16, N'ERROR[SS3]: Synonym ' + QUOTENAME(@synonymSchema) + N'.' + QUOTENAME(@synonymName) + N' not found and @CreateSynonym not enabled' WHERE NOT EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = @synonymName AND schema_id = SCHEMA_ID(@synonymSchema)) AND @sourceTable <> N'' AND @sourceTable4Parts <> N'' AND @CreateOrUpdateSynonym <> 1
            UNION ALL SELECT 3, 16, N'ERROR[SS4]: Synonym ' + QUOTENAME(@synonymSchema) + N'.' + QUOTENAME(@synonymName) + N' must refers to a 2 (or more) parts name with at least a schema and table name: [schema_name].[table_name]' WHERE @sourceTable <> N'' AND @sourceTable4Parts <> N'' AND PARSENAME(@sourceTable, 2) IS NULL
            ;
        END TRY
        BEGIN CATCH;
            IF @@TRANCOUNT > 0 ROLLBACK;
            SET @message = N'ERROR[SS0]: error(s) occured while checking synonym';
            THROW;
        END CATCH; 

        -- Check Source Table
        IF NOT EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10)
        BEGIN
            BEGIN TRY
                SELECT @sourceTable4Parts = base_object_name FROM sys.synonyms WHERE [name] = @synonymName AND schema_id = SCHEMA_ID(@synonymSchema) AND (@sourceTable4Parts IS NULL OR @sourceTable4Parts = N'');

                SELECT @stmtSourceTableChecks = N'
                DROP TABLE IF EXISTS #tempASTable;
                BEGIN TRY
                    SELECT TOP(0) * INTO #tempSourceTableCheck FROM ' + @sourceTable4Parts + N';

                    SELECT @Columns = (
                        SELECT col.column_id, [column] = col.name, [type] = tpe.name, col.max_length, col.precision, col.scale
                        FROM tempdb.sys.columns AS col
                        INNER JOIN tempdb.sys.types AS tpe ON col.system_type_id = tpe.system_type_id AND tpe.system_type_id = tpe.user_type_id
                        WHERE [object_id] = OBJECT_ID(N''tempdb.dbo.#tempSourceTableCheck'') AND tpe.[name] <> N''timestamp''
                        FOR JSON PATH
                    );
                END TRY
                BEGIN CATCH
                    THROW;
                END CATCH
                ';
                -- retrieve Source columns
                EXEC sp_executesql @stmt = @stmtSourceTableChecks, @params = @paramsSourceTableChecks, @Message = NULL, @Columns = @sourceJsonColumns OUTPUT;

                -- Set default columns if not provided
                SELECT @expectedSourceColumns = ISNULL(LTRIM(RTRIM(@sourceExpectedColumns)), N'[{"column":"Id","type":"bigint"}]' );
                -- check columns
                WITH exp([name], [type], [max_length]) AS (
                    SELECT [name], [type], [max_length]/*, [precision], [scale]*/ FROM OPENJSON(@expectedSourceColumns, N'$')
                    WITH ([name] nvarchar(128) N'$.column', [type] nvarchar(128), max_length smallint/*, precision tinyint, scale tinyint*/)                
                ), col([name], [type], [max_length]) AS(
                    SELECT [name], [type], [max_length] FROM OPENJSON(@sourceJsonColumns, N'$')
                    WITH ([name] nvarchar(128) N'$.column', [type] nvarchar(128), max_length smallint)
                )
                INSERT INTO @json_errors([id], [severity], [message])
                SELECT 20, 16, N'ERROR[TS1]: No column retrieved from  Source table ' + @sourceTable4Parts + N' refered by synonym ' + QUOTENAME(@synonymSchema) + N'.' + QUOTENAME(@synonymName) WHERE NOT EXISTS(SELECT 1 FROM col)
                UNION ALL SELECT 20, 16, N'ERROR[TS2]: Expected column ' + QUOTENAME(x.[name]) + N' not found in  Source table ' + @sourceTable4Parts + N' refered by synonym ' + QUOTENAME(@synonymSchema) + N'.' + QUOTENAME(@synonymName) FROM exp x WHERE NOT EXISTS(SELECT 1 FROM col WHERE [name] = x.[name])
                UNION ALL SELECT 20, 16, N'ERROR[TS3]: Invalid type '+ QUOTENAME(c.[type]) + N' for column ' + QUOTENAME(x.[name]) + N' in  Source table ' + @sourceTable4Parts + N' refered by synonym ' + QUOTENAME(@synonymSchema) + N'.' + QUOTENAME(@synonymName) + N' (' + QUOTENAME(x.[type]) + N' expected)' FROM exp x INNER JOIN col c ON x.[name] = c.[name] AND x.[type] <> c.[type]
                ;
                SET @Columns = @sourceJsonColumns;
            END TRY
            BEGIN CATCH
                SET @message = N'ERROR[TS0]: error(s) occured while checking Source table';
                THROW;
            END CATCH
        END    
        ----------------------------------------------------------------------------------------------------      
        -- Create or Update Synonym(s) / Table / Columns
        ----------------------------------------------------------------------------------------------------
        IF NOT EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10)
        BEGIN
            -- Start Transaction for all upcoming schema changes
            BEGIN TRAN
            -- Create synonym if missing or outdated
            BEGIN TRY
                IF NOT EXISTS (SELECT 1 FROM sys.synonyms WHERE [name] = @synonymName AND schema_id = SCHEMA_ID(@synonymSchema) AND base_object_name = @sourceTable4Parts) AND @sourceTable4Parts <> N'' AND @CreateOrUpdateSynonym = 1
                BEGIN
                    SET @sql = NULL;
                    INSERT INTO @json_errors([id], [severity], [message]) SELECT 0, 10, N'Create or alter Synonym '+ QUOTENAME(@synonymSchema) + N'.' + QUOTENAME(@synonymName) + ' with base object ' + @sourceTable4Parts;
                    SELECT @sql = N'DROP SYNONYM ' + QUOTENAME(@synonymSchema) + N'.' + QUOTENAME(@synonymName) + N';' FROM sys.synonyms WHERE [name] = @synonymName AND schema_id = SCHEMA_ID(@synonymSchema);
                    SELECT @sql = ISNULL(@sql, N'') + N'CREATE SYNONYM ' + QUOTENAME(@synonymSchema) + N'.' + QUOTENAME(@synonymName) + N' FOR ' + @sourceTable4Parts + N';';
                    EXEC sp_executesql @statement = @sql;
                END
            END TRY
            BEGIN CATCH
                SET @message = N'ERROR[SU0]: error(s) occured while creating or updating source synomym';
                THROW;
            END CATCH
            SET @message = NULL;
            IF @@TRANCOUNT > 0 COMMIT
        END
    END TRY
    BEGIN CATCH
        SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
        IF @@TRANCOUNT > 0 ROLLBACK;
    END CATCH

--    SELECT TOP(100) 'message'= 'output', [Message] = LEFT([message], 4000), [Severity] = [severity], [State] = 1 FROM @json_errors ORDER BY [id] ASC
    -- Check / Set @IsValid flag
    SET @IsValid = IIF(NOT EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10) AND @ERROR_NUMBER IS NULL AND @message IS NULL, 1, 0);

    SET @Messages = --ISNULL(
    ( 
        SELECT [Procedure] = QUOTENAME(COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?')) + N'.' + QUOTENAME(COALESCE(OBJECT_NAME(@@PROCID), N'?')), [Message], [Severity], [State] 
        FROM (
            SELECT [Message], [Severity], [State] FROM (
                SELECT TOP(100) [Message] = LEFT([message], 4000), [Severity] = [severity], [State] = 1 FROM @json_errors ORDER BY [id] ASC
            ) err
            UNION ALL SELECT N'ERROR: ' + @ERROR_MESSAGE, 16, 1 WHERE @ERROR_MESSAGE IS NOT NULL
            UNION ALL SELECT @message, 16, 1 WHERE @message IS NOT NULL
        ) jsn
        FOR JSON PATH)
    --    , N'[]')
    ;
    RETURN 0;
END
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- DROP PROCEDURE [Maintenance].[SetArchiveTable]
----------------------------------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[SetArchiveTable]') AND type in (N'P'))
BEGIN
    EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [Maintenance].[SetArchiveTable] AS'
    PRINT '  + CREATE PROCEDURE: [Maintenance].[SetArchiveTable]';
END
ELSE PRINT '  = PROCEDURE [Maintenance].[SetArchiveTable] already exists' 
GO

PRINT '  ~ UPDATE PROCEDURE: [Maintenance].[SetArchiveTable]'
GO

ALTER PROCEDURE [Maintenance].[SetArchiveTable]
----------------------------------------------------------------------------------------------------
-- ### [Object]: PROCEDURE [Maintenance].[SetArchiveTable]
-- ### [Version]: 2023-10-06T11:29:36+02:00
-- ### [Source]: _src/Archive/ArchiveDB/Procedure_ArchiveDB.Maintenance.SetArchiveTable.sql
-- ### [Hash]: dc39c27 [SHA256-7BFFFCA6A305C98C99E2A8F1D371B2496F4FAB7F3D574F786FE123DA3F9D744E]
-- ### [Docs]: https://???.???
-- !!! ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!!!
-- !!! ~~~~~~~~~ NOT OFFICIALLY SUPPORTED BY UIPATH 
-- !!! ~~~~~~~~~ SQL Server >= 2016 SP1
----------------------------------------------------------------------------------------------------
    @SynonymSourceName nvarchar(256)
    , @SynonymSourceSchema nvarchar(256)
    , @SynonymArchiveName nvarchar(256)
    , @SynonymArchiveSchema nvarchar(256)
	, @ArchiveTableName nvarchar(250) = NULL
	, @ArchiveTableSchema nvarchar(250) = NULL
    , @ClusteredName nvarchar(128)
    , @ExcludeColumns nvarchar(MAX) = NULL
    , @IgnoreMissingColumns bit = 0
    , @CreateOrUpdateSynonym bit = 0
    , @CreateTable bit = 1
    , @UpdateTable bit = 1
    , @RemoveIdentity bit = 0
    , @SourceColumns nvarchar(MAX) OUTPUT
    , @IsValid bit = 0 OUTPUT
    , @Messages nvarchar(MAX) OUTPUT
AS
BEGIN
    BEGIN TRY 
        SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
		SET LOCK_TIMEOUT 5000;
        SET ARITHABORT ON;
        SET NOCOUNT ON;
        SET NUMERIC_ROUNDABORT OFF;

        ----------------------------------------------------------------------------------------------------
        -- Source Variables
        ----------------------------------------------------------------------------------------------------
        DECLARE @sourceIsValid bit = 0;
        DECLARE @sourceMessages nvarchar(MAX);
        DECLARE @sourceJsonColumns nvarchar(MAX);
        DECLARE @listSourceColumns TABLE(Id int, [name] nvarchar(128), [type] nvarchar(128), max_length smallint, precision tinyint, scale tinyint
            , [datatype] AS ( [type] + 
                    CASE WHEN type IN ('varchar', 'char', 'varbinary', 'binary', 'text') THEN '(' + CASE WHEN max_length = -1 THEN 'MAX' ELSE CAST(max_length AS VARCHAR(5)) END + ')'
                    WHEN type IN ('nvarchar', 'nchar', 'ntext') THEN '(' + CASE WHEN max_length = -1 THEN 'MAX' ELSE CAST(max_length / 2 AS VARCHAR(5)) END + ')'
                    WHEN type IN ('datetime2', 'time2', 'datetimeoffset') THEN '(' + CAST(scale AS VARCHAR(5)) + ')'
                    WHEN type IN ('decimal', 'numeric') THEN '(' + CAST([precision] AS VARCHAR(5)) + ',' + CAST(scale AS VARCHAR(5)) + ')'
                    WHEN type IN ('float') THEN '(' + CAST([precision] AS VARCHAR(5)) + ')'
                    ELSE '' END )
        );
        DECLARE @listExcludeColumns TABLE([key] nvarchar(4000), [value] nvarchar(MAX), [type] int);
        DECLARE @jsonExclude nvarchar(MAX);
        ----------------------------------------------------------------------------------------------------
        -- Archive variables
        ----------------------------------------------------------------------------------------------------
        DECLARE @archiveObjectId bigint;
        DECLARE @archiveTable nvarchar(256);
        DECLARE @archiveSchema nvarchar(128);
        DECLARE @archiveTable2Parts nvarchar(256);
        DECLARE @listArchiveColumns TABLE(Id int, [name] nvarchar(128), [type] nvarchar(128), max_length smallint, precision tinyint, scale tinyint
            , [datatype] AS ( [type] + 
                    CASE WHEN type IN ('varchar', 'char', 'varbinary', 'binary', 'text') THEN '(' + CASE WHEN max_length = -1 THEN 'MAX' ELSE CAST(max_length AS VARCHAR(5)) END + ')'
                    WHEN type IN ('nvarchar', 'nchar', 'ntext') THEN '(' + CASE WHEN max_length = -1 THEN 'MAX' ELSE CAST(max_length / 2 AS VARCHAR(5)) END + ')'
                    WHEN type IN ('datetime2', 'time2', 'datetimeoffset') THEN '(' + CAST(scale AS VARCHAR(5)) + ')'
                    WHEN type IN ('decimal', 'numeric') THEN '(' + CAST([precision] AS VARCHAR(5)) + ',' + CAST(scale AS VARCHAR(5)) + ')'
                    WHEN type IN ('float') THEN '(' + CAST([precision] AS VARCHAR(5)) + ')'
                    ELSE '' END )
        );
        ----------------------------------------------------------------------------------------------------
        -- Misc      
        ----------------------------------------------------------------------------------------------------
        DECLARE @sql nvarchar(max);
        ----------------------------------------------------------------------------------------------------      
        -- Message / Error Handling
        ----------------------------------------------------------------------------------------------------
        DECLARE @message nvarchar(MAX);
        DECLARE @ERROR_NUMBER INT, @ERROR_SEVERITY INT, @ERROR_STATE INT, @ERROR_PROCEDURE NVARCHAR(126), @ERROR_LINE INT, @ERROR_MESSAGE NVARCHAR(2048);
        DECLARE @space tinyint = 2;
        DECLARE @tab tinyint = 0;
        DECLARE @json_errors TABLE([id] tinyint NOT NULL, [severity] int NOT NULL, [message] nvarchar(MAX) NOT NULL);

        ----------------------------------------------------------------------------------------------------      
        -- Checks Synonyms and Tables
        ----------------------------------------------------------------------------------------------------
        -- Check Missing Synonym
        BEGIN TRY
            -- Synonym for Source table
            EXEC [Maintenance].[SetSourceTable] @SynonymName = @SynonymSourceName, @SynonymSchema = @SynonymSourceSchema
                , @Columns = @sourceJsonColumns OUTPUT
                , @IsValid = @sourceIsValid OUTPUT
                , @Messages = @sourceMessages OUTPUT
            ;
            INSERT INTO @json_errors([id], [severity], [message])
            SELECT 0, [Severity], [Message] FROM OPENJSON(@sourceMessages, N'$') WITH ([Message] nvarchar(MAX), [Severity] tinyint);
        END TRY
        BEGIN CATCH;
            IF @@TRANCOUNT > 0 ROLLBACK;
            SET @message = N'ERROR[SS0]: error(s) occured while checking Source synonym and table';
            THROW;
        END CATCH; 

        BEGIN TRY
            -- Synonym for Archive table
            SELECT @archiveTable = LTRIM(RTRIM(@ArchiveTableName)), @archiveSchema = LTRIM(RTRIM(@ArchiveTableSchema));
            SELECT @archiveTable2Parts = ISNULL(QUOTENAME(@archiveSchema) + N'.' + QUOTENAME(@archiveTable), N'');
            SELECT @archiveTable = ISNULL(@archiveTable, N''), @archiveSchema = ISNULL(@archiveSchema, N'');

            INSERT INTO @json_errors([id], [severity], [message])
            SELECT 11, 16, N'ERROR[AS1]: Synonym ' + QUOTENAME(@SynonymArchiveSchema) + N'.' + QUOTENAME(@SynonymArchiveName) + N' not found and @ArchiveTableName not provided' WHERE NOT EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = @SynonymArchiveName AND schema_id = SCHEMA_ID(@SynonymArchiveSchema)) AND  @archiveTable = N''
            UNION ALL SELECT 11, 16, N'ERROR[AS1]: Synonym ' + QUOTENAME(@SynonymArchiveSchema) + N'.' + QUOTENAME(@SynonymArchiveName) + N' not found and @ArchiveTableSchema not provided' WHERE NOT EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = @SynonymArchiveName AND schema_id = SCHEMA_ID(@SynonymArchiveSchema)) AND  @archiveSchema = N''
            UNION ALL SELECT 13, 16, N'ERROR[AS3]: Synonym ' + QUOTENAME(@SynonymArchiveSchema) + N'.' + QUOTENAME(@SynonymArchiveName) + N' not found and @CreateOrUpdateSynonym not enabled' WHERE  NOT EXISTS(SELECT 1 FROM sys.synonyms WHERE [name] = @SynonymArchiveName AND schema_id = SCHEMA_ID(@SynonymArchiveSchema)) AND @archiveTable2Parts <> N'' /*AND @archiveTable2Parts <> N''*/ AND @CreateOrUpdateSynonym <> 1
            ;
        END TRY
        BEGIN CATCH;
            IF @@TRANCOUNT > 0 ROLLBACK;
            SET @message = N'ERROR[AS0]: error(s) occured while checking Archive synonyms';
            THROW;
        END CATCH; 

        -- Check Source Table
        IF NOT EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10)
        BEGIN
            BEGIN TRY
                INSERT INTO @listSourceColumns([id], [name], [type], [max_length], [precision], [scale])
                SELECT [id], [name], [type], [max_length], [precision], [scale] FROM OPENJSON(@sourceJsonColumns, N'$')
                WITH ([id] int N'$.column_id', [name] nvarchar(128) N'$.column', [type] nvarchar(128), max_length smallint, precision tinyint, scale tinyint)

                IF @@ROWCOUNT = 0 INSERT INTO @json_errors([id], [severity], [message]) SELECT 20, 16, N'ERROR[TS1]: No column retrieved from remote source table';
            END TRY
            BEGIN CATCH
                SET @message = N'ERROR[TS0]: error(s) occured while checking remote source table';
                THROW;
            END CATCH
        END

        -- Check Exclude list
        IF NOT EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10)
        BEGIN
            BEGIN TRY
                SELECT @jsonExclude = ISNULL(LTRIM(RTRIM(@ExcludeColumns)), N'')
                IF ISJSON(@jsonExclude) = 1 
                BEGIN
                    INSERT INTO @listExcludeColumns([key], [value], [type])
                    SELECT [key], LTRIM(RTRIM([value])), [type] FROM OPENJSON(@ExcludeColumns, N'$')
                END
                INSERT INTO @json_errors([id], [severity], [message])
                SELECT 21, 16, N'ERROR[EX1] @ExcludeColumns is not a valid JSON string, an array of string(s) is expected: ["col1", "col2", ...]' WHERE @jsonExclude IS NOT NULL AND @jsonExclude <> N'' AND ISJSON(@ExcludeColumns) = 0
                UNION ALL SELECT 21, 16, N'ERROR[EX2] @ExcludeColumns contains invalid type(s), only an array of string(s) is expected: ["col1", "col2", ...]' WHERE EXISTS(SELECT 1 FROM @listExcludeColumns WHERE [type] <> 1) 
                UNION ALL SELECT 21, 16, N'ERROR[EX3] column ' + QUOTENAME(@clusteredName) + N' cannot be excluded (Primary / Clustered Key)' WHERE EXISTS(SELECT 1 FROM @listExcludeColumns WHERE [value] = @clusteredName) 
                UNION ALL SELECT 21, 16, N'ERROR[EX4] column ' + QUOTENAME(exc.[value]) + N' not found in Source table' FROM @listExcludeColumns exc WHERE exc.[value] <> N'' AND exc.[value] IS NOT NULL AND exc.[type] = 1 AND NOT EXISTS(SELECT 1 FROM @listSourceColumns WHERE [name] = exc.[value]) 
            END TRY
            BEGIN CATCH
                SET @message = N'ERROR[EX0]: error(s) occured while checking exclude list';
                THROW;
            END CATCH
        END


        -- Check Archive Table
        IF NOT EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10)
        BEGIN
            BEGIN TRY
                SELECT @archiveTable2Parts = base_object_name FROM sys.synonyms WHERE [name] = @SynonymArchiveName AND schema_id = SCHEMA_ID(@SynonymArchiveSchema) AND (@archiveTable2Parts IS NULL OR @archiveTable2Parts = N'');
                SELECT @archiveSchema = ISNULL(LTRIM(RTRIM(@ArchiveTableSchema)), PARSENAME(@archiveTable2Parts, 2)), @archiveTable = ISNULL(LTRIM(RTRIM(@ArchiveTableName)), PARSENAME(@archiveTable2Parts, 1)), @archiveObjectId = OBJECT_ID(@archiveTable2Parts)

                INSERT INTO @listArchiveColumns([id], [name], [type], [max_length], [precision], [scale])
                SELECT col.column_id, [column] = col.name, [type] = tpe.name, col.max_length, col.precision, col.scale
                FROM sys.columns AS col
                INNER JOIN sys.types AS tpe ON col.system_type_id = tpe.system_type_id AND tpe.system_type_id = tpe.user_type_id
                WHERE [object_id] = @archiveObjectId;

                INSERT INTO @json_errors([id], [severity], [message])
                SELECT 31, 16, N'ERROR[TA1]: Synonym ' + QUOTENAME(@SynonymArchiveSchema) + N'.' + QUOTENAME(@SynonymArchiveName) + N' refers to a non existing schema: ' + QUOTENAME(@archiveSchema) WHERE NOT EXISTS (SELECT 1 FROM sys.schemas WHERE [schema_id] = SCHEMA_ID(@archiveSchema))
                UNION ALL SELECT 32, 16, N'ERROR[TA2]: @CreateTable is not enabled and Synonym ' + QUOTENAME(@SynonymArchiveSchema) + N'.' + QUOTENAME(@SynonymArchiveName) + N' refers to a non existing table: ' + @archiveTable + N'' WHERE @archiveObjectId IS NULL AND @CreateTable <> 1
                UNION ALL SELECT 33, 16, N'ERROR[TA3]: datatype mismatch between source and archive table on column ' + QUOTENAME(src.[name]) + N': ' + src.[datatype] + N' vs ' + arc.[datatype]  FROM @listSourceColumns src
                    INNER JOIN @listArchiveColumns arc ON arc.[name] = src.[name] AND (arc.[type] <> src.[type] OR arc.[max_length] <> src.[max_length] OR arc.[precision] <> src.[precision] OR arc.[scale] <> src.[scale])
                UNION ALL SELECT 34, 16, N'ERROR[TA4]: missing column on archive table (@UpdateTable not set): ' +  QUOTENAME(src.[name]) FROM @listSourceColumns src 
                    WHERE NOT EXISTS(SELECT 1 FROM @listArchiveColumns WHERE [name] = src.[name]) AND @UpdateTable <> 1 AND @archiveObjectId IS NOT NULL AND (@IgnoreMissingColumns IS NULL OR @IgnoreMissingColumns = 0)
                UNION ALL SELECT 35, 16, N'ERROR[TA5]: Synonym ' + QUOTENAME(@SynonymArchiveSchema) + N'.' + QUOTENAME(@SynonymArchiveName) + N' must refer to a user table: ' + QUOTENAME(@archiveSchema) + N'.' + QUOTENAME(@archiveTable) WHERE OBJECTPROPERTY(@archiveObjectId, 'IsTable') = 0
                UNION ALL SELECT 36, 16, N'ERROR[TA6]: Synonym ' + QUOTENAME(@SynonymArchiveSchema) + N'.' + QUOTENAME(@SynonymArchiveName) + N' must refer to a user table with no IDENTITY column: ' + QUOTENAME(@archiveSchema) + N'.' + QUOTENAME(@archiveTable) WHERE OBJECTPROPERTY(@archiveObjectId, 'TableHasIdentity') = 1
                UNION ALL SELECT 37, 16, N'ERROR[TA7]: column is missing from source table:' + @clusteredName WHERE NOT EXISTS(SELECT 1 FROM @listSourceColumns WHERE [name] = @clusteredName)
                ;
            END TRY
            BEGIN CATCH
                IF @@TRANCOUNT > 0 ROLLBACK;
                SET @message = N'ERROR[TA0]: error(s) occured while checking archive table';
                THROW;
            END CATCH
        END   

        ----------------------------------------------------------------------------------------------------      
        -- Create or Update Synonym / Table / Column
        ----------------------------------------------------------------------------------------------------
        IF NOT EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10)
        BEGIN
            -- Get list of Columns from source table
            SELECT @SourceColumns = NULL;
            DELETE col FROM @listSourceColumns col WHERE EXISTS(SELECT 1 FROM @listExcludeColumns WHERE [value] = col.[name])
            SELECT @SourceColumns = COALESCE(@SourceColumns + N', "' + QUOTENAME([name]) + N'"', N'"' + QUOTENAME([name]) + N'"') FROM @listSourceColumns col ORDER BY Id;
            SELECT @SourceColumns = N'['+ @SourceColumns + N']';

            -- Start Transaction for all upcoming schema changes
            BEGIN TRAN
            -- Create Archive synonym if missing or outdated
            BEGIN TRY
                IF NOT EXISTS (SELECT 1 FROM sys.synonyms WHERE [name] = @SynonymArchiveName AND schema_id = SCHEMA_ID(@SynonymArchiveSchema) AND base_object_name = @archiveTable2Parts) AND @archiveTable2Parts <> N'' AND @CreateOrUpdateSynonym = 1
                BEGIN
                    SET @sql = NULL;
                    INSERT INTO @json_errors([id], [severity], [message]) SELECT 10, 10, N'Create or alter Archive Synonym '+ QUOTENAME(@SynonymArchiveSchema) + N'.' + QUOTENAME(@SynonymArchiveName) + ' with base object: ' + @archiveTable2Parts;
                    SELECT @sql = N'DROP SYNONYM ' + QUOTENAME(@SynonymArchiveSchema) + N'.' + QUOTENAME(@SynonymArchiveName) + N';' FROM sys.synonyms WHERE [name] = @SynonymArchiveName AND schema_id = SCHEMA_ID(@SynonymArchiveSchema);
                    SELECT @sql = ISNULL(@sql, N'') + N'CREATE SYNONYM ' + QUOTENAME(@SynonymArchiveSchema) + N'.' + QUOTENAME(@SynonymArchiveName) + N' FOR ' + @archiveTable2Parts + N';';
                    EXEC sp_executesql @statement = @sql;
                END
            END TRY
            BEGIN CATCH
                IF @@TRANCOUNT > 0 ROLLBACK;
                SET @message = N'ERROR[SU1]: error(s) occured while creating or updating Archive synomym';
                THROW;
            END CATCH
            -- Create Archive base table or Add missing column(s)
            BEGIN TRY
                IF ( (@archiveObjectId IS NULL AND @CreateTable = 1) OR @UpdateTable = 1) AND EXISTS( SELECT 1 FROM @listSourceColumns src WHERE [name] <> @clusteredName AND NOT EXISTS(SELECT 1 FROM @listArchiveColumns WHERE [name] = src.[name]) )
                BEGIN
                    -- Prepare CREATE/ALTER statement
                    SELECT @sql = NULL;
                    SELECT @sql = COALESCE(@sql + N', ' + q.[query], q.[query])
                    FROM (
                        SELECT TOP(1024) [query] = QUOTENAME(src.[name]) + N' ' + src.[datatype] + N' NULL' + CHAR(13) + CHAR(10) 
                        FROM @listSourceColumns src WHERE [name] <> @clusteredName AND NOT EXISTS(SELECT 1 FROM @listArchiveColumns WHERE [name] = src.[name]) AND src.[datatype] <> N'timestamp' --AND @UpdateTable <> 1 AND @archiveObjectId IS NOT NULL
                        ORDER BY Id ASC
                    ) q([query]);
                    SELECT @message = COALESCE(@message + N', ' + src.[name], src.[name]) FROM @listSourceColumns src WHERE NOT EXISTS(SELECT 1 FROM @listArchiveColumns WHERE [name] = src.[name]) ORDER BY Id ASC;

                    IF @archiveObjectId IS NULL --> CREATE
                    BEGIN
                        INSERT INTO @json_errors([id], [severity], [message])
                        SELECT 30, 10, N'Create table refered to by archive synonym: ' + QUOTENAME(@SynonymArchiveSchema) + N'.' + QUOTENAME(@SynonymArchiveName) + N' => '+ QUOTENAME(@archiveSchema) + N'.' + QUOTENAME(@archiveTable) + N' (' + @message + N')';
                        SELECT @sql = N'CREATE TABLE ' + QUOTENAME(@archiveSchema) + '.' + QUOTENAME(@archiveTable) + '(' + QUOTENAME([name]) + ' ['+ datatype + '] NOT NULL CONSTRAINT [PK_' + @archiveSchema + '.'+ @archiveTable + '] PRIMARY KEY CLUSTERED ([Id] ASC)' + CHAR(13) + CHAR(10) + N', ' + @sql + N');'
                        FROM @listSourceColumns WHERE [name] = @clusteredName;
                    END
                    ELSE --> ALTER
                    BEGIN
                        INSERT INTO @json_errors([id], [severity], [message])
                        SELECT 30, 10, N'Add missing column(s) to table refered to by archive synonym: ' + QUOTENAME(@SynonymArchiveSchema) + N'.' + QUOTENAME(@SynonymArchiveName) + N' => '+ QUOTENAME(@archiveSchema) + N'.' + QUOTENAME(@archiveTable) + N' (' + @message + N')';
                        SELECT @sql = N'ALTER TABLE '+ QUOTENAME(@archiveSchema) + '.' + QUOTENAME(@archiveTable) + ' ADD ' + @sql + N';';
                    END

                    -- Execute create/alter table
                    EXEC sp_executesql @stmt = @sql;
                END
            END TRY
            BEGIN CATCH
                IF @@TRANCOUNT > 0 ROLLBACK;
                SET @message = N'ERROR[SU2]: error(s) occured while creating or updating source table';
                THROW;
            END CATCH
            SET @message = NULL;
            IF @@TRANCOUNT > 0 COMMIT
        END
    END TRY
    BEGIN CATCH
        SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
        IF @@TRANCOUNT > 0 ROLLBACK;
    END CATCH

--    SELECT TOP(100) 'message'= 'output', [Message] = LEFT([message], 4000), [Severity] = [severity], [State] = 1 FROM @json_errors ORDER BY [id] ASC
    -- Check / Set @IsValid flag
    SET @IsValid = IIF(NOT EXISTS(SELECT 1 FROM @json_errors WHERE [severity] > 10) AND @ERROR_NUMBER IS NULL AND @message IS NULL, 1, 0);

    SET @Messages = --ISNULL(
    ( 
        SELECT [Procedure] = QUOTENAME(COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?')) + N'.' + QUOTENAME(COALESCE(OBJECT_NAME(@@PROCID), N'?')), [Message], [Severity], [State] 
        FROM (
            SELECT [Message], [Severity], [State] FROM (
                SELECT TOP(100) [Message] = LEFT([message], 4000), [Severity] = [severity], [State] = 1 FROM @json_errors ORDER BY [id] ASC
            ) err
            UNION ALL SELECT N'ERROR: ' + @ERROR_MESSAGE, 16, 1 WHERE @ERROR_MESSAGE IS NOT NULL
            UNION ALL SELECT @message, 16, 1 WHERE @message IS NOT NULL
        ) jsn
        FOR JSON PATH)
    --    , N'[]')
    ;
    RETURN 0;
END
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- DROP PROCEDURE [Maintenance].[SetSourceTableTenants]
----------------------------------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[SetSourceTableTenants]') AND type in (N'P'))
BEGIN
    EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [Maintenance].[SetSourceTableTenants] AS'
    PRINT '  + CREATE PROCEDURE: [Maintenance].[SetSourceTableTenants]';
END
ELSE PRINT '  = PROCEDURE [Maintenance].[SetSourceTableTenants] already exists' 
GO

PRINT '  ~ UPDATE PROCEDURE: [Maintenance].[SetSourceTableTenants]'
GO

ALTER PROCEDURE [Maintenance].[SetSourceTableTenants]
----------------------------------------------------------------------------------------------------
-- ### [Object]: PROCEDURE [Maintenance].[SetSourceTableTenants]
-- ### [Version]: 2023-10-06T11:29:36+02:00
-- ### [Source]: _src/Archive/ArchiveDB/Procedure_ArchiveDB.Maintenance.SetSourceTableTenants.sql
-- ### [Hash]: dc39c27 [SHA256-74B9F309996353A8903409232EB175195D9750AAFE7F4EA80EBD411C921E8007]
-- ### [Docs]: https://???.???
-- !!! ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!!!
-- !!! ~~~~~~~~~ NOT OFFICIALLY SUPPORTED BY UIPATH 
-- !!! ~~~~~~~~~ SQL Server >= 2016 SP1
----------------------------------------------------------------------------------------------------
	@SourceTableFullParts nvarchar(256) = NULL
    , @CreateOrUpdateSynonym bit = 0
    , @Columns nvarchar(MAX) = NULL OUTPUT
    , @IsValid bit = 0 OUTPUT
    , @Messages nvarchar(MAX) = NULL OUTPUT
AS
BEGIN
    BEGIN TRY 
        SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
		SET LOCK_TIMEOUT 5000;
        SET ARITHABORT ON;
        SET NOCOUNT ON;
        SET NUMERIC_ROUNDABORT OFF;

        ----------------------------------------------------------------------------------------------------
        -- Settings
        ----------------------------------------------------------------------------------------------------
        DECLARE @synonymSourceName nvarchar(256) = N'Synonym_Source_Tenants';
        DECLARE @synonymSourceSchema nvarchar(256) = N'Maintenance';
        DECLARE @clusteredName nvarchar(128) = N'Id';
        ----------------------------------------------------------------------------------------------------      
        -- Message / Error Handling
        ----------------------------------------------------------------------------------------------------
        DECLARE @message nvarchar(MAX);
        DECLARE @ERROR_NUMBER INT, @ERROR_SEVERITY INT, @ERROR_STATE INT, @ERROR_PROCEDURE NVARCHAR(126), @ERROR_LINE INT, @ERROR_MESSAGE NVARCHAR(2048) ;

        ----------------------------------------------------------------------------------------------------
        -- Call Main Checks Procedure      
        ----------------------------------------------------------------------------------------------------
        EXEC [Maintenance].[SetSourceTable]
            @SynonymName = @synonymSourceName
            , @SynonymSchema = @synonymSourceSchema
            , @SourceTableFullParts = @SourceTableFullParts
            , @SourceExpectedColumns = N'[{"column":"Id","type":"int"}, {"column":"Key","type":"nvarchar"}, {"column":"Name","type":"nvarchar"}, {"column":"IsDeleted","type":"bit"}]'
            , @CreateOrUpdateSynonym = @CreateOrUpdateSynonym
            , @Columns = @Columns OUTPUT
            , @IsValid = @IsValid OUTPUT
            , @Messages = @Messages OUTPUT
        ;
    END TRY
    BEGIN CATCH
        SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
        SET @message = N'ERROR[CH0]: error(s) occured while checking source Tenants table';
        SET @Messages = 
        ( 
            SELECT [Procedure] = QUOTENAME(COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?')) + N'.' + QUOTENAME(COALESCE(OBJECT_NAME(@@PROCID), N'?')), [Message], [Severity], [State] 
            FROM (
                SELECT [Message]  = N'ERROR: ' + @ERROR_MESSAGE, [Severity] = 16, [State] = 1 WHERE @ERROR_MESSAGE IS NOT NULL
                UNION ALL SELECT @message, 16, 1 WHERE @message IS NOT NULL
            ) jsn
            FOR JSON PATH
        );
        SET @IsValid = 0;
    END CATCH
    RETURN 0;
END
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- DROP PROCEDURE [Maintenance].[ValidateArchiveObjectsRobotLicenseLogs]
----------------------------------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[ValidateArchiveObjectsRobotLicenseLogs]') AND type in (N'P'))
BEGIN
    EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [Maintenance].[ValidateArchiveObjectsRobotLicenseLogs] AS'
    PRINT '  + CREATE PROCEDURE: [Maintenance].[ValidateArchiveObjectsRobotLicenseLogs]';
END
ELSE PRINT '  = PROCEDURE [Maintenance].[ValidateArchiveObjectsRobotLicenseLogs] already exists' 
GO

PRINT '  ~ UPDATE PROCEDURE: [Maintenance].[ValidateArchiveObjectsRobotLicenseLogs]'
GO

ALTER PROCEDURE [Maintenance].[ValidateArchiveObjectsRobotLicenseLogs]
----------------------------------------------------------------------------------------------------
-- ### [Object]: PROCEDURE [Maintenance].[ValidateArchiveObjectsRobotLicenseLogs]
-- ### [Version]: 2023-09-07T18:38:18+02:00
-- ### [Source]: _src/Archive/ArchiveDB/RobotLicenseLogs/Procedure_ArchiveDB.Maintenance.ValidateArchiveObjectsRobotLicenseLogs.sql
-- ### [Hash]: b0839d6 [SHA256-FB3F9BEADD6511BAF11596359827BDCA23F3B6D43C9D869CCC0D7ECA0140D186]
-- ### [Docs]: https://???.???
-- !!! ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!!!
-- !!! ~~~~~~~~~ NOT OFFICIALLY SUPPORTED BY UIPATH 
-- !!! ~~~~~~~~~ SQL Server >= 2016 SP1
----------------------------------------------------------------------------------------------------
	@ArchiveTableFullParts nvarchar(256) = NULL
	, @SourceTableFullParts nvarchar(256) = NULL
    , @ASyncStatusTableFullParts nvarchar(256) = NULL
    , @ExcludeColumns nvarchar(MAX) = NULL
    , @IgnoreMissingColumns nvarchar(MAX) = NULL
    , @CreateOrUpdateSynonyms bit = 0
    , @CreateTable bit = 0
    , @UpdateTable bit = 0
    , @SourceColumns nvarchar(MAX) = NULL OUTPUT
    , @IsValid bit = 0 OUTPUT
    , @Messages nvarchar(MAX) = NULL OUTPUT
AS
BEGIN
    BEGIN TRY 
        SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
		SET LOCK_TIMEOUT 5000;
        SET ARITHABORT ON;
        SET NOCOUNT ON;
        SET NUMERIC_ROUNDABORT OFF;
        ----------------------------------------------------------------------------------------------------
        -- Settings
        ----------------------------------------------------------------------------------------------------
        DECLARE @synonymSourceName nvarchar(256) = N'Synonym_Source_RobotLicenseLogs';
        DECLARE @synonymSourceSchema nvarchar(256) = N'Maintenance';
        DECLARE @synonymArchiveName nvarchar(256) = N'Synonym_Archive_RobotLicenseLogs';
        DECLARE @synonymArchiveSchema nvarchar(256) = N'Maintenance';
        DECLARE @synonymASyncStatusName nvarchar(256) = N'Synonym_ASyncStatus_RobotLicenseLogs';
        DECLARE @synonymASyncStatusSchema nvarchar(256) = N'Maintenance';
        DECLARE @clusteredName nvarchar(128) = N'Id';
        ----------------------------------------------------------------------------------------------------      
        -- Message / Error Handling
        ----------------------------------------------------------------------------------------------------
        DECLARE @message nvarchar(MAX);
        DECLARE @ERROR_NUMBER INT, @ERROR_SEVERITY INT, @ERROR_STATE INT, @ERROR_PROCEDURE NVARCHAR(126), @ERROR_LINE INT, @ERROR_MESSAGE NVARCHAR(2048) ;

        ----------------------------------------------------------------------------------------------------
        -- Call Main Checks Procedure      
        ----------------------------------------------------------------------------------------------------
        EXEC [Maintenance].[ValidateArchiveObjects]
            @SynonymSourceName = @synonymSourceName
            , @SynonymSourceSchema = @synonymSourceSchema
            , @SynonymArchiveName = @synonymArchiveName
            , @SynonymArchiveSchema = @synonymArchiveSchema
            , @synonymASyncStatusName = @synonymASyncStatusName
            , @synonymASyncStatusSchema = @synonymASyncStatusSchema
            , @ClusteredName = @clusteredName
            , @ArchiveTableFullParts = @ArchiveTableFullParts
            , @SourceTableFullParts = @SourceTableFullParts
            , @ExcludeColumns = @ExcludeColumns
            , @IgnoreMissingColumns = @IgnoreMissingColumns
            , @ASyncStatusTableFullParts = @ASyncStatusTableFullParts
            , @CreateOrUpdateSynonyms = @CreateOrUpdateSynonyms
            , @CreateTable = @CreateTable
            , @UpdateTable = @UpdateTable
            , @SourceColumns = @SourceColumns OUTPUT
            , @IsValid = @IsValid OUTPUT
            , @Messages = @Messages  OUTPUT
        ;
    END TRY
    BEGIN CATCH
        SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
        SET @message = N'ERROR[CH0]: error(s) occured while checking source and archive Audit Logs objects';
        SET @Messages = 
        ( 
            SELECT [Procedure] = QUOTENAME(COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?')) + N'.' + QUOTENAME(COALESCE(OBJECT_NAME(@@PROCID), N'?')), [Message], [Severity], [State] 
            FROM (
                SELECT [Message]  = N'ERROR: ' + @ERROR_MESSAGE, [Severity] = 16, [State] = 1 WHERE @ERROR_MESSAGE IS NOT NULL
                UNION ALL SELECT @message, 16, 1 WHERE @message IS NOT NULL
            ) jsn
            FOR JSON PATH
        );
        SET @IsValid = 0;
    END CATCH
    RETURN 0;
END
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- DROP PROCEDURE [Maintenance].[SetSourceTableRobotLicenseLogs]
----------------------------------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[SetSourceTableRobotLicenseLogs]') AND type in (N'P'))
BEGIN
    EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [Maintenance].[SetSourceTableRobotLicenseLogs] AS'
    PRINT '  + CREATE PROCEDURE: [Maintenance].[SetSourceTableRobotLicenseLogs]';
END
ELSE PRINT '  = PROCEDURE [Maintenance].[SetSourceTableRobotLicenseLogs] already exists' 
GO

PRINT '  ~ UPDATE PROCEDURE: [Maintenance].[SetSourceTableRobotLicenseLogs]'
GO

ALTER PROCEDURE [Maintenance].[SetSourceTableRobotLicenseLogs]
----------------------------------------------------------------------------------------------------
-- ### [Object]: PROCEDURE [Maintenance].[SetSourceTableRobotLicenseLogs]
-- ### [Version]: 2023-10-06T11:29:36+02:00
-- ### [Source]: _src/Archive/ArchiveDB/RobotLicenseLogs/Procedure_ArchiveDB.Maintenance.SetSourceTableRobotLicenseLogs.sql
-- ### [Hash]: dc39c27 [SHA256-4C24CCDF540BBE1228DC48E30DD5123F822E099DFC5EAD3FB0C31EC92B4303F6]
-- ### [Docs]: https://???.???
-- !!! ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!!!
-- !!! ~~~~~~~~~ NOT OFFICIALLY SUPPORTED BY UIPATH 
-- !!! ~~~~~~~~~ SQL Server >= 2016 SP1
----------------------------------------------------------------------------------------------------
	@SourceTableFullParts nvarchar(256) = NULL
    , @CreateOrUpdateSynonym bit = 0
    , @Columns nvarchar(MAX) = NULL OUTPUT
    , @IsValid bit = 0 OUTPUT
    , @Messages nvarchar(MAX) = NULL OUTPUT
AS
BEGIN
    BEGIN TRY 
        SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
		SET LOCK_TIMEOUT 5000;
        SET ARITHABORT ON;
        SET NOCOUNT ON;
        SET NUMERIC_ROUNDABORT OFF;

        ----------------------------------------------------------------------------------------------------
        -- Settings
        ----------------------------------------------------------------------------------------------------
        DECLARE @synonymSourceName nvarchar(256) = N'Synonym_Source_RobotLicenseLogs';
        DECLARE @synonymSourceSchema nvarchar(256) = N'Maintenance';
        DECLARE @clusteredName nvarchar(128) = N'Id';
        ----------------------------------------------------------------------------------------------------      
        -- Message / Error Handling
        ----------------------------------------------------------------------------------------------------
        DECLARE @message nvarchar(MAX);
        DECLARE @ERROR_NUMBER INT, @ERROR_SEVERITY INT, @ERROR_STATE INT, @ERROR_PROCEDURE NVARCHAR(126), @ERROR_LINE INT, @ERROR_MESSAGE NVARCHAR(2048) ;

        ----------------------------------------------------------------------------------------------------
        -- Call Main Checks Procedure      
        ----------------------------------------------------------------------------------------------------
        EXEC [Maintenance].[SetSourceTable]
            @SynonymName = @synonymSourceName
            , @SynonymSchema = @synonymSourceSchema
            , @SourceTableFullParts = @SourceTableFullParts
            , @SourceExpectedColumns = N'[{"column":"Id","type":"bigint"}, {"column":"TenantId","type":"int"}, {"column":"StartDate","type":"datetime"}, {"column":"EndDate","type":"datetime"}]'
            , @CreateOrUpdateSynonym = @CreateOrUpdateSynonym
            , @Columns = @Columns OUTPUT
            , @IsValid = @IsValid OUTPUT
            , @Messages = @Messages OUTPUT
        ;
    END TRY
    BEGIN CATCH
        SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
        SET @message = N'ERROR[CH0]: error(s) occured while checking source RobotLicenseLogs table';
        SET @Messages = 
        ( 
            SELECT [Procedure] = QUOTENAME(COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?')) + N'.' + QUOTENAME(COALESCE(OBJECT_NAME(@@PROCID), N'?')), [Message], [Severity], [State] 
            FROM (
                SELECT [Message]  = N'ERROR: ' + @ERROR_MESSAGE, [Severity] = 16, [State] = 1 WHERE @ERROR_MESSAGE IS NOT NULL
                UNION ALL SELECT @message, 16, 1 WHERE @message IS NOT NULL
            ) jsn
            FOR JSON PATH
        );
        SET @IsValid = 0;
    END CATCH
    RETURN 0;
END
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- DROP PROCEDURE [Maintenance].[SetSourceTableASyncStatus_RobotLicenseLogs]
----------------------------------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[SetSourceTableASyncStatus_RobotLicenseLogs]') AND type in (N'P'))
BEGIN
    EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [Maintenance].[SetSourceTableASyncStatus_RobotLicenseLogs] AS'
    PRINT '  + CREATE PROCEDURE: [Maintenance].[SetSourceTableASyncStatus_RobotLicenseLogs]';
END
ELSE PRINT '  = PROCEDURE [Maintenance].[SetSourceTableASyncStatus_RobotLicenseLogs] already exists' 
GO

PRINT '  ~ UPDATE PROCEDURE: [Maintenance].[SetSourceTableASyncStatus_RobotLicenseLogs]'
GO

ALTER PROCEDURE [Maintenance].[SetSourceTableASyncStatus_RobotLicenseLogs]
----------------------------------------------------------------------------------------------------
-- ### [Object]: PROCEDURE [Maintenance].[SetSourceTableASyncStatus_RobotLicenseLogs]
-- ### [Version]: 2023-10-06T11:29:36+02:00
-- ### [Source]: _src/Archive/ArchiveDB/RobotLicenseLogs/Procedure_ArchiveDB.Maintenance.SetSourceTableASyncStatus_RobotLicenseLogs.sql
-- ### [Hash]: dc39c27 [SHA256-C3D64B9B7753847463253687E9ECC4753B00D17CD4B65C12528902EBD3A8D55D]
-- ### [Docs]: https://???.???
-- !!! ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!!!
-- !!! ~~~~~~~~~ NOT OFFICIALLY SUPPORTED BY UIPATH 
-- !!! ~~~~~~~~~ SQL Server >= 2016 SP1
----------------------------------------------------------------------------------------------------
	@SourceTableFullParts nvarchar(256) = NULL
    , @CreateOrUpdateSynonym bit = 0
    , @Columns nvarchar(MAX) = NULL OUTPUT
    , @IsValid bit = 0 OUTPUT
    , @Messages nvarchar(MAX) = NULL OUTPUT
AS
BEGIN
    BEGIN TRY 
        SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
		SET LOCK_TIMEOUT 5000;
        SET ARITHABORT ON;
        SET NOCOUNT ON;
        SET NUMERIC_ROUNDABORT OFF;

        ----------------------------------------------------------------------------------------------------
        -- Settings
        ----------------------------------------------------------------------------------------------------
        DECLARE @synonymSourceName nvarchar(256) = N'Synonym_ASyncStatus_RobotLicenseLogs';
        DECLARE @synonymSourceSchema nvarchar(256) = N'Maintenance';
        DECLARE @clusteredName nvarchar(128) = N'Id';
        ----------------------------------------------------------------------------------------------------      
        -- Message / Error Handling
        ----------------------------------------------------------------------------------------------------
        DECLARE @message nvarchar(MAX);
        DECLARE @ERROR_NUMBER INT, @ERROR_SEVERITY INT, @ERROR_STATE INT, @ERROR_PROCEDURE NVARCHAR(126), @ERROR_LINE INT, @ERROR_MESSAGE NVARCHAR(2048) ;

        ----------------------------------------------------------------------------------------------------
        -- Call Main Checks Procedure      
        ----------------------------------------------------------------------------------------------------
        EXEC [Maintenance].[SetSourceTable]
            @SynonymName = @synonymSourceName
            , @SynonymSchema = @synonymSourceSchema
            , @SourceTableFullParts = @SourceTableFullParts
            , @SourceExpectedColumns = N'[{"column":"SyncId","type":"bigint"}, {"column":"IsDeleted","type":"bit"}]'
            , @CreateOrUpdateSynonym = @CreateOrUpdateSynonym
            , @Columns = @Columns OUTPUT
            , @IsValid = @IsValid OUTPUT
            , @Messages = @Messages OUTPUT
        ;
    END TRY
    BEGIN CATCH
        SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
        SET @message = N'ERROR[CH0]: error(s) occured while checking source ASyncStatus_RobotLicenseLogs table';
        SET @Messages = 
        ( 
            SELECT [Procedure] = QUOTENAME(COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?')) + N'.' + QUOTENAME(COALESCE(OBJECT_NAME(@@PROCID), N'?')), [Message], [Severity], [State] 
            FROM (
                SELECT [Message]  = N'ERROR: ' + @ERROR_MESSAGE, [Severity] = 16, [State] = 1 WHERE @ERROR_MESSAGE IS NOT NULL
                UNION ALL SELECT @message, 16, 1 WHERE @message IS NOT NULL
            ) jsn
            FOR JSON PATH
        );
        SET @IsValid = 0;
    END CATCH
    RETURN 0;
END
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- DROP PROCEDURE [Maintenance].[SetArchiveTableRobotLicenseLogs]
----------------------------------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[SetArchiveTableRobotLicenseLogs]') AND type in (N'P'))
BEGIN
    EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [Maintenance].[SetArchiveTableRobotLicenseLogs] AS'
    PRINT '  + CREATE PROCEDURE: [Maintenance].[SetArchiveTableRobotLicenseLogs]';
END
ELSE PRINT '  = PROCEDURE [Maintenance].[SetArchiveTableRobotLicenseLogs] already exists' 
GO

PRINT '  ~ UPDATE PROCEDURE: [Maintenance].[SetArchiveTableRobotLicenseLogs]'
GO

ALTER PROCEDURE [Maintenance].[SetArchiveTableRobotLicenseLogs]
----------------------------------------------------------------------------------------------------
-- ### [Object]: PROCEDURE [Maintenance].[SetArchiveTableRobotLicenseLogs]
-- ### [Version]: 2023-10-06T11:29:36+02:00
-- ### [Source]: _src/Archive/ArchiveDB/RobotLicenseLogs/Procedure_ArchiveDB.Maintenance.SetArchiveTableRobotLicenseLogs.sql
-- ### [Hash]: dc39c27 [SHA256-EBBF8908622FE17E842EB3E1AF7F162F4BDD043F749C3B29137DC0A4BBDAF55E]
-- ### [Docs]: https://???.???
-- !!! ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!!!
-- !!! ~~~~~~~~~ NOT OFFICIALLY SUPPORTED BY UIPATH 
-- !!! ~~~~~~~~~ SQL Server >= 2016 SP1
----------------------------------------------------------------------------------------------------
	@ArchiveTableName nvarchar(250) = NULL
	, @ArchiveTableSchema nvarchar(250) = NULL
    , @ExcludeColumns nvarchar(MAX) = NULL
    , @IgnoreMissingColumns nvarchar(MAX) = NULL
    , @CreateOrUpdateSynonym bit = 1
    , @CreateTable bit = 1
    , @UpdateTable bit = 1
    , @RemoveIdentity bit = 0
    , @SourceColumns nvarchar(MAX) = NULL OUTPUT
    , @IsValid bit = 0 OUTPUT
    , @Messages nvarchar(MAX) = NULL OUTPUT
AS
BEGIN
    BEGIN TRY 
        SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
		SET LOCK_TIMEOUT 5000;
        SET ARITHABORT ON;
        SET NOCOUNT ON;
        SET NUMERIC_ROUNDABORT OFF;

        ----------------------------------------------------------------------------------------------------
        -- Settings
        ----------------------------------------------------------------------------------------------------
        DECLARE @synonymSourceName nvarchar(256) = N'Synonym_Source_RobotLicenseLogs';
        DECLARE @synonymSourceSchema nvarchar(256) = N'Maintenance';
        DECLARE @synonymArchiveName nvarchar(256) = N'Synonym_Archive_RobotLicenseLogs';
        DECLARE @synonymArchiveSchema nvarchar(256) = N'Maintenance';
        DECLARE @clusteredName nvarchar(128) = N'Id';
        ----------------------------------------------------------------------------------------------------      
        -- Message / Error Handling
        ----------------------------------------------------------------------------------------------------
        DECLARE @message nvarchar(MAX);
        DECLARE @ERROR_NUMBER INT, @ERROR_SEVERITY INT, @ERROR_STATE INT, @ERROR_PROCEDURE NVARCHAR(126), @ERROR_LINE INT, @ERROR_MESSAGE NVARCHAR(2048) ;

        ----------------------------------------------------------------------------------------------------
        -- Call Main Checks Procedure      
        ----------------------------------------------------------------------------------------------------
        EXEC [Maintenance].[SetArchiveTable]
            @SynonymSourceName = @synonymSourceName, @SynonymSourceSchema = @synonymSourceSchema
            , @SynonymArchiveName = @synonymArchiveName, @SynonymArchiveSchema = @synonymArchiveSchema
            , @ArchiveTableName = @ArchiveTableName, @ArchiveTableSchema = @ArchiveTableSchema
            , @ClusteredName = @clusteredName
            , @ExcludeColumns = @ExcludeColumns
            , @IgnoreMissingColumns = @IgnoreMissingColumns
            , @CreateOrUpdateSynonym = @CreateOrUpdateSynonym
            , @CreateTable = @CreateTable
            , @UpdateTable = @UpdateTable
            , @RemoveIdentity = @RemoveIdentity
            , @SourceColumns = @SourceColumns OUTPUT
            , @IsValid = @IsValid OUTPUT
            , @Messages = @messages OUTPUT
        ;
    END TRY
    BEGIN CATCH
        SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
        SET @message = N'ERROR[CH0]: error(s) occured while checking archive RobotLicenseLogs objects';
        SET @Messages = 
        ( 
            SELECT [Procedure] = QUOTENAME(COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?')) + N'.' + QUOTENAME(COALESCE(OBJECT_NAME(@@PROCID), N'?')), [Message], [Severity], [State] 
            FROM (
                SELECT [Message]  = N'ERROR: ' + @ERROR_MESSAGE, [Severity] = 16, [State] = 1 WHERE @ERROR_MESSAGE IS NOT NULL
                UNION ALL SELECT @message, 16, 1 WHERE @message IS NOT NULL
            ) jsn
            FOR JSON PATH
        );
        SET @IsValid = 0;
    END CATCH
    RETURN 0;
END
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO


----------------------------------------------------------------------------------------------------
-- DROP PROCEDURE [Maintenance].[ParseJsonArchiveRobotLicenseLogs]
----------------------------------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[ParseJsonArchiveRobotLicenseLogs]') AND type in (N'P'))
BEGIN
        EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [Maintenance].[ParseJsonArchiveRobotLicenseLogs] AS'
    PRINT '  + CREATE PROCEDURE: [Maintenance].[ParseJsonArchiveRobotLicenseLogs]';
END
ELSE PRINT '  = PROCEDURE [Maintenance].[ParseJsonArchiveRobotLicenseLogs] already exists' 
GO

PRINT '  ~ UPDATE PROCEDURE: [Maintenance].[ParseJsonArchiveRobotLicenseLogs]'
GO

ALTER PROCEDURE [Maintenance].[ParseJsonArchiveRobotLicenseLogs]
----------------------------------------------------------------------------------------------------
-- ### [Object]: PROCEDURE [Maintenance].[ParseJsonArchiveRobotLicenseLogs]
-- ### [Version]: 2023-10-06T11:29:36+02:00
-- ### [Source]: _src/Archive/ArchiveDB/RobotLicenseLogs/Procedure_ArchiveDB.Maintenance.ParseJsonArchiveRobotLicenseLogs.sql
-- ### [Hash]: dc39c27 [SHA256-7E5F2C81B0F0009CDE6748088D812DF77FE8D89720F3D91791552069FDFA3153]
-- ### [Docs]: https://???.???
-- !!! ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!!!
-- !!! ~~~~~~~~~ NOT OFFICIALLY SUPPORTED BY UIPATH 
-- !!! ~~~~~~~~~ SQL Server >= 2016 SP1
----------------------------------------------------------------------------------------------------
	@Filters nvarchar(MAX)
    , @Settings nvarchar(MAX) OUTPUT
    , @Messages nvarchar(MAX) OUTPUT
	, @IsValid bit = 0 OUTPUT
    , @AfterHours int = NULL
    , @DeleteDelayHhours int = NULL
AS
BEGIN
    BEGIN TRY
        SET ARITHABORT ON;
        SET NOCOUNT ON;
        SET NUMERIC_ROUNDABORT OFF;

        ----------------------------------------------------------------------------------------------------
        -- Local Run Variables
        ----------------------------------------------------------------------------------------------------
        DECLARE @json nvarchar(MAX);

        -- JSON elements local tables
        DECLARE @jsons TABLE ([key] int, [value] nvarchar(MAX), [type] int, [type_name] nvarchar(10));
        DECLARE @jsonArray_elements TABLE([key] int, [name] nvarchar(MAX), [value] nvarchar(MAX), [type] int, [type_name] nvarchar(10));
        DECLARE @jsonArray_tenants TABLE([key] int, [value_name] nvarchar(100), [value_id] int, [Tenant_Name] nvarchar(128), [Tenant_Id] int, [keep] bit, [exclude] nvarchar(128), [IsDeleted] bit);
        DECLARE @jsonValues_tenants TABLE([key] int, [value_name] nvarchar(128), [value_id] int)--, [Tenant_Name] nvarchar(128), [TenantId] int);
        DECLARE @jsonValues_exclude TABLE([key] int, [value_name] nvarchar(128), [value_id] int)--, [Tenant_Name] nvarchar(128), [TenantId] int);
        DECLARE @json_errors TABLE([id] tinyint NOT NULL, [key] int NOT NULL, [message] nvarchar(MAX) NOT NULL);

        DECLARE @elements_settings TABLE([key] int, [delete_only] bit, [after_hours] int, [delete_delay_hours] int,  [disabled] bit);

        ----------------------------------------------------------------------------------------------------
        -- Constant / Default value
        ----------------------------------------------------------------------------------------------------
        DECLARE @json_types TABLE(id tinyint, [name] nvarchar(10));
        INSERT INTO @json_types(id, [name]) VALUES (0, 'null'), (1, 'string'), (2, 'number'), (3, 'true/false'), (4, 'array'), (5, 'object');

        ----------------------------------------------------------------------------------------------------      
        -- Message / Error Handling
        ----------------------------------------------------------------------------------------------------
        DECLARE @message nvarchar(MAX);
--        DECLARE @messages TABLE(id int IDENTITY(0, 1) PRIMARY KEY, [date] datetime2 DEFAULT SYSDATETIME(), [procedure] nvarchar(MAX) NOT NULL DEFAULT QUOTENAME(COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?')) + N'.' + QUOTENAME(COALESCE(OBJECT_NAME(@@PROCID), N'?')), [message] nvarchar(MAX) NOT NULL, severity tinyint NOT NULL, state tinyint NOT NULL, [number] int, [line] int);
        DECLARE @ERROR_NUMBER INT, @ERROR_SEVERITY INT, @ERROR_STATE INT, @ERROR_PROCEDURE NVARCHAR(126), @ERROR_LINE INT, @ERROR_MESSAGE NVARCHAR(2048) ;

		----------------------------------------------------------------------------------------------------
        -- Check JSON Errors
        ----------------------------------------------------------------------------------------------------
        SET @IsValid = 0;
		SET @json = LTRIM(RTRIM(REPLACE(REPLACE(@Filters, CHAR(10), N''), CHAR(13), N'')));

		SELECT @message = CASE WHEN ISNULL(@json, N'') = N'' THEN 'ERROR: Parameter @Filters is NULL or empty'
			WHEN ISJSON(@json) = 0 THEN 'ERROR: Parameter @Filters is not a valid json string'
			WHEN LEFT(@json, 1) = N'{' THEN 'ERROR: Parameter @Filters is a {} object literal'
			WHEN LEFT(@json, 1) <> N'[' THEN 'ERROR: Parameter @Filters is invalid'
			ELSE NULL END;

		IF @message IS NULL
		BEGIN
            BEGIN TRY
                INSERT INTO @jsons([key], [value], [type], [type_name])
                SELECT [key] + 1, [value], [type], tps.[name]
                FROM OPENJSON(@json) jsn
                INNER JOIN @json_types tps ON tps.id = jsn.[type]

    			IF @@ROWCOUNT = 0 SET @message = N'ERROR: Parameter @Filters array contains no {} object elements';
            END TRY
            BEGIN CATCH
                SET @message = ERROR_MESSAGE();
            END CATCH
        END

		IF @message IS NOT NULL
		BEGIN
            RAISERROR(N'a valid JSON string with a [] array of {} object literal(s) is expected', 16, 1)
		END

        SET @message = NULL;

        ----------------------------------------------------------------------------------------------------
        -- Parse and extract from JSON string
        ----------------------------------------------------------------------------------------------------
        -- get each element from each object in main array
        BEGIN TRY;
            INSERT INTO @jsonArray_elements([key], [name], [value], [type], [type_name])
                SELECT jsn.[key], name = CASE WHEN LTRIM(RTRIM(elm.[key])) IN (N'disable', N'disabled') THEN N'disabled' ELSE LTRIM(RTRIM(elm.[key])) END, LTRIM(RTRIM(elm.[value])), elm.[type], tps.[name]
                FROM @jsons jsn
                CROSS APPLY OPENJSON(jsn.[value]) elm
                INNER JOIN @json_types tps ON tps.id = elm.[type]
                WHERE jsn.[type] = 5 AND elm.[key] NOT IN (N'comment', N'comments');
        END TRY
        BEGIN CATCH;
            SET @message = N'ERROR[R1]: error(s) occured while retrieving elements from JSON string';
            THROW;
        END CATCH
        
        -- get defaut settigns for each objects in main aray
        BEGIN TRY;
            INSERT INTO @elements_settings([key], [delete_only], [after_hours], [delete_delay_hours], [disabled])
            SELECT jsn.[key]
                , [delete_only] = CASE WHEN ( SELECT COUNT(*) FROM @jsonArray_elements WHERE [key] = jsn.[key] AND [name] = N'delete_only' ) > 1 THEN 0 ELSE
                    ISNULL( ( SELECT MIN(ISNULL(IIF( ([type] = 3 AND [value] = N'true') OR ([type] = 2 AND [value] >= 1) , 1, 0), 0)) FROM @jsonArray_elements WHERE [key] = jsn.[key] AND [name] = N'delete_only' AND [type] IN (2, 3, 4) ), 0)
                    END
                , [after_hours] = (SELECT MAX([value]) FROM @jsonArray_elements WHERE [key] = jsn.[key] AND [name] = N'after_hours' AND [type] = 2 )
                , [delete_delay_hours] = (SELECT MAX([value]) FROM @jsonArray_elements WHERE [key] = jsn.[key] AND [name] = N'delete_delay_hours' AND [type] = 2 )
                , [disabled] = CASE WHEN ( SELECT COUNT(*) FROM @jsonArray_elements WHERE [key] = jsn.[key] AND [name] = N'disabled' ) > 1 THEN 0 ELSE
                    ISNULL( ( SELECT MIN(ISNULL(IIF( ([type] = 3 AND [value] = N'true') OR ([type] = 2 AND [value] >= 1) , 1, 0), 0)) FROM @jsonArray_elements WHERE [key] = jsn.[key] AND [name] = N'disabled' AND [type] IN (2, 3, 4) ), 0)
                    END
            FROM @jsons jsn
            WHERE jsn.[type] = 5
            ORDER BY [key];
        END TRY 
        BEGIN CATCH;
            SET @message = N'ERROR[R2]: error(s) occured while retrieving elements'' settings from JSON string';
            THROW;
        END CATCH;

        ----------------------------------------------------------------------------------------------------
        -- Parse and extract Tenants
        ----------------------------------------------------------------------------------------------------
        -- get all names and aliases from tenants objets 
        BEGIN TRY;
            INSERT INTO @jsonValues_tenants([key], [value_name], [value_id])
            SELECT DISTINCT elm.[key]
                , [value_name] =  LEFT(LTRIM(RTRIM( REPLACE( IIF(val.[type] = 1, val.[value], IIF(elm.[type] = 1, elm.[value], NULL) ), N'*', N'%') )), 128)
                , [value_id] =  CAST( IIF(val.[type] = 2, val.[value], IIF(elm.[type] = 2, elm.[value], NULL) ) AS int)
            FROM @jsonArray_elements elm
            INNER JOIN @elements_settings stg ON stg.[key] = elm.[key]
            OUTER APPLY OPENJSON( IIF(elm.[type] = 4, elm.[value], NULL) ) val
            WHERE elm.[name] IN (N'tenants') AND ( ( elm.[type] IN (1, 2) ) OR (elm.[type] = 4 AND val.[type] IN (1, 2) ) ) AND stg.[disabled] = 0;
        END TRY 
        BEGIN CATCH;
            SET @message = N'ERROR[R6]: error(s) occured while retrieving "tenants" name(s) and alias(es) from JSON string';
            THROW;
        END CATCH;

        -- get all names and aliases from exclude objets 
        BEGIN TRY
            INSERT INTO @jsonValues_exclude([key], [value_name], [value_id])
            SELECT DISTINCT elm.[key]
                , [value_name] =  LEFT(LTRIM(RTRIM( REPLACE( IIF(val.[type] = 1, val.[value], IIF(elm.[type] = 1, elm.[value], NULL) ), N'*', N'%') )), 128)
                , [value_id] =  CAST( IIF(val.[type] = 2, val.[value], IIF(elm.[type] = 2, elm.[value], NULL) ) AS int)
            FROM @jsonArray_elements elm
            INNER JOIN @elements_settings stg ON stg.[key] = elm.[key]
            OUTER APPLY OPENJSON( IIF(elm.[type] = 4, elm.[value], NULL) ) val
            WHERE elm.[name] IN (N'exclude') AND ( ( elm.[type] IN (1, 2) ) OR (elm.[type] = 4 AND val.[type] IN (1, 2) ) ) AND stg.[disabled] = 0;
        END TRY 
        BEGIN CATCH;
            SET @message = N'ERROR[R6]: error(s) occured while retrieving "tenants" name(s) and alias(es) from JSON string';
            THROW;
        END CATCH;

        -- extract tenant from JSON string and matches them with [Maintenance].[Synonym_Source_Tenants]
        BEGIN TRY;
            WITH list AS (
                -- exact matches
                SELECT [type] = 'exact', jst.[key], jst.[value_name], jst.[value_id], tnt.[Name], tnt.Id
                    , [keep] = IIF(tnt.Id IS NOT NULL, 1, 0)
                    , [exclude] = NULL
                    , IsDeleted = tnt.IsDeleted
                FROM @jsonValues_tenants jst
                LEFT JOIN [Maintenance].[Synonym_Source_Tenants] tnt ON tnt.Id = jst.[value_id] OR tnt.[Name] LIKE jst.[value_name]
                WHERE ( jst.[value_id] IS NOT NULL OR ( CHARINDEX(N'%', jst.[value_name], 1) = 0 AND jst.[value_name] NOT IN (N'#ACTIVE_TENANTS#', N'#OTHER_TENANTS#', N'#DELETED_TENANTS#') ) )
                UNION ALL
                -- filter matches
                SELECT [type] = N'partial', jst.[key], jst.[value_name], jst.[value_id], tnt.[Name], tnt.Id
                    , [keep] = IIF(NOT EXISTS(SELECT TOP(1) 1 FROM @jsonValues_exclude WHERE [key] = jst.[key] AND (tnt.[Name] LIKE [value_name] OR tnt.[Id] = [value_id]) ) AND tnt.IsDeleted = 0, 1, 0)
                    , [exclude] = (SELECT TOP(1) ISNULL([value_name], [value_id]) FROM @jsonValues_exclude WHERE [key] = jst.[key] AND (tnt.[Name] LIKE [value_name] OR tnt.[Id] = [value_id]) )
                    , IsDeleted = tnt.IsDeleted
                FROM @jsonValues_tenants jst
                LEFT JOIN [Maintenance].[Synonym_Source_Tenants] tnt ON tnt.Id = jst.[value_id] OR (tnt.[Name] LIKE jst.[value_name])
                WHERE jst.[value_id] IS NULL AND CHARINDEX(N'%', jst.[value_name], 1) > 0
                UNION ALL 
                -- alias
                SELECT [type] = IIF(jst.[value_name] = N'#ACTIVE_TENANTS#', N'active', N'deleted'), jst.[key], jst.[value_name], jst.[value_id], tnt.[Name], tnt.Id
                    , [keep] = IIF(NOT EXISTS(SELECT TOP(1) 1 FROM @jsonValues_exclude WHERE [key] = jst.[key] AND (tnt.[Name] LIKE [value_name] OR tnt.[Id] = [value_id])), 1, 0)
                    , [exclude] = (SELECT TOP(1) ISNULL([value_name], [value_id]) FROM @jsonValues_exclude WHERE [key] = jst.[key] AND (tnt.[Name] LIKE [value_name] OR tnt.[Id] = [value_id]) )
                    , IsDeleted = tnt.IsDeleted
                FROM @jsonValues_tenants jst
                INNER JOIN (VALUES(N'#ACTIVE_TENANTS#', 0, N'active'), (N'#DELETED_TENANTS#', 1, N'deleted') ) sts([name], [status], [type]) ON jst.[value_name] = sts.name
                LEFT JOIN [Maintenance].[Synonym_Source_Tenants] tnt ON tnt.IsDeleted = sts.[status]
                    AND jst.[value_name] = sts.[name]
                    AND NOT EXISTS(SELECT 1 FROM @jsonValues_tenants WHERE [key] <> jst.[key] AND value_name = jst.[value_name])
            )
            INSERT INTO @jsonArray_tenants([key], [value_name], [value_id], [Tenant_Name], [Tenant_Id], [keep], [exclude], [IsDeleted])
            SELECT lst.[key], lst.[value_name], lst.[value_id], lst.[Name], lst.Id, [keep], [exclude], [IsDeleted] FROM list lst
            UNION ALL
            -- others
            SELECT jst.[key], jst.[value_name], jst.[value_id], tnt.[Name], tnt.[Id]
                , [keep] = IIF(NOT EXISTS(SELECT TOP(1) 1 FROM @jsonValues_exclude WHERE [key] = jst.[key] AND (tnt.[Name] LIKE [value_name] OR tnt.[Id] = [value_id])), 1, 0)
                , [exclude] = (SELECT TOP(1) ISNULL([value_name], [value_id]) FROM @jsonValues_exclude WHERE [key] = jst.[key] AND (tnt.[Name] LIKE [value_name] OR tnt.[Id] = [value_id]) )
                , 0
            FROM @jsonValues_tenants jst
            CROSS APPLY (
                SELECT [name], [Id] FROM [Maintenance].[Synonym_Source_Tenants] WHERE [IsDeleted] = 0 AND NOT EXISTS( SELECT  1 FROM @jsonValues_tenants WHERE [key] <> jst.[key] AND value_name = N'#OTHER_TENANTS#') 
                UNION ALL 
                SELECT NULL, NULL WHERE EXISTS( SELECT  1 FROM @jsonValues_tenants WHERE [key] <> jst.[key] AND value_name = N'#OTHER_TENANTS#') 
                EXCEPT 
                SELECT [name], [Id] FROM list WHERE [Name] IS NOT NULL AND [keep] = 1
            ) tnt
            WHERE jst.[value_name] = N'#OTHER_TENANTS#';
        END TRY
        BEGIN CATCH;
            SET @message = N'ERROR[R7]: error(s) occured while matching Tenants table with "tenants" name(s) and alias(es) from JSON string';
            THROW;
        END CATCH;

        UPDATE @jsonArray_tenants SET [keep] = 0 WHERE [IsDeleted] IS NULL AND [value_name] IN (N'#ACTIVE_TENANTS#', N'#DELETED_TENANTS#')

        ----------------------------------------------------------------------------------------------------
        -- JSON string - keys and types checks
        ----------------------------------------------------------------------------------------------------
        -- 0 J0 array contains invalid type (not {}) 
        BEGIN TRY;
            INSERT INTO @json_errors([id], [key], [message])
            SELECT [id] = 0, [key] = jsn.[key], [message] = N'ERROR[J0]: array #' + CAST(jsn.[key] AS nvarchar(100)) + N' => invalid type "' + jsn.[type_name] COLLATE DATABASE_DEFAULT + N'" (only {} object literal expected)'
            FROM @jsons jsn 
            WHERE jsn.[type] <> 5;
        END TRY
        BEGIN CATCH;
            SET @message = N'ERROR[J0]: error(s) occured while checking invalid type(s) in main array in JSON string';
            THROW;
        END CATCH;

        BEGIN TRY;
            -- 1 - J1: array contains invalid key (not 'tenants', N'delete_only', N'after_hours', N'delete_delay_hours') 
            INSERT INTO @json_errors([id], [key], [message])
            SELECT DISTINCT [id] = 1, [key] = elm.[key], [message] = N'ERROR[J1]: array #' + CAST(elm.[key] AS nvarchar(10)) + N' => invalid key "' + elm.[name] COLLATE DATABASE_DEFAULT + N'"'
            FROM @jsonArray_elements elm 
            INNER JOIN @elements_settings sts ON sts.[key] = elm.[key]
            WHERE sts.[disabled] = 0 AND elm.[name] NOT IN (N'tenants', N'exclude', N'delete_only', N'after_hours', N'delete_delay_hours', N'disabled', N'comment', N'comments');
        END TRY
        BEGIN CATCH;
            SET @message = N'ERROR[J1]: error(s) occured while checking invalid key(s) in JSON string';
            THROW;
        END CATCH;

        BEGIN TRY;
            -- 2 - J2: missing key(s) in object ('tenants')
            INSERT INTO @json_errors([id], [key], [message])
            SELECT DISTINCT [id] = 2, [key] = jsn.[key], N'ERROR[J2]: array #' + CAST(jsn.[key] AS nvarchar(10)) + N' => missing key "' + v.[name]
            FROM @jsons jsn 
            INNER JOIN @elements_settings sts ON sts.[key] = jsn.[key]
            CROSS JOIN (VALUES(N'tenants')) v([name]) 
            WHERE sts.[disabled] = 0 AND NOT EXISTS(SELECT 1 FROM @jsonArray_elements WHERE [key] = jsn.[key] AND [name] = v.[name]);
        END TRY
        BEGIN CATCH;
            SET @message = N'ERROR[J2]: error(s) occured while checking invalid key(s) in JSON string';
            THROW;
        END CATCH;

        BEGIN TRY;
            -- 3 - J3: invalid type for key (tenants => number, string or array ; delete_only => true/false or 0/1 ; others => number)
            INSERT INTO @json_errors([id], [key], [message])
            SELECT DISTINCT [id] = 3, [key] = elm.[key], [message] = N'ERROR[J3]: array #' + CAST(elm.[key] AS nvarchar(10)) + N' => invalid "' + elm.[type_name] + '" type for key "' + elm.[name] + N'" (only ' +
                CASE WHEN elm.[name] IN (N'tenants', N'exclude') THEN N'number, string or array' 
                WHEN elm.[name] = N'delete_only' THEN N'true/false or 0/1' 
                WHEN elm.[name] = N'disabled' THEN N'true/false or 0/1' 
                ELSE N'number' END + N' expected)'
            FROM @elements_settings sts 
            INNER JOIN @jsonArray_elements elm ON elm.[key] = sts.[key] AND sts.[disabled] = 0
            WHERE (elm.[name] = N'tenants' AND elm.[type] NOT IN (1, 2, 4)) 
                OR (elm.[name] = N'delete_only' AND elm.[type] NOT IN (2, 3))
                OR (elm.[name] IN (N'after_hours', N'delete_delay_hours') AND elm.[type] <> 2)
                OR (elm.[name] = N'disabled' AND elm.[type] NOT IN (2, 3));
        END TRY
        BEGIN CATCH;
            SET @message = N'ERROR[J3]: error(s) occured while checking key(s) with invalid type(s) in JSON string';
            THROW;
        END CATCH;

        -- 4 - J4: duplicate elements
        BEGIN TRY
            INSERT INTO @json_errors([id], [key], [message])
            SELECT [id] = 4, [key] = elm.[key], [message] = N'ERROR[J4]: array #' + CAST(elm.[key] AS nvarchar(10)) + N' => duplicate "' + elm.[name] + '" found ' + CAST(COUNT(*) AS nvarchar(10)) + N' times (only 1 expected)'
            FROM @elements_settings sts 
            INNER JOIN @jsonArray_elements elm ON elm.[key] = sts.[key] AND sts.[disabled] = 0
            GROUP BY elm.[key], elm.[name]
            HAVING COUNT(*) > 1;
        END TRY
        BEGIN CATCH;
            SET @message = N'ERROR[J4]: error(s) occured while checking duplicate elements in JSON string';
            THROW;
        END CATCH;

        -- 5 - J5: empty tenants
        BEGIN TRY
            INSERT INTO @json_errors([id], [key], [message])
            SELECT DISTINCT [id] = 5, [key] =elm.[key], [message] = N'ERROR[J5]: array #' + CAST(elm.[key] AS nvarchar(10)) + N' => "' + elm.[name] + '" array is empty (' +  CASE WHEN elm.[name] IN (N'tenants', N'exclude') THEN N'number(s) or string(s)' ELSE N'archive or delete object(s)' END + N' expected)'
            FROM @elements_settings sts 
            INNER JOIN @jsonArray_elements elm ON elm.[key] = sts.[key] AND sts.[disabled] = 0
            OUTER APPLY OPENJSON(elm.[value]) val
            WHERE elm.[name] IN (N'tenants', N'exclude') AND elm.[type] = 4 AND val.[key] IS NULL;
        END TRY
        BEGIN CATCH;
            SET @message = N'ERROR[J5]: error(s) occured while checking empty array(s) ("tenants") in JSON string';
            THROW;
        END CATCH;

        ----------------------------------------------------------------------------------------------------
        -- JSON string - Tenants checks 
        ----------------------------------------------------------------------------------------------------
        -- 6 - T1: invalid type in tenants array (only number or string)
        BEGIN TRY;
            INSERT INTO @json_errors([id], [key], [message])
            SELECT DISTINCT [id] = 6, [key] = elm.[key], [message] = N'ERROR[T0]: array #' + CAST(elm.[key] AS nvarchar(10)) + N' => invalid "' + tps.[name] COLLATE DATABASE_DEFAULT + '" type in "' + elm.[name] + '" array (only ' + CASE WHEN elm.[name] IN (N'tenants', N'exclude') THEN N'number(s) or string(s)' ELSE N'object(s)' END + N' expected)'
            FROM @elements_settings sts 
            INNER JOIN @jsonArray_elements elm ON elm.[key] = sts.[key] AND sts.[disabled] = 0
            CROSS APPLY OPENJSON(elm.[value]) val
            INNER JOIN @json_types tps ON tps.id = val.[type]
            WHERE elm.[type] = 4 AND ( ( elm.[name] IN (N'tenants', N'exclude') AND val.[type] NOT IN (1, 2) ) );
        END TRY
        BEGIN CATCH;
            SET @message = N'ERROR[T0]: error(s) occured while checking invalid type(s) in "tenants" array(s) in JSON string';
            THROW;
        END CATCH;

        -- 7 - T2: missing tenants
        BEGIN TRY;
            INSERT INTO @json_errors([id], [key], [message])
            SELECT DISTINCT [id] = 7, [key] = tnt.[key], [message] = N'ERROR[T1]: array #' + CAST(tnt.[key] AS nvarchar(10)) + N' => tenant "'+ tnt.[value_name] + '" doesn''t exists'
            FROM @jsonArray_tenants tnt
            WHERE tnt.[Tenant_Id] IS NULL AND tnt.[value_name] NOT IN (N'#ACTIVE_TENANTS#', N'#OTHER_TENANTS#', N'#DELETED_TENANTS#');
        END TRY
        BEGIN CATCH;
            SET @message = N'ERROR[T1]: error(s) occured while checking existing "tenants" in JSON string';
            THROW;
        END CATCH;

        -- 8 - T3: duplicate keyword (#ACTIVE_TENANTS#, #OTHER_TENANTS#, #DELETED_TENANTS#)
        BEGIN TRY;
            INSERT INTO @json_errors([id], [key], [message])
            SELECT [id] = 8, [key] = tnt1.[key], [message] = N'ERROR[T2]: array #' + CAST(tnt1.[key] AS nvarchar(10)) + N' => duplicate keyword "'+ tnt1.[value_name] + N' found in element ' + CAST(tnt2.[key] AS nvarchar(10)) + N' (can be used only once)'
            FROM @jsonArray_tenants tnt1
            INNER JOIN @jsonArray_tenants tnt2 ON tnt1.[value_name] = tnt2.[value_name] AND tnt1.[key] < tnt2.[key] AND NOT EXISTS (SELECT 1 FROM @jsonArray_tenants WHERE [value_name] = tnt2.[value_name] AND [key] < tnt1.[key])
            WHERE tnt1.[Tenant_Id] IS NULL AND tnt1.[value_name] IN (N'#ACTIVE_TENANTS#', N'#OTHER_TENANTS#', N'#DELETED_TENANTS#');
        END TRY
        BEGIN CATCH;
            SET @message = N'ERROR[T2]: error(s) occured while checking duplicate keyword(s) in "tenants" in JSON string';
            THROW;
        END CATCH;

        -- 9 - T4: duplicate tenants
        BEGIN TRY;
            INSERT INTO @json_errors([id], [key], [message])
            SELECT [id] = 9, [key] = tnt1.[key], [message] = N'ERROR[T3]: array #' + CAST(tnt1.[key] AS nvarchar(10)) + N' => duplicate tenant "'+ tnt1.[Tenant_Name] + N'" (id=' + CAST(tnt1.[Tenant_Id] AS nvarchar(10)) + N', value=' + ISNULL(N'"'+ tnt1.[value_name] + N'"', tnt1.[value_id]) + N') found in #' + CAST(tnt2.[key] AS nvarchar(10)) + N' (value=' + ISNULL(N'"'+ tnt2.[value_name] + N'"', tnt2.[value_id]) + N')'
            FROM @jsonArray_tenants tnt1
            INNER JOIN @jsonArray_tenants tnt2 ON tnt1.Tenant_Name = tnt2.[Tenant_Name] AND tnt1.[key] < tnt2.[key] AND NOT EXISTS (SELECT 1 FROM @jsonArray_tenants WHERE [Tenant_Name] = tnt2.[Tenant_Name] AND [key] < tnt1.[key] AND [keep] = 1)
            WHERE tnt1.[keep] = 1 AND tnt2.[keep] = 1;
        END TRY
        BEGIN CATCH;
            SET @message = N'ERROR[T3]: error(s) occured while checking duplicate tenants in JSON string';
            THROW;
        END CATCH;

        ----------------------------------------------------------------------------------------------------
        -- Missing After Hours checks
        ----------------------------------------------------------------------------------------------------
        BEGIN TRY;
            INSERT INTO @json_errors([id], [key], [message])
            SELECT [id] = 18, [key] = els.[key], [message] = N'ERROR[H0]: array #' + CAST(els.[key] AS nvarchar(10)) + N' => @AfterHours default value is not provided and "after_hours" value missing in both element objects'
            FROM @elements_settings els 
            WHERE els.[disabled] = 0 AND @AfterHours IS NULL AND els.[after_hours] IS NULL;
        END TRY 
        BEGIN CATCH
            SET @message = N'ERROR[H0]: errors occured while checking [after_hours] parameter(s)';
            THROW;
        END CATCH

        ----------------------------------------------------------------------------------------------------
        -- Missing Delete Delay checks
        ----------------------------------------------------------------------------------------------------
        BEGIN TRY;
            INSERT INTO @json_errors([id], [key], [message])
            SELECT [id] = 19, [key] = els.[key], [message] = N'ERROR[H1]: array #' + CAST(els.[key] AS nvarchar(10)) + N' => @DeleteDelayHours default value is not provided and "delete_delay_hours" value missing in both element objects'
            FROM @elements_settings els 
            WHERE els.[disabled] = 0 AND @DeleteDelayHhours IS NULL AND els.[delete_delay_hours] IS NULL;
        END TRY 
        BEGIN CATCH
            SET @message = N'ERROR[H1]: errors occured while checking [after_hours] parameter(s)';
            THROW;
        END CATCH

        IF NOT EXISTS(SELECT 1 FROM @json_errors) 
        BEGIN 
            SET @IsValid = 1;

            SELECT @Settings = (
                SELECT DISTINCT [t] = tnt.[Tenant_Id]
                    , [o] = 0 --elm.[delete_only] 
                    , [h] = COALESCE(elm.[after_hours], @AfterHours)
                    , [d] = COALESCE(elm.[delete_delay_hours], @DeleteDelayHhours, 0)
                FROM @elements_settings elm
                INNER JOIN @jsonArray_tenants tnt ON tnt.[key] = elm.[key]
                WHERE elm.[disabled] = 0 AND tnt.[keep] = 1
                ORDER BY  [t]
                FOR JSON PATH, INCLUDE_NULL_VALUES
            )
        END
    END TRY
    BEGIN CATCH
        SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
    END CATCH

    --IF @@TRANCOUNT > 0 ROLLBACK;
    --SET @Messages = ( SELECT * FROM (SELECT TOP(100) [message], [severity] = 10, [state] = 1 FROM @json_errors ORDER BY [key] ASC, [id] ASC) x FOR XML RAW('message'), TYPE );
    SET @Messages = ( 
        SELECT [Procedure] = QUOTENAME(COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?')) + N'.' + QUOTENAME(COALESCE(OBJECT_NAME(@@PROCID), N'?')), [Message], [Severity], [State] 
        FROM (
            SELECT [Message], [Severity], [State] FROM (
                SELECT TOP(100) [Message] = LEFT([message], 4000), [Severity] = 10, [State] = 1 FROM @json_errors ORDER BY [key] ASC, [id] ASC
            ) err
            UNION ALL SELECT N'ERROR: ' + @ERROR_MESSAGE, 10, 1 WHERE @ERROR_MESSAGE IS NOT NULL
            UNION ALL SELECT @message, 10, 1 WHERE @message IS NOT NULL
        ) jsn
        FOR JSON PATH);
    RETURN 0
END
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- DROP PROCEDURE [Maintenance].[AddArchiveTriggerRobotLicenseLogs]
----------------------------------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[AddArchiveTriggerRobotLicenseLogs]') AND type in (N'P'))
BEGIN
    EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [Maintenance].[AddArchiveTriggerRobotLicenseLogs] AS'
    PRINT '  + CREATE PROCEDURE: [Maintenance].[AddArchiveTriggerRobotLicenseLogs]';
END
ELSE PRINT '  = PROCEDURE [Maintenance].[AddArchiveTriggerRobotLicenseLogs] already exists' 
GO

PRINT '  ~ UPDATE PROCEDURE: [Maintenance].[AddArchiveTriggerRobotLicenseLogs]'
GO

ALTER PROCEDURE [Maintenance].[AddArchiveTriggerRobotLicenseLogs]
----------------------------------------------------------------------------------------------------
-- ### [Object]: PROCEDURE [Maintenance].[AddArchiveTriggerRobotLicenseLogs]
-- ### [Version]: 2023-10-06T11:29:36+02:00
-- ### [Source]: _src/Archive/ArchiveDB/RobotLicenseLogs/Procedure_ArchiveDB.Maintenance.AddArchiveTriggerRobotLicenseLogs.sql
-- ### [Hash]: dc39c27 [SHA256-BB293A22AC398AD345528534E98AC89710BF39F16FEF49B9D840DA1F7D812C67]
-- ### [Docs]: https://???.???
-- !!! ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!!!
-- !!! ~~~~~~~~~ NOT OFFICIALLY SUPPORTED BY UIPATH 
-- !!! ~~~~~~~~~ SQL Server >= 2016 SP1
----------------------------------------------------------------------------------------------------
	@Name nvarchar(100) = NULL
    , @ArchiveTriggerTime datetime = NULL -- Replace by current date/time when NULL
    , @ArchiveAfterHours int = NULL -- Override missing/empty 'after_hours' value in JSON filters
	, @DeleteDelayHours int = 0 -- Override missing/empty delete_delay_hours value in JSON filters'
	, @Filters nvarchar(MAX)
    , @RepeatArchive [bit] = 0
    , @RepeatOffsetHours [smallint] = NULL
    , @RepeatUntil [datetime] = NULL
    , @ParentArchiveId [bigint] = NULL
    , @SynchronousDelete bit = 0 
    , @ForceSynchronousDelete bit = 0
    , @DryRunOnly nvarchar(MAX) = NULL -- Y{es} or N{o} => Only Check Parameters (default if NULL = Y)
    /* Messge Logging */
    , @SaveMessagesToTable nvarchar(MAX) = 'Y' -- Y{es} or N{o} => Save to [maintenance].[messages] table (default if NULL = Y)
    , @SavedToRunId int = NULL OUTPUT
--    , @OutputMessagesToDataset nvarchar(MAX) = 'N' -- Y{es} or N{o} => Output Messages Result set
--    , @Verbose nvarchar(MAX) = NULL -- Y{es} = Print all messages < 10
AS
BEGIN
    BEGIN TRY 
        SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
		SET LOCK_TIMEOUT 5000;
        SET ARITHABORT ON;
        SET NOCOUNT ON;
        SET NUMERIC_ROUNDABORT OFF;

        ----------------------------------------------------------------------------------------------------
        -- Local Run Variables
        ----------------------------------------------------------------------------------------------------
        DECLARE @runId int;  
        DECLARE @dryRun bit;

        DECLARE @startTime datetime = SYSDATETIME();
        DECLARE @startTimeFloat float(53);
        SELECT @startTimeFloat = CAST(@startTime AS float(53));

        DECLARE @tenantsSynonymIsValid bit;
        DECLARE @tenantsSynonymMessages nvarchar(MAX);        

        DECLARE @triggerTime datetime;
        DECLARE @triggerFloatingTime float(53);
        DECLARE @globalDeleteDelay int = 0;
        DECLARE @globalAfterHours int = NULL;

        DECLARE @logToTable bit;
        DECLARE @returnValue int = 1;

        ----------------------------------------------------------------------------------------------------
        -- JSON Validation
        ----------------------------------------------------------------------------------------------------
        DECLARE @json_IsValid bit = 0
        DECLARE @json_filters nvarchar(MAX);
        DECLARE @json_errors nvarchar(MAX);
        DECLARE @json_settings nvarchar(MAX);

        ----------------------------------------------------------------------------------------------------
        -- Archive / Filters
        ----------------------------------------------------------------------------------------------------
        DECLARE @Ids TABLE(Id bigint);
        DECLARE @listFilters TABLE([syncId] bigint, [tenants] int, [deleteOnly] bit, [archiveDate] datetime, [deleteDate] datetime, [next] datetime)
        DECLARE @countValidFilters int, @countDuplicateFilters int;
        DECLARE @targetTimestamp datetime;
        DECLARE @archiveId bigint;
        ----------------------------------------------------------------------------------------------------
        -- Constant / Default value
        ----------------------------------------------------------------------------------------------------
        DECLARE @paramsYesNo TABLE ([id] tinyint IDENTITY(0, 1) PRIMARY KEY CLUSTERED, [parameter] nvarchar(MAX), [value] int)
        DECLARE @floatingDay float(53) = 1;
        DECLARE @floatingHour float(53) = @floatingDay / 24;
        DECLARE @constDefaultDeleteDelay int = 0;
        DECLARE @constDefaultAfterHours int = NULL;
        ----------------------------------------------------------------------------------------------------
        -- Server Info 
        ----------------------------------------------------------------------------------------------------
        DECLARE @productVersion nvarchar(MAX) = CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(MAX));
        DECLARE @engineEdition int =  CAST(ISNULL(SERVERPROPERTY('EngineEdition'), 0) AS int);
        DECLARE @minProductVersion nvarchar(MAX) = N'13.0.4001.0 (SQL Server 2016 SP1)'
        DECLARE @version numeric(18, 10);
        DECLARE @minVersion numeric(18, 10) = 13.0040010000;
        DECLARE @minCompatibilityLevel int = 130;
        DECLARE @hostPlatform nvarchar(256); 
        ----------------------------------------------------------------------------------------------------
        -- Proc Info
        ----------------------------------------------------------------------------------------------------
        DECLARE @paramsGetProcInfo nvarchar(MAX) = N'@procid int, @info nvarchar(MAX), @output nvarchar(MAX) OUTPUT'
        DECLARE @stmtGetProcInfo nvarchar(MAX) = N'
            DECLARE @definition nvarchar(MAX) = OBJECT_DEFINITION(@procid), @keyword nvarchar(MAX) = REPLICATE(''-'', 2) + SPACE(1) + REPLICATE(''#'', 3) + SPACE(1) + QUOTENAME(LTRIM(RTRIM(@info))) + '':'';
			DECLARE @eol char(1) = IIF(CHARINDEX( CHAR(13) , @definition) > 0, CHAR(13), CHAR(10));
			SET @output = ''''+ LTRIM(RTRIM( SUBSTRING(@definition, NULLIF(CHARINDEX(@keyword, @definition), 0 ) + LEN(@keyword), CHARINDEX( @eol , @definition, CHARINDEX(@keyword, @definition) + LEN(@keyword) + 1) - CHARINDEX(@keyword, @definition) - LEN(@keyword) ))) + '''';
        ';
        DECLARE @procSchemaName nvarchar(MAX) = COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?');
        DECLARE @procObjecttName nvarchar(MAX) = COALESCE(OBJECT_NAME(@@PROCID), N'?');
        DECLARE @procName nvarchar(MAX) = QUOTENAME(COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?')) + N'.' + QUOTENAME(COALESCE(OBJECT_NAME(@@PROCID), N'?')); 
        DECLARE @versionDatetime nvarchar(MAX);
        ----------------------------------------------------------------------------------------------------      
        -- Message / Error Handling
        ----------------------------------------------------------------------------------------------------
        DECLARE @lineSeparator nvarchar(MAX) = N'----------------------------------------------------------------------------------------------------';
        DECLARE @lineBreak nvarchar(MAX) = N'';
        DECLARE @message nvarchar(MAX);
        DECLARE @string nvarchar(MAX);
        DECLARE @space tinyint = 2 ;
        DECLARE @tab tinyint = 0 ;
        /* Log Stack */
        DECLARE @stmtEmptyMessagesStack nvarchar(MAX) = N'
            SELECT 
                [Date] = m.value(''Date[1]'', ''datetime'')
                , [Procedure] = m.value(''Procedure[1]'', ''nvarchar(MAX)'')
                , [Message] = m.value(''Message[1]'', ''nvarchar(MAX)'')
                , [Severity] = m.value(''Severity[1]'', ''int'')
                , [State] = m.value(''State[1]'', ''int'')
                , [Number] = m.value(''Number[1]'', ''int'')
                , [Line] = m.value(''Line[1]'', ''int'')
            FROM @MessagesStack.nodes(''/messages/message'') x(m)
            SET @MessagesStack = NULL;
        ';
        DECLARE @paramsEmptyMessagesStack nvarchar(MAX) = N'@MessagesStack xml = NULL OUTPUT';
        DECLARE @MessagesStack xml;
        /* Errors */
        DECLARE @ERROR_NUMBER INT, @ERROR_SEVERITY INT, @ERROR_STATE INT, @ERROR_PROCEDURE NVARCHAR(126), @ERROR_LINE INT, @ERROR_MESSAGE NVARCHAR(2048) ;
        DECLARE @errorCount int = 0;
        /* Messages */
        DECLARE @levelVerbose int;    
        DECLARE @outputDataset bit;
        DECLARE @messages TABLE(id int IDENTITY(0, 1) PRIMARY KEY, [date] datetime2 DEFAULT SYSDATETIME(), [procedure] nvarchar(MAX) NOT NULL DEFAULT QUOTENAME(COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?')) + N'.' + QUOTENAME(COALESCE(OBJECT_NAME(@@PROCID), N'?')), [message] nvarchar(MAX) NOT NULL, severity tinyint NOT NULL, state tinyint NOT NULL, [number] int, [line] int);
        /* Cursor */ 
        DECLARE CursorMessages CURSOR FAST_FORWARD LOCAL FOR SELECT [Date], [Procedure], [Message], [Severity], [State], [Number], [Line] FROM @messages ORDER BY [ID] ASC;
        DECLARE @cursorDate datetime;
        DECLARE @cursorProcedure nvarchar(MAX);
        DECLARE @cursorMessage nvarchar(MAX);
        DECLARE @cursorSeverity tinyint;
        DECLARE @cursorState tinyint;
        DECLARE @cursorNumber int;
        DECLARE @cursorLine int;    

        ----------------------------------------------------------------------------------------------------
        -- START
        ----------------------------------------------------------------------------------------------------
        IF @ParentArchiveId IS NOT NULL SET @tab = 4 ELSE SET @tab = 0;

        ----------------------------------------------------------------------------------------------------
        -- Gather General & Server Info
        ----------------------------------------------------------------------------------------------------

        -- Get Proc Version
/*
        EXEC sp_executesql @stmt = @stmtGetProcInfo, @params = @paramsGetProcInfo, @procid = @@PROCID, @info = N'Version', @output = @versionDatetime OUTPUT;
        INSERT INTO [Maintenance].[Runs]([Type], [Info], [StartTime]) SELECT N'Add Archive RobotLicenseLogs', N'PROCEDURE ' + @procName, @startTime;
        INSERT INTO @messages ([Message], Severity, [State])   
        SELECT 'Add Archive RobotLicenseLogs...' , 10, 1;
*/
        ----------------------------------------------------------------------------------------------------
        -- Create new Run Id
        ----------------------------------------------------------------------------------------------------
        INSERT INTO @messages ([Message], Severity, [State]) 
        SELECT SPACE(@tab+ @space * 0) + N'Add Archive Trigger' , 10, 1
        UNION ALL SELECT SPACE(@tab+ @space * 1) + N'Parameter Run Id [' + CAST(@SavedToRunId AS nvarchar(100)) + N'] is already ended.' , 10, 1 FROM [Maintenance].[Runs] WHERE Id = @SavedToRunId AND EndDate IS NOT NULL
        UNION ALL SELECT SPACE(@tab+ @space * 1) + N'Parameter Run Id [' + CAST(@SavedToRunId AS nvarchar(100)) + N'] not found.', 10, 1 WHERE @SavedToRunId IS NOT NULL AND NOT EXISTS (SELECT 1 FROM [Maintenance].[Runs] WHERE Id = @SavedToRunId)
        
        IF @SavedToRunId IS NULL OR NOT EXISTS(SELECT 1 FROM [Maintenance].[Runs] WHERE Id = @SavedToRunId AND EndDate IS NULL)
        BEGIN
            INSERT INTO [Maintenance].[Runs]([Type], [Info], [StartTime]) SELECT N'Add Archive RobotLicenseLogs Trigger', N'PROCEDURE ' + @procName, @startTime;
            SELECT @runId = @@IDENTITY, @SavedToRunId = @@IDENTITY;
            INSERT INTO @messages ([Message], Severity, [State]) VALUES 
                (SPACE(@tab+ @space * 1) + N'Messages saved to new Run Id: ' + CONVERT(nvarchar(MAX), @runId), 10, 1);
        END
        ELSE SELECT @runId = @SavedToRunId;

        ----------------------------------------------------------------------------------------------------
        -- Check SQL Server Requierements
        ----------------------------------------------------------------------------------------------------
        -- Get Proc Version
--        EXEC sp_executesql @stmt = @stmtGetProcInfo, @params = @paramsGetProcInfo, @procid = @@PROCID, @info = N'Version', @output = @versionDatetime OUTPUT;

        -- Get SQL Server Server Version
        WITH release (id, position, version) AS
        (
            SELECT id = 0, position = CHARINDEX(N'.', @productVersion, 1)
                , version = LEFT(@productVersion, CHARINDEX(N'.', @productVersion, 1))
            UNION ALL
            SELECT id = r.id + 1, position = CHARINDEX(N'.', @productVersion, position + 1)
                , version = r.version + RIGHT(v.[space] + SUBSTRING(@productVersion, position + 1, COALESCE( NULLIF(CHARINDEX('.', @productVersion, position + 1), 0), LEN(@productVersion) + 1) - position - 1), LEN(v.[space]) )
            FROM release r INNER JOIN (VALUES(0, '00'), (1, '00'), (2, '0000'), (3, '0000') ) AS v(id, space) ON v.id = r.id + 1
            WHERE position < LEN(@productVersion) + 1
        )
        SELECT TOP(1) @version = CAST(version AS numeric(18, 10))
        FROM release
        ORDER BY Id DESC

        -- Get Host info
        IF @version >= 14 SELECT @hostPlatform = host_platform FROM sys.dm_os_host_info ELSE SET @HostPlatform = 'Windows'

        INSERT INTO @messages([Message], Severity, [State])
        -- Check min required version
        SELECT N'ERROR: Current SQL Server version is ' + @productVersion + N'. Only version ' + @minProductVersion + + N' or higher is supported.', 16, 1 WHERE @version < @minVersion
        -- Check Database Compatibility Level
        UNION ALL SELECT 'ERROR: Database ' + QUOTENAME(DB_NAME(DB_ID())) + ' Compatibility Level is set to '+ CAST([compatibility_level] AS nvarchar(MAX)) + '. Compatibility level 130 or higher is requiered.', 16, 1 FROM sys.databases WHERE database_id = DB_ID() AND [compatibility_level] < @minCompatibilityLevel
        -- Check opened transation(s)
        --UNION ALL SELECT 'The transaction count is not 0.', 16, 1 WHERE @@TRANCOUNT <> 0
        -- Check uses_ansi_nulls
        UNION ALL SELECT 'ERROR: ANSI_NULLS must be set to ON for this Stored Procedure', 16, 1 FROM sys.sql_modules WHERE [object_id] = @@PROCID AND uses_ansi_nulls <> 1
        -- Check uses_quoted_identifier
        UNION ALL SELECT 'ERROR: QUOTED_IDENTIFIER must be set to ON for this Stored Procedure', 16, 1 FROM sys.sql_modules WHERE [object_id] = @@PROCID AND uses_quoted_identifier <> 1;

        BEGIN TRY
            EXEC [Maintenance].[SetSourceTableTenants] @IsValid = @tenantsSynonymIsValid OUTPUT, @Messages = @tenantsSynonymMessages OUTPUT;

            INSERT INTO @messages([message], [severity], [state])
            SELECT [Message], [Severity], 0 FROM OPENJSON(@tenantsSynonymMessages, N'$') WITH ([Message] nvarchar(MAX), [Severity] tinyint)
            UNION ALL SELECT 'Tenants synonym must be set using [Maintenance].[SetSourceTableTenants]', 16, 0 WHERE @tenantsSynonymIsValid = 0;
        END TRY
        BEGIN CATCH
            SET @message = N'ERROR: error(s) occurcered while validating Tenants synonym with with [Maintenance].[SetSourceTableTenants]'
            INSERT INTO @messages ([Message], Severity, [State]) VALUES
                    (ERROR_MESSAGE(), 10, 1)
                    , (@message, 16, 1)
        END CATCH

        ----------------------------------------------------------------------------------------------------
        -- Parameters
        ----------------------------------------------------------------------------------------------------
        -- Convert Yes / No varations to bit
        INSERT INTO @paramsYesNo([parameter], [value]) VALUES(N'NO', 0), (N'N', 0), (N'0', 0), (N'YES', 1), (N'Y', 1), (N'1', 1);

        -- Check Dry Run
        SELECT @dryRun = [value] FROM @paramsYesNo WHERE [parameter] = LTRIM(RTRIM(@DryRunOnly));
        IF @dryRun IS NULL 
        BEGIN
            SET @dryRun = 1;
            INSERT INTO @messages ([Message], Severity, [State]) VALUES (N'@DryRunOnly is NULL. Default value will be used (Yes).', 10, 1);
        END

        -- Check @SaveMessagesToTable parameter
        SELECT @logToTable = [value] FROM @paramsYesNo WHERE [parameter] = ISNULL(LTRIM(RTRIM(@SaveMessagesToTable)), N'Y');
        IF @logToTable IS NULL 
        BEGIN
            INSERT INTO @messages ([Message], Severity, [State]) VALUES
                ( 'Parameter @SaveMessagesToTable is invalid: ' + LTRIM(RTRIM(@SaveMessagesToTable)), 10, 1)
                , ('Usage: @SaveMessagesToTable = Y{es} or N{o}', 10, 1)
                , ('Parameter @SaveMessagesToTable is invalid: ' + LTRIM(RTRIM(@SaveMessagesToTable)), 16, 1);
        END

        SET @levelVerbose = 10;

        ----------------------------------------------------------------------------------------------------
        -- Parameters' Validation
        ----------------------------------------------------------------------------------------------------
        -- Check @@ArchiveTriggerTime
        SELECT @triggerTime = ISNULL(@ArchiveTriggerTime, SYSDATETIME());
        SELECT @triggerFloatingTime = CAST(@triggerTime AS float(53));

        -- Check Parameters
        IF EXISTS(SELECT 1 FROM @messages WHERE severity >= 16) 
        BEGIN 
            SELECT @message = N'Invalid configuration' WHERE @message IS NULL;
        END 
        ELSE
        BEGIN
            SELECT @json_filters = LTRIM(RTRIM(@Filters));

            -- Check @ArchiveAfterHours / @DeleteDelayHours
            SELECT @globalDeleteDelay = ISNULL(@DeleteDelayHours, @constDefaultDeleteDelay);
            SELECT @globalAfterHours = ISNULL(@ArchiveAfterHours, @constDefaultAfterHours);

            -- Output errors and warning
            INSERT INTO @messages ([Message], Severity, [State]) 
            SELECT SPACE(@tab+ @space * 1) + N'@ArchiveTriggerTime is NULL. Current Date & Time will be used', 10, 1 WHERE @ArchiveTriggerTime IS NULL
            UNION ALL SELECT SPACE(@tab+ @space * 1) + N'@ArchiveAfterHours is NULL. "after_hours" value(s) from JSON string will be used', 10, 1 WHERE @ArchiveAfterHours IS NULL AND @json_filters NOT IN (N'ALL', N'ACTIVE_TENANTS', N'DELETED_TENANTS')
            UNION ALL SELECT N'ERROR: @ArchiveAfterHours must be provided when keyword "' + @json_filters + N'" is used', 16, 1 WHERE @ArchiveAfterHours IS NULL AND @json_filters IN (N'ALL', N'ACTIVE_TENANTS', N'DELETED_TENANTS')
            UNION ALL SELECT SPACE(@tab+ @space * 1) + N'@DeleteDelayHours is NULL. "delete_delay_hours" value(s) from JSON string will be used when available or default value otherwise (' + CAST(@constDefaultDeleteDelay AS nvarchar(100)) N')', 10, 1 WHERE @DeleteDelayHours IS NULL
            UNION ALL SELECT N'ERROR: @ArchiveAfterHours must be at least 0 (>= 0)', 16, 1 WHERE @ArchiveAfterHours < 0
            UNION ALL SELECT N'@RepeatOffsetHours must be greater than 1 when @RepeatArchive is set (@RepeatOffsetHours = ' + ISNULL(CAST(@RepeatOffsetHours AS nvarchar(100)), N'NULL') + ')', 16, 1 WHERE (@RepeatOffsetHours IS NULL OR @RepeatOffsetHours < 1) AND @RepeatArchive = 1
            UNION ALL SELECT N'@RepeatUntil must be greater than the current date and time', 16, 1 WHERE (@RepeatUntil <= @triggerTime) AND @RepeatArchive = 1
            ;

        END

        -- Check Filters Definition
        IF EXISTS(SELECT 1 FROM @messages WHERE severity >= 16) 
        BEGIN 
            SELECT @message =  N'Invalid parameters' WHERE @message IS NULL;
        END 
        ELSE
        BEGIN
            IF @json_filters IN (N'ALL', N'ACTIVE_TENANTS', N'DELETED_TENANTS', N'#ALL#', N'#ACTIVE_TENANTS#', N'#DELETED_TENANTS#')
            BEGIN
                SELECT @json_filters = UPPER(@json_filters);
                INSERT INTO @messages ([Message], Severity, [State]) SELECT SPACE(@tab+ @space * 1) + N'@Filters keyword used: ' + @json_filters, 10, 1
                INSERT INTO @messages ([Message], Severity, [State]) VALUES (SPACE(@tab+ @space * 1) + N'Create JSON string and parameters...', 10, 1)
                SELECT @json_filters = ( SELECT [tenants] = N'#' + [tenants] + N'#' FROM (VALUES(N'ACTIVE_TENANTS'), (N'DELETED_TENANTS')) t([tenants]) WHERE @json_filters = N'ALL' OR @json_filters = N'#ALL#' OR @json_filters = [tenants] OR @json_filters = N'#'+ [tenants] + N'#' FOR JSON PATH )

                INSERT INTO @messages ([Message], Severity, [State]) SELECT SPACE(@tab+ @space * 1) + N'Resulting JSON string: ' + @json_filters, 10, 1
            END
            ELSE
            BEGIN
                INSERT INTO @messages ([Message], Severity, [State]) VALUES (SPACE(@tab+ @space * 1) + N'@Filters not a keyword ("ALL", "ACTIVE_TENANTS" or "DELETED_TENANTS"), valid JSON string expected', 10, 1)
            END

            BEGIN TRY            
                INSERT INTO @messages ([Message], Severity, [State]) VALUES (SPACE(@tab+ @space * 1) + N'Checking @Filters...', 10, 1)
                EXEC [Maintenance].[ParseJsonArchiveRobotLicenseLogs] @Filters = @json_filters, @Settings = @json_settings OUTPUT, @Messages = @json_errors OUTPUT, @IsValid = @json_IsValid OUTPUT, @AfterHours = @globalAfterHours, @DeleteDelayHhours = @globalDeleteDelay;
            END TRY
            BEGIN CATCH
                SET @message = N'ERROR: error(s) occurcered while validating settings with [Maintenance].[ParseJsonArchiveRobotLicenseLogs]'
                INSERT INTO @messages ([Message], Severity, [State]) VALUES
                        (ERROR_MESSAGE(), 10, 1)
                        , (@message, 16, 1)
                SET @json_IsValid = 0;
            END CATCH

            IF @json_IsValid = 1
            BEGIN
                INSERT INTO @messages ([Message], Severity, [State]) VALUES (SPACE(@tab+ @space * 1) + N'JSON string and parameters are valid', 10, 1);
                IF @dryRun = 1 SET @message = N'Dry Run' ELSE SET @message = N'Add Trigger';
            END
            ELSE 
            BEGIN;
                INSERT INTO @messages([procedure], [message], [severity], [state])
                SELECT [Procedure], [Message], [Severity], [State] FROM OPENJSON(@json_errors, N'$')
                    WITH ([Procedure] nvarchar(MAX) '$.Procedure', [Message] nvarchar(MAX) '$.Message', [Severity] smallint '$.Severity', [State] smallint '$.State');

                INSERT INTO @messages ([Message], Severity, [State]) VALUES (N'ERROR: JSON string and parameters are invalid, review previous error(s)', 16, 1);
                SET @message = N'Invalid JSON string and parameters';
            END 
        END

        SELECT @errorCount = COUNT(*) FROM @messages WHERE severity >= 16;
        ----------------------------------------------------------------------------------------------------
        -- Add new Archive RobotLicenseLogs
        ----------------------------------------------------------------------------------------------------
        BEGIN TRY
            INSERT INTO [Maintenance].[Archive_RobotLicenseLogs]([ParentArchiveId], [CurrentRunId], [PreviousRunIds], [Name], [Definition], [ArchiveTriggerTime], [ArchiveAfterHours], [DeleteDelayHours], [TargetId], [TargetTimestamp], [RepeatArchive], [RepeatOffsetHours], [RepeatUntil]
                -- , [AddNextArchive], [NextOffsetHours]
                , [IsDryRun], [IsSuccess], [IsError], [IsCanceled], [Message], [IsFinished], [FinishedOnDate]
                , [CountValidFilters], [CountDuplicateFilters] )
            SELECT @ParentArchiveId, @runId, (SELECT [runid] = @runId, [message] = @message, [timestamp] = SYSDATETIME() FOR JSON PATH), @Name, @Filters, @triggerTime, @ArchiveAfterHours, @DeleteDelayHours, NULL, @triggerTime, @RepeatArchive, @RepeatOffsetHours, @RepeatUntil
                , @dryRun, 0, IIF(@errorCount = 0 AND @json_IsValid = 1, 0, 1), 0, @message
                , @dryRun, IIF(@dryRun = 1, SYSDATETIME(), NULL)
                , 0, 0
            SET @archiveId = @@IDENTITY;
            
            INSERT INTO @messages ([Message], Severity, [State]) VALUES (SPACE(@tab+ @space * 1) + N'Archive RobotLicenseLogs Trigger created (Id = ' + CAST(@archiveId AS nvarchar(100)) + N')' + IIF(@errorCount > 0, N' with error(s)', N'') + N'.', 10, 1);
        END TRY
        BEGIN CATCH
            SET @message = N'ERROR: error(s) occurcered while adding Archive RobotLicenseLogs trigger to [Maintenance].[Archive_RobotLicenseLogs]'
            INSERT INTO @messages ([Message], Severity, [State]) VALUES
                    (ERROR_MESSAGE(), 10, 1)
                    , (@message, 16, 1)
        END CATCH 
        ----------------------------------------------------------------------------------------------------
        -- Check Error(s) count
        ----------------------------------------------------------------------------------------------------

        IF NOT EXISTS(SELECT 1 FROM @messages WHERE severity >= 16) AND @dryRun = 0
        BEGIN

            BEGIN TRY
                -- retrieve parsed filters and update target dates
                INSERT @listFilters ([tenants], [deleteOnly], [archiveDate], [deleteDate])
                SELECT [tenants] = [t], [deleteOnly] = [o]
                    , [TargetTimestamp] = CAST(@triggerTime -ABS(@floatingHour * [h]) AS datetime) 
                    , [DeleteAfterDatetime] = CAST(@triggerTime - ABS(@floatingHour * [h]) + ABS(@floatingHour * [d]) AS datetime) 
                FROM OPENJSON(@json_settings) WITH ([t] int, [l] int, [o] int, [h] int, [d] int) jsn

                SELECT @countValidFilters = COUNT(*) FROM @listFilters lst WHERE NOT EXISTS (SELECT 1 FROM [Maintenance].[Filter_RobotLicenseLogs] flt WHERE flt.TenantId = lst.tenants AND flt.TargetTimestamp >= lst.archiveDate);
                SELECT @countDuplicateFilters = COUNT(*) - @countValidFilters, @targetTimestamp = MAX(archiveDate) FROM @listFilters;

                IF @countValidFilters > 0
                BEGIN 
                    BEGIN TRAN

                    -- insert archive sync by target Delete date
                    INSERT INTO [Maintenance].[Sync_RobotLicenseLogs](ArchiveId, DeleteAfterDatetime)
                    OUTPUT inserted.Id INTO @Ids(Id)
                    SELECT DISTINCT @archiveId, deleteDate FROM @listFilters ORDER BY deleteDate DESC

                    -- match filters with inserted Sync Id
                    UPDATE lst SET syncId = ids.Id
                    FROM @Ids ids 
                    INNER JOIN [Maintenance].[Sync_RobotLicenseLogs] snc ON snc.Id = ids.Id
                    INNER JOIN @listFilters lst ON lst.deleteDate = snc.DeleteAfterDatetime

                    -- insert valid filter(s)
                    INSERT INTO [Maintenance].[Filter_RobotLicenseLogs]([SyncId],[TenantId], [DeleteOnly], [TargetTimestamp], [PreviousTimestamp])
                    SELECT lst.syncId, lst.tenants, lst.deleteOnly, lst.archiveDate, ISNULL(last.TargetTimestamp, 0) -- 0 => 19010101
                    FROM @listFilters lst
                    OUTER APPLY (SELECT MAX(TargetTimestamp) FROM [Maintenance].[Filter_RobotLicenseLogs] flt WHERE flt.TenantId = lst.tenants AND flt.TargetTimestamp < lst.archiveDate) last(TargetTimestamp) -- retrieve previous target date
                    WHERE NOT EXISTS (SELECT 1 FROM [Maintenance].[Filter_RobotLicenseLogs] flt WHERE flt.TenantId = lst.tenants AND flt.TargetTimestamp >= lst.archiveDate) -- remove existing filter(s) with a newer date

                    SET @message = SPACE(@tab+ @space * 1) + N'Valid Filter(s) added: ' + CAST(@countValidFilters AS nvarchar(100)) + N' , duplicate(s) found (' + CAST(@countDuplicateFilters AS nvarchar(100)) + N')';
                    INSERT INTO @messages ([Message], Severity, [State]) VALUES (@message, 10, 1);

                    IF @@TRANCOUNT > 0 COMMIT
                END
                ELSE
                BEGIN
                    SET @message = SPACE(@tab+ @space * 1) + N'No Filters added, only duplicate found (' + CAST(@countDuplicateFilters AS nvarchar(100)) + N')';
                    INSERT INTO @messages ([Message], Severity, [State]) VALUES (@message, 10, 1)
                END
                -- update valid and duplicate filter(s) count
                UPDATE arc SET [CountValidFilters] = @countValidFilters, [CountDuplicateFilters] = @countDuplicateFilters, [TargetTimestamp] = @targetTimestamp FROM [Maintenance].[Archive_RobotLicenseLogs] arc WHERE arc.Id = @archiveId;

                INSERT INTO @messages ([Message], Severity, [State])
                SELECT SPACE(@tab+ @space * 1) + N'Repeat enabled every [' + CAST(@RepeatOffsetHours AS nvarchar(100)) + N'] hour(s)' + IIF(@RepeatUntil IS NOT NULL, N' until [' + CONVERT(nvarchar(100), @RepeatUntil, 120) + N']', N''), 10, 1 WHERE @RepeatArchive = 1

            END TRY
            BEGIN CATCH
                SET @message = N'ERROR: error(s) occurcered while adding Sync and Filters'
                INSERT INTO @messages ([Message], Severity, [State]) VALUES
                        (ERROR_MESSAGE(), 10, 1)
                        , (@message, 16, 1)
                IF @@TRANCOUNT > 0 ROLLBACK
            END CATCH 
        END
    END TRY
    BEGIN CATCH
        -- Get Unknown error
        SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();

        -- Save Unknwon Errror
        INSERT INTO @messages ([Message], Severity, [State], [Number], [Line])
        SELECT @ERROR_MESSAGE, @ERROR_SEVERITY, @ERROR_STATE, @ERROR_NUMBER, @ERROR_LINE;
    END CATCH
    
    ----------------------------------------------------------------------------------------------------
    -- Print & Save Errors & Messages
    ----------------------------------------------------------------------------------------------------
    SELECT @errorCount = COUNT(*) FROM @messages WHERE severity >= 16;

    OPEN CursorMessages;
    FETCH CursorMessages INTO @cursorDate, @cursorProcedure, @cursorMessage, @cursorSeverity, @cursorState, @cursorNumber, @cursorLine;

    IF CURSOR_STATUS('local', 'CursorMessages') = 1
    BEGIN
        WHILE @@FETCH_STATUS = 0
        BEGIN;
            IF @logToTable = 0 OR @errorCount > 0 OR @cursorSeverity >= @levelVerbose OR @cursorSeverity > 10 RAISERROR('%s', @cursorSeverity, @cursorState, @cursorMessage) WITH NOWAIT;
            --IF @cursorSeverity >= 16 RAISERROR('', 10, 1) WITH NOWAIT;
            FETCH CursorMessages INTO @cursorDate, @cursorProcedure, @cursorMessage, @cursorSeverity, @cursorState, @cursorNumber, @cursorLine;
        END
    END
    ELSE 
    BEGIN
        SET @message = 'Execution has been canceled: Error Opening Messages Cursor';
        INSERT INTO @messages ([Message], Severity, [State]) VALUES (@message, 16, 1);
        SET @errorCount = @errorCount +1;
        RAISERROR(@message, 16, 1);
    END 

	IF CURSOR_STATUS('local', 'CursorMessages') >= 0 CLOSE CursorMessages;
	IF CURSOR_STATUS('local', 'CursorMessages') >= -1 DEALLOCATE CursorMessages;

----------------------------------------------------------------------------------------------------
--
----------------------------------------------------------------------------------------------------
    BEGIN TRY 
        -- Records existing messages
        IF @runId IS NOT NULL 
        BEGIN
            INSERT INTO [Maintenance].[Messages](RunId, [Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
            SELECT @runId, [Date], [Procedure], [Message], [Severity], [State], [Number], [Line]
            FROM @messages 
            ORDER BY Id ASC;
        END

        ----------------------------------------------------------------------------------------------------
        -- End Run on Error(s)
        ----------------------------------------------------------------------------------------------------
        IF @errorCount > 0
        BEGIN
            SET @returnValue = 3;
            SET @message = N'Incorrect Parameters, see previous Error(s): ' + CAST(@errorCount AS nvarchar(MAX)) + N' found';
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 16, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = 0, @MessagesStack = @MessagesStack OUTPUT;
            -- RETURN 123; => catch
        END
        ----------------------------------------------------------------------------------------------------
        -- End Run on Dry Run
        ----------------------------------------------------------------------------------------------------
        IF @dryRun <> 0 
        BEGIN
            INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
            EXEC sp_executesql @stmt = @stmtEmptyMessagesStack, @params = @paramsEmptyMessagesStack, @MessagesStack = @MessagesStack OUTPUT;

            SET @returnValue = 1;

            SET @message = '@DryRunOnly is enabled (check output and parameters and set @DryRunOnly to No when ready)';
            EXEC [Maintenance].[AddRunMessage] @RunId = @RunId, @Procedure = @procName, @Message = @message, @Severity = 11, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = 0, @MessagesStack = @MessagesStack OUTPUT;
            -- RETURN 1; => catch
        END

        --DELETE FROM @messages;
        SELECT @Message = SPACE(@tab+ @space * 0) + 'Valid Archive RobotLicenseLogs Trigger added (SUCCESS)';
		EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @Message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

        INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
        EXEC sp_executesql @stmt = @stmtEmptyMessagesStack, @params = @paramsEmptyMessagesStack, @MessagesStack = @MessagesStack OUTPUT;

        SET @returnValue = 0; -- Success
    END TRY
    BEGIN CATCH
        SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
        SELECT @errorCount = COUNT(*) FROM @messages WHERE severity >= 16;

        -- Record error message
        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @ERROR_MESSAGE, @Severity = @ERROR_SEVERITY, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @RaiseError = 0, @MessagesStack = @MessagesStack OUTPUT;

        IF @@TRANCOUNT > 0 ROLLBACK TRAN;

        INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
        EXEC sp_executesql @stmt = @stmtEmptyMessagesStack, @params = @paramsEmptyMessagesStack, @MessagesStack = @MessagesStack OUTPUT;

        IF @errorCount = 0
        BEGIN
            IF @dryRun <> 0 
            BEGIN 
                -- Output Message result set
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = 'Test Archive RobotLicenseLogs Trigger added (DRY RUN)', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
            END
            ELSE 
            BEGIN
                SET @message = N'Execution finished with error(s)'
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = 'Invalid Archive RobotLicenseLogs Trigger added (FAIL)', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                SET @errorCount = @errorCount + 1;
                SET @returnValue = 4;
            END
        END
        ELSE
        BEGIN
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = 'Invalid Archive RobotLicenseLogs Trigger added (INCORRECT PARAMETERS)', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
        END

            UPDATE arc SET [CountValidFilters] = 0, [CountDuplicateFilters] = 0, [IsDryRun] = @dryRun, [IsError] = @errorCount, [IsFinished] = 1, [FinishedOnDate] = SYSDATETIME()
            FROM [Maintenance].[Archive_RobotLicenseLogs] arc WHERE arc.Id = @archiveId;
        RAISERROR(@ERROR_MESSAGE, @ERROR_SEVERITY, @ERROR_STATE);

        SET @returnValue = ISNULL(@returnValue, 255);
    END CATCH

    ----------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------
    INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
    EXEC sp_executesql @stmt = @stmtEmptyMessagesStack, @params = @paramsEmptyMessagesStack, @MessagesStack = @MessagesStack OUTPUT;

    -- Output Messages result set
    IF @outputDataset = 1 SELECT [RunId] = @RunId, [Date], [Procedure], [Message], [Severity], [State], [Number], [Line] FROM @messages ORDER BY [id] ASC;

    SET @returnValue = ISNULL(@returnValue, 255);
    IF @runId IS NOT NULL AND (@SavedToRunId IS NULL OR @SavedToRunId <> @runId) UPDATE [Maintenance].[Runs] SET [EndDate] = SYSDATETIME(), [ErrorStatus] = @returnValue WHERE Id = @runId;

    ----------------------------------------------------------------------------------------------------
    -- End
    ----------------------------------------------------------------------------------------------------
    RETURN @returnValue;
END
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- DROP PROCEDURE [Maintenance].[ArchiveRobotLicenseLogs]
----------------------------------------------------------------------------------------------------

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[ArchiveRobotLicenseLogs]') AND type in (N'P'))
BEGIN
    EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [Maintenance].[ArchiveRobotLicenseLogs] AS'
    PRINT '  + CREATE PROCEDURE: [Maintenance].[ArchiveRobotLicenseLogs]';
END
ELSE PRINT '  = PROCEDURE [Maintenance].[ArchiveRobotLicenseLogs] already exists' 
GO

PRINT '  ~ UPDATE PROCEDURE: [Maintenance].[ArchiveRobotLicenseLogs]'
GO  

ALTER PROCEDURE [Maintenance].[ArchiveRobotLicenseLogs]
----------------------------------------------------------------------------------------------------
-- ### [Object]: PROCEDURE [Maintenance].[ArchiveRobotLicenseLogs]
-- ### [Version]: 2023-10-06T11:29:36+02:00
-- ### [Source]: _src/Archive/ArchiveDB/RobotLicenseLogs/Procedure_ArchiveDB.Maintenance.ArchiveRobotLicenseLogs.sql
-- ### [Hash]: dc39c27 [SHA256-FAAAA167679F8A106AE41DCA9AB411063A5D994712D80DB198491E9E02CE8EC9]
-- ### [Docs]: https://???.???
-- !!! ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!!!
-- !!! ~~~~~~~~~ NOT OFFICIALLY SUPPORTED BY UIPATH 
-- !!! ~~~~~~~~~ SQL Server >= 2016 SP1
----------------------------------------------------------------------------------------------------
    /* Row count settings */
    @RowsDeletedForEachLoop int = 10000 -- Don't go above 50.000 (min = 1000, Max = 100.000)
    , @MaxConcurrentFilters int = NULL -- NULL or 0 = default value => number of filter in chronoligical order processed by a single batch
    /* Loop Limits */
    , @MaxRunMinutes int = NULL -- NULL or 0 = unlimited
    , @MaxBatchesLoops int = NULL -- NULL or 0 - unlimited
    /* Dry Run */
--    , @DryRunOnly nvarchar(MAX) = NULL -- Y{es} or N{o} => Only Check Parameters (default if NULL = Y)
    /* Delete settings */
	, @SynchronousDeleteIfNoDelay bit = 0
	, @IgnoreDeleteDelay bit = 0

    /* Archive table(s) settings */
    , @CreateArchiveTable nvarchar(MAX) = NULL -- Y{es} or N{o} => Archive table refered by Synonym is create when missing (default if NULL or empty = N)
    , @UpdateArchiveTable nvarchar(MAX) = NULL -- Y{es} or N{o} => Archive table refered by Synonym is update when column(s) are missing (default if NULL or empty = N)
    , @ExcludeColumns nvarchar(MAX) = NULL -- JSON string with array of string with column name(s)
    /* Error Handling */
    , @OnErrorRetry tinyint = NULL -- between 0 and 20 => retry up to 20 times (default if NULL = 10)
    , @OnErrorWaitMillisecond smallint = 1000 -- wait for milliseconds between each Retry (default if NULL = 1000ms)
    /* Messge Logging */
    , @SaveMessagesToTable nvarchar(MAX) = 'Y' -- Y{es} or N{o} => Save to [maintenance].[messages] table (default if NULL = Y)
    , @SavedMessagesRetentionDays smallint = 30
    , @SavedToRunId int = NULL OUTPUT
    , @OutputMessagesToDataset nvarchar(MAX) = 'N' -- Y{es} or N{o} => Output Messages Result set
    , @Verbose nvarchar(MAX) = NULL -- Y{es} = Print all messages < 10
AS
BEGIN
    BEGIN TRY 
        SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
		SET LOCK_TIMEOUT 5000;
        SET ARITHABORT ON;
        SET NOCOUNT ON;
        SET NUMERIC_ROUNDABORT OFF;

        ----------------------------------------------------------------------------------------------------
        -- Local Run Variables
        ----------------------------------------------------------------------------------------------------
        DECLARE @runId int;  
        DECLARE @dryRun bit;
        DECLARE @isTimeOut bit = 0;

        DECLARE @startTime datetime = SYSDATETIME();
        DECLARE @startTimeFloat float(53);

		DECLARE @loopStart datetime;
        DECLARE @maxRunDateTime datetime;
        DECLARE @logToTable bit;
        DECLARE @maxCreationTime datetime;
        DECLARE @MaxErrorRetry tinyint;
        DECLARE @errorDelay smallint;
        DECLARE @errorWait datetime;
        DECLARE @returnValue int = 1;
        ----------------------------------------------------------------------------------------------------
        -- Filters
        ----------------------------------------------------------------------------------------------------
--        DECLARE @listFilters TABLE (OrderId smallint, ArchiveId bigint, SyncId bigint, CurrentId bigint, TargetId bigint, LastId bigint, TargetTimestamp datetime, PreviousTimestamp datetime , TenantId int, LevelId int, DeleteOnly bit, NoDelay bit, PRIMARY KEY(TenantId, LevelId));
        DECLARE @countFilterIds int;
        DECLARE @countArchiveIds int;
        DECLARE @targetTimestamp datetime;
        ----------------------------------------------------------------------------------------------------
        -- Archive settings
        ----------------------------------------------------------------------------------------------------
        DECLARE @topLoopFilters smallint;
        DECLARE @maxBatches smallint;
        ----------------------------------------------------------------------------------------------------
        -- Archive loop
        ----------------------------------------------------------------------------------------------------
        DECLARE @minId bigint = 0;
        DECLARE @maxId bigint;
        DECLARE @currentId bigint
        DECLARE @currentLoopId bigint;
        DECLARE @maxLoopDeleteRows int;
        DECLARE @loopCount int = 0;
        DECLARE @batchCount int = 0;
        ----------------------------------------------------------------------------------------------------
        -- Archive query
        ----------------------------------------------------------------------------------------------------
        DECLARE @archivedColumns nvarchar(MAX);
        DECLARE @sqlArchive nvarchar(MAX);
        ----------------------------------------------------------------------------------------------------
        -- Delete 
        ----------------------------------------------------------------------------------------------------
        DECLARE @deleteIfNoDelay bit = 0
        DECLARE @ignoreDelay bit = 0
        ----------------------------------------------------------------------------------------------------
        -- Count row Ids
        ----------------------------------------------------------------------------------------------------
        DECLARE @countRowIds bigint;
        DECLARE @totalRowIds bigint;
        DECLARE @globalRowIds bigint;
        DECLARE @countDeleteOnlyIds int;
        DECLARE @totalDeleteOnlyIds int;
        DECLARE @globalDeleteOnlyIds bigint;
        DECLARE @countDeleted int;
        DECLARE @totalDeleted int;
        DECLARE @globalDeleted int;
        DECLARE @filtersArchived bigint;
        DECLARE @globalFiltersArchived bigint;
        DECLARE @syncArchived BIGINT
        DECLARE @triggerArchived bigint;
        DECLARE @globalTriggerArchived bigint;
        ----------------------------------------------------------------------------------------------------
        -- Archive Repeat
        ----------------------------------------------------------------------------------------------------
        DECLARE @cursorId bigint;
        DECLARE @cursorName nvarchar(100);
        DECLARE @cursorPreviousRunIds nvarchar(MAX);
        DECLARE @cursorDefinition nvarchar(max);
        DECLARE @cursorArchiveTriggerTime datetime;
        DECLARE @cursorArchiveAfterHours smallint;
        DECLARE @cursorDeleteDelayHours smallint;
        DECLARE @cursorRepeatArchive bit;
        DECLARE @cursorRepeatOffsetHours smallint;
        DECLARE @cursorRepeatUntil datetime;
        DECLARE @cursorCountValidFilters int;
        DECLARE @countRepeat int;

        ----------------------------------------------------------------------------------------------------
        -- Constant / Default value
        ----------------------------------------------------------------------------------------------------
        DECLARE @maxDeleteRows int = 100*1000; -- Raise an error if @RowsDeletedForEachLoop is bigger than this value
        DECLARE @minDeleteRows int = 1*1000; -- Raise an error if @RowsDeletedForEachLoop is smaller than this value
        DECLARE @defaultDeleteRows int = 10*1000;
        DECLARE @verboseBelowLevel int = 10; -- don't print message with Severity < 10 unless Verbose is set to Y

        DECLARE @maxCountFilters int = 10*1000 -- Raise an error if @MaxConcurrentFilters is bigger than this value
        DECLARE @minCountFilters int = 1 -- Raise an error if @MaxConcurrentFilters is bigger than this value
        DECLARE @defaultCountFilters int = 1000 -- used when @MaxConcurrentFilters is null

        DECLARE @paramsYesNo TABLE ([id] tinyint IDENTITY(0, 1) PRIMARY KEY CLUSTERED, [parameter] nvarchar(MAX), [value] int)
        DECLARE @floatingDay float(53) = 1;
        DECLARE @floatingHour float(53) = @floatingDay / 24;
        DECLARE @constDefaultDeleteDelay int = 0;
        DECLARE @constDefaultAfterHours int = NULL;
        ----------------------------------------------------------------------------------------------------
        -- Server Info 
        ----------------------------------------------------------------------------------------------------
        DECLARE @productVersion nvarchar(MAX) = CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(MAX));
        DECLARE @engineEdition int =  CAST(ISNULL(SERVERPROPERTY('EngineEdition'), 0) AS int);
        DECLARE @minProductVersion nvarchar(MAX) = N'13.0.4001.0 (SQL Server 2016 SP1)'
        DECLARE @version numeric(18, 10);
        DECLARE @minVersion numeric(18, 10) = 13.0040010000;
        DECLARE @minCompatibilityLevel int = 130;
        DECLARE @hostPlatform nvarchar(256); 
        ----------------------------------------------------------------------------------------------------
        -- Proc Info
        ----------------------------------------------------------------------------------------------------
        DECLARE @paramsGetProcInfo nvarchar(MAX) = N'@procid int, @info nvarchar(MAX), @output nvarchar(MAX) OUTPUT'
        DECLARE @stmtGetProcInfo nvarchar(MAX) = N'
            DECLARE @definition nvarchar(MAX) = OBJECT_DEFINITION(@procid), @keyword nvarchar(MAX) = REPLICATE(''-'', 2) + SPACE(1) + REPLICATE(''#'', 3) + SPACE(1) + QUOTENAME(LTRIM(RTRIM(@info))) + '':'';
			DECLARE @eol char(1) = IIF(CHARINDEX( CHAR(13) , @definition) > 0, CHAR(13), CHAR(10));
			SET @output = ''''+ LTRIM(RTRIM( SUBSTRING(@definition, NULLIF(CHARINDEX(@keyword, @definition), 0 ) + LEN(@keyword), CHARINDEX( @eol , @definition, CHARINDEX(@keyword, @definition) + LEN(@keyword) + 1) - CHARINDEX(@keyword, @definition) - LEN(@keyword) ))) + '''';
        ';
        DECLARE @procSchemaName nvarchar(MAX) = COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?');
        DECLARE @procObjecttName nvarchar(MAX) = COALESCE(OBJECT_NAME(@@PROCID), N'?');
        DECLARE @procName nvarchar(MAX) = QUOTENAME(COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?')) + N'.' + QUOTENAME(COALESCE(OBJECT_NAME(@@PROCID), N'?')); 
        DECLARE @versionDatetime nvarchar(MAX);
        ----------------------------------------------------------------------------------------------------
        -- Synonym
        ----------------------------------------------------------------------------------------------------
        DECLARE @synonymMessages nvarchar(MAX);
        DECLARE @sourceColumns nvarchar(MAX);
        DECLARE @synonymIsValid bit;
        DECLARE @synonymCreateTable bit;
        DECLARE @synonymUpdateTable bit;
        DECLARE @synonymExcludeColumns nvarchar(MAX);
        DECLARE @isValid bit;
        ----------------------------------------------------------------------------------------------------      
        -- Message / Error Handling
        ----------------------------------------------------------------------------------------------------
        /* Format */
        DECLARE @lineSeparator nvarchar(MAX) = N'----------------------------------------------------------------------------------------------------';
        DECLARE @lineBreak nvarchar(MAX) = N'';
        DECLARE @message nvarchar(MAX);
        DECLARE @string nvarchar(MAX);
        DECLARE @tab tinyint = 2;
        /* Log Stack */
        DECLARE @stmtEmptyMessagesStack nvarchar(MAX) = N'
            SELECT 
                [Date] = m.value(''Date[1]'', ''datetime'')
                , [Procedure] = m.value(''Procedure[1]'', ''nvarchar(MAX)'')
                , [Message] = m.value(''Message[1]'', ''nvarchar(MAX)'')
                , [Severity] = m.value(''Severity[1]'', ''int'')
                , [State] = m.value(''State[1]'', ''int'')
                , [Number] = m.value(''Number[1]'', ''int'')
                , [Line] = m.value(''Line[1]'', ''int'')
            FROM @MessagesStack.nodes(''/messages/message'') x(m)
            SET @MessagesStack = NULL;
        ';
        DECLARE @paramsEmptyMessagesStack nvarchar(MAX) = N'@MessagesStack xml = NULL OUTPUT';
        DECLARE @MessagesStack xml;
        /* Errors */
        DECLARE @ERROR_NUMBER INT, @ERROR_SEVERITY INT, @ERROR_STATE INT, @ERROR_PROCEDURE NVARCHAR(126), @ERROR_LINE INT, @ERROR_MESSAGE NVARCHAR(2048) ;
        DECLARE @errorCount int = 0;
        DECLARE @countErrorRetry tinyint;
        /* Messages */
        DECLARE @messages TABLE(id int IDENTITY(0, 1) PRIMARY KEY, [date] datetime2 DEFAULT SYSDATETIME(), [procedure] nvarchar(MAX) NOT NULL DEFAULT QUOTENAME(COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?')) + N'.' + QUOTENAME(COALESCE(OBJECT_NAME(@@PROCID), N'?')), [message] nvarchar(MAX) NOT NULL, severity tinyint NOT NULL, state tinyint NOT NULL, [number] int, [line] int);
        DECLARE @levelVerbose int;    
        DECLARE @outputDataset bit;
        /* Cursor */ 
        DECLARE CursorMessages CURSOR FAST_FORWARD LOCAL FOR SELECT [Date], [Procedure], [Message], [Severity], [State], [Number], [Line] FROM @messages ORDER BY [ID] ASC;
        DECLARE @cursorDate datetime;
        DECLARE @cursorProcedure nvarchar(MAX);
        DECLARE @cursorMessage nvarchar(MAX);
        DECLARE @cursorSeverity tinyint;
        DECLARE @cursorState tinyint;
        DECLARE @cursorNumber int;
        DECLARE @cursorLine int;    

        ----------------------------------------------------------------------------------------------------
        -- START
        ----------------------------------------------------------------------------------------------------
        SELECT @startTime = SYSDATETIME();
        SELECT @startTimeFloat = CAST(@startTime AS float(53));

        ----------------------------------------------------------------------------------------------------
        -- Gather General & Server Info
        ----------------------------------------------------------------------------------------------------

        -- Get Run Time limit or NULL (unlimited)
	    SET @maxRunDateTime = CASE WHEN ABS(@MaxRunMinutes) >= 1 THEN DATEADD(MINUTE, ABS(@MaxRunMinutes), @startTime) ELSE NULL END;

        -- Output Start & Stop info
        INSERT INTO @messages([Message], Severity, [State]) VALUES 
            ( @lineSeparator, 10, 1)
            , (N'Start Time = ' + CONVERT(nvarchar(MAX), @startTime, 121), 10, 1 )
            , (N'MAX Run Time = ' + ISNULL(CONVERT(nvarchar(MAX), @maxRunDateTime, 121), N'NULL (=> unlimited)'), 10, 1 )
            , (@lineBreak, 10, 1);
/*        -- Get Host info
        IF @version >= 14 SELECT @hostPlatform = host_platform FROM sys.dm_os_host_info ELSE SET @HostPlatform = 'Windows'
*/
        ----------------------------------------------------------------------------------------------------
        -- Check SQL Server Requierements
        ----------------------------------------------------------------------------------------------------
        -- Get Proc Version
--        EXEC sp_executesql @stmt = @stmtGetProcInfo, @params = @paramsGetProcInfo, @procid = @@PROCID, @info = N'Version', @output = @versionDatetime OUTPUT;

        -- Get SQL Server Server Version
        WITH release (id, position, version) AS
        (
            SELECT id = 0, position = CHARINDEX(N'.', @productVersion, 1)
                , version = LEFT(@productVersion, CHARINDEX(N'.', @productVersion, 1))
            UNION ALL
            SELECT id = r.id + 1, position = CHARINDEX(N'.', @productVersion, position + 1)
                , version = r.version + RIGHT(v.[space] + SUBSTRING(@productVersion, position + 1, COALESCE( NULLIF(CHARINDEX('.', @productVersion, position + 1), 0), LEN(@productVersion) + 1) - position - 1), LEN(v.[space]) )
            FROM release r INNER JOIN (VALUES(0, '00'), (1, '00'), (2, '0000'), (3, '0000') ) AS v(id, space) ON v.id = r.id + 1
            WHERE position < LEN(@productVersion) + 1
        )
        SELECT TOP(1) @version = CAST(version AS numeric(18, 10))
        FROM release
        ORDER BY Id DESC

        -- Get Host info
        IF @version >= 14 SELECT @hostPlatform = host_platform FROM sys.dm_os_host_info ELSE SET @HostPlatform = 'Windows'

        INSERT INTO @messages([Message], Severity, [State])
        -- Check min required version
        SELECT N'ERROR: Current SQL Server version is ' + @productVersion + N'. Only version ' + @minProductVersion + + N' or higher is supported.', 16, 1 WHERE @version < @minVersion
        -- Check Database Compatibility Level
        UNION ALL SELECT 'ERROR: Database ' + QUOTENAME(DB_NAME(DB_ID())) + ' Compatibility Level is set to '+ CAST([compatibility_level] AS nvarchar(MAX)) + '. Compatibility level 130 or higher is requiered.', 16, 1 FROM sys.databases WHERE database_id = DB_ID() AND [compatibility_level] < @minCompatibilityLevel
        -- Check opened transation(s)
        --UNION ALL SELECT 'The transaction count is not 0.', 16, 1 WHERE @@TRANCOUNT <> 0
        -- Check uses_ansi_nulls
        UNION ALL SELECT 'ERROR: ANSI_NULLS must be set to ON for this Stored Procedure', 16, 1 FROM sys.sql_modules WHERE [object_id] = @@PROCID AND uses_ansi_nulls <> 1
        -- Check uses_quoted_identifier
        UNION ALL SELECT 'ERROR: QUOTED_IDENTIFIER must be set to ON for this Stored Procedure', 16, 1 FROM sys.sql_modules WHERE [object_id] = @@PROCID AND uses_quoted_identifier <> 1;

        ----------------------------------------------------------------------------------------------------
        -- Parameters
        ----------------------------------------------------------------------------------------------------
        SET @levelVerbose = @VerboseBelowLevel;

        -- Convert Yes / No varations to bit
        INSERT INTO @paramsYesNo([parameter], [value]) VALUES(N'NO', 0), (N'N', 0), (N'0', 0), (N'YES', 1), (N'Y', 1), (N'1', 1);

        ----------------------------------------------------------------------------------------------------
        -- Parameters' Validation
        ----------------------------------------------------------------------------------------------------
       INSERT INTO @messages([Message], Severity, [State])
        VALUES 
            ( @lineSeparator, 10, 1)
            , ( N'Validation', 10, 1)
            , ( @lineSeparator, 10, 1)

        -- Check @RowsDeletedForEachLoop
        SET @maxLoopDeleteRows = ISNULL(NULLIF(@RowsDeletedForEachLoop, 0), @defaultDeleteRows);
        IF @maxLoopDeleteRows < @minDeleteRows OR @maxLoopDeleteRows > @MaxDeleteRows 
        BEGIN
            INSERT INTO @messages ([Message], Severity, [State]) VALUES
                ( 'Parameter @RowsDeletedForEachLoop is invalid: ' + LTRIM(RTRIM(CAST(@RowsDeletedForEachLoop AS nvarchar(MAX)))), 10, 1)
                , ('USAGE: use a value between ' + CAST(@minDeleteRows AS nvarchar(MAX)) + N' and ' + CAST(@maxDeleteRows AS nvarchar(MAX)) +  N'', 10, 1)
                , ('Parameter @RowsDeletedForEachLoop is invalid', 16, 1);
        END

        -- Check @MaxConcurrentFilters
        SET @topLoopFilters = ISNULL(NULLIF(@MaxConcurrentFilters, 0), @defaultCountFilters);
        IF @topLoopFilters < @minCountFilters OR @topLoopFilters > @maxCountFilters 
        BEGIN
            INSERT INTO @messages ([Message], Severity, [State]) VALUES
                ( 'Parameter @MaxConcurrentFilters is invalid: ' + LTRIM(RTRIM(CAST(@MaxConcurrentFilters AS nvarchar(MAX)))), 10, 1)
                , ('USAGE: use a value between ' + CAST(@minCountFilters AS nvarchar(MAX)) + N' and ' + CAST(@defaultCountFilters AS nvarchar(MAX)) +  N'', 10, 1)
                , ('Parameter @RowsDeletedForEachLoop is invalid', 16, 1);
        END

        -- Check @MaxBatchesLoops
        SET @maxBatches = NULLIF(@MaxBatchesLoops, 0);

        -- Check @SynchronousDeleteIfNoDelay
        SELECT @deleteIfNoDelay = [value] FROM @paramsYesNo WHERE [parameter] = LTRIM(RTRIM(@SynchronousDeleteIfNoDelay));
        INSERT INTO @messages ([Message], Severity, [State]) SELECT N'@SynchronousDeleteIfNoDelay is NULL or empty. Default value will be used (No)', 10, 1 WHERE @deleteIfNoDelay IS NULL;
        SET @deleteIfNoDelay = ISNULL(@deleteIfNoDelay, 0);

        -- Check @IgnoreDeleteDelay
        SELECT @ignoreDelay = [value] FROM @paramsYesNo WHERE [parameter] = LTRIM(RTRIM(@IgnoreDeleteDelay));
        INSERT INTO @messages ([Message], Severity, [State]) SELECT N'@IgnoreDeleteDelay is NULL or empty. Default value will be used (No).', 10, 1 WHERE @ignoreDelay IS NULL;
        INSERT INTO @messages ([Message], Severity, [State]) SELECT N'@SynchronousDeleteIfNoDelay must be set if @IgnoreDeleteDelay is set', 16, 1 WHERE @ignoreDelay = 1 AND @deleteIfNoDelay <> 1;
        SET @ignoreDelay = ISNULL(@ignoreDelay, 0);

        -- Check Create Archive Table
        SELECT @synonymCreateTable = [value] FROM @paramsYesNo WHERE [parameter] = LTRIM(RTRIM(@CreateArchiveTable));
        INSERT INTO @messages ([Message], Severity, [State]) SELECT N'@CreateArchiveTable is NULL or empty. Default value will be used (No).', 10, 1 WHERE @synonymCreateTable IS NULL;
        SET @synonymCreateTable = ISNULL(@synonymCreateTable, 0);

        -- Check Update Archive Table
        SELECT @synonymUpdateTable = [value] FROM @paramsYesNo WHERE [parameter] = LTRIM(RTRIM(@UpdateArchiveTable));
        INSERT INTO @messages ([Message], Severity, [State]) SELECT N'@UpdateArchiveTable is NULL or empty. Default value will be used (No).', 10, 1 WHERE @synonymUpdateTable IS NULL;
        SET @synonymUpdateTable = ISNULL(@synonymUpdateTable, 0);

        -- Check Exclude columns
        SELECT @synonymExcludeColumns = NULLIF(LTRIM(RTRIM(@ExcludeColumns)), N'');
        IF ISJSON(@synonymExcludeColumns) = 0 
        INSERT INTO @messages ([Message], Severity, [State]) SELECT N'ERROR: @synonymExcludeColumns is not a valid JSON string with an array of string(s) ["col1", "col2", ...]', 16, 1 WHERE ISJSON(@synonymExcludeColumns) = 0;

		----------------------------------------------------------------------------------------------------
        -- Check Output Settings
        ----------------------------------------------------------------------------------------------------
        -- Check @OutputMessagesToDataset parameter
        SELECT @outputDataset = [value] FROM @paramsYesNo WHERE [parameter] = ISNULL(LTRIM(RTRIM(@OutputMessagesToDataset)), N'N');
        IF @outputDataset IS NULL 
        BEGIN
            INSERT INTO @messages ([Message], Severity, [State]) VALUES
                ( 'Parameter @OutputMessagesToDataset is invalid: ' + LTRIM(RTRIM(@OutputMessagesToDataset)), 10, 1)
                , ('Usage: @OutputMessagesToDataset = Y{es} or N{o}', 10, 1)
                , ('Parameter @OutputMessagesToDataset is invalid: ' + LTRIM(RTRIM(@OutputMessagesToDataset)), 16, 1);
        END

		----------------------------------------------------------------------------------------------------
        -- Check Permissions
        ----------------------------------------------------------------------------------------------------
/*
        -- Check SELECT & DELETE permission on [dbo].[RobotLicenseLogs]
        INSERT INTO @messages ([Message], Severity, [State])
        SELECT 'Permission not effectively granted: ' + UPPER(p.permission_name), 10, 1
        FROM (VALUES(N'', N'SELECT'), (N'', N'DELETE')) AS p (subentity_name, permission_name)
        LEFT JOIN sys.fn_my_permissions(N'[dbo].[RobotLicenseLogs]', N'OBJECT') eff ON eff.subentity_name = p.subentity_name AND eff.permission_name = p.permission_name
        WHERE eff.permission_name IS NULL
        ORDER BY p.permission_name;

        IF @@ROWCOUNT > 0 
        BEGIN
            INSERT INTO @messages ([Message], Severity, [State]) VALUES
                (N'Error: missing permission', 10, 1)
                , (N'SELECT and DELETE permissions are required on [dbo].[RobotLicenseLogs] table', 16, 1);
        END
*/
        -- Check @SaveMessagesToTable parameter
        SELECT @logToTable = [value] FROM @paramsYesNo WHERE [parameter] = ISNULL(LTRIM(RTRIM(@SaveMessagesToTable)), N'Y');
        IF @logToTable IS NULL 
        BEGIN
            INSERT INTO @messages ([Message], Severity, [State]) VALUES
                ( 'Parameter @SaveMessagesToTable is invalid: ' + LTRIM(RTRIM(@SaveMessagesToTable)), 10, 1)
                , ('Usage: @SaveMessagesToTable = Y{es} or N{o}', 10, 1)
                , ('Parameter @SaveMessagesToTable is invalid: ' + LTRIM(RTRIM(@SaveMessagesToTable)), 16, 1);
        END

        -- Check Messages' Tables
        INSERT INTO @messages ([Message], Severity, [State])
        SELECT N'The table ' + QUOTENAME(t.[schema]) + N'.' + QUOTENAME(t.name) + ' is missing.', 10, 1
        FROM ( VALUES('Maintenance', 'Runs'), ('Maintenance', 'Messages')) as t([schema], [name])
        LEFT JOIN sys.objects obj ON obj.name = t.name
        LEFT JOIN sys.schemas sch ON sch.[schema_id] = obj.[schema_id] AND sch.name = t.[schema]
        WHERE (obj.[type] <> 'U' OR  obj.object_id IS NULL) AND obj.object_id IS NULL AND @logToTable = 1;

        -- Update @logToTable ON missing table(s)
        IF @@ROWCOUNT > 0 
        BEGIN
            SET @logToTable = 0;
            INSERT INTO @messages ([Message], Severity, [State]) VALUES
                ('Procedure''s execution (Errors & Messages) can''t be saved to [Messages] table', 16, 1);
        END 
        ELSE
        BEGIN
            -- Check missing permissions on Runs and Messages Table
            WITH p AS(
                SELECT entity_name, subentity_name, permission_name --'Permission not effectively granted: ' + UPPER(p.permission_name)
                FROM (VALUES
                    (N'[Maintenance].[Runs]', N'', N'INSERT')
                    , (N'[Maintenance].[Runs]', N'', N'DELETE')
                    , (N'[Maintenance].[Messages]', N'', N'INSERT')
                    , (N'[Maintenance].[Messages]', N'', N'DELETE')
                ) AS p (entity_name, subentity_name, permission_name)
            )
            INSERT INTO @messages ([Message], Severity, [State])
            SELECT 'Permission not effectively granted on ' + p.entity_name + N': ' + UPPER(p.permission_name), 10, 1
            FROM p
            LEFT JOIN (
                SELECT ca.entity_name, ca.subentity_name, ca.permission_name FROM (SELECT DISTINCT entity_name FROM p) AS t
                CROSS APPLY sys.fn_my_permissions(t.entity_name, N'OBJECT') ca
            ) eff ON eff.entity_name = p.entity_name AND eff.subentity_name = p.subentity_name AND eff.permission_name = p.permission_name
            WHERE eff.permission_name IS NULL 
            ORDER BY p.entity_name, p.permission_name;

            IF @@ROWCOUNT > 0 
            BEGIN
                INSERT INTO @messages ([Message], Severity, [State]) VALUES
                    (N'Error: missing permission', 10, 1)
                    , (N'WHEN @SaveMessagesToTable is set to Y or YES, INSERT and DELETE permissions are required on [Maintenance].[Runs] and [Maintenance].[Messages] tables', 16, 1);
            END
        END

        ----------------------------------------------------------------------------------------------------
        -- Check Error(s) count
        ----------------------------------------------------------------------------------------------------

        SELECT @errorCount = COUNT(*) FROM @messages WHERE severity > 10;
        IF @errorCount > 0 INSERT INTO @messages ([Message], Severity, [State]) SELECT N'End, see previous Error(s): ' + CAST(@errorCount AS nvarchar(10)) + N' found', 16, 1;

        ----------------------------------------------------------------------------------------------------
        -- Settings
        ----------------------------------------------------------------------------------------------------

        IF  NOT EXISTS(SELECT 1 FROM @messages WHERE severity > 10)
        BEGIN
            -- Check @OnErrorRetry
            SET @MaxErrorRetry = @OnErrorRetry;
            IF @MaxErrorRetry IS NULL 
            BEGIN
                SET @MaxErrorRetry = 5;
                INSERT INTO @messages ([Message], Severity, [State]) VALUES (N'@OnErrorRetry is NULL. Default value will be used (5 times).', 10, 1);
            END
            IF @MaxErrorRetry > 20
            BEGIN
                SET @MaxErrorRetry = 20;
                INSERT INTO @messages ([Message], Severity, [State]) VALUES (N'@OnErrorRetry is bigger than 20. Max value will be used (20 times)', 10, 1);
            END

            -- Check @OnErrorWaitMillisecond
            IF @OnErrorWaitMillisecond IS NULL 
            BEGIN
                INSERT INTO @messages ([Message], Severity, [State]) VALUES (N'@OnErrorWaitMillisecond is NULL. Default value will be used (1000 ms).', 10, 1);
            END
            SELECT @errorDelay = ISNULL(ABS(@OnErrorWaitMillisecond), 1000);
            SELECT @errorWait = DATEADD(MILLISECOND, @errorDelay, 0);

            INSERT INTO @messages ([Message], Severity, [State]) VALUES (N'@@LOCK_TIMEOUT = ' + CAST(@@LOCK_TIMEOUT AS nvarchar(MAX)), 10 , 1);

/*            -- Check Dry Run
            SELECT @dryRun = [value] FROM @paramsYesNo WHERE [parameter] = LTRIM(RTRIM(@DryRunOnly));
            IF @dryRun IS NULL 
            BEGIN
                SET @dryRun = 1;
                INSERT INTO @messages ([Message], Severity, [State]) VALUES (N'@DryRunOnly is NULL. Default value will be used (Yes).', 10, 1);
            END*/

            -- Verbose Level
            SELECT @levelVerbose = CASE WHEN [value] = 1 THEN 0 ELSE @verboseBelowLevel END FROM @paramsYesNo WHERE [parameter] = LTRIM(RTRIM(@Verbose));
            IF @levelVerbose IS NULL 
            BEGIN
                SET @levelVerbose = @verboseBelowLevel;
                INSERT INTO @messages ([Message], Severity, [State]) VALUES (N'@Verbose is NULL. Default value will be used (No).', 10, 1);
            END

            INSERT INTO @messages ([Message], Severity, [State]) VALUES
                (N'Run until = ' + ISNULL(CONVERT(nvarchar(MAX), @maxRunDateTime, 121), N'unlimited'), 10, 1 )
            INSERT INTO @messages ([Message], Severity, [State]) VALUES
                (N'Max batches = ' + ISNULL(CONVERT(nvarchar(MAX), @maxBatches, 121), N'unlimited'), 10, 1 )
		END

        ----------------------------------------------------------------------------------------------------
        -- Create new Run Id
        ----------------------------------------------------------------------------------------------------
        INSERT INTO @messages ([Message], Severity, [State]) 
        SELECT N'Parameter Run Id [' + CAST(@SavedToRunId AS nvarchar(100)) + N'] is already ended.' , 10, 1 FROM [Maintenance].[Runs] WHERE Id = @SavedToRunId AND EndDate IS NOT NULL
        UNION ALL SELECT N'Parameter Run Id [' + CAST(@SavedToRunId AS nvarchar(100)) + N'] not found.', 10, 1 WHERE @SavedToRunId IS NOT NULL AND NOT EXISTS (SELECT 1 FROM [Maintenance].[Runs] WHERE Id = @SavedToRunId)
        
        IF @SavedToRunId IS NULL OR NOT EXISTS(SELECT 1 FROM [Maintenance].[Runs] WHERE Id = @SavedToRunId AND EndDate IS NULL)
        BEGIN
            INSERT INTO [Maintenance].[Runs]([Type], [Info], [StartTime]) SELECT N'Add Archive RobotLicenseLogs Trigger', N'PROCEDURE ' + @procName, @startTime;
            SELECT @runId = @@IDENTITY, @SavedToRunId = @@IDENTITY;
            INSERT INTO @messages ([Message], Severity, [State]) VALUES 
                (N'Messages saved to new Run Id: ' + CONVERT(nvarchar(MAX), @runId), 10, 1);
        END
        ELSE SELECT @runId = @SavedToRunId;

        ----------------------------------------------------------------------------------------------------
        -- Check Main Archive objects
        ----------------------------------------------------------------------------------------------------
        IF NOT EXISTS(SELECT 1 FROM @messages WHERE severity > 10)
        BEGIN
            -- Check Synonyms and source/Archive tables
            BEGIN TRY            
                SET @isValid = 1;
                EXEC [Maintenance].[SetSourceTableRobotLicenseLogs] @Messages = @synonymMessages OUTPUT, @IsValid = @synonymIsValid OUTPUT, @Columns = @sourceColumns OUTPUT;
                INSERT INTO @messages ([Procedure], [Message], Severity, [State]) SELECT [Procedure], [Message], [Severity], [State] FROM OPENJSON(@synonymMessages, N'$') WITH ([Procedure] nvarchar(MAX), [Message] nvarchar(MAX), [Severity] tinyint, [State] tinyint);
                SELECT @isValid = IIF(@isValid = 1 AND @synonymIsValid = 1, 1, 0);
                EXEC [Maintenance].[SetArchiveTableRobotLicenseLogs] @Messages = @synonymMessages OUTPUT, @IsValid = @synonymIsValid OUTPUT, @CreateTable = @synonymCreateTable, @UpdateTable = @synonymUpdateTable, @SourceColumns = @sourceColumns OUTPUT, @ExcludeColumns = @synonymExcludeColumns;
                INSERT INTO @messages ([Procedure], [Message], Severity, [State]) SELECT [Procedure], [Message], [Severity], [State] FROM OPENJSON(@synonymMessages, N'$') WITH ([Procedure] nvarchar(MAX), [Message] nvarchar(MAX), [Severity] tinyint, [State] tinyint);
                SELECT @isValid = IIF(@isValid = 1 AND @synonymIsValid = 1, 1, 0);
--                EXEC [Maintenance].[ValidateArchiveObjectsRobotLicenseLogs] @Messages = @synonymMessages OUTPUT, @IsValid = @synonymIsValid OUTPUT, @CreateTable = @synonymCreateTable, @UpdateTable = @synonymUpdateTable, @SourceColumns = @sourceColumns OUTPUT, @ExcludeColumns = @synonymExcludeColumns;
--                INSERT INTO @messages ([Procedure], [Message], Severity, [State]) SELECT [Procedure], [Message], [Severity], [State] FROM OPENJSON(@synonymMessages, N'$') WITH ([Procedure] nvarchar(MAX), [Message] nvarchar(MAX), [Severity] tinyint, [State] tinyint);

                INSERT INTO @messages ([Message], Severity, [State]) SELECT 'ERORR: Synonyms and archive/source tables checks failed, see previous errors', 16, 1 WHERE ISNULL(@isValid, 0) = 0;
            END TRY
            BEGIN CATCH
                -- Get Unknown error
                SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();

                INSERT INTO @messages ([Procedure], [Message], Severity, [State])
                SELECT [Procedure], [Message], [Severity], [State] FROM OPENJSON(@synonymMessages, N'$') WITH ([Procedure] nvarchar(MAX), [Message] nvarchar(MAX), [Severity] tinyint, [State] tinyint);

                INSERT INTO @messages ([Message], Severity, [State]) VALUES 
                    (N'ERORR: error(s) occured while checking synonyms and archive/source tables', 16, 1);
                THROW;
            END CATCH
        END
        -- Prepare columns and archive query
        IF NOT EXISTS(SELECT 1 FROM @messages WHERE severity > 10)
        BEGIN
            BEGIN TRY
                SELECT @archivedColumns = NULL;
                SELECT @archivedColumns = COALESCE(@archivedColumns + N', ' + [value], [value]) FROM OPENJSON(@sourceColumns);
                SELECT @sqlArchive = N'
                    INSERT INTO [Maintenance].[Synonym_Archive_RobotLicenseLogs](' + @archivedColumns + N')
                    SELECT ' + @archivedColumns + N' 
                    FROM #tempListIds ids
                    INNER JOIN [Maintenance].[Synonym_Source_RobotLicenseLogs] src ON ids.tempId = src.Id
                    WHERE tempDeleteOnly = 0 AND NOT EXISTS(SELECT 1 FROM [Maintenance].[Synonym_Archive_RobotLicenseLogs] WHERE Id = ids.tempId)';
                INSERT INTO @messages ([Message], Severity, [State]) VALUES (N'Archive query: ' + ISNULL(@sqlArchive, N'-'), 10, 1);
            END TRY
            BEGIN CATCH
                -- Get Unknown error
                SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
                -- Save Unknwon Errror
                INSERT INTO @messages ([Message], Severity, [State]) VALUES (N'ERORR: error(s) occured while preparing archive SQL query', 16, 1);
                THROW;
            END CATCH
        END
    END TRY
    BEGIN CATCH
        -- Get Unknown error
        SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
        -- Save Unknwon Errror
        INSERT INTO @messages ([Message], Severity, [State], [Number], [Line])
        SELECT @ERROR_MESSAGE, @ERROR_SEVERITY, @ERROR_STATE, @ERROR_NUMBER, @ERROR_LINE;
    END CATCH

    ----------------------------------------------------------------------------------------------------
    -- Print & Save Errors & Messages
    ----------------------------------------------------------------------------------------------------
    SELECT @errorCount = COUNT(*) FROM @messages WHERE severity > 10;
    OPEN CursorMessages;
    FETCH CursorMessages INTO @cursorDate, @cursorProcedure, @cursorMessage, @cursorSeverity, @cursorState, @cursorNumber, @cursorLine;

    IF CURSOR_STATUS('local', 'CursorMessages') = 1
    BEGIN
        WHILE @@FETCH_STATUS = 0
        BEGIN;
            IF @logToTable = 0 OR @errorCount > 0 OR @cursorSeverity >= @levelVerbose OR @cursorSeverity > 10 RAISERROR('%s', @cursorSeverity, @cursorState, @cursorMessage) WITH NOWAIT;
            IF @cursorSeverity >= 16 RAISERROR('', 10, 1) WITH NOWAIT;
            FETCH CursorMessages INTO @cursorDate, @cursorProcedure, @cursorMessage, @cursorSeverity, @cursorState, @cursorNumber, @cursorLine;
        END
    END
    ELSE 
    BEGIN
        SET @message = 'Execution has been canceled: Error Opening Messages Cursor';
        INSERT INTO @messages ([Message], Severity, [State]) VALUES (@message, 16, 1);
        SET @errorCount = @errorCount +1;
        RAISERROR(@message, 16, 1);
    END 

	IF CURSOR_STATUS('local', 'CursorMessages') >= 0 CLOSE CursorMessages;
	IF CURSOR_STATUS('local', 'CursorMessages') >= -1 DEALLOCATE CursorMessages;

    BEGIN TRY 
        -- Records existing messages
        IF @runId IS NOT NULL 
        BEGIN
            INSERT INTO [Maintenance].[Messages](RunId, [Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
            SELECT @runId, [Date], [Procedure], [Message], [Severity], [State], [Number], [Line]
            FROM @messages 
            ORDER BY Id ASC;
        END

        ----------------------------------------------------------------------------------------------------
        -- End Run on Error(s)
        ----------------------------------------------------------------------------------------------------
        IF EXISTS(SELECT 1 FROM @messages WHERE severity > 10)
        BEGIN
            SET @returnValue = 3;
            SET @message = N'Incorrect Parameters or Settings, see previous Error(s): ' + CAST(@errorCount AS nvarchar(MAX)) + N' found';
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 16, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = 0, @MessagesStack = @MessagesStack OUTPUT;
            -- RETURN 123; => catch
        END
        ----------------------------------------------------------------------------------------------------
        -- End Run on Dry Run
        ----------------------------------------------------------------------------------------------------
        IF @dryRun <> 0 
        BEGIN
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineBreak, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

            INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
            EXEC sp_executesql @stmt = @stmtEmptyMessagesStack, @params = @paramsEmptyMessagesStack, @MessagesStack = @MessagesStack OUTPUT;

            SET @returnValue = 1;

            SET @message = 'DRY RUN ONLY (check output and parameters and set @DryRunOnly to No when ready)';
            EXEC [Maintenance].[AddRunMessage] @RunId = @RunId, @Procedure = @procName, @Message = @message, @Severity = 11, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = 0, @MessagesStack = @MessagesStack OUTPUT;
            -- RETURN 1; => catch
        END
        ----------------------------------------------------------------------------------------------------
        -- Remove Saved messages
        ----------------------------------------------------------------------------------------------------
        --DELETE FROM @messages;

        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineBreak, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

        ----------------------------------------------------------------------------------------------------
        -- Archive
        ----------------------------------------------------------------------------------------------------
        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = 'Start Archive', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

        INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
        EXEC sp_executesql @stmt = @stmtEmptyMessagesStack, @params = @paramsEmptyMessagesStack, @MessagesStack = @MessagesStack OUTPUT;

        INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
        EXEC sp_executesql @stmt = @stmtEmptyMessagesStack, @params = @paramsEmptyMessagesStack, @MessagesStack = @MessagesStack OUTPUT;

        -- Create temp table (Filter List / Repeat Triggers)
        DROP TABLE IF EXISTS #tempListFilters;
        CREATE TABLE #tempListFilters(OrderId smallint, ArchiveId bigint, SyncId bigint, CurrentId bigint, TargetId bigint, LastId bigint, TargetTimestamp datetime, PreviousTimestamp datetime , TenantId int, DeleteOnly bit, NoDelay bit, PRIMARY KEY(TenantId, SyncId));
        DROP TABLE IF EXISTS #tempRepeatTriggersTable;
        CREATE TABLE #tempRepeatTriggersTable(Id bigint, [Name] nvarchar(100), /*[PreviousRunIds] bigint,*/ [Definition] nvarchar(MAX)/*, TargetTimestamp*/, ArchiveTriggerTime datetime, ArchiveAfterHours smallint, DeleteDelayHours smallint, RepeatArchive bit, RepeatOffsetHours smallint, RepeatUntil datetime, CountValidFilters int)

        -- Batch Loop 
        WHILE 0 >= 0
        BEGIN
            SET @batchCount = @batchCount + 1;
            SET @message = N'Start new filter batch [' + CAST(@batchCount AS nvarchar(100)) + N']';
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = 0, @MessagesStack = @MessagesStack OUTPUT;
            BEGIN TRY
                -- Init #tempListFilters
                TRUNCATE TABLE #tempListFilters;
                SELECT @totalRowIds = 0, @totalDeleteOnlyIds = 0, @totalDeleted = 0

                --  Get Filters
                INSERT INTO #tempListFilters(ArchiveId, SyncId, CurrentId, TargetId, TargetTimestamp, PreviousTimestamp, TenantId, DeleteOnly, NoDelay, OrderId)
                SELECT TOP(@topLoopFilters) snc.ArchiveId, flt.SyncId, CurrentId = ISNULL(flt.CurrentId, 0), flt.TargetId, flt.TargetTimeStamp, flt.PreviousTimestamp , flt.TenantId, flt.DeleteOnly
                    , IIF( @deleteIfNoDelay = 1 AND (snc.DeleteAfterDatetime = flt.TargetTimestamp OR @ignoreDelay = 1), 1, 0)
                    , OrderId = ROW_NUMBER() OVER(PARTITION BY flt.TenantId ORDER BY flt.TargetTimeStamp ASC)
                FROM [Maintenance].[Filter_RobotLicenseLogs] flt 
                INNER JOIN [Maintenance].[Sync_RobotLicenseLogs] snc ON snc.Id = flt.SyncId
                INNER JOIN [Maintenance].[Archive_RobotLicenseLogs] arc ON arc.Id = snc.ArchiveId
                WHERE flt.IsArchived = 0 AND snc.IsArchived = 0 AND (flt.TargetId IS NULL OR flt.CurrentId IS NULL OR flt.CurrentId < flt.TargetId) AND arc.ToDo = 1 AND arc.ArchiveTriggerTime < @StartTime
                ORDER BY flt.PreviousTimestamp ASC, TargetTimestamp ASC;

                SELECT @countFilterIds = ISNULL(COUNT(*), 0), @countArchiveIds = ISNULL(COUNT(DISTINCT ArchiveId), 0) FROM #tempListFilters;
                SELECT @targetTimestamp = MAX(TargetTimeStamp) FROM #tempListFilters WHERE TargetTimeStamp IS NOT NULL;

                SELECT @maxId = MAX(Id) FROM [Maintenance].[Synonym_Source_RobotLicenseLogs] /*WITH(INDEX([IX_Machine]))*/ WHERE EndDate <= @targetTimestamp;
                DECLARE @maxTargetId bigint;

                SELECT @maxTargetId = MAX(ISNULL(TargetId, 0)) FROM #tempListFilters;
                IF @maxId < @maxTargetId SET @maxId = @maxTargetId;
            END TRY
            BEGIN CATCH
                -- Get Unknown error
                SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
                -- Save Unknwon Errror
                SET @message = N'  Error(s) occured while retrieving filters';
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = @ERROR_STATE, @Number = @ERROR_NUMBER, @Line = @ERROR_LINE, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                THROW;
            END CATCH

            ----------------------------------------------------------------------------------------------------
            -- Add Repeat Trigger(s)
            ----------------------------------------------------------------------------------------------------
            BEGIN TRY
                -- Init #tempRepeatTriggersTable
                TRUNCATE TABLE #tempRepeatTriggersTable;

                INSERT INTO #tempRepeatTriggersTable(Id, [Name], [Definition], ArchiveTriggerTime, ArchiveAfterHours, DeleteDelayHours, RepeatArchive, RepeatOffsetHours, RepeatUntil, CountValidFilters)
                SELECT Id, [Name]/*, [PreviousRunIds]*/, [Definition]/*, TargetTimestamp*/, CAST( CAST(ArchiveTriggerTime AS float(53)) + ABS(@floatingHour * RepeatOffsetHours) AS datetime), ArchiveAfterHours, DeleteDelayHours, RepeatArchive, RepeatOffsetHours, RepeatUntil, CountValidFilters
                FROM [Maintenance].[Archive_RobotLicenseLogs] arc 
                WHERE arc.[ToDo] = 1 AND arc.RepeatArchive = 1 AND (RepeatUntil IS NULL OR RepeatUntil >= CAST( CAST(ArchiveTriggerTime AS float(53)) + ABS(@floatingHour * RepeatOffsetHours) AS datetime))
                    AND NOT EXISTS (SELECT  1 FROM [Maintenance].[Archive_RobotLicenseLogs] WHERE Id > arc.Id AND ParentArchiveId = arc.Id) 
                    AND ( (arc.CountValidFilters = 0 AND arc.[TargetTimestamp] <= ISNULL(@targetTimestamp, SYSDATETIME())) OR EXISTS(SELECT 1 FROM #tempListFilters WHERE ArchiveId = arc.Id) ) 
                ORDER BY Id ASC;

                SELECT @countRepeat = COUNT(*) FROM #tempRepeatTriggersTable;
                IF @countRepeat > 0 
                BEGIN
                    SET @message = SPACE(@tab * 1) + N'Repeat Archive Trigger(s)... [' + CAST(@countRepeat AS nvarchar(100)) + N']';
                    EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                    UPDATE arc SET PreviousRunIds = (
                        SELECT [runid], [message], [timestamp] FROM (
                            SELECT [runid] = @runId, [message] = N'Repeat Archive Trigger', [timestamp] = SYSDATETIME()
                            UNION ALL SELECT [runid], [message], [timestamp] FROM OPENJSON(PreviousRunIds) WITH ([runid] nvarchar(MAX) N'$.runid', [message] nvarchar(MAX) N'$.message', [timestamp] datetime2 N'$.timestamp')
                        ) v FOR JSON PATH
                    )
                    FROM [Maintenance].[Archive_RobotLicenseLogs] arc 
                    INNER JOIN #tempRepeatTriggersTable tbl ON tbl.Id = arc.Id;

                    IF CURSOR_STATUS('local', 'CursorRepeatClose') >= 0 CLOSE CursorRepeatClose;
                    IF CURSOR_STATUS('local', 'CursorRepeatClose') >= -1 DEALLOCATE CursorRepeatClose;

                    DECLARE CursorRepeatClose CURSOR FAST_FORWARD LOCAL FOR 
                        SELECT Id, [Name]/*, [PreviousRunIds]*/, [Definition]/*, TargetTimestamp*/, ArchiveTriggerTime, ArchiveAfterHours, DeleteDelayHours, RepeatArchive, RepeatOffsetHours, RepeatUntil, CountValidFilters FROM #tempRepeatTriggersTable;

                    OPEN CursorRepeatClose;
                    FETCH CursorRepeatClose INTO @cursorId, @cursorName, @cursorDefinition, @cursorArchiveTriggerTime, @cursorArchiveAfterHours, @cursorDeleteDelayHours, @cursorRepeatArchive, @cursorRepeatOffsetHours, @cursorRepeatUntil, @cursorCountValidFilters

                    -- Add Archive Trigger(s) (REPEAT)
                    IF CURSOR_STATUS('local', 'CursorRepeatClose') = 1
                    BEGIN
                        WHILE @@FETCH_STATUS = 0
                        BEGIN;
                            IF @cursorRepeatArchive = 1
                            BEGIN
                                SET @message = SPACE(@tab * 2) + N'Repeat Archive Id [' + CAST(@cursorId AS nvarchar(100)) + N'] on ' + CONVERT(nvarchar(100), @cursorArchiveTriggerTime, 120);
                                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                                EXEC [Maintenance].[AddArchiveTriggerRobotLicenseLogs] @SavedTorunId = @runId, @DryRunOnly = 0, @Name = @cursorName, @ArchiveTriggerTime = @cursorArchiveTriggerTime, @ArchiveAfterHours = @cursorArchiveAfterHours, @DeleteDelayHours = @cursorDeleteDelayHours, @Filters = @cursorDefinition, @RepeatArchive = @cursorRepeatArchive, @RepeatOffsetHours = @cursorRepeatOffsetHours, @RepeatUntil = @cursorRepeatUntil, @ParentArchiveId = @cursorId
                            END
                            FETCH CursorRepeatClose INTO @cursorId, @cursorName, @cursorDefinition, @cursorArchiveTriggerTime, @cursorArchiveAfterHours, @cursorDeleteDelayHours, @cursorRepeatArchive, @cursorRepeatOffsetHours, @cursorRepeatUntil, @cursorCountValidFilters
                        END
                    END
                    ELSE 
                    BEGIN
                        SET @message = 'Execution has been canceled: Error Opening Messages Cursor (duplicate triggers)';
                        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                    END

                    IF CURSOR_STATUS('local', 'CursorRepeatClose') >= 0 CLOSE CursorRepeatClose;
                    IF CURSOR_STATUS('local', 'CursorRepeatClose') >= -1 DEALLOCATE CursorRepeatClose;
                END
            END TRY
            BEGIN CATCH
                -- Get Unknown error
                SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
                -- Save Unknwon Errror
                SET @message = N'  Error(s) occured while duplicating triggers';
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = @ERROR_STATE, @Number = @ERROR_NUMBER, @Line = @ERROR_LINE, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                THROW;
            END CATCH

            INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
            EXEC sp_executesql @stmt = @stmtEmptyMessagesStack, @params = @paramsEmptyMessagesStack, @MessagesStack = @MessagesStack OUTPUT;

            IF @countFilterIds < 1
            BEGIN
                SET @message = SPACE(@tab * 1) + N'No remaining filters found in [Maintenance].[Filter_RobotLicenseLogs]'
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
            END
            ELSE
            BEGIN
                -- Update status for Archive with no valid filter
                IF EXISTS (SELECT 1 FROM [Maintenance].[Archive_RobotLicenseLogs] arc WHERE arc.[ToDo] = 1 AND arc.CountValidFilters = 0 AND arc.[TargetTimestamp] < @targetTimestamp)
                BEGIN
                    SELECT @message = NULL;
                    SELECT  @message = COALESCE(@message + ', ' + CAST(Id AS nvarchar(100)), CAST(Id AS nvarchar(100)) ) FROM [Maintenance].[Archive_RobotLicenseLogs] arc WHERE arc.[ToDo] = 1 AND arc.CountValidFilters = 0 AND arc.[TargetTimestamp] <= @targetTimestamp ORDER BY Id;
                    SET @message = SPACE(@tab * 1) + N'Close Archive Trigger(s) with no filter(s): ' + @message;
                    EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                    UPDATE arc SET IsFinished = 1, FinishedOnDate = SYSDATETIME(), CurrentRunId = @runid, [Message] = N'Archive Trigger finished (no filters)'
                        , PreviousRunIds = (
                        SELECT [runid], [message], [timestamp] FROM (
                            SELECT [runid] = @runId, [message] = N'Archive Trigger finished (no filters)', [timestamp] = SYSDATETIME()
                            UNION ALL SELECT [runid], [message], [timestamp] FROM OPENJSON(PreviousRunIds) WITH ([runid] nvarchar(MAX) N'$.runid', [message] nvarchar(MAX) N'$.message', [timestamp] datetime2 N'$.timestamp')
                        ) v FOR JSON PATH
                    )
                    FROM [Maintenance].[Archive_RobotLicenseLogs] arc WHERE arc.[ToDo] = 1 AND arc.CountValidFilters = 0 AND arc.[TargetTimestamp] <= @targetTimestamp;
                END    

                SET @message =  SPACE(@tab * 1) + N'Archive Filter(s) found: ' + ISNULL(CAST(@countFilterIds AS nvarchar(100)), N'-');
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                SET @message = NULL;
                SELECT  @message = COALESCE(@message + ', ' + [Archive], [Archive]) FROM (SELECT [ArchiveId], [Archive] = CAST(ArchiveId AS nvarchar(100)) + N'('+ CAST(COUNT(*) AS nvarchar(100)) + N')' FROM #tempListFilters GROUP BY [ArchiveId]) flt ORDER BY ArchiveId;
                SET @message =  SPACE(@tab * 2) + N'Processing Trigger Archive Id(s): ' + @message;
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                SET @message =  SPACE(@tab * 2) + N'Target timestamp: ' + ISNULL(CONVERT(nvarchar(100), @targetTimestamp, 120), N'-');
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                SET @message =  SPACE(@tab * 2) + N'Target Id: ' + ISNULL(CAST(@maxId AS nvarchar(100)), N'-');
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                -- Update TargetId 
                UPDATE #tempListFilters SET TargetId = @maxId; -- IIF(@maxId > TargetId OR TargetId IS NULL, @maxId, TargetId);

                BEGIN TRY
                    ---------------------------------------------------------------------------------------------------+
                    -- START TRANSACTION BLOCK                                                                         |
                    ---------------------------------------------------------------------------------------------------+
                    BEGIN TRAN;                                                                                      --|
                                                                                                                     --|
                    UPDATE flt SET TargetId = lst.TargetId                                                           --|
                    FROM [Maintenance].[Filter_RobotLicenseLogs] flt                                                        --|
                    INNER JOIN #tempListFilters lst ON lst.SyncId = flt.SyncId AND lst.TenantId = flt.TenantId;      --|
                                                                                                                     --|
                    -- Add Last Current Id to each 1st filter per Tenant from most recent Archiving trigger(s) per Tenant
                    UPDATE lst SET LastId = oap.LastId FROM #tempListFilters lst                                     --|
                    CROSS APPLY (                                                                                    --|
                        SELECT LastId = MAX(TargetId)                                                                --|
                        FROM [Maintenance].[Filter_RobotLicenseLogs] flt                                                    --|
                        WHERE flt.TenantId = lst.TenantId AND flt.IsArchived = 1 AND flt.CurrentId = flt.TargetId    --|
                    ) oap                                                                                            --|
                    WHERE lst.OrderId = 1;                                                                           --|
                                                                                                                     --|
                    UPDATE arc SET PreviousRunIds = (                                                                --|
                        SELECT [runid], [message], [timestamp] FROM (                                                --|
                            SELECT [runid] = @runId, [message] = N'Process Filter(s) [' + CAST((SELECT COUNT(*) FROM #tempListFilters WHERE ArchiveId = arc.Id) AS nvarchar(100)) + N']', [timestamp] = SYSDATETIME()
                            UNION ALL SELECT [runid], [message], [timestamp] FROM OPENJSON(PreviousRunIds) WITH ([runid] nvarchar(MAX) N'$.runid', [message] nvarchar(MAX) N'$.message', [timestamp] datetime2 N'$.timestamp')
                        ) v FOR JSON PATH                                                                            --|
                    ), CurrentRunId = @runid                                                                         --|
                    FROM [Maintenance].[Archive_RobotLicenseLogs] arc                                                       --|
                    WHERE EXISTS (SELECT 1 FROM #tempListFilters WHERE ArchiveId = arc.Id);                          --|
                                                                                                                     --|
                    IF @@TRANCOUNT > 0 COMMIT;                                                                       --|
                    ---------------------------------------------------------------------------------------------------+
                    -- END TRANSACTION BLOCK                                                                           |
                    ---------------------------------------------------------------------------------------------------+
                END TRY
                BEGIN CATCH
                    -- Get Unknown error
                    SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();

                    IF @@TRANCOUNT > 0 ROLLBACK;
                    -- Save Unknwon Errror
                    SET @message = N'  Error(s) occured while updating filters';
                    EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = @ERROR_STATE, @Number = @ERROR_NUMBER, @Line = @ERROR_LINE, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                    THROW;
                END CATCH

                -- Create temp table for Ids
                DROP TABLE IF EXISTS #tempListIds;
                CREATE TABLE #tempListIds(tempId bigint PRIMARY KEY CLUSTERED, tempSyncId bigint, tempDeleteOnly bit, tempNoDelay bit, INDEX [IX_DeleteOnly] NONCLUSTERED (tempId) WHERE tempDeleteOnly = 0);

                -- Init Counts
                SELECT @currentId = 0;
                SELECT @currentId = MIN(flt.CurrentId) FROM #tempListFilters flt;

                SET @message = SPACE(@tab * 1) + N'Start Archiving';
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                WHILE 1 = 1 -- Loop Archinving
                BEGIN
                    BEGIN TRY
                        TRUNCATE TABLE #tempListIds;

                        -- Get Ids from source table
                        INSERT INTO #tempListIds(tempId, tempSyncId, tempDeleteOnly, tempNoDelay)
                        SELECT TOP(@maxLoopDeleteRows) src.Id, flt.SyncId, flt.DeleteOnly, flt.NoDelay
                        FROM #tempListFilters flt
                        INNER JOIN [Maintenance].[Synonym_Source_RobotLicenseLogs] src ON src.TenantId = flt.TenantId
                        WHERE ( (src.EndDate >= flt.PreviousTimestamp AND src.EndDate < flt.TargetTimestamp) OR (flt.LastId IS NOT NULL AND src.EndDate < flt.PreviousTimestamp AND src.Id > flt.LastId ) ) AND src.Id >= flt.CurrentId AND src.Id <= @maxId AND src.Id >= @currentId
                            AND EndDate IS NOT NULL
                        ORDER BY src.Id ASC;
 
                        SELECT @countRowIds = @@ROWCOUNT;
                        SELECT @currentLoopId = MAX(tempId) + 1 FROM #tempListIds;

                        IF NOT @countRowIds > 0 
                        BEGIN
                            SET @message = SPACE(@tab * 1) + N'Nothing left to Archive in current batch...';
                            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                            -- If nothing left, save status...
                            -- Update IsArchived when current Id(s) reach Target Id
                            UPDATE flt SET CurrentId = @maxId, IsArchived = 1
                            FROM [Maintenance].[Filter_RobotLicenseLogs] flt
                            INNER JOIN #tempListFilters lst ON lst.TenantId = flt.TenantId AND lst.SyncId = flt.SyncId --AND flt.CurrentId >= flt.TargetId

                            SELECT @filtersArchived = @@ROWCOUNT, @globalFiltersArchived = ISNULL(@globalFiltersArchived, 0) + @@ROWCOUNT;
                            BREAK;
                        END
                        -----------------------------------------------------------------------------------------------------------------------+
                        -- START TRANSACTION BLOCK                                                                                             |
                        -----------------------------------------------------------------------------------------------------------------------+
                        BEGIN TRAN                                                                                                           --|
                        EXEC sp_executesql @stmt = @sqlArchive;                                                                              --|
                                                                                                                                             --|
                        -- Add Delete info                                                                                                   --|
                        INSERT INTO [Maintenance].[Delete_RobotLicenseLogs](SyncId, Id)                                                      --|
                        SELECT tempSyncId, tempId FROM #tempListIds ids                                                                      --|
                        WHERE NOT EXISTS(SELECT 1 FROM [Maintenance].[Delete_RobotLicenseLogs] WHERE Id = ids.tempId) AND tempNoDelay = 0;   --|
                                                                                                                                             --|
                        DELETE src FROM #tempListIds ids INNER JOIN [Maintenance].[Synonym_Source_RobotLicenseLogs] src ON src.Id = ids.tempId WHERE @ignoreDelay = 1 OR ids.tempNoDelay = 1;
                                                                                                                                             --|
                        -- Update current Id(s)                                                                                              --|
                        UPDATE flt SET CurrentId = @currentLoopId                                                                            --|
                        FROM [Maintenance].[Filter_RobotLicenseLogs] flt                                                                     --|
                        INNER JOIN #tempListFilters lst ON lst.TenantId = flt.TenantId AND lst.SyncId = flt.SyncId                           --|
                        WHERE @currentLoopId IS NOT NULL;                                                                                    --|
                                                                                                                                             --|
                        IF @@TRANCOUNT > 0 COMMIT;                                                                                           --|
                        -----------------------------------------------------------------------------------------------------------------------+
                        -- START TRANSACTION BLOCK                                                                                             |
                        -----------------------------------------------------------------------------------------------------------------------+

                        --SELECT @minId = @currentId, @currentId = MAX(tempId) + 1, @countRowIds = COUNT(*), @totalRowIds = @totalRowIds + COUNT(*), @countDeleteOnlyIds = ISNULL(SUM(CAST(tempDeleteOnly AS tinyint)), 0), @totalDeleteOnlyIds = @totalDeleteOnlyIds + ISNULL(SUM(CAST(tempDeleteOnly AS tinyint)), 0) FROM #tempListIds;
                        SELECT @countDeleteOnlyIds = @countRowIds - COUNT(*) FROM #tempListIds WHERE tempDeleteOnly = 0
                        SELECT @countDeleted = COUNT(*) FROM #tempListIds WHERE tempNoDelay = 1
                        SELECT @minId = @currentId, @currentId = @currentLoopId
                        SELECT @totalRowIds = @totalRowIds + ISNULL(@countRowIds, 0), @totalDeleteOnlyIds = @totalDeleteOnlyIds + ISNULL(@countDeleteOnlyIds, 0), @totalDeleted = @totalDeleted + ISNULL(@countDeleted, 0), @loopCount = @loopCount + 1;

                        SET @message = SPACE(@tab * 2) + CAST(@loopCount AS nvarchar(100)) + N'- Ids ' + CAST(@minId AS nvarchar(100)) + N' to ' + CAST(@currentId - 1 AS nvarchar(100)) + N': ' + CAST(@countRowIds AS nvarchar(100)) + N' (archive = '+ CAST(@countRowIds - @countDeleteOnlyIds AS nvarchar(100)) + N', delete only = ' + CAST(@countDeleteOnlyIds AS nvarchar(100)) + N') / delete (no delay = ' + CAST(@countDeleted AS nvarchar(100)) + N', async = ' + CAST(@countRowIds - @countDeleted AS nvarchar(100)) + N')';
                        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                        -- Exit when @MaxRunMinutes is exceeded
                        IF SYSDATETIME() > @maxRunDateTime 
                        BEGIN
                            SET @isTimeOut = 1;
                            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineBreak, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                            SELECT @message = N'TIME OUT:' + (SELECT CAST(DATEDIFF(MINUTE, @startTime, SYSDATETIME()) AS nvarchar(MAX)) ) + N' min (@MaxRunMinutes = ' + ISNULL(CAST(@MaxRunMinutes AS nvarchar(MAX)), N'' ) + N')';
                            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                            BREAK;
                        END 

                    END TRY
                    BEGIN CATCH
                        -- Get Unknown error
                        SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();

                        IF @@TRANCOUNT > 0 ROLLBACK;
                        -- Save Unknwon Errror
                        SET @message = N'  Error(s) occured while updating filters';
                        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = @ERROR_STATE, @Number = @ERROR_NUMBER, @Line = @ERROR_LINE, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                        THROW;
                    END CATCH
                END --WHILE Archive

                INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
                EXEC sp_executesql @stmt = @stmtEmptyMessagesStack, @params = @paramsEmptyMessagesStack, @MessagesStack = @MessagesStack OUTPUT;

                SELECT @message = SPACE(@tab * 1) + N'Total: ' + CAST(@totalRowIds AS nvarchar(100)) + N' (archive = '+ CAST(@totalRowIds - @totalDeleteOnlyIds AS nvarchar(100)) + N', delete only = ' + CAST(@totalDeleteOnlyIds AS nvarchar(100)) + N') / delete (no delay = ' + CAST(@totalDeleted AS nvarchar(100)) + N', async = ' + CAST(@totalRowIds - @totalDeleted AS nvarchar(100)) + N')';;
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                -- Update Sync status when all filters are Archived
                UPDATE snc SET IsArchived = 1
                    , IsDeleted = IIF( @deleteIfNoDelay = 1 AND NOT EXISTS(SELECT 1 FROM [Maintenance].[Delete_RobotLicenseLogs] WHERE syncId = snc.Id), 1, 0)
                    , DeletedOnDate = IIF( @deleteIfNoDelay = 1 AND NOT EXISTS(SELECT 1 FROM [Maintenance].[Delete_RobotLicenseLogs] WHERE syncId = snc.Id), SYSDATETIME(), NULL)
                    , IsSynced = IIF( @deleteIfNoDelay = 1 AND NOT EXISTS(SELECT 1 FROM [Maintenance].[Delete_RobotLicenseLogs] WHERE syncId = snc.Id), 1, 0)
                    , SyncedOnDate = IIF( @deleteIfNoDelay = 1 AND NOT EXISTS(SELECT 1 FROM [Maintenance].[Delete_RobotLicenseLogs] WHERE syncId = snc.Id), SYSDATETIME(), NULL)
                    , FirstASyncId = (SELECT MIN(Id) FROM [Maintenance].[Delete_RobotLicenseLogs] WHERE syncId = snc.Id)
                    , LastASyncId = (SELECT MAX(Id) FROM [Maintenance].[Delete_RobotLicenseLogs] WHERE syncId = snc.Id)
                    , CountASyncIds = (SELECT COUNT(*) FROM [Maintenance].[Delete_RobotLicenseLogs] WHERE syncId = snc.Id)
                FROM [Maintenance].[Sync_RobotLicenseLogs] snc
                INNER JOIN [Maintenance].[Archive_RobotLicenseLogs] arc ON arc.Id = snc.ArchiveId
                WHERE EXISTS(SELECT 1 FROM #tempListFilters WHERE SyncId = snc.Id) AND
                    NOT EXISTS (SELECT 1 FROM [Maintenance].[Filter_RobotLicenseLogs] WHERE SyncId = snc.Id AND IsArchived = 0);
                SELECT @syncArchived = @@ROWCOUNT;

                -- Update Archive status when all sync and filters are archived
                UPDATE arc SET IsArchived = 1, ArchivedOnDate = SYSDATETIME()
                    , IsDeleted = IIF(NOT EXISTS (SELECT 1 FROM [Maintenance].[Sync_RobotLicenseLogs] WHERE ArchiveId = lst.ArchiveId AND IsSynced = 0), 1, 0)
                    , DeletedOnDate = IIF(NOT EXISTS (SELECT 1 FROM [Maintenance].[Sync_RobotLicenseLogs] WHERE ArchiveId = lst.ArchiveId AND IsSynced = 0), SYSDATETIME(), NULL)
                    , IsFinished = IIF(NOT EXISTS (SELECT 1 FROM [Maintenance].[Sync_RobotLicenseLogs] WHERE ArchiveId = lst.ArchiveId AND IsSynced = 0), 1, 0)
                    , FinishedOnDate = IIF(NOT EXISTS (SELECT 1 FROM [Maintenance].[Sync_RobotLicenseLogs] WHERE ArchiveId = lst.ArchiveId AND IsSynced = 0), SYSDATETIME(), NULL)
                    , IsSuccess = IIF(NOT EXISTS (SELECT 1 FROM [Maintenance].[Sync_RobotLicenseLogs] WHERE ArchiveId = lst.ArchiveId AND IsSynced = 0), 1, 0)
                    , [Message] = IIF(NOT EXISTS (SELECT 1 FROM [Maintenance].[Sync_RobotLicenseLogs] WHERE ArchiveId = lst.ArchiveId AND IsSynced = 0), N'Cleanup finished', 'Archiving finished')
                    , PreviousRunIds = (
                        SELECT [runid], [message], [timestamp] FROM (
                            SELECT [runid] = @runId, [message] = N'Cleanup finished', [timestamp] = SYSDATETIME() WHERE NOT EXISTS (SELECT 1 FROM [Maintenance].[Sync_RobotLicenseLogs] WHERE ArchiveId = lst.ArchiveId AND IsSynced = 0)
                            UNION ALL SELECT [runid] = @runId, [message] = N'All Archive Filter(s) finished]', [timestamp] = SYSDATETIME()
                            UNION ALL SELECT [runid], [message], [timestamp] FROM OPENJSON(PreviousRunIds) WITH ([runid] nvarchar(MAX) N'$.runid', [message] nvarchar(MAX) N'$.message', [timestamp] datetime2 N'$.timestamp')
                        ) v FOR JSON PATH
                    )
                FROM [Maintenance].[Archive_RobotLicenseLogs] arc
                INNER JOIN #tempListFilters lst ON lst.ArchiveId = arc.Id
                WHERE NOT EXISTS (SELECT 1 FROM [Maintenance].[Sync_RobotLicenseLogs] WHERE ArchiveId = lst.ArchiveId AND IsArchived = 0);

                SELECT @triggerArchived = @@ROWCOUNT, @globalTriggerArchived = ISNULL(@globalTriggerArchived, 0) + @@ROWCOUNT;

                SELECT @message = SPACE(@tab * 2) + N'Archive Filter(s) finished: ' + CAST(ISNULL(@filtersArchived, 0) AS nvarchar(100)) + N' / ' + CAST(ISNULL(@countFilterIds, 0) AS nvarchar(100)) + N' (remaining filters = '+ CAST(ISNULL(COUNT(*), 0) AS nvarchar(100)) + N')' FROM [Maintenance].[Filter_RobotLicenseLogs] WHERE IsArchived <> 1;
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                SELECT @message = SPACE(@tab * 2) + N'Archive Sync(s) finished: ' + ISNULL(CAST(@syncArchived AS nvarchar(100)), N'-');
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                SELECT @message = SPACE(@tab * 1) + N'Archive(s) finished: ' + ISNULL(CAST(@triggerArchived AS nvarchar(100)), N'-') + N' (in progress = ' + CAST(@countArchiveIds - ISNULL(@triggerArchived, 0) AS nvarchar(100)) + N', to do = '+ CAST(ISNULL(COUNT(*), 0) AS nvarchar(100)) + N')' FROM [Maintenance].[Archive_RobotLicenseLogs] WHERE [Todo] = 1 AND ArchiveTriggerTime < @StartTime;
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
            END

            SELECT @message = SPACE(@tab * 0) + N'Filter(s) batch finished [' + cAST(@batchCount AS nvarchar(100)) + N']';
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

            -- Save grant total
            SELECT @globalRowIds = ISNULL(@globalRowIds, 0) + (@totalRowIds), @globalDeleteOnlyIds = ISNULL(@globalDeleteOnlyIds, 0) + @totalDeleteOnlyIds, @globalDeleted = ISNULL(@globalDeleted, 0) + @totalDeleted;

            IF  (@countFilterIds < 1 AND @countRepeat < 1) OR  @batchCount >= @maxBatches OR SYSDATETIME() > @maxRunDateTime
            BEGIN
                SELECT @message = N'Archiving is finished ' + CASE 
                WHEN (@countFilterIds < 1 AND @countRepeat < 1) THEN N'(no remaining filters)'
                WHEN SYSDATETIME() > @maxRunDateTime THEN N'(time out)' 
                WHEN  @batchCount >= @maxBatches THEN N'(@MaxBatchesLoops limit reached [@MaxBatchesLoops = ' + CAST(@MaxBatchesLoops AS nvarchar(100)) + N'])'
                ELSE N'xxx' END;
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                BREAK
            END

        END --WHILE Batch

        SELECT @message = SPACE(@tab * 1) + N'Rows processed: ' + CAST(ISNULL(@globalRowIds, 0) AS nvarchar(100)) + N' (archived = ' + CAST(ISNULL(@globalRowIds - @globalDeleteOnlyIds, 0) AS nvarchar(100)) + N', delete only '+ CAST(ISNULL(@globalDeleteOnlyIds, 0) AS nvarchar(100)) + N')';
        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

        SELECT @message = SPACE(@tab * 2) + N'Deleted:  no delay = ' + CAST(ISNULL(@globalDeleted, 0) AS nvarchar(100)) + N', async =' + CAST(ISNULL(@globalRowIds - @globalDeleted, 0) AS nvarchar(100)) + N'';
        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

        SELECT @message = SPACE(@tab * 1) + 'Remaining Archive Trigger(s): ' + CAST(COUNT(*) AS nvarchar(100)) + IIF(COUNT(*) > 0, N' (outstanding = '+ CAST(SUM(IIF(arc.ArchiveTriggerTime < @startTime, 1, 0)) AS nvarchar(100)) + N', upcoming = ' + CAST(SUM(IIF(arc.ArchiveTriggerTime >= @startTime, 1, 0)) AS nvarchar(100)) + N')', '')
        FROM [Maintenance].[Archive_RobotLicenseLogs] arc
        WHERE arc.ToDo = 1
        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

        SELECT @message = SPACE(@tab * 1) + 'Remaining Filter(s): ' + CAST(COUNT(*) AS nvarchar(100)) + IIF(COUNT(*) > 0, N' (outstanding = '+ CAST(SUM(IIF(arc.ArchiveTriggerTime < @startTime, 1, 0)) AS nvarchar(100)) + N', upcoming = ' + CAST(SUM(IIF(arc.ArchiveTriggerTime >= @startTime, 1, 0)) AS nvarchar(100)) + N')', '')
        FROM [Maintenance].[Filter_RobotLicenseLogs] flt 
        INNER JOIN [Maintenance].[Sync_RobotLicenseLogs] snc ON snc.Id = flt.SyncId
        INNER JOIN [Maintenance].[Archive_RobotLicenseLogs] arc ON arc.Id = snc.ArchiveId 
        WHERE flt.IsArchived = 0 AND snc.IsArchived = 0 AND (flt.TargetId IS NULL OR flt.CurrentId IS NULL OR flt.CurrentId < flt.TargetId) AND arc.ToDo = 1 --AND arc.ArchiveTriggerTime < @StartTime
        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

        INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
        EXEC sp_executesql @stmt = @stmtEmptyMessagesStack, @params = @paramsEmptyMessagesStack, @MessagesStack = @MessagesStack OUTPUT;

        ----------------------------------------------------------------------------------------------------
        -- Runs Cleanup
        ----------------------------------------------------------------------------------------------------
        IF @logToTable = 1 
        BEGIN
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineBreak, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = 'Old Runs cleanup', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

            EXEC [Maintenance].[DeleteRuns] @CleanupAfterDays = @SavedMessagesRetentionDays, @RunId = @runId, @Procedure = @procName, @MessagesStack = @MessagesStack OUTPUT;
        END

		EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineBreak, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
		EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
		EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = 'END (SUCCESS)', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

        INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
        EXEC sp_executesql @stmt = @stmtEmptyMessagesStack, @params = @paramsEmptyMessagesStack, @MessagesStack = @MessagesStack OUTPUT;

        SET @returnValue = 0; -- Success
    END TRY
    BEGIN CATCH
        SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();

        -- Record error message
        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @ERROR_MESSAGE, @Severity = @ERROR_SEVERITY, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @RaiseError = 0, @MessagesStack = @MessagesStack OUTPUT;

        IF @@TRANCOUNT > 0 ROLLBACK TRAN;

        INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
        EXEC sp_executesql @stmt = @stmtEmptyMessagesStack, @params = @paramsEmptyMessagesStack, @MessagesStack = @MessagesStack OUTPUT;

        IF @errorCount = 0
        BEGIN
            IF @dryRun <> 0 
            BEGIN 
                -- Output Message result set
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = 'END (DRY RUN)', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
            END
            ELSE 
            BEGIN
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineBreak, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                SET @message = N'Execution finished with error(s)'
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                SET @message = N'Rows deleted: ' + ISNULL(CAST(@totalRowIds AS nvarchar(MAX)), '0');
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                SET @message = N'Elapsed time : ' + CAST(CAST(DATEADD(SECOND, DATEDIFF(SECOND, @startTime, SYSDATETIME()), 0) AS time) AS nvarchar(MAX));
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineBreak, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = 'END (FAIL)', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                SET @returnValue = 4
            END
        END
        ELSE
        BEGIN
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = 'END (INCORRECT PARAMETERS)', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
        END

        RAISERROR(@ERROR_MESSAGE, @ERROR_SEVERITY, @ERROR_STATE);

        SET @returnValue = ISNULL(@returnValue, 255);
    END CATCH

    ----------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------
    INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
    EXEC sp_executesql @stmt = @stmtEmptyMessagesStack, @params = @paramsEmptyMessagesStack, @MessagesStack = @MessagesStack OUTPUT;

    -- Output Messages result set
    IF @outputDataset = 1 SELECT [RunId] = @RunId, [Date], [Procedure], [Message], [Severity], [State], [Number], [Line] FROM @messages ORDER BY [id] ASC;

    SET @returnValue = ISNULL(@returnValue, 255);
    IF @runId IS NOT NULL UPDATE [Maintenance].[Runs] SET [EndDate] = SYSDATETIME(), [ErrorStatus] = @returnValue WHERE Id = @runId;

    ----------------------------------------------------------------------------------------------------
    -- End
    ----------------------------------------------------------------------------------------------------
    RETURN @returnValue;
END
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- DROP PROCEDURE [Maintenance].[SimpleArchivingRobotLicenseLogs]
----------------------------------------------------------------------------------------------------

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[SimpleArchivingRobotLicenseLogs]') AND type in (N'P'))
BEGIN
    EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [Maintenance].[SimpleArchivingRobotLicenseLogs] AS'
    PRINT '  + CREATE PROCEDURE: [Maintenance].[SimpleArchivingRobotLicenseLogs]';
END
ELSE PRINT '  = PROCEDURE [Maintenance].[SimpleArchivingRobotLicenseLogs] already exists' 
GO

PRINT '  ~ UPDATE PROCEDURE: [Maintenance].[SimpleArchivingRobotLicenseLogs]'
GO  

ALTER PROCEDURE [Maintenance].[SimpleArchivingRobotLicenseLogs]
----------------------------------------------------------------------------------------------------
-- ### [Object]: PROCEDURE [Maintenance].[SimpleArchivingRobotLicenseLogs]
-- ### [Version]: 2023-10-06T11:29:36+02:00
-- ### [Source]: _src/Archive/ArchiveDB/RobotLicenseLogs/Procedure_ArchiveDB.Maintenance.SimpleArchivingRobotLicenseLogs.sql
-- ### [Hash]: dc39c27 [SHA256-B6965C5328205D761A108E28A532776AD4AF039A87D0426FA5FAA3DD862B2E7F]
-- ### [Docs]: https://???.???
-- !!! ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!!!
-- !!! ~~~~~~~~~ NOT OFFICIALLY SUPPORTED BY UIPATH 
-- !!! ~~~~~~~~~ SQL Server >= 2016 SP1
----------------------------------------------------------------------------------------------------
    @RetentionDate datetime = NULL
    , @RetentionDays int = NULL
    , @RetentionHours int = NULL
    , @RoundUpPreviousDay bit = 1
    /* Row count settings */
    , @RowsDeletedForEachLoop int = 10000 -- Don't go above 50.000 (min = 1000, Max = 100.000)
    , @MaxConcurrentFilters int = NULL -- NULL or 0 = default value => number of filter in chronoligical order processed by a single batch
    /* Loop Limits */
    , @MaxRunMinutes int = NULL -- NULL or 0 = unlimited
    , @MaxBatchesLoops int = NULL -- NULL or 0 - unlimited
    /* Dry Run */
--    , @DryRunOnly nvarchar(MAX) = NULL -- Y{es} or N{o} => Only Check Parameters (default if NULL = Y)
    /* Delete settings */
	, @SynchronousDeleteIfNoDelay bit = 1
	, @IgnoreDeleteDelay bit = 1

    /* Archive table(s) settings */
    , @CreateArchiveTable nvarchar(MAX) = 1 -- Y{es} or N{o} => Archive table refered by Synonym is create when missing (default if NULL or empty = N)
    , @UpdateArchiveTable nvarchar(MAX) = 1 -- Y{es} or N{o} => Archive table refered by Synonym is update when column(s) are missing (default if NULL or empty = N)
    , @ExcludeColumns nvarchar(MAX) = NULL -- JSON string with array of string with column name(s)
    /* Error Handling */
    , @OnErrorRetry tinyint = NULL -- between 0 and 20 => retry up to 20 times (default if NULL = 10)
    , @OnErrorWaitMillisecond smallint = 1000 -- wait for milliseconds between each Retry (default if NULL = 1000ms)
    /* Messge Logging */
    , @SaveMessagesToTable nvarchar(MAX) = 'Y' -- Y{es} or N{o} => Save to [maintenance].[messages] table (default if NULL = Y)
    , @SavedMessagesRetentionDays smallint = 30
    , @SavedToRunId int = NULL OUTPUT
    , @OutputMessagesToDataset nvarchar(MAX) = 'N' -- Y{es} or N{o} => Output Messages Result set
    , @Verbose nvarchar(MAX) = NULL -- Y{es} = Print all messages < 10
AS
BEGIN
    BEGIN TRY 
        SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
		SET LOCK_TIMEOUT 5000;
        SET ARITHABORT ON;
        SET NOCOUNT ON;
        SET NUMERIC_ROUNDABORT OFF;

        ----------------------------------------------------------------------------------------------------
        -- Local Run Variables
        ----------------------------------------------------------------------------------------------------
        DECLARE @returnValue int = 1;
        DECLARE @triggerDate datetime;
        DECLARE @triggerFloatingDate float(53);
        DECLARE @floatingDay float(53) = 1;
        DECLARE @floatingHour float(53) = @floatingDay / 24;
        ----------------------------------------------------------------------------------------------------
        --
        ----------------------------------------------------------------------------------------------------
        IF @RetentionDate IS NULL AND @RetentionDays IS NULL AND @RetentionHours IS NULL RAISERROR(N'@RetentionDate or @RetentionDays/@RetentionHours must be provided', 16, 1);
        IF @RetentionDate IS NOT NULL AND (@RetentionDays IS NOT NULL OR @RetentionHours IS NOT NULL) RAISERROR(N'@RetentionDate AND @RetentionDays/@RetentionHours cannot be used together', 16, 1);
        IF @RetentionDate > SYSDATETIME() RAISERROR(N'@RetentionDate must be a past date', 16, 1);

        -- Use current Date if @RetentionDate is missing
        SELECT @triggerDate = ISNULL(@RetentionDate, SYSDATETIME());

        -- Remove time part (=> begining of day)
        IF @RoundUpPreviousDay = 1 SELECT @triggerDate = CAST(@triggerDate AS DATE);
        SELECT @triggerFloatingDate = CAST(@triggerDate AS float(53));
        -- Remove retention Days/Hours
        SELECT @triggerDate = CAST(@triggerFloatingDate -ISNULL(ABS(@floatingDay  * @RetentionDays), 0) -ISNULL(ABS(@floatingHour * @RetentionHours), 0) AS datetime) 

        -- Add Trigger
        EXEC [Maintenance].[AddArchiveTriggerRobotLicenseLogs] @ArchiveTriggerTime = @triggerDate, @Filters = N'ALL', @ArchiveAfterHours = 0, @DeleteDelayHours = 0, @DryRunOnly = 0;
        -- Start Archive
        EXEC [Maintenance].[ArchiveRobotLicenseLogs] @RowsDeletedForEachLoop = @RowsDeletedForEachLoop, @MaxConcurrentFilters = @MaxConcurrentFilters, @MaxRunMinutes = @MaxRunMinutes, @MaxBatchesLoops = @MaxBatchesLoops
            , @SynchronousDeleteIfNoDelay = @SynchronousDeleteIfNoDelay, @IgnoreDeleteDelay = @IgnoreDeleteDelay
            , @CreateArchiveTable = @CreateArchiveTable, @UpdateArchiveTable = @UpdateArchiveTable, @ExcludeColumns = @ExcludeColumns

        SET @returnValue = 0; -- Success
    END TRY
    BEGIN CATCH
        THROW;
    END CATCH

    SET @returnValue = ISNULL(@returnValue, 255);
    --IF @runId IS NOT NULL UPDATE [Maintenance].[Runs] SET [EndDate] = SYSDATETIME(), [ErrorStatus] = @returnValue WHERE Id = @runId;

    ----------------------------------------------------------------------------------------------------
    -- End
    ----------------------------------------------------------------------------------------------------
    RETURN @returnValue;
END
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO

----------------------------------------------------------------------------------------------------
-- DROP PROCEDURE [Maintenance].[CleanupSyncedRobotLicenseLogs]
----------------------------------------------------------------------------------------------------

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[CleanupSyncedRobotLicenseLogs]') AND type in (N'P'))
BEGIN
    EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [Maintenance].[CleanupSyncedRobotLicenseLogs] AS'
    PRINT '  + CREATE PROCEDURE: [Maintenance].[CleanupSyncedRobotLicenseLogs]';
END
ELSE PRINT '  = PROCEDURE [Maintenance].[CleanupSyncedRobotLicenseLogs] already exists' 
GO

PRINT '  ~ UPDATE PROCEDURE: [Maintenance].[CleanupSyncedRobotLicenseLogs]'
GO  

ALTER PROCEDURE [Maintenance].[CleanupSyncedRobotLicenseLogs]
----------------------------------------------------------------------------------------------------
-- ### [Object]: PROCEDURE [Maintenance].[CleanupSyncedRobotLicenseLogs]
-- ### [Version]: 2023-10-06T11:29:36+02:00
-- ### [Source]: _src/Archive/ArchiveDB/RobotLicenseLogs/Procedure_ArchiveDB.Maintenance.CleanupSyncedRobotLicenseLogs.sql
-- ### [Hash]: dc39c27 [SHA256-A785BE42BD048ED426C2AB0042F897DB3E369FF0DA09794B3043B609081ED155]
-- ### [Docs]: https://???.???
-- !!! ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!!!
-- !!! ~~~~~~~~~ NOT OFFICIALLY SUPPORTED BY UIPATH 
-- !!! ~~~~~~~~~ SQL Server >= 2016 SP1
----------------------------------------------------------------------------------------------------
    /* Row count settings */
    @RowsDeletedForEachLoop int = 10000 -- Don't go above 50.000 (min = 1000, Max = 100.000)
    /* Loop Limits */
    , @MaxRunMinutes int = NULL -- NULL or 0 = unlimited
    /* Dry Run */
--    , @DryRunOnly nvarchar(MAX) = NULL -- Y{es} or N{o} => Only Check Parameters (default if NULL = Y)
    /* Error Handling */
    , @OnErrorRetry tinyint = NULL -- between 0 and 20 => retry up to 20 times (default if NULL = 10)
    , @OnErrorWaitMillisecond smallint = 1000 -- wait for milliseconds between each Retry (default if NULL = 1000ms)
    /* Messge Logging */
    , @SaveMessagesToTable nvarchar(MAX) = 'Y' -- Y{es} or N{o} => Save to [maintenance].[messages] table (default if NULL = Y)
    , @SavedMessagesRetentionDays smallint = 30
    , @SavedToRunId int = NULL OUTPUT
    , @OutputMessagesToDataset nvarchar(MAX) = 'N' -- Y{es} or N{o} => Output Messages Result set
    , @Verbose nvarchar(MAX) = NULL -- Y{es} = Print all messages < 10
AS
BEGIN
    BEGIN TRY 
        SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
		SET LOCK_TIMEOUT 5000;
        SET ARITHABORT ON;
        SET NOCOUNT ON;
        SET NUMERIC_ROUNDABORT OFF;

        ----------------------------------------------------------------------------------------------------
        -- Local Run Variables
        ----------------------------------------------------------------------------------------------------
        DECLARE @runId int;  
        DECLARE @dryRun bit;
        DECLARE @isTimeOut bit = 0;
        DECLARE @IsFinished bit = 0;

        DECLARE @startTime datetime = SYSDATETIME();
        DECLARE @startTimeFloat float(53);

		DECLARE @loopStart datetime;
        DECLARE @maxRunDateTime datetime;
        DECLARE @logToTable bit;
        DECLARE @MaxErrorRetry tinyint;
        DECLARE @errorDelay smallint;
        DECLARE @errorWait datetime;
        DECLARE @returnValue int = 1;
        ----------------------------------------------------------------------------------------------------
        -- Sync Cursor
        ----------------------------------------------------------------------------------------------------
        DECLARE @tempListSync TABLE([Id] [bigint] PRIMARY KEY NOT NULL, [DeleteOnDate] datetime NULL);
        DECLARE @cursorSyncId bigint;
        DECLARE @cursorDeleteOnDate datetime;

        ----------------------------------------------------------------------------------------------------
        -- Delete Loop
        ----------------------------------------------------------------------------------------------------
        DECLARE @maxLoopDeleteRows int;
        DECLARE @firstId bigint;
        DECLARE @lastId bigint;
        ----------------------------------------------------------------------------------------------------
        -- Count row Ids
        ----------------------------------------------------------------------------------------------------
        DECLARE @countIds bigint;
        DECLARE @totalIds bigint;
        DECLARE @globalIds bigint;
        ----------------------------------------------------------------------------------------------------
        -- Constant / Default value
        ----------------------------------------------------------------------------------------------------
        DECLARE @maxDeleteRows int = 100*1000; -- Raise an error if @RowsDeletedForEachLoop is bigger than this value
        DECLARE @minDeleteRows int = 1*1000; -- Raise an error if @RowsDeletedForEachLoop is smaller than this value
        DECLARE @defaultDeleteRows int = 10*1000;
        DECLARE @verboseBelowLevel int = 10; -- don't print message with Severity < 10 unless Verbose is set to Y

        DECLARE @paramsYesNo TABLE ([id] tinyint IDENTITY(0, 1) PRIMARY KEY CLUSTERED, [parameter] nvarchar(MAX), [value] int)
        DECLARE @floatingDay float(53) = 1;
        DECLARE @floatingHour float(53) = @floatingDay / 24;
        DECLARE @constDefaultDeleteDelay int = 0;
        DECLARE @constDefaultAfterHours int = NULL;
        ----------------------------------------------------------------------------------------------------
        -- Server Info 
        ----------------------------------------------------------------------------------------------------
        DECLARE @productVersion nvarchar(MAX) = CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(MAX));
        DECLARE @engineEdition int =  CAST(ISNULL(SERVERPROPERTY('EngineEdition'), 0) AS int);
        DECLARE @minProductVersion nvarchar(MAX) = N'13.0.4001.0 (SQL Server 2016 SP1)'
        DECLARE @version numeric(18, 10);
        DECLARE @minVersion numeric(18, 10) = 13.0040010000;
        DECLARE @minCompatibilityLevel int = 130;
        DECLARE @hostPlatform nvarchar(256); 
        ----------------------------------------------------------------------------------------------------
        -- Proc Info
        ----------------------------------------------------------------------------------------------------
        DECLARE @paramsGetProcInfo nvarchar(MAX) = N'@procid int, @info nvarchar(MAX), @output nvarchar(MAX) OUTPUT'
        DECLARE @stmtGetProcInfo nvarchar(MAX) = N'
            DECLARE @definition nvarchar(MAX) = OBJECT_DEFINITION(@procid), @keyword nvarchar(MAX) = REPLICATE(''-'', 2) + SPACE(1) + REPLICATE(''#'', 3) + SPACE(1) + QUOTENAME(LTRIM(RTRIM(@info))) + '':'';
			DECLARE @eol char(1) = IIF(CHARINDEX( CHAR(13) , @definition) > 0, CHAR(13), CHAR(10));
			SET @output = ''''+ LTRIM(RTRIM( SUBSTRING(@definition, NULLIF(CHARINDEX(@keyword, @definition), 0 ) + LEN(@keyword), CHARINDEX( @eol , @definition, CHARINDEX(@keyword, @definition) + LEN(@keyword) + 1) - CHARINDEX(@keyword, @definition) - LEN(@keyword) ))) + '''';
        ';
        DECLARE @procSchemaName nvarchar(MAX) = COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?');
        DECLARE @procObjecttName nvarchar(MAX) = COALESCE(OBJECT_NAME(@@PROCID), N'?');
        DECLARE @procName nvarchar(MAX) = QUOTENAME(COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?')) + N'.' + QUOTENAME(COALESCE(OBJECT_NAME(@@PROCID), N'?')); 
        DECLARE @versionDatetime nvarchar(MAX);
        ----------------------------------------------------------------------------------------------------
        -- Synonym
        ----------------------------------------------------------------------------------------------------
        DECLARE @synonymMessages nvarchar(MAX);
        DECLARE @synonymIsValid bit;
        ----------------------------------------------------------------------------------------------------      
        -- Message / Error Handling
        ----------------------------------------------------------------------------------------------------
        /* Format */
        DECLARE @lineSeparator nvarchar(MAX) = N'----------------------------------------------------------------------------------------------------';
        DECLARE @lineBreak nvarchar(MAX) = N'';
        DECLARE @message nvarchar(MAX);
        DECLARE @string nvarchar(MAX);
        DECLARE @tab tinyint = 2;
        /* Log Stack */
        DECLARE @stmtEmptyMessagesStack nvarchar(MAX) = N'
            SELECT 
                [Date] = m.value(''Date[1]'', ''datetime'')
                , [Procedure] = m.value(''Procedure[1]'', ''nvarchar(MAX)'')
                , [Message] = m.value(''Message[1]'', ''nvarchar(MAX)'')
                , [Severity] = m.value(''Severity[1]'', ''int'')
                , [State] = m.value(''State[1]'', ''int'')
                , [Number] = m.value(''Number[1]'', ''int'')
                , [Line] = m.value(''Line[1]'', ''int'')
            FROM @MessagesStack.nodes(''/messages/message'') x(m)
            SET @MessagesStack = NULL;
        ';
        DECLARE @paramsEmptyMessagesStack nvarchar(MAX) = N'@MessagesStack xml = NULL OUTPUT';
        DECLARE @MessagesStack xml;
        /* Errors */
        DECLARE @ERROR_NUMBER INT, @ERROR_SEVERITY INT, @ERROR_STATE INT, @ERROR_PROCEDURE NVARCHAR(126), @ERROR_LINE INT, @ERROR_MESSAGE NVARCHAR(2048) ;
        DECLARE @errorCount int = 0;
        DECLARE @countErrorRetry tinyint;
        /* Messages */
        DECLARE @messages TABLE(id int IDENTITY(0, 1) PRIMARY KEY, [date] datetime2 DEFAULT SYSDATETIME(), [procedure] nvarchar(MAX) NOT NULL DEFAULT QUOTENAME(COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?')) + N'.' + QUOTENAME(COALESCE(OBJECT_NAME(@@PROCID), N'?')), [message] nvarchar(MAX) NOT NULL, severity tinyint NOT NULL, state tinyint NOT NULL, [number] int, [line] int);
        DECLARE @levelVerbose int;    
        DECLARE @outputDataset bit;
        /* Cursor */ 
        DECLARE CursorMessages CURSOR FAST_FORWARD LOCAL FOR SELECT [Date], [Procedure], [Message], [Severity], [State], [Number], [Line] FROM @messages ORDER BY [ID] ASC;
        DECLARE @cursorDate datetime;
        DECLARE @cursorProcedure nvarchar(MAX);
        DECLARE @cursorMessage nvarchar(MAX);
        DECLARE @cursorSeverity tinyint;
        DECLARE @cursorState tinyint;
        DECLARE @cursorNumber int;
        DECLARE @cursorLine int;    

        ----------------------------------------------------------------------------------------------------
        -- START
        ----------------------------------------------------------------------------------------------------
        SELECT @startTime = SYSDATETIME();
        SELECT @startTimeFloat = CAST(@startTime AS float(53));

        ----------------------------------------------------------------------------------------------------
        -- Gather General & Server Info
        ----------------------------------------------------------------------------------------------------

        -- Get Run Time limit or NULL (unlimited)
	    SET @maxRunDateTime = CASE WHEN ABS(@MaxRunMinutes) >= 1 THEN DATEADD(MINUTE, ABS(@MaxRunMinutes), @startTime) ELSE NULL END;

        -- Output Start & Stop info
        INSERT INTO @messages([Message], Severity, [State]) VALUES 
            ( @lineSeparator, 10, 1)
            , (N'Start Time = ' + CONVERT(nvarchar(MAX), @startTime, 121), 10, 1 )
            , (N'MAX Run Time = ' + ISNULL(CONVERT(nvarchar(MAX), @maxRunDateTime, 121), N'NULL (=> unlimited)'), 10, 1 )
            , (@lineBreak, 10, 1);
/*        -- Get Host info
        IF @version >= 14 SELECT @hostPlatform = host_platform FROM sys.dm_os_host_info ELSE SET @HostPlatform = 'Windows'
*/
        ----------------------------------------------------------------------------------------------------
        -- Check SQL Server Requierements
        ----------------------------------------------------------------------------------------------------
        -- Get Proc Version
--        EXEC sp_executesql @stmt = @stmtGetProcInfo, @params = @paramsGetProcInfo, @procid = @@PROCID, @info = N'Version', @output = @versionDatetime OUTPUT;

        -- Get SQL Server Server Version
        WITH release (id, position, version) AS
        (
            SELECT id = 0, position = CHARINDEX(N'.', @productVersion, 1)
                , version = LEFT(@productVersion, CHARINDEX(N'.', @productVersion, 1))
            UNION ALL
            SELECT id = r.id + 1, position = CHARINDEX(N'.', @productVersion, position + 1)
                , version = r.version + RIGHT(v.[space] + SUBSTRING(@productVersion, position + 1, COALESCE( NULLIF(CHARINDEX('.', @productVersion, position + 1), 0), LEN(@productVersion) + 1) - position - 1), LEN(v.[space]) )
            FROM release r INNER JOIN (VALUES(0, '00'), (1, '00'), (2, '0000'), (3, '0000') ) AS v(id, space) ON v.id = r.id + 1
            WHERE position < LEN(@productVersion) + 1
        )
        SELECT TOP(1) @version = CAST(version AS numeric(18, 10))
        FROM release
        ORDER BY Id DESC

        -- Get Host info
        IF @version >= 14 SELECT @hostPlatform = host_platform FROM sys.dm_os_host_info ELSE SET @HostPlatform = 'Windows'

        INSERT INTO @messages([Message], Severity, [State])
        -- Check min required version
        SELECT N'ERROR: Current SQL Server version is ' + @productVersion + N'. Only version ' + @minProductVersion + + N' or higher is supported.', 16, 1 WHERE @version < @minVersion
        -- Check Database Compatibility Level
        UNION ALL SELECT 'ERROR: Database ' + QUOTENAME(DB_NAME(DB_ID())) + ' Compatibility Level is set to '+ CAST([compatibility_level] AS nvarchar(MAX)) + '. Compatibility level 130 or higher is requiered.', 16, 1 FROM sys.databases WHERE database_id = DB_ID() AND [compatibility_level] < @minCompatibilityLevel
        -- Check opened transation(s)
        --UNION ALL SELECT 'The transaction count is not 0.', 16, 1 WHERE @@TRANCOUNT <> 0
        -- Check uses_ansi_nulls
        UNION ALL SELECT 'ERROR: ANSI_NULLS must be set to ON for this Stored Procedure', 16, 1 FROM sys.sql_modules WHERE [object_id] = @@PROCID AND uses_ansi_nulls <> 1
        -- Check uses_quoted_identifier
        UNION ALL SELECT 'ERROR: QUOTED_IDENTIFIER must be set to ON for this Stored Procedure', 16, 1 FROM sys.sql_modules WHERE [object_id] = @@PROCID AND uses_quoted_identifier <> 1;

        ----------------------------------------------------------------------------------------------------
        -- Parameters
        ----------------------------------------------------------------------------------------------------
        SET @levelVerbose = @VerboseBelowLevel;

        -- Convert Yes / No varations to bit
        INSERT INTO @paramsYesNo([parameter], [value]) VALUES(N'NO', 0), (N'N', 0), (N'0', 0), (N'YES', 1), (N'Y', 1), (N'1', 1);

        ----------------------------------------------------------------------------------------------------
        -- Parameters' Validation
        ----------------------------------------------------------------------------------------------------
       INSERT INTO @messages([Message], Severity, [State])
        VALUES 
            ( @lineSeparator, 10, 1)
            , ( N'Validation', 10, 1)
            , ( @lineSeparator, 10, 1)

        -- Check @RowsDeletedForEachLoop
        SET @maxLoopDeleteRows = ISNULL(NULLIF(@RowsDeletedForEachLoop, 0), @defaultDeleteRows);
        IF @maxLoopDeleteRows < @minDeleteRows OR @maxLoopDeleteRows > @MaxDeleteRows 
        BEGIN
            INSERT INTO @messages ([Message], Severity, [State]) VALUES
                ( 'Parameter @RowsDeletedForEachLoop is invalid: ' + LTRIM(RTRIM(CAST(@RowsDeletedForEachLoop AS nvarchar(MAX)))), 10, 1)
                , ('USAGE: use a value between ' + CAST(@minDeleteRows AS nvarchar(MAX)) + N' and ' + CAST(@maxDeleteRows AS nvarchar(MAX)) +  N'', 10, 1)
                , ('Parameter @RowsDeletedForEachLoop is invalid', 16, 1);
        END

		----------------------------------------------------------------------------------------------------
        -- Check Output Settings
        ----------------------------------------------------------------------------------------------------
        -- Check @OutputMessagesToDataset parameter
        SELECT @outputDataset = [value] FROM @paramsYesNo WHERE [parameter] = ISNULL(LTRIM(RTRIM(@OutputMessagesToDataset)), N'N');
        IF @outputDataset IS NULL 
        BEGIN
            INSERT INTO @messages ([Message], Severity, [State]) VALUES
                ( 'Parameter @OutputMessagesToDataset is invalid: ' + LTRIM(RTRIM(@OutputMessagesToDataset)), 10, 1)
                , ('Usage: @OutputMessagesToDataset = Y{es} or N{o}', 10, 1)
                , ('Parameter @OutputMessagesToDataset is invalid: ' + LTRIM(RTRIM(@OutputMessagesToDataset)), 16, 1);
        END

		----------------------------------------------------------------------------------------------------
        -- Check Permissions
        ----------------------------------------------------------------------------------------------------
        -- Check SELECT & DELETE permission on [dbo].[RobotLicenseLogs]
        /*INSERT INTO @messages ([Message], Severity, [State])
        SELECT 'Permission not effectively granted: ' + UPPER(p.permission_name), 10, 1
        FROM (VALUES(N'', N'SELECT'), (N'', N'DELETE')) AS p (subentity_name, permission_name)
        LEFT JOIN sys.fn_my_permissions(N'[dbo].[RobotLicenseLogs]', N'OBJECT') eff ON eff.subentity_name = p.subentity_name AND eff.permission_name = p.permission_name
        WHERE eff.permission_name IS NULL
        ORDER BY p.permission_name;

        IF @@ROWCOUNT > 0 
        BEGIN
            INSERT INTO @messages ([Message], Severity, [State]) VALUES
                (N'Error: missing permission', 10, 1)
                , (N'SELECT and DELETE permissions are required on [dbo].[RobotLicenseLogs] table', 16, 1);
        END*/

        -- Check @SaveMessagesToTable parameter
        SELECT @logToTable = [value] FROM @paramsYesNo WHERE [parameter] = ISNULL(LTRIM(RTRIM(@SaveMessagesToTable)), N'Y');
        IF @logToTable IS NULL 
        BEGIN
            INSERT INTO @messages ([Message], Severity, [State]) VALUES
                ( 'Parameter @SaveMessagesToTable is invalid: ' + LTRIM(RTRIM(@SaveMessagesToTable)), 10, 1)
                , ('Usage: @SaveMessagesToTable = Y{es} or N{o}', 10, 1)
                , ('Parameter @SaveMessagesToTable is invalid: ' + LTRIM(RTRIM(@SaveMessagesToTable)), 16, 1);
        END

        -- Check Messages' Tables
        INSERT INTO @messages ([Message], Severity, [State])
        SELECT N'The table ' + QUOTENAME(t.[schema]) + N'.' + QUOTENAME(t.name) + ' is missing.', 10, 1
        FROM ( VALUES('Maintenance', 'Runs'), ('Maintenance', 'Messages')) as t([schema], [name])
        LEFT JOIN sys.objects obj ON obj.name = t.name
        LEFT JOIN sys.schemas sch ON sch.[schema_id] = obj.[schema_id] AND sch.name = t.[schema]
        WHERE (obj.[type] <> 'U' OR  obj.object_id IS NULL) AND obj.object_id IS NULL AND @logToTable = 1;

        -- Update @logToTable ON missing table(s)
        IF @@ROWCOUNT > 0 
        BEGIN
            SET @logToTable = 0;
            INSERT INTO @messages ([Message], Severity, [State]) VALUES
                ('Procedure''s execution (Errors & Messages) can''t be saved to [Messages] table', 16, 1);
        END 
        ELSE
        BEGIN
            -- Check missing permissions on Runs and Messages Table
            WITH p AS(
                SELECT entity_name, subentity_name, permission_name --'Permission not effectively granted: ' + UPPER(p.permission_name)
                FROM (VALUES
                    (N'[Maintenance].[Runs]', N'', N'INSERT')
                    , (N'[Maintenance].[Runs]', N'', N'DELETE')
                    , (N'[Maintenance].[Messages]', N'', N'INSERT')
                    , (N'[Maintenance].[Messages]', N'', N'DELETE')
                ) AS p (entity_name, subentity_name, permission_name)
            )
            INSERT INTO @messages ([Message], Severity, [State])
            SELECT 'Permission not effectively granted on ' + p.entity_name + N': ' + UPPER(p.permission_name), 10, 1
            FROM p
            LEFT JOIN (
                SELECT ca.entity_name, ca.subentity_name, ca.permission_name FROM (SELECT DISTINCT entity_name FROM p) AS t
                CROSS APPLY sys.fn_my_permissions(t.entity_name, N'OBJECT') ca
            ) eff ON eff.entity_name = p.entity_name AND eff.subentity_name = p.subentity_name AND eff.permission_name = p.permission_name
            WHERE eff.permission_name IS NULL 
            ORDER BY p.entity_name, p.permission_name;

            IF @@ROWCOUNT > 0 
            BEGIN
                INSERT INTO @messages ([Message], Severity, [State]) VALUES
                    (N'Error: missing permission', 10, 1)
                    , (N'WHEN @SaveMessagesToTable is set to Y or YES, INSERT and DELETE permissions are required on [Maintenance].[Runs] and [Maintenance].[Messages] tables', 16, 1);
            END
        END

        ----------------------------------------------------------------------------------------------------
        -- Check Error(s) count
        ----------------------------------------------------------------------------------------------------

        SELECT @errorCount = COUNT(*) FROM @messages WHERE severity > 10;
        IF @errorCount > 0 INSERT INTO @messages ([Message], Severity, [State]) SELECT N'End, see previous Error(s): ' + CAST(@errorCount AS nvarchar(10)) + N' found', 16, 1;

        ----------------------------------------------------------------------------------------------------
        -- Settings
        ----------------------------------------------------------------------------------------------------

        IF  NOT EXISTS(SELECT 1 FROM @messages WHERE severity > 10)
        BEGIN
            -- Check @OnErrorRetry
            SET @MaxErrorRetry = @OnErrorRetry;
            IF @MaxErrorRetry IS NULL 
            BEGIN
                SET @MaxErrorRetry = 5;
                INSERT INTO @messages ([Message], Severity, [State]) VALUES (N'@OnErrorRetry is NULL. Default value will be used (5 times).', 10, 1);
            END
            IF @MaxErrorRetry > 20
            BEGIN
                SET @MaxErrorRetry = 20;
                INSERT INTO @messages ([Message], Severity, [State]) VALUES (N'@OnErrorRetry is bigger than 20. Max value will be used (20 times)', 10, 1);
            END

            -- Check @OnErrorWaitMillisecond
            IF @OnErrorWaitMillisecond IS NULL 
            BEGIN
                INSERT INTO @messages ([Message], Severity, [State]) VALUES (N'@OnErrorWaitMillisecond is NULL. Default value will be used (1000 ms).', 10, 1);
            END
            SELECT @errorDelay = ISNULL(ABS(@OnErrorWaitMillisecond), 1000);
            SELECT @errorWait = DATEADD(MILLISECOND, @errorDelay, 0);

            INSERT INTO @messages ([Message], Severity, [State]) VALUES (N'@@LOCK_TIMEOUT = ' + CAST(@@LOCK_TIMEOUT AS nvarchar(MAX)), 10 , 1);

/*            -- Check Dry Run
            SELECT @dryRun = [value] FROM @paramsYesNo WHERE [parameter] = LTRIM(RTRIM(@DryRunOnly));
            IF @dryRun IS NULL 
            BEGIN
                SET @dryRun = 1;
                INSERT INTO @messages ([Message], Severity, [State]) VALUES (N'@DryRunOnly is NULL. Default value will be used (Yes).', 10, 1);
            END*/

            -- Verbose Level
            SELECT @levelVerbose = CASE WHEN [value] = 1 THEN 0 ELSE @verboseBelowLevel END FROM @paramsYesNo WHERE [parameter] = LTRIM(RTRIM(@Verbose));
            IF @levelVerbose IS NULL 
            BEGIN
                SET @levelVerbose = @verboseBelowLevel;
                INSERT INTO @messages ([Message], Severity, [State]) VALUES (N'@Verbose is NULL. Default value will be used (No).', 10, 1);
            END

            INSERT INTO @messages ([Message], Severity, [State]) VALUES
                (N'Run until = ' + ISNULL(CONVERT(nvarchar(MAX), @maxRunDateTime, 121), N'unlimited'), 10, 1 )
		END

        ----------------------------------------------------------------------------------------------------
        -- Create new Run Id
        ----------------------------------------------------------------------------------------------------
        INSERT INTO @messages ([Message], Severity, [State]) 
        SELECT N'Parameter Run Id [' + CAST(@SavedToRunId AS nvarchar(100)) + N'] is already ended.' , 10, 1 FROM [Maintenance].[Runs] WHERE Id = @SavedToRunId AND EndDate IS NOT NULL
        UNION ALL SELECT N'Parameter Run Id [' + CAST(@SavedToRunId AS nvarchar(100)) + N'] not found.', 10, 1 WHERE @SavedToRunId IS NOT NULL AND NOT EXISTS (SELECT 1 FROM [Maintenance].[Runs] WHERE Id = @SavedToRunId)
        
        IF @SavedToRunId IS NULL OR NOT EXISTS(SELECT 1 FROM [Maintenance].[Runs] WHERE Id = @SavedToRunId AND EndDate IS NULL)
        BEGIN
            INSERT INTO [Maintenance].[Runs]([Type], [Info], [StartTime]) SELECT N'Add Archive RobotLicenseLogs Trigger', N'PROCEDURE ' + @procName, @startTime;
            SELECT @runId = @@IDENTITY, @SavedToRunId = @@IDENTITY;
            INSERT INTO @messages ([Message], Severity, [State]) VALUES 
                (N'Messages saved to new Run Id: ' + CONVERT(nvarchar(MAX), @runId), 10, 1);
        END
        ELSE SELECT @runId = @SavedToRunId;

        ----------------------------------------------------------------------------------------------------
        -- Check Main Archive objects
        ----------------------------------------------------------------------------------------------------
        IF NOT EXISTS(SELECT 1 FROM @messages WHERE severity > 10)
        BEGIN
            -- Check Synonyms and source/Archive tables
            BEGIN TRY            
                EXEC [Maintenance].[ValidateArchiveObjectsRobotLicenseLogs] @IgnoreMissingColumns = 1, @Messages = @synonymMessages OUTPUT, @IsValid = @synonymIsValid OUTPUT;

                INSERT INTO @messages ([Procedure], [Message], Severity, [State])
                SELECT [Procedure], [Message], [Severity], [State] FROM OPENJSON(@synonymMessages, N'$') WITH ([Procedure] nvarchar(MAX), [Message] nvarchar(MAX), [Severity] tinyint, [State] tinyint);

                INSERT INTO @messages ([Message], Severity, [State])
                SELECT 'ERORR: Synonyms and Archive tables checks failed, see previous errors', 16, 1 WHERE ISNULL(@synonymIsValid, 0) = 0
            END TRY
            BEGIN CATCH
                INSERT INTO @messages ([Procedure], [Message], Severity, [State])
                SELECT [Procedure], [Message], [Severity], [State] FROM OPENJSON(@synonymMessages, N'$') WITH ([Procedure] nvarchar(MAX), [Message] nvarchar(MAX), [Severity] tinyint, [State] tinyint);

                INSERT INTO @messages ([Message], Severity, [State]) VALUES 
                    (N'ERORR: error(s) occured while checking synonyms and Archive tables', 16, 1);
                THROW;
            END CATCH
        END
    END TRY
    BEGIN CATCH
        -- Get Unknown error
        SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
        -- Save Unknwon Errror
        INSERT INTO @messages ([Message], Severity, [State], [Number], [Line])
        SELECT @ERROR_MESSAGE, @ERROR_SEVERITY, @ERROR_STATE, @ERROR_NUMBER, @ERROR_LINE;
    END CATCH

    ----------------------------------------------------------------------------------------------------
    -- Print & Save Errors & Messages
    ----------------------------------------------------------------------------------------------------
    SELECT @errorCount = COUNT(*) FROM @messages WHERE severity > 10;
    OPEN CursorMessages;
    FETCH CursorMessages INTO @cursorDate, @cursorProcedure, @cursorMessage, @cursorSeverity, @cursorState, @cursorNumber, @cursorLine;

    IF CURSOR_STATUS('local', 'CursorMessages') = 1
    BEGIN
        WHILE @@FETCH_STATUS = 0
        BEGIN;
            IF @logToTable = 0 OR @errorCount > 0 OR @cursorSeverity >= @levelVerbose OR @cursorSeverity > 10 RAISERROR('%s', @cursorSeverity, @cursorState, @cursorMessage) WITH NOWAIT;
            IF @cursorSeverity >= 16 RAISERROR('', 10, 1) WITH NOWAIT;
            FETCH CursorMessages INTO @cursorDate, @cursorProcedure, @cursorMessage, @cursorSeverity, @cursorState, @cursorNumber, @cursorLine;
        END
    END
    ELSE 
    BEGIN
        SET @message = 'Execution has been canceled: Error Opening Messages Cursor';
        INSERT INTO @messages ([Message], Severity, [State]) VALUES (@message, 16, 1);
        SET @errorCount = @errorCount +1;
        RAISERROR(@message, 16, 1);
    END 

	IF CURSOR_STATUS('local', 'CursorMessages') >= 0 CLOSE CursorMessages;
	IF CURSOR_STATUS('local', 'CursorMessages') >= -1 DEALLOCATE CursorMessages;

    BEGIN TRY 
        -- Records existing messages
        IF @runId IS NOT NULL 
        BEGIN
            INSERT INTO [Maintenance].[Messages](RunId, [Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
            SELECT @runId, [Date], [Procedure], [Message], [Severity], [State], [Number], [Line]
            FROM @messages 
            ORDER BY Id ASC;
        END

        ----------------------------------------------------------------------------------------------------
        -- End Run on Error(s)
        ----------------------------------------------------------------------------------------------------
        IF EXISTS(SELECT 1 FROM @messages WHERE severity > 10)
        BEGIN
            SET @returnValue = 3;
            SET @message = N'Incorrect Parameters or Settings, see previous Error(s): ' + CAST(@errorCount AS nvarchar(MAX)) + N' found';
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 16, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = 0, @MessagesStack = @MessagesStack OUTPUT;
            -- RETURN 123; => catch
        END
        ----------------------------------------------------------------------------------------------------
        -- End Run on Dry Run
        ----------------------------------------------------------------------------------------------------
        IF @dryRun <> 0 
        BEGIN
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineBreak, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

            INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
            EXEC sp_executesql @stmt = @stmtEmptyMessagesStack, @params = @paramsEmptyMessagesStack, @MessagesStack = @MessagesStack OUTPUT;

            SET @returnValue = 1;

            SET @message = 'DRY RUN ONLY (check output and parameters and set @DryRunOnly to No when ready)';
            EXEC [Maintenance].[AddRunMessage] @RunId = @RunId, @Procedure = @procName, @Message = @message, @Severity = 11, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = 0, @MessagesStack = @MessagesStack OUTPUT;
            -- RETURN 1; => catch
        END
        ----------------------------------------------------------------------------------------------------
        -- Remove Saved messages
        ----------------------------------------------------------------------------------------------------
        --DELETE FROM @messages;

        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineBreak, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

        ----------------------------------------------------------------------------------------------------
        -- Archive
        ----------------------------------------------------------------------------------------------------
        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = 'Start ASync RobotLicenseLogs Cleanup', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

        INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
        EXEC sp_executesql @stmt = @stmtEmptyMessagesStack, @params = @paramsEmptyMessagesStack, @MessagesStack = @MessagesStack OUTPUT;

        INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
        EXEC sp_executesql @stmt = @stmtEmptyMessagesStack, @params = @paramsEmptyMessagesStack, @MessagesStack = @MessagesStack OUTPUT;

        BEGIN TRY
            INSERT INTO @tempListSync([Id], [DeleteOnDate])
            SELECT sts.SyncId, sts.DeletedOnDate
            FROM [Maintenance].[Synonym_ASyncStatus_RobotLicenseLogs] sts
            INNER JOIN [Maintenance].[Sync_RobotLicenseLogs] syn ON syn.Id = sts.SyncId
            WHERE sts.IsDeleted = 1 AND syn.IsSynced <> 1;

            IF @@ROWCOUNT = 0
            BEGIN 
                SET @message = N'Nothing to sync';
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 0, @Number = @ERROR_NUMBER, @Line = @ERROR_LINE, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
            END
        END TRY
        BEGIN CATCH
            -- Get Unknown error
            SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
            -- Save Unknwon Errror
            SET @message = N'  Error(s) occured while retrieving archived Sync Id(s)';
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = @ERROR_STATE, @Number = @ERROR_NUMBER, @Line = @ERROR_LINE, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

            THROW;
        END CATCH

        SET @globalIds = 0;

        IF CURSOR_STATUS('local', 'CursorSyncIds') >= 0 CLOSE CursorSyncIds;
        IF CURSOR_STATUS('local', 'CursorSyncIds') >= -1 DEALLOCATE CursorSyncIds;

        DECLARE CursorSyncIds CURSOR FAST_FORWARD LOCAL FOR 
            SELECT [Id], [DeleteOnDate] FROM @tempListSync ORDER BY [DeleteOnDate] ASC;

        OPEN CursorSyncIds;
        FETCH CursorSyncIds INTO @cursorSyncId, @cursorDeleteOnDate;

        IF CURSOR_STATUS('local', 'CursorSyncIds') = 1
        BEGIN 
            WHILE @@FETCH_STATUS = 0 --> Cursor Loop Sync Id
            BEGIN;
                SET @message = N'Sync Id [' + CAST(@cursorSyncId AS nvarchar(100)) + N']: ';
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                SET @totalIds = 0;
                WHILE 0 >= 0
                BEGIN
                    DELETE TOP(@maxLoopDeleteRows) FROM [Maintenance].[Delete_RobotLicenseLogs] WHERE [SyncId] = @cursorSyncId;
                    SELECT @countIds = @@ROWCOUNT;

                    IF @countIds = 0
                    BEGIN
                        SET @message = SPACE(@tab * 1) + 'Cleanup finished (total = ' + CAST(@totalIds AS nvarchar(100)) + N')';
                        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                        UPDATE stt SET [IsDeleted] = 1, [DeletedOnDate] = @cursorDeleteOnDate, [IsSynced] = 1, [SyncedOnDate] = SYSDATETIME() FROM [Maintenance].[Sync_RobotLicenseLogs] stt WHERE [Id] = @cursorSyncId;

                        SELECT @IsFinished = IIF(EXISTS (SELECT 1 FROM [Maintenance].[Sync_RobotLicenseLogs] WHERE ArchiveId = (SELECT ArchiveId FROM [Maintenance].[Sync_RobotLicenseLogs] WHERE Id = @cursorSyncId) AND IsSynced <> 1), 0, 1);

                        UPDATE arc SET 
                            [CurrentRunId] = @runId
                            , IsDeleted = @IsFinished
                            , DeletedOnDate = IIF(@IsFinished = 1, @cursorDeleteOnDate, NULL)
                            , IsFinished = @IsFinished
                            , FinishedOnDate = IIF(@IsFinished = 1, SYSDATETIME(), NULL)
                            , IsSuccess = @IsFinished
                            , [Message] = IIF(@IsFinished = 1, N'All Archive Filter(s) and cleanup finished', N'Partial Cleanup')
                            , PreviousRunIds = (
                                SELECT [runid], [message], [timestamp] FROM (
                                    SELECT [runid] = @runId, [message] = N'All Archive Filter(s) and cleanup finished]', [timestamp] = SYSDATETIME() WHERE @IsFinished = 1
                                    UNION ALL SELECT [runid] = @runId, [message] = IIF(@IsFinished = 1, N'Cleanup finished', N'Partial Cleanup'), [timestamp] = SYSDATETIME()
                                    UNION ALL SELECT [runid], [message], [timestamp] FROM OPENJSON(PreviousRunIds) WITH ([runid] nvarchar(MAX) N'$.runid', [message] nvarchar(MAX) N'$.message', [timestamp] datetime2 N'$.timestamp')
                                ) v FOR JSON PATH
                            )
                        FROM [Maintenance].[Archive_RobotLicenseLogs] arc
                        INNER JOIN [Maintenance].[Sync_RobotLicenseLogs] syn ON syn.ArchiveId = arc.Id
                        WHERE syn.Id = @cursorSyncId;

                        BREAK;
                    END

                    SET @message = SPACE(@tab * 1) + 'cleanup: ' + CAST(@countIds AS nvarchar(100)) + N'';
                    EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                    SELECT @firstId = @lastId + 1, @totalIds = @totalIds + @countIds, @globalIds = @globalIds + @countIds;                    

                    IF SYSDATETIME() > @maxRunDateTime
                    BEGIN
                        SET @isTimeOut = 1;
                        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineBreak, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                        SELECT @message = N'TIME OUT:' + (SELECT CAST(DATEDIFF(MINUTE, @startTime, SYSDATETIME()) AS nvarchar(MAX)) ) + N' min (@MaxRunMinutes = ' + ISNULL(CAST(@MaxRunMinutes AS nvarchar(MAX)), N'NULL' ) + N')';
                        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                        BREAK;
                    END 
                END --> end loop
                IF @isTimeOut = 1 BREAK;

                FETCH CursorSyncIds INTO @cursorSyncId, @cursorDeleteOnDate;
            END --> Cursor Loop Sync Id
        END
        ELSE 
        BEGIN
            SET @message = 'Execution has been canceled: Error Opening Sync Ids Cursor';
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
            RAISERROR(@message, 16, 1);
        END

        IF CURSOR_STATUS('local', 'CursorSyncIds') >= 0 CLOSE CursorSyncIds;
        IF CURSOR_STATUS('local', 'CursorSyncIds') >= -1 DEALLOCATE CursorSyncIds;

        SET @message = 'Sync Id Cleanup finished' + IIF(@isTimeOut = 1, N' (TIME OUT)', N'');
        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
        SET @message = SPACE(@tab * 1) + 'Rows deleted = ' + ISNULL(CAST(@globalIds AS nvarchar(100)), 0);
        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

        INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
        EXEC sp_executesql @stmt = @stmtEmptyMessagesStack, @params = @paramsEmptyMessagesStack, @MessagesStack = @MessagesStack OUTPUT;

        ----------------------------------------------------------------------------------------------------
        -- Runs Cleanup
        ----------------------------------------------------------------------------------------------------
        IF @logToTable = 1 
        BEGIN
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineBreak, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = 'Old Runs cleanup', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

            EXEC [Maintenance].[DeleteRuns] @CleanupAfterDays = @SavedMessagesRetentionDays, @RunId = @runId, @Procedure = @procName, @MessagesStack = @MessagesStack OUTPUT;
        END

		EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineBreak, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
		EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
		EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = 'END (SUCCESS)', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

        INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
        EXEC sp_executesql @stmt = @stmtEmptyMessagesStack, @params = @paramsEmptyMessagesStack, @MessagesStack = @MessagesStack OUTPUT;

        SET @returnValue = 0; -- Success
    END TRY
    BEGIN CATCH
        SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();

        -- Record error message
        EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @ERROR_MESSAGE, @Severity = @ERROR_SEVERITY, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @RaiseError = 0, @MessagesStack = @MessagesStack OUTPUT;

        IF @@TRANCOUNT > 0 ROLLBACK TRAN;

        INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
        EXEC sp_executesql @stmt = @stmtEmptyMessagesStack, @params = @paramsEmptyMessagesStack, @MessagesStack = @MessagesStack OUTPUT;

        IF @errorCount = 0
        BEGIN
            IF @dryRun <> 0 
            BEGIN 
                -- Output Message result set
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = 'END (DRY RUN)', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
            END
            ELSE 
            BEGIN
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineBreak, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                SET @message = N'Execution finished with error(s)'
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                SET @message = N'Rows deleted: ' + ISNULL(CAST(@totalIds AS nvarchar(MAX)), '0');
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                SET @message = N'Elapsed time : ' + CAST(CAST(DATEADD(SECOND, DATEDIFF(SECOND, @startTime, SYSDATETIME()), 0) AS time) AS nvarchar(MAX));
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineBreak, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = @lineSeparator, @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = 'END (FAIL)', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;

                SET @returnValue = 4
            END
        END
        ELSE
        BEGIN
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @procName, @Message = 'END (INCORRECT PARAMETERS)', @Severity = 10, @State = 1, @VerboseLevel = @levelVerbose, @LogToTable = @logToTable, @MessagesStack = @MessagesStack OUTPUT;
        END

        RAISERROR(@ERROR_MESSAGE, @ERROR_SEVERITY, @ERROR_STATE);

        SET @returnValue = ISNULL(@returnValue, 255);
    END CATCH

    ----------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------
    INSERT INTO @messages([Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
    EXEC sp_executesql @stmt = @stmtEmptyMessagesStack, @params = @paramsEmptyMessagesStack, @MessagesStack = @MessagesStack OUTPUT;

    -- Output Messages result set
    IF @outputDataset = 1 SELECT [RunId] = @RunId, [Date], [Procedure], [Message], [Severity], [State], [Number], [Line] FROM @messages ORDER BY [id] ASC;

    SET @returnValue = ISNULL(@returnValue, 255);
    IF @runId IS NOT NULL UPDATE [Maintenance].[Runs] SET [EndDate] = SYSDATETIME(), [ErrorStatus] = @returnValue WHERE Id = @runId;

    ----------------------------------------------------------------------------------------------------
    -- End
    ----------------------------------------------------------------------------------------------------
    RETURN @returnValue;
END
GO
