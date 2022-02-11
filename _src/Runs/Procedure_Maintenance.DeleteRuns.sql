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
-- ### [Version]: 2020-10-01 00:00:00                                                         
-- ### [Source]: ??????
-- ### [Hash]: ??????
-- ### [Docs]: https://???.???
----------------------------------------------------------------------------------------------------
    @CleanupAfterDays tinyint = 30
    , @RunId int = NULL
    , @Procedure nvarchar(max) = NULL
    , @LogsStack xml = NULL OUTPUT
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

            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @Procedure, @Message = N'Runs cleanup started', @Severity = 10, @State = 1, @VerboseLevel = 10, @LogToTable = 1, @LogsStack = @logsStack OUTPUT;
            SET @message = N'Keep Runs from past days: ' + CAST(@CleanupAfterDays AS nvarchar(10));
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @Procedure, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = 10, @LogToTable = 1, @LogsStack = @logsStack OUTPUT;
            SET @message =  N'Delete Runs before: ' + CONVERT(nvarchar(20), DATEADD(DAY, - @CleanupAfterDays, SYSDATETIME()), 121);
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @Procedure, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = 10, @LogToTable = 1, @LogsStack = @logsStack OUTPUT;

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
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @Procedure, @Message = @message, @Severity = 10, @State = 1, @VerboseLevel = 10, @LogToTable = 1, @LogsStack = @logsStack OUTPUT;
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @Procedure, @Message = N'Runs Cleanup Finished', @Severity = 10, @State = 1, @VerboseLevel = 10, @LogToTable = 1, @LogsStack = @logsStack OUTPUT;

            UPDATE [Maintenance].[Runs] SET [EndDate] = SYSDATETIME(), [ErrorStatus] = 0 WHERE Id = @localRunId AND @RunId IS NULL;
        END TRY
        BEGIN CATCH
            SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
            IF @@TRANCOUNT > 0 ROLLBACK;

            IF @localRunId IS NOT NULL
            BEGIN
                SET @message = N'Error: ' + @ERROR_MESSAGE;
                EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @Procedure, @Message = @message, @Severity = @ERROR_SEVERITY, @State = @ERROR_STATE, @VerboseLevel = 10, @LogToTable = 1, @RaiseError = 0, @LogsStack = @logsStack OUTPUT;
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
            EXEC [Maintenance].[AddRunMessage] @RunId = @runId, @Procedure = @Procedure, @Message = @message, @Severity = @ERROR_SEVERITY, @State = @ERROR_STATE, @VerboseLevel = 10, @LogToTable = 1, @RaiseError = 0, @LogsStack = @logsStack OUTPUT;

            UPDATE [Maintenance].[Runs] SET [EndDate] = SYSDATETIME(), [ErrorStatus] = 4 WHERE Id = @localRunId;
        END;

        THROW;
    END CATCH
END

GO

