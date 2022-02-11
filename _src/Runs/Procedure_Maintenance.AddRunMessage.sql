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
-- ### [Version]: 2020-10-01 00:00:00                                                         
-- ### [Source]: ??????
-- ### [Hash]: ??????
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
    , @LogsStack xml = NULL OUTPUT
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
        SET @LogsStack = COALESCE(@logsStack, N'<messages></messages>');
        SET @Message = COALESCE(@Message, N'');

        IF @LogToTable = 1 
        BEGIN
            INSERT INTO [Maintenance].[Messages](RunId, [Date], [Procedure], [Message], [Severity], [State], [Number], [Line])
            SELECT @RunId, @date, @Procedure, @Message, @Severity, @state, @Number, @Line WHERE @RunId IS NOT NULL;
        END

        SET @log = (  SELECT * FROM (SELECT SYSDATETIME(), @Procedure, @Message, @Severity, @state, @Number, @Line) x([Date], [Procedure], [Message], [Severity], [State], [Number], [Line]) FOR XML PATH('message') )
        SET @logsStack.modify('insert sql:variable("@log") as last into (/messages)[1]')

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

