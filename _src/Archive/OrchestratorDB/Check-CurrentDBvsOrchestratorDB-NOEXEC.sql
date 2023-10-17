SET NOEXEC OFF;
GO
----------------------------------------------------------------------------------------------------
-- 1. Orchestrator Database must be selected
-- 2. The value of @OrchestratorDatabaseName must be set to the name of the Orchestrator Database
----------------------------------------------------------------------------------------------------
--\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/
DECLARE @OrchestratorDatabaseName sysname = N'<<-orchestrator database->>'; --<== UPDATE NAME
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
--!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
----------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------
-- DB Check
----------------------------------------------------------------------------------------------------
BEGIN TRY
    DECLARE @message nvarchar(1000);
    SET @OrchestratorDatabaseName = ISNULL(@OrchestratorDatabaseName, N'');
    IF DB_NAME() <> @OrchestratorDatabaseName
    BEGIN
        SET @message = N'Current Database ==> ''' + DB_NAME() + N''' <=='; RAISERROR(@message, 10, 1);
        SET @message = N'@OrchestratorDatabaseName ==> ''' + @OrchestratorDatabaseName + N''' <=='; RAISERROR(@message, 10, 1);
        RAISERROR(N'The Orchestrator Database must be selected and @OrchestratorDatabaseName value must match the Orchestrator Database Name', 16, 1);
        SET NOEXEC ON;
    END
END TRY
BEGIN CATCH
    SET @message = ERROR_MESSAGE();
    RAISERROR(@message, 16, 1)
    PRINT N''
    RAISERROR(N'Script execution canceled', 16, 1);
    SET NOEXEC ON;
END CATCH;
IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
GO
