SET NOEXEC OFF;
GO
----------------------------------------------------------------------------------------------------
-- 1. Orchestrator Database must be selected
-- 2. The value of @OrchestratorDatabaseName must be set to the name of the Orchestrator Database
----------------------------------------------------------------------------------------------------
--\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/
DECLARE @OrchestratorDatabaseName sysname = N'<orchestrator database>'; --<== UPDATE NAME
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
--!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
----------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------
-- DB Check
----------------------------------------------------------------------------------------------------
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
SET NOCOUNT ON;

DECLARE @message nvarchar(1000) = N'The Orchestrator Database must be selected and @OrchestratorDatabaseName value must match the Orchestrator Databse Name';
IF DB_NAME() <> @OrchestratorDatabaseName 
BEGIN
    RAISERROR(N'The Orchestrator Database must be selected and @OrchestratorDatabaseName value must match the Orchestrator Databse Name', 16, 1);
    SET @message = N'Current Database ==> ''' + DB_NAME() + N''' <=='; RAISERROR(@message, 10, 1);
    SET @message = N'@OrchestratorDatabaseName ==> ''' + @OrchestratorDatabaseName + N''' <=='; RAISERROR(@message, 10, 1);
    RAISERROR(N'Script execution canceled', 16, 1);
    SET NOEXEC ON;
END
IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
GO
