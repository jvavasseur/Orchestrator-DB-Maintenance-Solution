SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO
----------------------------------------------------------------------------------------------------
-- Update both DB names
----------------------------------------------------------------------------------------------------
DECLARE @OrchestratorDB sysname = N'Orchestrator' --> MUST be updated with Orchestrator DB name
DECLARE @ArchiveDB sysname = N'Archive' --> MUST be updated with Archive DB name
----------------------------------------------------------------------------------------------------
-- Settings used on Orchestrator DB
----------------------------------------------------------------------------------------------------
DECLARE @PasswordInOrchestratorDB sysname = N'<p@ssw0rd-user-orchestratorDB>xxx'; --> can be updated with any valid password
DECLARE @UserInOrchestratorDB sysname = N'ArchivingUserReadOrchestratorDB';
DECLARE @CredentialNameForArchiveDB sysname = N'ArchivingCredentialForArchiveDB';
DECLARE @DatasourceNameForArchiveDB sysname = N'ArchivingDatasourceForArchiveDB';
DECLARE @DatasourceLocationForArchiveDB sysname = N'jv-sql-db-server.database.windows.net';
----------------------------------------------------------------------------------------------------
-- Settings used on Archive DB
----------------------------------------------------------------------------------------------------
DECLARE @PasswordInArchiveDB sysname = N'<p@ssw0rd-user-ArchiveDB>yyy'; --> can be updated with any valid password
DECLARE @UserInArchiveDB sysname = N'ArchivingUserReadArchiveDB';
DECLARE @CredentialNameForOrchestratorDB sysname = N'ArchivingCredentialForOrchestratorDB';
DECLARE @DatasourceNameForOrchestratorDB sysname = N'ArchivingDatasourceForOrchestratorDB';
DECLARE @DatasourceLocationForOrchestratorDB sysname = N'jv-sql-db-server.database.windows.net';
----------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------
DECLARE @username sysname, @userpassword sysname, @credential sysname, @credentialuser sysname, @credentialpassword sysname, @credential_id int;
DECLARE @datasource sysname, @location sysname, @database sysname
DECLARE @message nvarchar(4000), @sql nvarchar(MAX);
SET @message = N'Current database must be either Orchestrator or Archive database: ' + QUOTENAME(DB_NAME()) + ' is not ' + QUOTENAME(@OrchestratorDB) + ' or ' + QUOTENAME(@ArchiveDB);
IF DB_NAME() NOT IN (@OrchestratorDB, @ArchiveDB) THROW 50000, @message, 1


SELECT @username = @UserInOrchestratorDB, @userpassword = @PasswordInOrchestratorDB
    , @credential = @CredentialNameForArchiveDB, @credentialuser = @UserInArchiveDB, @credentialpassword = @PasswordInArchiveDB
    , @datasource = @DatasourceNameForArchiveDB, @location = @DatasourceLocationForArchiveDB, @database = @ArchiveDB
WHERE DB_NAME() = @OrchestratorDB;
SELECT @username = @UserInArchiveDB, @userpassword = @PasswordInArchiveDB
    , @credential = @CredentialNameForOrchestratorDB, @credentialuser = @UserInOrchestratorDB, @credentialpassword = @PasswordInOrchestratorDB
    , @datasource = @DatasourceNameForOrchestratorDB, @location = @DatasourceLocationForOrchestratorDB , @database = @OrchestratorDB
WHERE DB_NAME() = @ArchiveDB;

IF EXISTS(SELECT 1 FROM sys.external_data_sources WHERE [name] = @datasource)
BEGIN 
    PRINT N' X DROP DATA SOURCE: ' + QUOTENAME(@datasource); 
    SET @sql = N'DROP EXTERNAL DATA SOURCE '+ QUOTENAME(@datasource) + ';';
    EXEC sp_executesql @stmt = @sql;
END
ELSE PRINT N' ~ DATA SOURCE already remove: ' + QUOTENAME(@datasource); 

IF EXISTS(SELECT 1 FROM sys.database_scoped_credentials WHERE [name] = @credential)
BEGIN 
    PRINT N' X DROP DATABASE SCOPE CREDENTIAL: ' + QUOTENAME(@credential); 
    SET @sql = N'DROP DATABASE SCOPED CREDENTIAL '+ QUOTENAME(@credential) + N';';
    EXEC sp_executesql @stmt = @sql;
END
ELSE PRINT N' ~ DATABASE SCOPE CREDENTIAL already remove: ' + QUOTENAME(@credential); 

IF EXISTS(SELECT 1 FROM sys.database_principals WHERE [name] = @username AND [type] = N's')
BEGIN
    PRINT N' X DROP USER ' + QUOTENAME(@username); 
    SET @sql = N'DROP USER '+ QUOTENAME(@username) + N';';
    EXEC sp_executesql @stmt = @sql;
END
ELSE PRINT N' ~ USER already remove: ' + QUOTENAME(@username); 

--SELECT * FROM sys.database_scoped_credentials WHERE [name] = @credential
SELECT 'database_scoped_credentials', * FROM sys.database_scoped_credentials --WHERE [name] = @credential -- 65540
SELECT 'database_credentials', * FROM sys.database_credentials
SELECT 'external_data_sources', * FROM sys.external_data_sources --WHERE [name] = @datasource -- 
