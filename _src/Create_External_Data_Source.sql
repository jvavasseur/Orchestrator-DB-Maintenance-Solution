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

SET @message = N'USER ' + QUOTENAME(@username) + ' is not of type SQL_USER'
IF EXISTS(SELECT 1 FROM sys.database_principals WHERE [name] = @username AND [type] <> N's') THROW 50000, @message, 1;

IF NOT EXISTS(SELECT 1 FROM sys.database_principals WHERE [name] = @username)
BEGIN
    PRINT N' + CREATE USER ' + QUOTENAME(@username); 
    SET @sql = N'CREATE USER '+ QUOTENAME(@username) + N' WITH PASSWORD = ''' + @userpassword + ''';';
    EXEC sp_executesql @stmt = @sql;
END
ELSE 
BEGIN
    PRINT N' = USER already exists: ' + QUOTENAME(@username); 
    SET @sql = N'ALTER USER '+ QUOTENAME(@username) + N' WITH PASSWORD = ''' + @userpassword + ''';';
    PRINT N' ~ Update password for USER: ' + QUOTENAME(@username); 
    EXEC sp_executesql @stmt = @sql;
END

IF NOT EXISTS(SELECT * FROM sys.database_role_members AS drm
    INNER JOIN sys.database_principals rle ON drm.role_principal_id = rle.principal_id
    INNER JOIN sys.database_principals mbr ON drm.member_principal_id = mbr.principal_id
    WHERE rle.[name] = 'db_datareader' AND rle.[type] = 'R' AND mbr.[name] = @username)
BEGIN
    PRINT N' ~ Add '+ QUOTENAME(@username) + ' to role: db_datareader';
    EXEC sp_addrolemember [db_datareader], @username;
END


IF NOT EXISTS(SELECT 1 FROM sys.database_scoped_credentials WHERE [name] = @credential)
BEGIN 
    PRINT N' + CREATE DATABASE SCOPE CREDENTIAL: ' + QUOTENAME(@credential); 
    SET @sql = N'CREATE DATABASE SCOPED CREDENTIAL '+ QUOTENAME(@credential) + N' WITH IDENTITY = ''' + @credentialuser + ''', SECRET = ''' + @credentialpassword + ''';' ;
    EXEC sp_executesql @stmt = @sql;
END
ELSE
BEGIN
    PRINT N' ~ UPDATE DATABASE SCOPE CREDENTIAL: ' + QUOTENAME(@credential); 
    SET @sql = N'ALTER DATABASE SCOPED CREDENTIAL '+ QUOTENAME(@credential) + N' WITH IDENTITY = ''' + @credentialuser + ''', SECRET = ''' + @credentialpassword + ''';' ;
    EXEC sp_executesql @stmt = @sql;
END
SELECT @credential_id = [credential_id] FROM sys.database_scoped_credentials WHERE [name] = @credential

IF NOT EXISTS(SELECT 1 FROM sys.external_data_sources WHERE [name] = @datasource)
BEGIN 
    PRINT N' + CREATE DATA SOURCE: ' + QUOTENAME(@datasource); 
    SET @sql = N'CREATE EXTERNAL DATA SOURCE '+ QUOTENAME(@datasource) + ' WITH ( TYPE = RDBMS , LOCATION = ''' + @location + ''', DATABASE_NAME = ''' + @database + ''', CREDENTIAL = ' + QUOTENAME(@credential) + ') ;'
print @sql
    EXEC sp_executesql @stmt = @sql;
END
ELSE IF EXISTS(SELECT 1 FROM sys.external_data_sources WHERE [name] = @datasource AND ([location] <> @location OR [database_name] <> @database OR [credential_id] <> @credential_id))
BEGIN
    PRINT N' ~ UPDATE DATA SOURCE: ' + QUOTENAME(@datasource); 
    SET @sql = N'ALTER EXTERNAL DATA SOURCE '+ QUOTENAME(@datasource) + ' SET LOCATION = ''' + @location + ''', DATABASE_NAME = ''' + @database + ''', CREDENTIAL = ' + QUOTENAME(@credential) + ';'
print @sql
    EXEC sp_executesql @stmt = @sql;
END
ELSE PRINT N' = DATA SOURCE already exists: ' + QUOTENAME(@datasource); 

--SELECT * FROM sys.database_scoped_credentials WHERE [name] = @credential
SELECT 'database_scoped_credentials', * FROM sys.database_scoped_credentials --WHERE [name] = @credential -- 65540
SELECT 'database_credentials', * FROM sys.database_credentials
SELECT 'external_data_sources', * FROM sys.external_data_sources --WHERE [name] = @datasource -- 
