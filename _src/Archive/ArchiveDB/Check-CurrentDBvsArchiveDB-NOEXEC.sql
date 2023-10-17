SET NOEXEC OFF;
GO
----------------------------------------------------------------------------------------------------
-- 1. Archive Database must be selected
-- 2. The value of @ArchiveDatabaseName must be set to the name of the Archive Database
----------------------------------------------------------------------------------------------------
--\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/
DECLARE @ArchiveDatabaseName sysname = N'<archive database>'; --<== UPDATE NAME
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
--!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
----------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------
-- DB Check
----------------------------------------------------------------------------------------------------
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
SET NOCOUNT ON;

DECLARE @message nvarchar(1000) = N'The Archive Database must be selected and @ArchiveDatabaseName value must match the Archive Databse Name';
IF DB_NAME() <> @ArchiveDatabaseName 
BEGIN
    RAISERROR(N'The Archive Database must be selected and @ArchiveDatabaseName value must match the Archive Databse Name', 16, 1);
    SET @message = N'Current Database ==> ''' + DB_NAME() + N''' <=='; RAISERROR(@message, 10, 1);
    SET @message = N'@ArchiveDatabaseName ==> ''' + @ArchiveDatabaseName + N''' <=='; RAISERROR(@message, 10, 1);
    RAISERROR(N'Script execution canceled', 16, 1);
    SET NOEXEC ON;
END
IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
select 1;

GO
select 2;
