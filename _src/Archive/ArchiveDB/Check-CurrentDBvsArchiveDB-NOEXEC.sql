SET NOEXEC OFF;
GO
----------------------------------------------------------------------------------------------------
-- 1. Archive Database must be selected
-- 2. The value of @ArchiveDatabaseName must be set to the name of the Archive Database
----------------------------------------------------------------------------------------------------
--\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/
DECLARE @ArchiveDatabaseName sysname = N'<<-archive database->>'; --<== UPDATE NAME
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
--!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
----------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------
-- DB Check
----------------------------------------------------------------------------------------------------
BEGIN TRY
    DECLARE @message nvarchar(1000);
    SET @ArchiveDatabaseName = ISNULL(@ArchiveDatabaseName, N'');
    IF DB_NAME() <> @ArchiveDatabaseName
    BEGIN
        SET @message = N'Current Database ==> ''' + DB_NAME() + N''' <=='; RAISERROR(@message, 10, 1);
        SET @message = N'@ArchiveDatabaseName ==> ''' + @ArchiveDatabaseName + N''' <=='; RAISERROR(@message, 10, 1);
        RAISERROR(N'The Archive Database must be selected and @ArchiveDatabaseName value must match the Archive Database Name', 16, 1);
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
