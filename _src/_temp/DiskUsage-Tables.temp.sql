USE UiPath_Production
GO
SET NOCOUNT ON;
/*
https://www.red-gate.com/simple-talk/sql/learn-sql-server/using-the-for-xml-clause-to-return-query-results-as-xml/

https://docs.microsoft.com/en-us/sql/relational-databases/security/metadata-visibility-configuration?view=sql-server-ver15
-- sys.all_objects = The visibility of the metadata in catalog views is limited to securables that a user either owns or on which the user has been granted some permission.


Requires membership in the public role
-- sys.configurations = Requires membership in the public role.
-- sys.schemas = Requires membership in the public role.

Others:
-- sys.databases = 
-- sys.dm_db_partition_stats = VIEW DATABASE STATE
-- sys.dm_os_sys_info = On SQL Server, requires VIEW SERVER STATE permission. On SQL Database Premium Tiers, requires the VIEW DATABASE STATE permission in the database. On SQL Database Standard and Basic Tiers, requires the Server admin or an Azure Active Directory admin account.
-- sys.internal_tables = 

GO

SELECT
(select value from sys.database_scoped_configurations as dsc where dsc.name = 'MAXDOP') AS [MaxDop],
(select value_for_secondary from sys.database_scoped_configurations as dsc where dsc.name = 'MAXDOP') AS [MaxDopForSecondary],
(select value from sys.database_scoped_configurations as dsc where dsc.name = 'LEGACY_CARDINALITY_ESTIMATION') AS [LegacyCardinalityEstimation],
(select ISNULL(value_for_secondary, 2) from sys.database_scoped_configurations as dsc where dsc.name = 'LEGACY_CARDINALITY_ESTIMATION') AS [LegacyCardinalityEstimationForSecondary],
(select value from sys.database_scoped_configurations as dsc where dsc.name = 'PARAMETER_SNIFFING') AS [ParameterSniffing],
(select ISNULL(value_for_secondary, 2) from sys.database_scoped_configurations as dsc where dsc.name = 'PARAMETER_SNIFFING') AS [ParameterSniffingForSecondary],
(select value from sys.database_scoped_configurations as dsc where dsc.name = 'QUERY_OPTIMIZER_HOTFIXES') AS [QueryOptimizerHotfixes],
(select ISNULL(value_for_secondary, 2) from sys.database_scoped_configurations as dsc where dsc.name = 'QUERY_OPTIMIZER_HOTFIXES') AS [QueryOptimizerHotfixesForSecondary]
*/

--select * from sys.databases
--select * from sys.database_scoped_configurations

-- https://docs.microsoft.com/en-us/sql/relational-databases/security/metadata-visibility-configuration?view=sql-server-ver15

DECLARE @ERROR_NUMBER int, @ERROR_MESSAGE nvarchar(4000), @ERROR_STATE int, @ERROR_SEVERITY int, @ERROR_LINE int;
DECLARE @output TABLE(type varchar(10), name varchar(50), data xml)
DECLARE @stmt nvarchar(max) = N'', @params nvarchar(max), @xml xml, @database sysname = DB_NAME();

SET @database = DB_NAME();

-- configuration => sys.configurations
BEGIN TRY
    INSERT INTO @output(type, name, data)
    SELECT 'system', 'sys.configurations', (
        SELECT [@id] = configuration_id,
            [@name] = name,
            [@value_in_use] = value_in_use
        FROM sys.configurations
        FOR XML PATH('sys.configurations')
    )
END TRY
BEGIN CATCH
    SELECT @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE(), @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE();
    INSERT INTO @output(type, name, data)
    SELECT 'system', 'sys.configurations', (
        SELECT [@ERROR_NUMBER] = @ERROR_NUMBER
            , [@ERROR_SEVERITY] = @ERROR_SEVERITY
            , [@ERROR_STATE] = @ERROR_STATE
            , [@ERROR_MESSAGE] = @ERROR_MESSAGE
        FOR XML PATH('error')
    )
	RAISERROR(N'!!!! ERROR !!!! See message(s) below', 10, @ERROR_STATE) WITH NOWAIT;
	RAISERROR(@ERROR_MESSAGE, 10, @ERROR_STATE) WITH NOWAIT;
END CATCH



-- system => sys.dm_os_sys_info
BEGIN TRY
   INSERT INTO @output(type, name, data)
    SELECT 'system', 'sys.dm_os_sys_info', (
        SELECT * FROM sys.dm_os_sys_info
        FOR XML PATH('')
    )
END TRY
BEGIN CATCH
    SELECT @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE(), @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE();
    INSERT INTO @output(type, name, data)
    SELECT 'system', 'sys.dm_os_sys_info', (
        SELECT [@ERROR_NUMBER] = @ERROR_NUMBER
            , [@ERROR_SEVERITY] = @ERROR_SEVERITY
            , [@ERROR_STATE] = @ERROR_STATE
            , [@ERROR_MESSAGE] = @ERROR_MESSAGE
        FOR XML PATH('error')
    )
	RAISERROR(N'!!!! ERROR !!!! See message(s) below', 10, @ERROR_STATE) WITH NOWAIT;
	RAISERROR(@ERROR_MESSAGE, 10, @ERROR_STATE) WITH NOWAIT;
END CATCH

-- Disk Usage by Table
BEGIN TRY
   INSERT INTO @output(type, name, data)
    SELECT 'tables', 'diskusage-by-tables', (
        SELECT
            [@schemaname] = shm.name,
            [@tablename] = obj.name,
            [@rowcount] = dta.rows,
            [@reserved-kb] = (dta.reserved + ISNULL(itp.reserved, 0)) * 8, 
            [@data-kb] = dta.data * 8,
            [@indexes-kb] = (CASE WHEN (dta.used + ISNULL(itp.used, 0)) > dta.data THEN (dta.used + ISNULL(itp.used, 0)) - dta.data ELSE 0 END) * 8,
            [@unused-kb] = (CASE WHEN (dta.reserved + ISNULL(itp.reserved, 0)) > dta.used THEN (dta.reserved + ISNULL(itp.reserved, 0)) - dta.used ELSE 0 END) * 8
        FROM (
            SELECT prs.object_id,
                [rows] = SUM( CASE WHEN prs.index_id < 2 THEN row_count ELSE 0 END ),
                [reserved] = SUM (prs.reserved_page_count),
                [data] = SUM ( prs.lob_used_page_count + prs.row_overflow_used_page_count + 
                    CASE WHEN prs.index_id < 2 THEN prs.in_row_data_page_count ELSE 0 END ),
                [used] = SUM (prs.used_page_count)
            FROM sys.dm_db_partition_stats prs
            WHERE prs.object_id NOT IN (SELECT object_id FROM sys.tables WHERE is_memory_optimized = 1)
            GROUP BY prs.object_id
        ) AS dta
        LEFT OUTER JOIN (
            SELECT int.parent_id,
                [reserved] = SUM(prs.reserved_page_count),
                [used] = SUM(prs.used_page_count)
            FROM sys.dm_db_partition_stats prs
            INNER JOIN sys.internal_tables int ON (int.object_id = prs.object_id)
            WHERE int.internal_type IN (202,204)
            GROUP BY int.parent_id
        ) AS itp ON itp.parent_id = dta.object_id
        INNER JOIN sys.all_objects obj  ON dta.object_id = obj.object_id
        INNER JOIN sys.schemas shm ON obj.schema_id = shm.schema_id
        WHERE obj.type <> N'S' and obj.type <> N'IT'
        ORDER BY shm.name, obj.name
        FOR XML PATH('table')
    )
END TRY
BEGIN CATCH
    SELECT @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE(), @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE();
    INSERT INTO @output(type, name, data)
    SELECT 'tables', 'diskusage-by-tables', (
        SELECT [@ERROR_NUMBER] = @ERROR_NUMBER
            , [@ERROR_SEVERITY] = @ERROR_SEVERITY
            , [@ERROR_STATE] = @ERROR_STATE
            , [@ERROR_MESSAGE] = @ERROR_MESSAGE
        FOR XML PATH('error')
    )
	RAISERROR(N'!!!! ERROR !!!! See message(s) below', 10, @ERROR_STATE) WITH NOWAIT;
	RAISERROR(@ERROR_MESSAGE, 10, @ERROR_STATE) WITH NOWAIT;
END CATCH


-- Database
BEGIN TRY
   INSERT INTO @output(type, name, data)
    SELECT 'database', 'sys.databases', (
        SELECT * FROM sys.databases
        WHERE name = DB_NAME()
        FOR XML PATH('')
    )
END TRY
BEGIN CATCH
    SELECT @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE(), @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE();
    INSERT INTO @output(type, name, data)
    SELECT 'database', 'sys.databases', (
        SELECT [@ERROR_NUMBER] = @ERROR_NUMBER
            , [@ERROR_SEVERITY] = @ERROR_SEVERITY
            , [@ERROR_STATE] = @ERROR_STATE
            , [@ERROR_MESSAGE] = @ERROR_MESSAGE
        FOR XML PATH('error')
    )
	RAISERROR(N'!!!! ERROR !!!! See message(s) below', 10, @ERROR_STATE) WITH NOWAIT;
	RAISERROR(@ERROR_MESSAGE, 10, @ERROR_STATE) WITH NOWAIT;
END CATCH

-- Database files
/*BEGIN TRY
   INSERT INTO @output(type, name, data)
    SELECT 'database', 'sys.database_files', (
        SELECT [@id] = file_id, [@type] = type, [@type_desc] = type_desc
            , [@state] = state, [@state_desc] = state_desc, [@is_read_only] = is_read_only, [@is_sparse] = is_sparse, [@is_percent_growth] = is_percent_growth
            , [@file-data_space_id] = data_space_id, [@filename] = name, [@physical_name] = physical_name
            , [@size] = size, [@size-mb] = CAST(size as bigint) * 8 / 1024, [@size-max] = max_size
            , [@growth] = growth, [@growth-mb] = CAST(growth as bigint) * 8 / 1024
        FROM sys.database_files
        FOR XML PATH('file')
    )
END TRY
BEGIN CATCH
    INSERT INTO @output(type, name, data)
    SELECT 'database', 'sys.database_files', (
        SELECT [@ERROR_NUMBER] = ERROR_NUMBER()
            , [@ERROR_SEVERITY] = ERROR_SEVERITY()
            , [@ERROR_STATE] = ERROR_STATE()
            , [@ERROR_MESSAGE] = ERROR_MESSAGE()
        FOR XML PATH('error')
    )
END CATCH*/

-- Database files
BEGIN TRY
   INSERT INTO @output(type, name, data)
    SELECT 'database', 'sys.database_files', (
        SELECT [@file_id] = file_id, [@type] = dbf.type, [@type_desc] = dbf.type_desc
            , [@state] = state, [@state_desc] = state_desc, [@is_read_only] = is_read_only, [@is_sparse] = is_sparse, [@is_percent_growth] = is_percent_growth
            , [@filegroup_ame] = dts.name, [@data_space_id] = dbf.data_space_id, [@filename] = dbf.name, [@physical_name] = physical_name
            , [@size] = size, [@size-mb] = CONVERT(decimal(10, 2), size / 128.0)
            , [@sizemax] = dbf.max_size, [@sizemax-mb] = CASE WHEN dbf.max_size = 268435456 OR dbf.max_size = -1 THEN -1 ELSE CONVERT(decimal(10, 2), dbf.max_size/128.0 ) END
            , [@growth] = growth, [@growth-mb] = CONVERT(decimal(10, 2), growth / 128.0)
            , [@spaceused] = FILEPROPERTY(dbf.name, 'SpaceUsed'), [@spaceused-mb] = CAST(CAST(FILEPROPERTY(dbf.name, 'SpaceUsed') AS int)/128.0 AS decimal(15,2)) 
            , [@spacefree] = dbf.size - FILEPROPERTY(dbf.name, 'SpaceUsed'), [@spacefree-mb] = CONVERT(decimal(10, 2), dbf.size/128.0 - CAST(FILEPROPERTY(dbf.name, 'SpaceUsed') AS int)/128.0 )
        FROM sys.database_files dbf
        LEFT OUTER JOIN sys.data_spaces AS dts ON dbf.data_space_id = dts.data_space_id
        FOR XML PATH('file')--, ELEMENTS XSINIL
    )
END TRY
BEGIN CATCH
    SELECT @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE(), @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE();
    INSERT INTO @output(type, name, data)
    SELECT 'database', 'sys.database_files', (
        SELECT [@ERROR_NUMBER] = @ERROR_NUMBER
            , [@ERROR_SEVERITY] = @ERROR_SEVERITY
            , [@ERROR_STATE] = @ERROR_STATE
            , [@ERROR_MESSAGE] = @ERROR_MESSAGE
        FOR XML PATH('error')
    )
	RAISERROR(N'!!!! ERROR !!!! See message(s) below', 10, @ERROR_STATE) WITH NOWAIT;
	RAISERROR(@ERROR_MESSAGE, 10, @ERROR_STATE) WITH NOWAIT;
END CATCH

-- disk usage
BEGIN TRY
;    WITH sysfiles AS (
        SELECT [dbsize] = SUM(CONVERT(bigint, CASE WHEN status & 64 = 0 THEN size ELSE 0 END))
            , [logsize] = SUM(CONVERT(bigint, CASE WHEN status & 64 != 0 THEN size ELSE 0 END))
        FROM dbo.sysfiles
    ), partitions AS (
        SELECT [reservedpages] = SUM(alu.total_pages)
            , [usedpages] = SUM(alu.used_pages)
            , [pages] = SUM(
                CASE WHEN itt.internal_type IN (202, 204) THEN 0
                WHEN alu.type <> 1 THEN alu.used_pages
                WHEN prt.index_id < 2 THEN alu.data_pages
                ELSE 0 END
            )
        FROM sys.partitions prt
        JOIN sys.allocation_units alu on prt.partition_id = alu.container_id
        LEFT JOIN sys.internal_tables itt on prt.object_id = itt.object_id
    )
    INSERT INTO @output(type, name, data)
    SELECT 'database', 'sys.partitions', (
        SELECT 
            [@database_size_mb] = CONVERT(decimal(20, 2), CAST(dbsize + logsize AS bigint) * 8 / 1024.0)
            , [@log_size_mb] = CONVERT(decimal(20, 2), logsize * 8 / 1024.0)
            , [@data_size_mb] = CONVERT(decimal (20, 2), CASE WHEN [dbsize] >= [reservedpages] THEN dbsize ELSE [reservedpages] END * 8 / 1024.0 )
            , [@reserved_space_mb] = CONVERT(decimal(20, 2), reservedpages * 8 / 1024.0)
            , [@unallocated_space_mb] = CONVERT(DECIMAL(20, 2), CASE WHEN [dbsize] >= [reservedpages] THEN (dbsize - reservedpages) * 8 / 1024.0 ELSE 0 END)
            , [@data_mb] = CONVERT(decimal(20, 2), pages * 8 / 1024.0)
            , [@index_mb] = CONVERT(decimal(20, 2), (usedpages - pages) * 8 / 1024.0)
            , [@unused_mb] = CONVERT(decimal(20, 2), (reservedpages - usedpages) * 8 / 1024.0)
        FROM sysfiles
        CROSS JOIN partitions
        FOR XML PATH('sys.partitions')
    )
END TRY
BEGIN CATCH
    SELECT @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE(), @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE();
    INSERT INTO @output(type, name, data)
    SELECT 'disk', 'sys.partitions', (
        SELECT [@ERROR_NUMBER] = @ERROR_NUMBER
            , [@ERROR_SEVERITY] = @ERROR_SEVERITY
            , [@ERROR_STATE] = @ERROR_STATE
            , [@ERROR_MESSAGE] = @ERROR_MESSAGE
        FOR XML PATH('error')
    )
	RAISERROR(N'!!!! ERROR !!!! See message(s) below', 10, @ERROR_STATE) WITH NOWAIT;
	RAISERROR(@ERROR_MESSAGE, 10, @ERROR_STATE) WITH NOWAIT;
END CATCH        
;

-- backup
BEGIN TRY
    SET @stmt = N'
    USE msdb;
    SET @xml = NULL;
    SELECT @xml = (
        SELECT [@Type] = typ.name
            , [@AverageBackupDurationMin] = AVG(DATEDIFF(second, backup_start_date, backup_finish_date))/60.0
            , [@BackupsCount] = COUNT(*)
            , [@BackupFirst] = MIN(bks.backup_start_date)
            , [@BackupLast] = MAX(bks.backup_start_date)
        FROM dbo.backupset bks
        INNER JOIN sys.databases dbs ON dbs.name = bks.database_name
        INNER JOIN (VALUES(N''D'', N''Database''), (N''I'', N''Differential''), (N''L'', N''Log'')) AS typ(type, name) ON typ. type = bks.type
        WHERE dbs.name = @database
        GROUP BY typ.name
        FOR XML PATH(''stats'')
    )
    ';
    SET @params = N'@database sysname, @xml xml OUTPUT';

    EXEC sp_executesql @stmt = @stmt, @params = @params, @database = @database, @xml = @xml OUTPUT

    INSERT INTO @output(type, name, data)
    SELECT 'backup', 'sys.backupset', @xml;

END TRY
BEGIN CATCH
    SELECT @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE(), @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE();
    INSERT INTO @output(type, name, data)
    SELECT 'backup', 'sys.backupset', (
        SELECT [@ERROR_NUMBER] = @ERROR_NUMBER
            , [@ERROR_SEVERITY] = @ERROR_SEVERITY
            , [@ERROR_STATE] = @ERROR_STATE
            , [@ERROR_MESSAGE] = @ERROR_MESSAGE
        FOR XML PATH('error')
    )
	RAISERROR(N'!!!! ERROR !!!! See message(s) below', 10, @ERROR_STATE) WITH NOWAIT;
	RAISERROR(@ERROR_MESSAGE, 10, @ERROR_STATE) WITH NOWAIT;
END CATCH        

BEGIN TRY
    SET @stmt = N'
    USE msdb;
    SET @xml = NULL;
    SELECT @xml = (
        SELECT DISTINCT [@backup_set_id] = bks.backup_set_id
            , [@backup_start_date] = bks.backup_start_date
            , [@duration_min] = CONVERT(decimal(10, 2), DATEDIFF(second, bks.backup_start_date, bks.backup_finish_date) / 60.0)
            , [@type] = typ.name
            , [@backup_size_mb] = bks.backup_size / 1024.0 / 1024.0 
            , [@backup_name] = bks.name
            , [@device_type] = bmf.device_type
            , [@user_name] = bks.user_name
            , [@recovery_model] = bks.recovery_model  
        FROM sys.databases dbs 
        INNER JOIN backupset bks ON bks.database_name = dbs.name
        INNER JOIN (VALUES(N''D'', N''Database''), (N''I'', N''Differential''), (N''L'', N''Log'')) AS typ(type, name) ON typ.type = bks.type
        LEFT JOIN backupmediaset bms on bks.media_set_id = bms.media_set_id
        LEFT JOIN backupmediafamily bmf on bmf.media_set_id = bms.media_set_id
        WHERE dbs.name = @Database
        ORDER BY bks.backup_start_date desc, bks.backup_set_id  
        FOR XML PATH(''backupset'')
    )
    ';
    SET @params = N'@database sysname, @xml xml OUTPUT';

    EXEC sp_executesql @stmt = @stmt, @params = @params, @database = @database, @xml = @xml OUTPUT

    INSERT INTO @output(type, name, data)
    SELECT 'backup', 'sys.backupset', @xml;

END TRY
BEGIN CATCH
    SELECT @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE(), @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE();
    INSERT INTO @output(type, name, data)
    SELECT 'backup', 'sys.backupset', (
        SELECT [@ERROR_NUMBER] = @ERROR_NUMBER
            , [@ERROR_SEVERITY] = @ERROR_SEVERITY
            , [@ERROR_STATE] = @ERROR_STATE
            , [@ERROR_MESSAGE] = @ERROR_MESSAGE
        FOR XML PATH('error')
    )
	RAISERROR(N'!!!! ERROR !!!! See message(s) below', 10, @ERROR_STATE) WITH NOWAIT;
	RAISERROR(@ERROR_MESSAGE, 10, @ERROR_STATE) WITH NOWAIT;
END CATCH        

SELECT [report/@name] = name, [report] = [data]
FROM @output out
LEFT JOIN (VALUES(0, 'database'), (1, 'system'), (2, 'disk'), (3, 'tables'), (4, 'backup')) AS tps(id, type) ON out.[type] = tps.[type]
ORDER BY tps.id
FOR XML PATH(''), root('output')

