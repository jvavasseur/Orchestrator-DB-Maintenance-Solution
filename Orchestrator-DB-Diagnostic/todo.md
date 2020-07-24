
<diagnostics>
	<advices>
		<advice />
	</advices>
	<reports>	
	<report name="sys.partitions" date="2020-02-18T15:48:30.9028525">
		<sys.partitions database_size_mb="20560.00" log_size_mb="560.00" data_size_mb="20000.00" reserved_space_mb="1919.61" unallocated_space_mb="18080.39" data_mb="1209.20" index_mb="226.44" unused_mb="483.97" />
	</report>
	<report name="sys.databases" date="2020-02-18T15:48:30.8987199">
		<name>

server
	sys.dm_os_sys_info
database
	sys.partitions
	sys.databases
	sys.database_files
	sys.configurations

diskusage-by-tables
tembp
	tempdb
backups
	sys.backupset
alwayson
vlf


A voir
EXEC sp_server_diagnostics; 

DBCC LOGINFO()
SELECT * FROM sys.dm_db_log_info(db_id())
https://docs.microsoft.com/en-us/sql/t-sql/functions/serverproperty-transact-sql?view=sql-server-ver15

use UiPath_Production
go
/*

SELECT
	Table_Name = OBJECT_NAME(idx.OBJECT_ID)
	, Index_Name = idx.name
	, Index_Type = stt.index_type_desc
	, stt.index_depth, stt. index_level
	, stt.avg_fragmentation_in_percent, stt.fragment_count, stt.page_count
FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, NULL) stt 
INNER JOIN sys.indexes idx  ON idx.object_id = stt.object_id AND idx.index_id = stt.index_id 
ORDER BY stt.avg_fragmentation_in_percent DESC

SELECT Table_Name = OBJECT_NAME(idx.OBJECT_ID) 
	   , Index_Name = idx.name
	   , Index_Type = idx.type_desc 
	   , IndexSizeKB = SUM(prt.used_page_count) * 8 
	   , NumOfSeeks = usg.user_seeks
	   , NumOfScans = usg.user_scans
	   , NumOfLookups = usg.user_lookups
	   , NumOfUpdates = usg.user_updates
	   , LastSeek = usg.last_user_seek
	   , LastScan = usg.last_user_scan
	   , LastLookup = usg.last_user_lookup 
	   , LastUpdate = usg.last_user_update
FROM sys.indexes idx
INNER JOIN sys.dm_db_index_usage_stats usg ON usg.index_id = idx.index_id AND usg.object_id = idx.object_id
INNER JOIN sys.dm_db_partition_stats prt ON prt.object_id=idx.object_id
WHERE OBJECTPROPERTY(idx.OBJECT_ID,'IsUserTable') = 1
GROUP BY OBJECT_NAME(idx.object_id) ,idx.name ,idx.type_desc ,usg.user_seeks ,usg.user_scans ,usg.user_lookups,usg.user_updates ,usg.last_user_seek ,usg.last_user_scan ,usg.last_user_lookup ,usg.last_user_update
*/

SELECT Table_Name = OBJECT_NAME(ops.OBJECT_ID)   
       , Index_Name = idx.name  
	   , Index_Type = idx.type_desc 
	   , IndexSizeKB = SUM(prt.used_page_count) * 8 
       , #OfInserts = ops.leaf_insert_count 
       , #updates = ops.leaf_update_count 
       , #fDeletes = ops.leaf_delete_count 	   
FROM sys.dm_db_index_operational_stats(NULL, NULL, NULL, NULL ) ops 
INNER JOIN sys.indexes AS idx ON idx.object_id = ops.object_id AND idx.index_id = ops.index_id 
INNER JOIN sys.dm_db_partition_stats prt on prt.object_id = idx.object_id
WHERE  OBJECTPROPERTY(idx.object_id, 'IsUserTable') = 1
GROUP BY OBJECT_NAME(ops.object_id), idx.name, idx.type_desc, ops.leaf_insert_count, ops.leaf_update_count, ops.leaf_delete_count
ORDER BY ops.leaf_insert_count + ops.leaf_update_count + ops.leaf_delete_count DESC

---------------------------------------------------------------------------
block
https://www.arbinada.com/en/node/1647
https://blog.sqlauthority.com/2017/01/09/sql-server-get-historical-deadlock-information-system-health-extended-events/
https://www.sqlservercentral.com/blogs/extracting-deadlock-information-using-system_health-extended-events
SELECT *
	, (
		SELECT TOP(1) x.n.value('@dbid', 'int')
		FROM event_data.nodes('//deadlock/resource-list/objectlock') x(n)
	)
FROM (
	SELECT xe.object_name
		, event_data = CAST(xe.event_data as xml).query('/event/data/value/child::*')
		, xe.file_name
		, xe.file_offset
		, xe.timestamp_utc
	FROM (
        SELECT CAST(target_data AS XML).value('(EventFileTarget/File/@name)[1]', 'nvarchar(max)') targetpath
        FROM sys.dm_xe_session_targets st
        INNER JOIN sys.dm_xe_sessions s ON s.address = st.event_session_address
        WHERE s.NAME = 'system_health' AND st.target_name = 'event_file'
	) target
	CROSS APPLY sys.fn_xe_file_target_read_file(LEFT(targetpath, LEN(targetpath) - CHARINDEX(N'_', REVERSE(targetpath))) + N'*.xel', NULL, NULL, NULL) xe
	WHERE xe.object_name LIKE '%dead%'
		

) as x
WHERE EXISTS(
	SELECT 1
	FROM event_data.nodes('//deadlock/resource-list/objectlock') x(n)
--	WHERE x.n.value('@dbid', 'int') = 27
)


SELECT XEvent.query('(event/data/value/deadlock)[1]') AS DeadlockGraph
FROM (
    SELECT XEvent.query('.') AS XEvent
    FROM (
        SELECT CAST(target_data AS XML) AS TargetData
        FROM sys.dm_xe_session_targets st
        INNER JOIN sys.dm_xe_sessions s ON s.address = st.event_session_address
        WHERE s.NAME = 'system_health'
            AND st.target_name = 'ring_buffer'
        ) AS Data
CROSS APPLY TargetData.nodes('RingBufferTarget/event[@name="xml_deadlock_report"]') AS XEventData(XEvent)
) AS source;

/*
SELECT
db.name DBName,
tl.request_session_id,
wt.blocking_session_id,
OBJECT_NAME(p.OBJECT_ID) BlockedObjectName,
tl.resource_type,
h1.TEXT AS RequestingText,
h2.TEXT AS BlockingTest,
tl.request_mode
FROM sys.dm_tran_locks AS tl
INNER JOIN sys.databases db ON db.database_id = tl.resource_database_id
INNER JOIN sys.dm_os_waiting_tasks AS wt ON tl.lock_owner_address = wt.resource_address
INNER JOIN sys.partitions AS p ON p.hobt_id = tl.resource_associated_entity_id
INNER JOIN sys.dm_exec_connections ec1 ON ec1.session_id = tl.request_session_id
INNER JOIN sys.dm_exec_connections ec2 ON ec2.session_id = wt.blocking_session_id
CROSS APPLY sys.dm_exec_sql_text(ec1.most_recent_sql_handle) AS h1
CROSS APPLY sys.dm_exec_sql_text(ec2.most_recent_sql_handle) AS h2

--The sys.dm_exec_requests DMV provides details on all of the processes running in SQL Server. With the WHERE condition listed below, only blocked processes will be returned.
SELECT * 
FROM sys.dm_exec_requests
WHERE blocking_session_id <> 0;

--The sys.dm_os_waiting_tasks DMV returns information about the tasks that are waiting on resources. To view the data, users should have SQL Server System Administrator or VIEW SERVER STATE permissions on the instance.
SELECT session_id, wait_duration_ms, wait_type, blocking_session_id 
FROM sys.dm_os_waiting_tasks 
WHERE blocking_session_id <> 0

SELECT * FROM sys.dm_os_wait_stats


SELECT BlockedSessionID = blocked.session_id
	, BlockingSessionID = blocked.blocking_session_id
	, BlockedText = blockedtext.text
	, BlockingText = blockingtext.text
FROM sys.dm_exec_requests blocked
LEFT JOIN sys.dm_exec_requests blocking ON blocked.blocking_session_id = blocking.session_id
CROSS APPLY sys.dm_exec_sql_text(blocked.sql_handle) blockedtext
CROSS APPLY sys.dm_exec_sql_text(blocking.sql_handle) blockingtext
WHERE blocked.session_id > 50;
*/

WITH T1 AS (
	SELECT blocking.session_id AS blocking_session_id 
		, blocked.session_id AS blocked_session_id 
		, waitstats.wait_type AS blocking_resource 
		, waitstats.wait_duration_ms
		, waitstats.resource_description 
		, DB_NAME(tl.resource_database_id) AS DatabaseName
		, blockedtext.text AS blocked_text 
		, blockingtext.text AS blocking_text
	FROM sys.dm_exec_connections AS blocking
	INNER JOIN sys.dm_exec_requests blocked ON blocking.session_id = blocked.blocking_session_id
	CROSS APPLY sys.dm_exec_sql_text(blocked.sql_handle) blockedtext
	CROSS APPLY sys.dm_exec_sql_text(blocking.most_recent_sql_handle) blockingtext
	INNER JOIN sys.dm_os_waiting_tasks waitstats ON waitstats.session_id = blocked.session_id
	INNER JOIN sys.dm_tran_locks tl ON tl.lock_owner_address = waitstats.resource_address
	WHERE waitstats.wait_duration_ms >= 2000
), T2A AS (
	SELECT blocking_session_id, resource_description
		, CHARINDEX('hobtid', resource_description) AS StartPos
		, SUBSTRING(resource_description, CHARINDEX('hobtid', resource_description), LEN(resource_description)) AS StartText
	FROM T1
), T2B AS (
	SELECT blocking_session_id, resource_description, StartPos, StartText, SUBSTRING(StartText, 0, CHARINDEX(' ', StartText)) AS hotbid_text
	FROM T2A
), T2C AS (
	SELECT blocking_session_id, resource_description, CAST(SUBSTRING(hotbid_text, CHARINDEX('=', hotbid_text)+1, LEN(hotbid_text)) AS BIGINT) AS hobt_id
	FROM T2B
), T2D AS (
	SELECT T2C.blocking_session_id, T2C.resource_description, OBJECT_SCHEMA_NAME(object_id) AS schema_name, object_name(object_id) as object_name, object_id, partition_id, index_id, partition_number, p.hobt_id, rows
	FROM sys.partitions p INNER JOIN T2C ON p.hobt_id = T2C.hobt_id
)
SELECT GETDATE() AS DateTimeCaptured, T1.blocking_session_id, T1.blocked_session_id, T1.blocking_resource, T1.wait_duration_ms, T1.resource_description
	, T1.DatabaseName, T1.blocking_text, T1.blocked_text
	, T2D.schema_name + '.' + T2D.object_name AS blocked_object_name, T2D.object_id AS blocked_object_id, T2D.index_id as blocked_index_id, i.name AS blocked_index_name
	, i.type_desc AS blocked_index_type
FROM T1 INNER JOIN T2D ON T1.blocking_session_id = T2D.blocking_session_id AND T1.resource_description = T2D.resource_description
INNER JOIN sys.indexes i ON i.object_id = T2D.object_id AND i.index_id = T2D.index_id


https://www.sqlshack.com/resolve-and-troubleshoot-sql-blocking-chain-with-root-session/


--  
version
https://docs.microsoft.com/en-us/sql/t-sql/functions/serverproperty-transact-sql?view=sql-server-ver15
https://database.guide/quick-script-that-returns-all-properties-from-serverproperty-in-sql-server-2017-2019/
SELECT Edition = SERVERPROPERTY('Edition'), EngineEdition = SERVERPROPERTY('EngineEdition'), ProductBuild = SERVERPROPERTY('ProductBuild'), ProductLevel = SERVERPROPERTY('ProductLevel'), ProductVersion = SERVERPROPERTY('ProductVersion')

-- cpu
https://newspark.nl/did-you-check-your-sql-database-server-cpu-core-usage-today/
SELECT scheduler_id,cpu_id, status, is_online, *
FROM sys.dm_os_schedulers

perf
https://www.brentozar.com/blitz/tempdb-data-files/
ALTER DATABASE [tempdb] ADD FILE (NAME = N'tempdev2', FILENAME = N'T:\MSSQL\DATA\tempdev2.ndf' , SIZE = 8GB , FILEGROWTH = 0);

https://www.sqlshack.com/how-to-collect-performance-and-system-information-in-sql-server/

---- perf monitor
https://www.red-gate.com/simple-talk/sql/database-administration/great-sql-server-debates-buffer-cache-hit-ratio/
https://www.itprotoday.com/sql-server/performance-counters

-- max worker thread
https://docs.microsoft.com/en-us/sql/database-engine/configure-windows/configure-the-max-worker-threads-server-configuration-option?view=sql-server-ver15
