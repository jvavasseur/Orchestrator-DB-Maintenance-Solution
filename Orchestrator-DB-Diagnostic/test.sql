USE UiPath_Production
GO

DECLARE @ERROR_NUMBER int, @ERROR_MESSAGE nvarchar(4000), @ERROR_STATE int, @ERROR_SEVERITY int, @ERROR_LINE int;
DECLARE @output TABLE([type] sysname, [name] sysname, [report] xml, [date] datetime2 DEFAULT SYSDATETIME())
DECLARE @stmt nvarchar(max) = N'', @params nvarchar(max), @xml xml, @database sysname = DB_NAME();

SET @database = DB_NAME();

INSERT INTO @output([type], [name], [report])
SELECT 'system', 'sys.configurations', (
    SELECT TOP 5 [@id] = configuration_id,
        [@name] = name,
        [@value_in_use] = value_in_use
    FROM sys.configurations
    FOR XML PATH('sys.configurations')
)

-- test xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
INSERT INTO @output([type], [name], [report])
SELECT 'system', 'sys.servers', (
    SELECT TOP 5 [@id] = s.server_id,
        [@name] = name,
        [@value_in_use] = s.product
    FROM sys.servers s
    FOR XML PATH('sys.servers')
)

   INSERT INTO @output([type], [name], [report])
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

-- test xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
   INSERT INTO @output([type], [name], [report])
    SELECT 'database', 'sys.databases', (
        SELECT top 5 [@file_id] = dbs.[database_id], [@type] = dbs.two_digit_year_cutoff, [@name] = dbs.name
        FROM sys.databases dbs
        FOR XML PATH('db')--, ELEMENTS XSINIL
    )

-- test xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
   INSERT INTO @output([type], [name], [report])
    SELECT 'xxx', 'tables', (
        SELECT top 5 [@file_id] = dbs.object_id, [@type] = dbs.type_desc, [@name] = dbs.name
        FROM sys.tables dbs
        FOR XML PATH('db')--, ELEMENTS XSINIL
    )
-- test xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
   INSERT INTO @output([type], [name], [report])
    SELECT 'xxx', 'col', (
        SELECT top 5 [@file_id] = dbs.object_id, [@type] = dbs.user_type_id, [@name] = dbs.name
        FROM sys.columns dbs
        FOR XML PATH('db')--, ELEMENTS XSINIL
    )

-- test xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
   INSERT INTO @output([type], [name], [report])
    SELECT 'xxx3', 'col', (
        SELECT top 5 [@file_id] = dbs.object_id, [@type] = dbs.user_type_id, [@name] = dbs.name
        FROM sys.columns dbs
        FOR XML PATH('db')--, ELEMENTS XSINIL
    )

SELECT * FROM @output

SELECT @xml = (
    SELECT [reports/@type] = COALESCE([type], 'Unknown')
        , [reports/@date] = null
        , [reports] = CAST((
            SELECT [report/@name] = [name]
                , [report/@date] = [date]
                , [report] = [report] 
            FROM @output out
            WHERE out.[type] = data.[type]
            FOR XML PATH('')
        ) AS xml)
    FROM (
        SELECT DISTINCT [type] = out.[type], [id] = tps.[id]
        FROM @output out
        LEFT JOIN (VALUES(0, 'database'), (1, 'system'), (2, 'disk'), (3, 'tables'), (4, 'backup')) AS tps(id, type) ON out.[type] = tps.[type]
    ) AS data
    ORDER BY CASE WHEN id IS NOT NULL THEN 0 ELSE 1 END, id, [type]
    FOR XML PATH(''), root('diagnostic')
)

SELECT @xml
--DECLARE @xdoc int;
--EXEC sp_xml_preparedocument @xdoc OUTPUT, @xml;


SELECT x.n.value('../@type', 'nvarchar(50)') 
    , x.n.value('@name', 'sysname')
--    , x.n.value('', 'nvarchar(50)')
    , x.n.query('*')
    , x.n.value('@date', 'datetime2')
FROM @xml.nodes('//diagnostic/reports/report') x(n)


SELECT [report/@name] = name
	, [report/@date] = [date]
	, [report] = [report]
FROM @output out
LEFT JOIN (VALUES(0, 'database'), (1, 'system'), (2, 'disk'), (3, 'tables'), (4, 'backup')) AS tps(id, type) ON out.[type] = tps.[type]
ORDER BY tps.id
FOR XML PATH(''), root('output')
