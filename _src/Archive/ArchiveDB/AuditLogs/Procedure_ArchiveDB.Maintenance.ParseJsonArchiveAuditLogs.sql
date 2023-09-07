SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET NOCOUNT ON;
GO


----------------------------------------------------------------------------------------------------
-- DROP PROCEDURE [Maintenance].[ParseJsonArchiveAuditLogs]
----------------------------------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Maintenance].[ParseJsonArchiveAuditLogs]') AND type in (N'P'))
BEGIN
        EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [Maintenance].[ParseJsonArchiveAuditLogs] AS'
    PRINT '  + CREATE PROCEDURE: [Maintenance].[ParseJsonArchiveAuditLogs]';
END
ELSE PRINT '  = PROCEDURE [Maintenance].[ParseJsonArchiveAuditLogs] already exists' 
GO

PRINT '  ~ UPDATE PROCEDURE: [Maintenance].[ParseJsonArchiveAuditLogs]'
GO

ALTER PROCEDURE [Maintenance].[ParseJsonArchiveAuditLogs]
----------------------------------------------------------------------------------------------------
-- ### [Object]: PROCEDURE [Maintenance].[ParseJsonArchiveAuditLogs]
-- ### [Version]: 2020-10-01 00:00:00                                                         
-- ### [Source]: ??????
-- ### [Hash]: ??????
-- ### [Docs]: https://???.???
-- !!! ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!!!
-- !!! ~~~~~~~~~ NOT OFFICIALLY SUPPORTED BY UIPATH 
-- !!! ~~~~~~~~~ SQL Server >= 2016 SP1
----------------------------------------------------------------------------------------------------
	@Filters nvarchar(MAX)
    , @Settings nvarchar(MAX) OUTPUT
    , @Messages nvarchar(MAX) OUTPUT
	, @IsValid bit = 0 OUTPUT
    , @AfterHours int = NULL
    , @DeleteDelayHhours int = NULL
AS
BEGIN
    BEGIN TRY
        SET ARITHABORT ON;
        SET NOCOUNT ON;
        SET NUMERIC_ROUNDABORT OFF;

        ----------------------------------------------------------------------------------------------------
        -- Local Run Variables
        ----------------------------------------------------------------------------------------------------
        DECLARE @json nvarchar(MAX);

        -- JSON elements local tables
        DECLARE @jsons TABLE ([key] int, [value] nvarchar(MAX), [type] int, [type_name] nvarchar(10));
        DECLARE @jsonArray_elements TABLE([key] int, [name] nvarchar(MAX), [value] nvarchar(MAX), [type] int, [type_name] nvarchar(10));
        DECLARE @jsonArray_tenants TABLE([key] int, [value_name] nvarchar(100), [value_id] int, [Tenant_Name] nvarchar(128), [Tenant_Id] int, [keep] bit, [exclude] nvarchar(128), [IsDeleted] bit);
        DECLARE @jsonValues_tenants TABLE([key] int, [value_name] nvarchar(128), [value_id] int)--, [Tenant_Name] nvarchar(128), [TenantId] int);
        DECLARE @jsonValues_exclude TABLE([key] int, [value_name] nvarchar(128), [value_id] int)--, [Tenant_Name] nvarchar(128), [TenantId] int);
        DECLARE @json_errors TABLE([id] tinyint NOT NULL, [key] int NOT NULL, [message] nvarchar(MAX) NOT NULL);

        DECLARE @elements_settings TABLE([key] int, [delete_only] bit, [after_hours] int, [delete_delay_hours] int,  [disabled] bit);

        ----------------------------------------------------------------------------------------------------
        -- Constant / Default value
        ----------------------------------------------------------------------------------------------------
        DECLARE @json_types TABLE(id tinyint, [name] nvarchar(10));
        INSERT INTO @json_types(id, [name]) VALUES (0, 'null'), (1, 'string'), (2, 'number'), (3, 'true/false'), (4, 'array'), (5, 'object');

        ----------------------------------------------------------------------------------------------------      
        -- Message / Error Handling
        ----------------------------------------------------------------------------------------------------
        DECLARE @message nvarchar(MAX);
--        DECLARE @messages TABLE(id int IDENTITY(0, 1) PRIMARY KEY, [date] datetime2 DEFAULT SYSDATETIME(), [procedure] nvarchar(MAX) NOT NULL DEFAULT QUOTENAME(COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?')) + N'.' + QUOTENAME(COALESCE(OBJECT_NAME(@@PROCID), N'?')), [message] nvarchar(MAX) NOT NULL, severity tinyint NOT NULL, state tinyint NOT NULL, [number] int, [line] int);
        DECLARE @ERROR_NUMBER INT, @ERROR_SEVERITY INT, @ERROR_STATE INT, @ERROR_PROCEDURE NVARCHAR(126), @ERROR_LINE INT, @ERROR_MESSAGE NVARCHAR(2048) ;

		----------------------------------------------------------------------------------------------------
        -- Check JSON Errors
        ----------------------------------------------------------------------------------------------------
        SET @IsValid = 0;
		SET @json = LTRIM(RTRIM(REPLACE(REPLACE(@Filters, CHAR(10), N''), CHAR(13), N'')));

		SELECT @message = CASE WHEN ISNULL(@json, N'') = N'' THEN 'ERROR: Parameter @Filters is NULL or empty'
			WHEN ISJSON(@json) = 0 THEN 'ERROR: Parameter @Filters is not a valid json string'
			WHEN LEFT(@json, 1) = N'{' THEN 'ERROR: Parameter @Filters is a {} object literal'
			WHEN LEFT(@json, 1) <> N'[' THEN 'ERROR: Parameter @Filters is invalid'
			ELSE NULL END;

		IF @message IS NULL
		BEGIN
            BEGIN TRY
                INSERT INTO @jsons([key], [value], [type], [type_name])
                SELECT [key] + 1, [value], [type], tps.[name]
                FROM OPENJSON(@json) jsn
                INNER JOIN @json_types tps ON tps.id = jsn.[type]

    			IF @@ROWCOUNT = 0 SET @message = N'ERROR: Parameter @Filters array contains no {} object elements';
            END TRY
            BEGIN CATCH
                SET @message = ERROR_MESSAGE();
            END CATCH
        END

		IF @message IS NOT NULL
		BEGIN
            RAISERROR(N'a valid JSON string with a [] array of {} object literal(s) is expected', 16, 1)
		END

        SET @message = NULL;

        ----------------------------------------------------------------------------------------------------
        -- Parse and extract from JSON string
        ----------------------------------------------------------------------------------------------------
        -- get each element from each object in main array
        BEGIN TRY;
            INSERT INTO @jsonArray_elements([key], [name], [value], [type], [type_name])
                SELECT jsn.[key], name = CASE WHEN LTRIM(RTRIM(elm.[key])) IN (N'disable', N'disabled') THEN N'disabled' ELSE LTRIM(RTRIM(elm.[key])) END, LTRIM(RTRIM(elm.[value])), elm.[type], tps.[name]
                FROM @jsons jsn
                CROSS APPLY OPENJSON(jsn.[value]) elm
                INNER JOIN @json_types tps ON tps.id = elm.[type]
                WHERE jsn.[type] = 5 AND elm.[key] NOT IN (N'comment', N'comments');
        END TRY
        BEGIN CATCH;
            SET @message = N'ERROR[R1]: error(s) occured while retrieving elements from JSON string';
            THROW;
        END CATCH
        
        -- get defaut settigns for each objects in main aray
        BEGIN TRY;
            INSERT INTO @elements_settings([key], [delete_only], [after_hours], [delete_delay_hours], [disabled])
            SELECT jsn.[key]
                , [delete_only] = CASE WHEN ( SELECT COUNT(*) FROM @jsonArray_elements WHERE [key] = jsn.[key] AND [name] = N'delete_only' ) > 1 THEN 0 ELSE
                    ISNULL( ( SELECT MIN(ISNULL(IIF( ([type] = 3 AND [value] = N'true') OR ([type] = 2 AND [value] >= 1) , 1, 0), 0)) FROM @jsonArray_elements WHERE [key] = jsn.[key] AND [name] = N'delete_only' AND [type] IN (2, 3, 4) ), 0)
                    END
                , [after_hours] = (SELECT MAX([value]) FROM @jsonArray_elements WHERE [key] = jsn.[key] AND [name] = N'after_hours' AND [type] = 2 )
                , [delete_delay_hours] = (SELECT MAX([value]) FROM @jsonArray_elements WHERE [key] = jsn.[key] AND [name] = N'delete_delay_hours' AND [type] = 2 )
                , [disabled] = CASE WHEN ( SELECT COUNT(*) FROM @jsonArray_elements WHERE [key] = jsn.[key] AND [name] = N'disabled' ) > 1 THEN 0 ELSE
                    ISNULL( ( SELECT MIN(ISNULL(IIF( ([type] = 3 AND [value] = N'true') OR ([type] = 2 AND [value] >= 1) , 1, 0), 0)) FROM @jsonArray_elements WHERE [key] = jsn.[key] AND [name] = N'disabled' AND [type] IN (2, 3, 4) ), 0)
                    END
            FROM @jsons jsn
            WHERE jsn.[type] = 5
            ORDER BY [key];
        END TRY 
        BEGIN CATCH;
            SET @message = N'ERROR[R2]: error(s) occured while retrieving elements'' settings from JSON string';
            THROW;
        END CATCH;

        ----------------------------------------------------------------------------------------------------
        -- Parse and extract Tenants
        ----------------------------------------------------------------------------------------------------
        -- get all names and aliases from tenants objets 
        BEGIN TRY;
            INSERT INTO @jsonValues_tenants([key], [value_name], [value_id])
            SELECT DISTINCT elm.[key]
                , [value_name] =  LEFT(LTRIM(RTRIM( REPLACE( IIF(val.[type] = 1, val.[value], IIF(elm.[type] = 1, elm.[value], NULL) ), N'*', N'%') )), 128)
                , [value_id] =  CAST( IIF(val.[type] = 2, val.[value], IIF(elm.[type] = 2, elm.[value], NULL) ) AS int)
            FROM @jsonArray_elements elm
            INNER JOIN @elements_settings stg ON stg.[key] = elm.[key]
            OUTER APPLY OPENJSON( IIF(elm.[type] = 4, elm.[value], NULL) ) val
            WHERE elm.[name] IN (N'tenants') AND ( ( elm.[type] IN (1, 2) ) OR (elm.[type] = 4 AND val.[type] IN (1, 2) ) ) AND stg.[disabled] = 0;
        END TRY 
        BEGIN CATCH;
            SET @message = N'ERROR[R6]: error(s) occured while retrieving "tenants" name(s) and alias(es) from JSON string';
            THROW;
        END CATCH;

        -- get all names and aliases from exclude objets 
        BEGIN TRY
            INSERT INTO @jsonValues_exclude([key], [value_name], [value_id])
            SELECT DISTINCT elm.[key]
                , [value_name] =  LEFT(LTRIM(RTRIM( REPLACE( IIF(val.[type] = 1, val.[value], IIF(elm.[type] = 1, elm.[value], NULL) ), N'*', N'%') )), 128)
                , [value_id] =  CAST( IIF(val.[type] = 2, val.[value], IIF(elm.[type] = 2, elm.[value], NULL) ) AS int)
            FROM @jsonArray_elements elm
            INNER JOIN @elements_settings stg ON stg.[key] = elm.[key]
            OUTER APPLY OPENJSON( IIF(elm.[type] = 4, elm.[value], NULL) ) val
            WHERE elm.[name] IN (N'exclude') AND ( ( elm.[type] IN (1, 2) ) OR (elm.[type] = 4 AND val.[type] IN (1, 2) ) ) AND stg.[disabled] = 0;
        END TRY 
        BEGIN CATCH;
            SET @message = N'ERROR[R6]: error(s) occured while retrieving "tenants" name(s) and alias(es) from JSON string';
            THROW;
        END CATCH;

        -- extract tenant from JSON string and matches them with [dbo].[tenants]
        BEGIN TRY;
            WITH list AS (
                -- exact matches
                SELECT [type] = 'exact', jst.[key], jst.[value_name], jst.[value_id], tnt.[Name], tnt.Id
                    , [keep] = IIF(tnt.Id IS NOT NULL, 1, 0)
                    , [exclude] = NULL
                    , IsDeleted = tnt.IsDeleted
                FROM @jsonValues_tenants jst
                LEFT JOIN [dbo].[Tenants] tnt ON tnt.Id = jst.[value_id] OR tnt.[Name] LIKE jst.[value_name]
                WHERE ( jst.[value_id] IS NOT NULL OR ( CHARINDEX(N'%', jst.[value_name], 1) = 0 AND jst.[value_name] NOT IN (N'#ACTIVE_TENANTS#', N'#OTHER_TENANTS#', N'#DELETED_TENANTS#') ) )
                UNION ALL
                -- filter matches
                SELECT [type] = N'partial', jst.[key], jst.[value_name], jst.[value_id], tnt.[Name], tnt.Id
                    , [keep] = IIF(NOT EXISTS(SELECT TOP(1) 1 FROM @jsonValues_exclude WHERE [key] = jst.[key] AND (tnt.[Name] LIKE [value_name] OR tnt.[Id] = [value_id]) ) AND tnt.IsDeleted = 0, 1, 0)
                    , [exclude] = (SELECT TOP(1) ISNULL([value_name], [value_id]) FROM @jsonValues_exclude WHERE [key] = jst.[key] AND (tnt.[Name] LIKE [value_name] OR tnt.[Id] = [value_id]) )
                    , IsDeleted = tnt.IsDeleted
                FROM @jsonValues_tenants jst
                LEFT JOIN [dbo].[Tenants] tnt ON tnt.Id = jst.[value_id] OR (tnt.[Name] LIKE jst.[value_name])
                WHERE jst.[value_id] IS NULL AND CHARINDEX(N'%', jst.[value_name], 1) > 0
                UNION ALL 
                -- alias
                SELECT [type] = IIF(jst.[value_name] = N'#ACTIVE_TENANTS#', N'active', N'deleted'), jst.[key], jst.[value_name], jst.[value_id], tnt.[Name], tnt.Id
                    , [keep] = IIF(NOT EXISTS(SELECT TOP(1) 1 FROM @jsonValues_exclude WHERE [key] = jst.[key] AND (tnt.[Name] LIKE [value_name] OR tnt.[Id] = [value_id])), 1, 0)
                    , [exclude] = (SELECT TOP(1) ISNULL([value_name], [value_id]) FROM @jsonValues_exclude WHERE [key] = jst.[key] AND (tnt.[Name] LIKE [value_name] OR tnt.[Id] = [value_id]) )
                    , IsDeleted = tnt.IsDeleted
                FROM @jsonValues_tenants jst
                INNER JOIN (VALUES(N'#ACTIVE_TENANTS#', 0, N'active'), (N'#DELETED_TENANTS#', 1, N'deleted') ) sts([name], [status], [type]) ON jst.[value_name] = sts.name
                LEFT JOIN [dbo].[Tenants] tnt ON tnt.IsDeleted = sts.[status]
                    AND jst.[value_name] = sts.[name]
                    AND NOT EXISTS(SELECT 1 FROM @jsonValues_tenants WHERE [key] <> jst.[key] AND value_name = jst.[value_name])
            )
            INSERT INTO @jsonArray_tenants([key], [value_name], [value_id], [Tenant_Name], [Tenant_Id], [keep], [exclude], [IsDeleted])
            SELECT lst.[key], lst.[value_name], lst.[value_id], lst.[Name], lst.Id, [keep], [exclude], [IsDeleted] FROM list lst
            UNION ALL
            -- others
            SELECT jst.[key], jst.[value_name], jst.[value_id], tnt.[Name], tnt.[Id]
                , [keep] = IIF(NOT EXISTS(SELECT TOP(1) 1 FROM @jsonValues_exclude WHERE [key] = jst.[key] AND (tnt.[Name] LIKE [value_name] OR tnt.[Id] = [value_id])), 1, 0)
                , [exclude] = (SELECT TOP(1) ISNULL([value_name], [value_id]) FROM @jsonValues_exclude WHERE [key] = jst.[key] AND (tnt.[Name] LIKE [value_name] OR tnt.[Id] = [value_id]) )
                , 0
            FROM @jsonValues_tenants jst
            CROSS APPLY (
                SELECT [name], [Id] FROM [dbo].[Tenants] WHERE [IsDeleted] = 0 AND NOT EXISTS( SELECT  1 FROM @jsonValues_tenants WHERE [key] <> jst.[key] AND value_name = N'#OTHER_TENANTS#') 
                UNION ALL 
                SELECT NULL, NULL WHERE EXISTS( SELECT  1 FROM @jsonValues_tenants WHERE [key] <> jst.[key] AND value_name = N'#OTHER_TENANTS#') 
                EXCEPT 
                SELECT [name], [Id] FROM list WHERE [Name] IS NOT NULL AND [keep] = 1
            ) tnt
            WHERE jst.[value_name] = N'#OTHER_TENANTS#';
        END TRY
        BEGIN CATCH;
            SET @message = N'ERROR[R7]: error(s) occured while matching Tenants table with "tenants" name(s) and alias(es) from JSON string';
            THROW;
        END CATCH;

        ----------------------------------------------------------------------------------------------------
        -- JSON string - keys and types checks
        ----------------------------------------------------------------------------------------------------
        -- 0 J0 array contains invalid type (not {}) 
        BEGIN TRY;
            INSERT INTO @json_errors([id], [key], [message])
            SELECT [id] = 0, [key] = jsn.[key], [message] = N'ERROR[J0]: array #' + CAST(jsn.[key] AS nvarchar(100)) + N' => invalid type "' + jsn.[type_name] COLLATE DATABASE_DEFAULT + N'" (only {} object literal expected)'
            FROM @jsons jsn 
            WHERE jsn.[type] <> 5;
        END TRY
        BEGIN CATCH;
            SET @message = N'ERROR[J0]: error(s) occured while checking invalid type(s) in main array in JSON string';
            THROW;
        END CATCH;

        BEGIN TRY;
            -- 1 - J1: array contains invalid key (not 'tenants', N'delete_only', N'after_hours', N'delete_delay_hours') 
            INSERT INTO @json_errors([id], [key], [message])
            SELECT DISTINCT [id] = 1, [key] = elm.[key], [message] = N'ERROR[J1]: array #' + CAST(elm.[key] AS nvarchar(10)) + N' => invalid key "' + elm.[name] COLLATE DATABASE_DEFAULT + N'"'
            FROM @jsonArray_elements elm 
            INNER JOIN @elements_settings sts ON sts.[key] = elm.[key]
            WHERE sts.[disabled] = 0 AND elm.[name] NOT IN (N'tenants', N'exclude', N'delete_only', N'after_hours', N'delete_delay_hours', N'disabled', N'comment', N'comments');
        END TRY
        BEGIN CATCH;
            SET @message = N'ERROR[J1]: error(s) occured while checking invalid key(s) in JSON string';
            THROW;
        END CATCH;

        BEGIN TRY;
            -- 2 - J2: missing key(s) in object ('tenants')
            INSERT INTO @json_errors([id], [key], [message])
            SELECT DISTINCT [id] = 2, [key] = jsn.[key], N'ERROR[J2]: array #' + CAST(jsn.[key] AS nvarchar(10)) + N' => missing key "' + v.[name]
            FROM @jsons jsn 
            INNER JOIN @elements_settings sts ON sts.[key] = jsn.[key]
            CROSS JOIN (VALUES(N'tenants')) v([name]) 
            WHERE sts.[disabled] = 0 AND NOT EXISTS(SELECT 1 FROM @jsonArray_elements WHERE [key] = jsn.[key] AND [name] = v.[name]);
        END TRY
        BEGIN CATCH;
            SET @message = N'ERROR[J2]: error(s) occured while checking invalid key(s) in JSON string';
            THROW;
        END CATCH;

        BEGIN TRY;
            -- 3 - J3: invalid type for key (tenants => number, string or array ; delete_only => true/false or 0/1 ; others => number)
            INSERT INTO @json_errors([id], [key], [message])
            SELECT DISTINCT [id] = 3, [key] = elm.[key], [message] = N'ERROR[J3]: array #' + CAST(elm.[key] AS nvarchar(10)) + N' => invalid "' + elm.[type_name] + '" type for key "' + elm.[name] + N'" (only ' +
                CASE WHEN elm.[name] IN (N'tenants', N'exclude') THEN N'number, string or array' 
                WHEN elm.[name] = N'delete_only' THEN N'true/false or 0/1' 
                WHEN elm.[name] = N'disabled' THEN N'true/false or 0/1' 
                ELSE N'number' END + N' expected)'
            FROM @elements_settings sts 
            INNER JOIN @jsonArray_elements elm ON elm.[key] = sts.[key] AND sts.[disabled] = 0
            WHERE (elm.[name] = N'tenants' AND elm.[type] NOT IN (1, 2, 4)) 
                OR (elm.[name] = N'delete_only' AND elm.[type] NOT IN (2, 3))
                OR (elm.[name] IN (N'after_hours', N'delete_delay_hours') AND elm.[type] <> 2)
                OR (elm.[name] = N'disabled' AND elm.[type] NOT IN (2, 3));
        END TRY
        BEGIN CATCH;
            SET @message = N'ERROR[J3]: error(s) occured while checking key(s) with invalid type(s) in JSON string';
            THROW;
        END CATCH;

        -- 4 - J4: duplicate elements
        BEGIN TRY
            INSERT INTO @json_errors([id], [key], [message])
            SELECT [id] = 4, [key] = elm.[key], [message] = N'ERROR[J4]: array #' + CAST(elm.[key] AS nvarchar(10)) + N' => duplicate "' + elm.[name] + '" found ' + CAST(COUNT(*) AS nvarchar(10)) + N' times (only 1 expected)'
            FROM @elements_settings sts 
            INNER JOIN @jsonArray_elements elm ON elm.[key] = sts.[key] AND sts.[disabled] = 0
            GROUP BY elm.[key], elm.[name]
            HAVING COUNT(*) > 1;
        END TRY
        BEGIN CATCH;
            SET @message = N'ERROR[J4]: error(s) occured while checking duplicate elements in JSON string';
            THROW;
        END CATCH;

        -- 5 - J5: empty tenants
        BEGIN TRY
            INSERT INTO @json_errors([id], [key], [message])
            SELECT DISTINCT [id] = 5, [key] =elm.[key], [message] = N'ERROR[J5]: array #' + CAST(elm.[key] AS nvarchar(10)) + N' => "' + elm.[name] + '" array is empty (' +  CASE WHEN elm.[name] IN (N'tenants', N'exclude') THEN N'number(s) or string(s)' ELSE N'archive or delete object(s)' END + N' expected)'
            FROM @elements_settings sts 
            INNER JOIN @jsonArray_elements elm ON elm.[key] = sts.[key] AND sts.[disabled] = 0
            OUTER APPLY OPENJSON(elm.[value]) val
            WHERE elm.[name] IN (N'tenants', N'exclude') AND elm.[type] = 4 AND val.[key] IS NULL;
        END TRY
        BEGIN CATCH;
            SET @message = N'ERROR[J5]: error(s) occured while checking empty array(s) ("tenants") in JSON string';
            THROW;
        END CATCH;

        ----------------------------------------------------------------------------------------------------
        -- JSON string - Tenants checks 
        ----------------------------------------------------------------------------------------------------
        -- 6 - T1: invalid type in tenants array (only number or string)
        BEGIN TRY;
            INSERT INTO @json_errors([id], [key], [message])
            SELECT DISTINCT [id] = 6, [key] = elm.[key], [message] = N'ERROR[T0]: array #' + CAST(elm.[key] AS nvarchar(10)) + N' => invalid "' + tps.[name] COLLATE DATABASE_DEFAULT + '" type in "' + elm.[name] + '" array (only ' + CASE WHEN elm.[name] IN (N'tenants', N'exclude') THEN N'number(s) or string(s)' ELSE N'object(s)' END + N' expected)'
            FROM @elements_settings sts 
            INNER JOIN @jsonArray_elements elm ON elm.[key] = sts.[key] AND sts.[disabled] = 0
            CROSS APPLY OPENJSON(elm.[value]) val
            INNER JOIN @json_types tps ON tps.id = val.[type]
            WHERE elm.[type] = 4 AND ( ( elm.[name] IN (N'tenants', N'exclude') AND val.[type] NOT IN (1, 2) ) );
        END TRY
        BEGIN CATCH;
            SET @message = N'ERROR[T0]: error(s) occured while checking invalid type(s) in "tenants" array(s) in JSON string';
            THROW;
        END CATCH;

        -- 7 - T2: missing tenants
        BEGIN TRY;
            INSERT INTO @json_errors([id], [key], [message])
            SELECT DISTINCT [id] = 7, [key] = tnt.[key], [message] = N'ERROR[T1]: array #' + CAST(tnt.[key] AS nvarchar(10)) + N' => tenant "'+ tnt.[value_name] + '" doesn''t exists'
            FROM @jsonArray_tenants tnt
            WHERE tnt.[Tenant_Id] IS NULL AND tnt.[value_name] NOT IN (N'#ACTIVE_TENANTS#', N'#OTHER_TENANTS#', N'#DELETED_TENANTS#');
        END TRY
        BEGIN CATCH;
            SET @message = N'ERROR[T1]: error(s) occured while checking existing "tenants" in JSON string';
            THROW;
        END CATCH;

        -- 8 - T3: duplicate keyword (#ACTIVE_TENANTS#, #OTHER_TENANTS#, #DELETED_TENANTS#)
        BEGIN TRY;
            INSERT INTO @json_errors([id], [key], [message])
            SELECT [id] = 8, [key] = tnt1.[key], [message] = N'ERROR[T2]: array #' + CAST(tnt1.[key] AS nvarchar(10)) + N' => duplicate keyword "'+ tnt1.[value_name] + N' found in element ' + CAST(tnt2.[key] AS nvarchar(10)) + N' (can be used only once)'
            FROM @jsonArray_tenants tnt1
            INNER JOIN @jsonArray_tenants tnt2 ON tnt1.[value_name] = tnt2.[value_name] AND tnt1.[key] < tnt2.[key] AND NOT EXISTS (SELECT 1 FROM @jsonArray_tenants WHERE [value_name] = tnt2.[value_name] AND [key] < tnt1.[key])
            WHERE tnt1.[Tenant_Id] IS NULL AND tnt1.[value_name] IN (N'#ACTIVE_TENANTS#', N'#OTHER_TENANTS#', N'#DELETED_TENANTS#');
        END TRY
        BEGIN CATCH;
            SET @message = N'ERROR[T2]: error(s) occured while checking duplicate keyword(s) in "tenants" in JSON string';
            THROW;
        END CATCH;

        -- 9 - T4: duplicate tenants
        BEGIN TRY;
            INSERT INTO @json_errors([id], [key], [message])
            SELECT [id] = 9, [key] = tnt1.[key], [message] = N'ERROR[T3]: array #' + CAST(tnt1.[key] AS nvarchar(10)) + N' => duplicate tenant "'+ tnt1.[Tenant_Name] + N'" (id=' + CAST(tnt1.[Tenant_Id] AS nvarchar(10)) + N', value=' + ISNULL(N'"'+ tnt1.[value_name] + N'"', tnt1.[value_id]) + N') found in #' + CAST(tnt2.[key] AS nvarchar(10)) + N' (value=' + ISNULL(N'"'+ tnt2.[value_name] + N'"', tnt2.[value_id]) + N')'
            FROM @jsonArray_tenants tnt1
            INNER JOIN @jsonArray_tenants tnt2 ON tnt1.Tenant_Name = tnt2.[Tenant_Name] AND tnt1.[key] < tnt2.[key] AND NOT EXISTS (SELECT 1 FROM @jsonArray_tenants WHERE [Tenant_Name] = tnt2.[Tenant_Name] AND [key] < tnt1.[key] AND [keep] = 1)
            WHERE tnt1.[keep] = 1 AND tnt2.[keep] = 1;
        END TRY
        BEGIN CATCH;
            SET @message = N'ERROR[T3]: error(s) occured while checking duplicate tenants in JSON string';
            THROW;
        END CATCH;

        ----------------------------------------------------------------------------------------------------
        -- Missing After Hours checks
        ----------------------------------------------------------------------------------------------------
        BEGIN TRY;
            INSERT INTO @json_errors([id], [key], [message])
            SELECT [id] = 18, [key] = els.[key], [message] = N'ERROR[H0]: array #' + CAST(els.[key] AS nvarchar(10)) + N' => @AfterHours default value is not provided and "after_hours" value missing in both element objects'
            FROM @elements_settings els 
            WHERE els.[disabled] = 0 AND @AfterHours IS NULL AND els.[after_hours] IS NULL;
        END TRY 
        BEGIN CATCH
            SET @message = N'ERROR[H0]: errors occured while checking [after_hours] parameter(s)';
            THROW;
        END CATCH

        ----------------------------------------------------------------------------------------------------
        -- Missing Delete Delay checks
        ----------------------------------------------------------------------------------------------------
        BEGIN TRY;
            INSERT INTO @json_errors([id], [key], [message])
            SELECT [id] = 19, [key] = els.[key], [message] = N'ERROR[H1]: array #' + CAST(els.[key] AS nvarchar(10)) + N' => @DeleteDelayHours default value is not provided and "delete_delay_hours" value missing in both element objects'
            FROM @elements_settings els 
            WHERE els.[disabled] = 0 AND @DeleteDelayHhours IS NULL AND els.[delete_delay_hours] IS NULL;
        END TRY 
        BEGIN CATCH
            SET @message = N'ERROR[H1]: errors occured while checking [after_hours] parameter(s)';
            THROW;
        END CATCH

        IF NOT EXISTS(SELECT 1 FROM @json_errors) 
        BEGIN 
            SET @IsValid = 1;

            SELECT @Settings = (
                SELECT DISTINCT [t] = tnt.[Tenant_Id]
                    , [o] = elm.[delete_only] 
                    , [h] = COALESCE(elm.[after_hours], @AfterHours)
                    , [d] = COALESCE(elm.[delete_delay_hours], @DeleteDelayHhours, 0)
                FROM @elements_settings elm
                INNER JOIN @jsonArray_tenants tnt ON tnt.[key] = elm.[key]
                WHERE elm.[disabled] = 0 AND tnt.[keep] = 1
                ORDER BY  [t]
                FOR JSON PATH, INCLUDE_NULL_VALUES
            )
        END
    END TRY
    BEGIN CATCH
        SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE(), @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();
    END CATCH

    --IF @@TRANCOUNT > 0 ROLLBACK;
    --SET @Messages = ( SELECT * FROM (SELECT TOP(100) [message], [severity] = 10, [state] = 1 FROM @json_errors ORDER BY [key] ASC, [id] ASC) x FOR XML RAW('message'), TYPE );
    SET @Messages = ( 
        SELECT [Procedure] = QUOTENAME(COALESCE(OBJECT_SCHEMA_NAME(@@PROCID), N'?')) + N'.' + QUOTENAME(COALESCE(OBJECT_NAME(@@PROCID), N'?')), [Message], [Severity], [State] 
        FROM (
            SELECT [Message], [Severity], [State] FROM (
                SELECT TOP(100) [Message] = LEFT([message], 4000), [Severity] = 10, [State] = 1 FROM @json_errors ORDER BY [key] ASC, [id] ASC
            ) err
            UNION ALL SELECT N'ERROR: ' + @ERROR_MESSAGE, 10, 1 WHERE @ERROR_MESSAGE IS NOT NULL
            UNION ALL SELECT @message, 10, 1 WHERE @message IS NOT NULL
        ) jsn
        FOR JSON PATH);
    RETURN 0
END
GO

declare @Filters nvarchar(MAX)
declare @Settings nvarchar(MAX) 
declare @Messages nvarchar(MAX) 
declare @IsValid bit = 0 
declare @AfterHours int = 4
declare @DeleteDelayHhours int = 5

SELECT @Filters = N'[     { "tenants": [" tenant_B1"], "after_hours": 11, "delete_delay_hours": 12, "disabled": 0 } 
,  { "tenants": [" tenant_B2"], "delete_only": 1, "after_hours": 11, "disabled": 0 } 
] '

EXEC [Maintenance].[ParseJsonArchiveAuditLogs]
	@Filters = @Filters
    , @Settings = @Settings OUTPUT
    , @Messages = @Messages OUTPUT
	, @IsValid = @IsValid OUTPUT
    , @AfterHours = @AfterHours 
    , @DeleteDelayHhours = @DeleteDelayHhours

select @Settings, @Messages, @IsValid