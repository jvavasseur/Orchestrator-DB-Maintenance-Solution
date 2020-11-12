SET NOCOUNT ON;
GO

-- Create an empty procedure if it doesn'texist yet...
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Diagnostic].[GetReportServer]') AND type in (N'P'))
BEGIN
	EXEC('CREATE PROCEDURE [Diagnostic].[GetReportServer] AS SELECT 1')
END

PRINT N' ~ ALTER PROCEDURE [Diagnostic].[GetReportServer]'
GO

----------------------------------------------------------------------------------------------------
-- 
-- 
----------------------------------------------------------------------------------------------------
--DROP PROCEDURE [Diagnostic].[GetReportServer]
ALTER PROCEDURE [Diagnostic].[GetReportServer]
    @advices bit = 0
	, @throwerror bit = 1
AS
BEGIN
    SET NOCOUNT ON;
    SET ARITHABORT ON;
    SET NUMERIC_ROUNDABORT OFF;

	BEGIN TRY
		DECLARE @msg nvarchar(2048), @ERROR_NUMBER int, @ERROR_MESSAGE nvarchar(4000), @ERROR_STATE int, @ERROR_SEVERITY int, @ERROR_LINE int, @ERROR_PROCEDURE sysname;
		DECLARE @stmt nvarchar(max) = N'', @params nvarchar(max);
		----------------------------------------------------------------------------------------------------
		DECLARE @output TABLE([type] sysname, [name] sysname, [report] xml, [error] xml, [date] datetime2 DEFAULT SYSDATETIME());
		DECLARE @type sysname, @name sysname;
		DECLARE @xml xml, @database sysname = DB_NAME();
		----------------------------------------------------------------------------------------------------

			SELECT @type = N'system', @name = N'sys.configurations';
			INSERT INTO @output([type], [name], [report])
			SELECT @type, @name, (
				SELECT TOP 5 [@id] = configuration_id,
					[@name] = name,
					[@value_in_use] = value_in_use
				FROM sys.configurations
				FOR XML PATH('sys.configurations')
			);
			SELECT @type = N'test', @name = N'sys.configurations';
			INSERT INTO @output([type], [name], [report])
			SELECT @type, @name, (
				SELECT TOP 5 [@id] = configuration_id,
					[@name] = name,
					[@value_in_use] = value_in_use
				FROM sys.configurations
				FOR XML PATH('sys.configurations')
			);

		BEGIN TRY
			SELECT @type = N'system', @name = N'sys.xxx';

			INSERT INTO @output([type], [name], [report])
			SELECT @type, @name, (
				SELECT TOP 5 [@id] = configuration_id,
					[@name] = name,
					[@value_in_use] = value_in_use
					, 1/ cast(value_in_use as int)
				FROM sys.configurations
				FOR XML PATH('sys.configurations')
			);
		END TRY
		BEGIN CATCH
			SELECT @ERROR_NUMBER = ERROR_NUMBER(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE()
				, @ERROR_PROCEDURE = ERROR_PROCEDURE(), @ERROR_LINE = ERROR_LINE(), @ERROR_MESSAGE = ERROR_MESSAGE();

    		INSERT INTO @output([type], [name], [error])
    		SELECT @type, @name, CAST((
				SELECT --[@type] = @type, [@name] = @name
					[message/@ERROR_NUMBER] = @ERROR_NUMBER
					, [message/@ERROR_SEVERITY] = @ERROR_SEVERITY
					, [message/@ERROR_STATE] = @ERROR_STATE
					, [message/@ERROR_PROCEDURE] = @ERROR_PROCEDURE
					, [message/@ERROR_LINE] = @ERROR_LINE
					, [message] = @ERROR_MESSAGE
				FOR XML PATH('')
		    ) AS xml);

			IF @throwerror = 1 
			BEGIN;
				THROW;
			END ELSE BEGIN
				RAISERROR(N'-- ERROR ----------------------------------------------------------------------------------------------------', 10, @ERROR_STATE) WITH NOWAIT;
				SET @msg = N'Msg %d, Level %d, State %d, Procedure %s, Line %d';
				RAISERROR(@msg, 10, @ERROR_STATE, @ERROR_NUMBER, @ERROR_SEVERITY, @ERROR_STATE, @ERROR_PROCEDURE, @ERROR_LINE) WITH NOWAIT;
				RAISERROR(@ERROR_MESSAGE, 10, @ERROR_STATE) WITH NOWAIT;
				RAISERROR(N'--------------------------------------------------------------------------------------------------------------', 10, @ERROR_STATE) WITH NOWAIT;
			END
		END CATCH

INSERT INTO @output SELECT * FROM @output WHERE error is not NULL
select * from @output;
update @output set error = null

		SELECT @xml = (
			SELECT 
				CAST((
					SELECT [error/@name] = [name]
							, [error/@type] = [type]
							, [error/@date] = [date]
							, [error] 
					FROM @output 
					WHERE error IS NOT NULL 
					FOR XML PATH(''), ROOT('errors')
				) AS xml)
				, CAST((
					SELECT [reports/@type] = COALESCE([type], 'Unknown')
						, [reports] = CAST((
							SELECT 
								[report/@name] = [name]
								, [report/@date] = [date]
								, [report] = [report] 
							FROM @output out
							WHERE out.[type] = data.[type]
							FOR XML PATH('')
						) AS xml)
					FROM (SELECT DISTINCT [type] = out.[type] FROM @output out) AS data
					ORDER BY [type]
					FOR XML PATH('')--, root('diagnostic')
				) AS xml)
			FOR XML PATH(''), root('diagnostic')
		)

		SELECT [diagnostic] = @xml;

	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0 ROLLBACK TRAN;
		THROW;
	END CATCH

END
GO


	EXEC [Diagnostic].[GetReportServer] @throwerror = 0
/*	EXEC [Diagnostic].[GetReportServer] @throwerror = 1

*/
SELECT * FROM sys.dm_db_log_info(db_id())
