DROP PROCEDURE [usp_generate_view_script];

CREATE PROCEDURE [usp_generate_view_script]
(
    @SourceSchema SYSNAME,
    @TargetSchema SYSNAME,
	@LikePattern NVARCHAR(255) = '%'
)

-- Objective : generate a view for all tables in a source schema (refining possible using @LikePattern Parameter) in a target schema
-- The procedure must be create and used in the database where the source tables reside

-- Usage EXEC [usp_generate_view_script] @SourceSchema='dbo', @TargetSchema='views', @LikePattern='Dim';

AS
BEGIN
    DECLARE @Columns VARCHAR(MAX) = '';
    DECLARE @Schema SYSNAME;
    DECLARE @Table SYSNAME;
    DECLARE @TableID INT;
    DECLARE @SQLSELECT NVARCHAR(MAX);
    DECLARE @SQLCommand NVARCHAR(MAX);

    --CURSOR
    DECLARE curTables CURSOR LOCAL FOR
    SELECT SCHEMA_NAME(schema_id) TABLE_SCHEMA,
           name TABLE_NAME,
           object_id
    FROM sys.tables
    WHERE TYPE = 'U'
          AND SCHEMA_NAME(schema_id) = @SourceSchema
		  AND name LIKE '%'+@LikePattern+'%'
    FOR READ ONLY;

    OPEN curTables;
    FETCH NEXT FROM curTables
    INTO @Schema,
         @Table,
         @TableID;

    WHILE (@@fetch_status <> -1)
    BEGIN

        IF (@@fetch_status <> -2)
        BEGIN
            SET @Columns = '';

            SELECT @Columns = @Columns + COALESCE(QUOTENAME(NAME) + ',', '')
            FROM sys.columns
            WHERE object_id = @TableID
            ORDER BY column_id;

            SET @Columns = SUBSTRING(@Columns, 1, LEN(@Columns) - 1);

            SET @SQLSELECT = N'SELECT ' + @Columns + N' FROM ' + QUOTENAME(@Schema) + N'.' + QUOTENAME(@Table) + N';';

            --PRINT @Sql

            PRINT @SQLCommand;

            BEGIN TRY

                SET @SQLCommand = N'DROP VIEW IF EXISTS ' + QUOTENAME(@TargetSchema) + N'.' + QUOTENAME(@Table) + N';';
                EXEC sp_executesql @statement = @SQLCommand;
                SET @SQLCommand = N'CREATE VIEW ' + QUOTENAME(@TargetSchema) + N'.' + QUOTENAME(@Table) + N' AS ' + @SQLSELECT;
                PRINT @SQLCommand;
                EXEC sp_executesql @statement = @SQLCommand;

            END TRY
            BEGIN CATCH

                DECLARE @ErrorMessage NVARCHAR(4000);
                DECLARE @ErrorSeverity INT;
                DECLARE @ErrorState INT;

                SELECT @ErrorMessage = ERROR_MESSAGE(),
                       @ErrorSeverity = ERROR_SEVERITY(),
                       @ErrorState = ERROR_STATE();

                RAISERROR(   @ErrorMessage,  -- Message text.  
                             @ErrorSeverity, -- Severity.  
                             @ErrorState     -- State.  
                         );

            END CATCH;

        END;

        FETCH NEXT FROM curTables
        INTO @Schema,
             @Table,
             @TableID;

    END;

    DEALLOCATE curTables;

END;