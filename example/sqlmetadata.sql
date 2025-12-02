/* =========[ 1) Per-tabel base_query genereren ]========= */
WITH BaseQueries AS
(
    SELECT
        sm.schema_name,
        sm.obj_name,
        b.[Bron],
        base_query = CAST(
        'SELECT ' +
        STRING_AGG(
            CONVERT(nvarchar(max),
                CASE WHEN sm.ordinal_position > 1 THEN ', ' ELSE '' END +
                CASE
                    -- Exact numeric
                    WHEN sm.data_type IN ('decimal','numeric') THEN
                        N'CAST(' + QUOTENAME(sm.column_name) + N' AS decimal('
                        + CAST(sm.numeric_precision AS varchar(10)) + N',' 
                        + CAST(sm.numeric_scale AS varchar(10)) + N')) AS ' 
                        + QUOTENAME(dbo.MakeSafeIdentifier(sm.column_name))

                    WHEN sm.data_type = 'money' THEN
                        N'CAST(' + QUOTENAME(sm.column_name) + N' AS decimal(19,4)) AS '
                        + QUOTENAME(dbo.MakeSafeIdentifier(sm.column_name))

                    WHEN sm.data_type = 'smallmoney' THEN
                        N'CAST(' + QUOTENAME(sm.column_name) + N' AS decimal(10,4)) AS '
                        + QUOTENAME(dbo.MakeSafeIdentifier(sm.column_name))

                    WHEN sm.data_type = 'tinyint' THEN
                        -- Spark/Delta 'byte' is signed; cast veilig naar smallint
                        N'CAST(' + QUOTENAME(sm.column_name) + N' AS smallint) AS '
                        + QUOTENAME(dbo.MakeSafeIdentifier(sm.column_name))

                    WHEN sm.data_type IN ('smallint','int','bigint','bit','float','real') THEN
                        N'CAST(' + QUOTENAME(sm.column_name) + N' AS ' + sm.data_type + N') AS '
                        + QUOTENAME(dbo.MakeSafeIdentifier(sm.column_name))

                    -- Date/time
                    WHEN sm.data_type = 'date' THEN
                        N'CAST(' + QUOTENAME(sm.column_name) + N' AS date) AS '
                        + QUOTENAME(dbo.MakeSafeIdentifier(sm.column_name))

                    WHEN sm.data_type = 'datetime' THEN
                        -- nette mapping naar µs-wereld
                        N'CAST(' + QUOTENAME(sm.column_name) + N' AS datetime2(3)) AS '
                        + QUOTENAME(dbo.MakeSafeIdentifier(sm.column_name))

                    WHEN sm.data_type = 'smalldatetime' THEN
                        N'CAST(' + QUOTENAME(sm.column_name) + N' AS datetime2(0)) AS '
                        + QUOTENAME(dbo.MakeSafeIdentifier(sm.column_name))

                    WHEN sm.data_type = 'datetime2' THEN
                        N'CAST(' + QUOTENAME(sm.column_name) + N' AS datetime2(6)) AS '
                        + QUOTENAME(dbo.MakeSafeIdentifier(sm.column_name))

                    WHEN sm.data_type = 'time' THEN
                        -- Delta kent geen time-only; sla op als 'HH:mm:ss'
                        N'CONVERT(varchar(8), ' + QUOTENAME(sm.column_name) + N', 108) AS '
                        + QUOTENAME(dbo.MakeSafeIdentifier(sm.column_name))
                        -- Als je fracties wilt: vervang bovenstaande door HH:mm:ss.fffffff-projectie

                    WHEN sm.data_type = 'datetimeoffset' THEN
                        -- normaliseer naar UTC timestamp
                        N'CAST(SWITCHOFFSET(' + QUOTENAME(sm.column_name) + N', ''+00:00'') AS datetime2(6)) AS '
                        + QUOTENAME(dbo.MakeSafeIdentifier(sm.column_name))

                    -- Character types (direct doorgeven)
                    WHEN sm.data_type IN ('char','varchar','nchar','nvarchar') THEN
                        QUOTENAME(sm.column_name) + N' AS ' + QUOTENAME(dbo.MakeSafeIdentifier(sm.column_name))

                    -- Legacy text
                    WHEN sm.data_type = 'text' THEN
                        N'CONVERT(varchar(max), ' + QUOTENAME(sm.column_name) + N') AS '
                        + QUOTENAME(dbo.MakeSafeIdentifier(sm.column_name))

                    WHEN sm.data_type = 'ntext' THEN
                        N'CONVERT(nvarchar(max), ' + QUOTENAME(sm.column_name) + N') AS '
                        + QUOTENAME(dbo.MakeSafeIdentifier(sm.column_name))

                    -- Binary types
                    WHEN sm.data_type IN ('binary','varbinary') THEN
                        QUOTENAME(sm.column_name) + N' AS ' + QUOTENAME(dbo.MakeSafeIdentifier(sm.column_name))

                    WHEN sm.data_type = 'image' THEN
                        N'CONVERT(varbinary(max), ' + QUOTENAME(sm.column_name) + N') AS '
                        + QUOTENAME(dbo.MakeSafeIdentifier(sm.column_name))

                    WHEN sm.data_type IN ('rowversion','timestamp') THEN
                        N'CAST(' + QUOTENAME(sm.column_name) + N' AS varbinary(8)) AS '
                        + QUOTENAME(dbo.MakeSafeIdentifier(sm.column_name))

                    -- Other specials
                    WHEN sm.data_type = 'uniqueidentifier' THEN
                        N'CONVERT(varchar(36), ' + QUOTENAME(sm.column_name) + N') AS '
                        + QUOTENAME(dbo.MakeSafeIdentifier(sm.column_name))

                    WHEN sm.data_type = 'xml' THEN
                        N'CONVERT(nvarchar(max), ' + QUOTENAME(sm.column_name) + N') AS '
                        + QUOTENAME(dbo.MakeSafeIdentifier(sm.column_name))

                    WHEN sm.data_type = 'hierarchyid' THEN
                        QUOTENAME(sm.column_name) + N'.ToString() AS ' + QUOTENAME(dbo.MakeSafeIdentifier(sm.column_name))

                    WHEN sm.data_type IN ('geometry','geography') THEN
                        QUOTENAME(sm.column_name) + N'.STAsBinary() AS ' + QUOTENAME(dbo.MakeSafeIdentifier(sm.column_name))

                    WHEN sm.data_type = 'sql_variant' THEN
                        N'CONVERT(nvarchar(max), ' + QUOTENAME(sm.column_name) + N') AS '
                        + QUOTENAME(dbo.MakeSafeIdentifier(sm.column_name))

                    -- Default: geef kolom direct door
                    ELSE
                        QUOTENAME(sm.column_name) + N' AS ' + QUOTENAME(dbo.MakeSafeIdentifier(sm.column_name))
                END
                )
                ,N''
        ) WITHIN GROUP (ORDER BY sm.ordinal_position)
        + ' FROM ' + QUOTENAME(sm.schema_name) + '.' + QUOTENAME(sm.obj_name)
        AS nvarchar(max))
    FROM dbo.sqlmetadata sm
    JOIN (
        select [Bron], [Server], [Database]
        from (
	    values
		    ('ccs_level', 'vmdwhidpweu01', 'InsuranceData_CCS_DWH'), -- 06:00
		    ('anva_meeus', 'vmdwhidpweu01\MEEUS', 'InsuranceData_ANVA_DWH'), -- 20:30
		    ('vizier', 'viz-sql01-mi-p.1d57ac4f4d63.database.windows.net', 'CRM_DWH'), -- 06:00
		    ('ods_reports', 'vmdwhodsanvpweu', 'OG_ODS_Reports'), -- 08:00
		    ('anva_concern', 'vmdwhidpweu01', 'InsuranceData_ANVA_DWH'), -- 21:00
		    ('insurance_data_im', 'vmdwhidpweu01', 'InsuranceData_OpGroen_DWH') -- 07:00
        ) as a([Bron], [Server], [Database])
    ) b on sm.server_name = b.[Server] and sm.[db_name] = b.[Database]
    WHERE 1=1
    --AND b.Bron = 'ccs_level'
    --AND sm.obj_name IN ('Relaties', 'Contactpersonen') --snp
    --AND b.Bron = 'vizier'
    --AND sm.obj_name = 'Retenties' --snp
    --AND b.Bron = 'anva_concern'
    --AND sm.obj_name = 'Dim_Schade' --snp
    --AND sm.obj_name = 'Fact_PremieFacturen' --wnd
    GROUP BY sm.schema_name, sm.obj_name, b.[Bron]
)

/* =========[ 2) Tabellen-array naar JSON ]========= */
, TablesJson AS
(
    SELECT
        bron           = bq.Bron,
        [name]         = bq.obj_name,
        [schema_name]  = bq.[schema_name],
        [enabled]      = IIF(ebd.obj_name is NULL, CAST(1 AS bit), CAST(0 AS bit)),
        size_class     = IIF(sz.obj_name is NULL, 'S', sz.[size_class]),
        load_mode      = IIF(lm.obj_name is NULL, N'snapshot', lm.[load_mode]),
        excluded       = IIF(ex.obj_name is NULL, 0, ex.[excluded]),
        delta_table    = bq.obj_name,        
        delta_schema   = bq.Bron,
        base_query     = bq.base_query,
        partition_column = wnd.[partition_column],
        granularity      = wnd.[granularity],
        lookback_months  = wnd.[lookback_months],
        [filter_column] = lm.[filter_column],
        [kind]          = lm.[kind]
    FROM BaseQueries bq
    LEFT JOIN (
        SELECT [Bron],[schema_name],[obj_name]
        FROM (
            VALUES
                ('anva_concern', 'dbo', 'Jobmonitor')
            ,   ('anva_concern', 'dbo', 'LaatsteVerversing')
            ,   ('anva_concern', 'dbo', 'Metadata')
            ,   ('anva_concern', 'dbo', 'VrijeLabels')
            ,   ('anva_concern', 'pbi', 'Nulmeting_Clausules')
            ,   ('anva_concern', 'pbi', 'Nulmeting_CodesDekking')
            ,   ('anva_concern', 'pbi', 'Nulmeting_CodesNAW')
            ,   ('anva_concern', 'pbi', 'Nulmeting_CodesPolis')
            ,   ('anva_concern', 'pbi', 'Nulmeting_LabelDekking')
            ,   ('anva_concern', 'pbi', 'Nulmeting_LabelNAW')
            ,   ('anva_concern', 'pbi', 'Nulmeting_LabelPolis')
            ,   ('anva_concern', 'pbi', 'Nulmeting_NAWDetails')
            ,   ('anva_concern', 'pbi', 'Nulmeting_NAWLabels')
            ,   ('anva_concern', 'pbi', 'Nulmeting_PolisDetails')
            ,   ('anva_concern', 'pbi', 'Nulmeting_PolisLabels')
            ,   ('anva_concern', 'pbi', 'Nulmeting_Voorwaarden')
            ,   ('anva_meeus', 'dbo', 'Jobmonitor')
            ,   ('anva_meeus', 'dbo', 'LaatsteVerversing')
            ,   ('anva_meeus', 'dbo', 'Metadata')
            ,   ('anva_meeus', 'dbo', 'VrijeLabels')
            ,   ('anva_meeus', 'pbi', 'Nulmeting_Clausules')
            ,   ('anva_meeus', 'pbi', 'Nulmeting_CodesDekking')
            ,   ('anva_meeus', 'pbi', 'Nulmeting_CodesNAW')
            ,   ('anva_meeus', 'pbi', 'Nulmeting_CodesPolis')
            ,   ('anva_meeus', 'pbi', 'Nulmeting_LabelDekking')
            ,   ('anva_meeus', 'pbi', 'Nulmeting_LabelNAW')
            ,   ('anva_meeus', 'pbi', 'Nulmeting_LabelPolis')
            ,   ('anva_meeus', 'pbi', 'Nulmeting_NAWDetails')
            ,   ('anva_meeus', 'pbi', 'Nulmeting_NAWLabels')
            ,   ('anva_meeus', 'pbi', 'Nulmeting_PolisDetails')
            ,   ('anva_meeus', 'pbi', 'Nulmeting_PolisLabels')
            ,   ('anva_meeus', 'pbi', 'Nulmeting_Voorwaarden')

        ) a ([Bron],[schema_name],[obj_name])
    ) ebd on bq.Bron = ebd.Bron and bq.[schema_name] = ebd.[schema_name] and bq.[obj_name] = ebd.[obj_name]
    LEFT JOIN ( -- define the table size_class
        SELECT [Bron],[schema_name],[obj_name],[size_class]
        FROM (
            VALUES
                ('anva_concern', 'pbi', 'Fact_PremieFacturen', 'L')
            ,   ('ccs_level', 'pbi', 'Fact_PremieBoekingen', 'L')
            ,   ('geintegreerd_model', 'pbi', 'Fact_PremieFacturen', 'L')
            ,   ('anva_meeus', 'pbi', 'Fact_PremieFacturen', 'L')
        ) a ([Bron],[schema_name],[obj_name], [size_class])
    ) sz on bq.Bron = sz.Bron and bq.[schema_name] = sz.[schema_name] and bq.[obj_name] = sz.[obj_name]
    LEFT JOIN ( -- define the table load_type
        SELECT [Bron],[schema_name],[obj_name],[load_mode],[filter_column],[kind]
        FROM (
            VALUES
                ('anva_concern', 'pbi', 'Fact_PremieFacturen', 'window', 'Boek_Datum', 'datetime')
            ,   ('ccs_level', 'pbi', 'Fact_PremieBoekingen', 'window', 'Boek_Datum', 'datetime')
            ,   ('geintegreerd_model', 'pbi', 'Fact_PremieFacturen', 'window', 'Boek_Datum', 'datetime')
            ,   ('anva_meeus', 'pbi', 'Fact_PremieFacturen', 'window', 'Boek_Datum', 'datetime')
            ,   ('vizier', 'dbo', 'Relaties', 'incremental','Updatedatum', 'stamp17')
            ,   ('vizier', 'dbo', 'Contactpersonen', 'incremental','Upd_dt', 'stamp17')
            ,   ('vizier', 'dbo', 'Sleutels', 'incremental','upd', 'stamp17')
            ,   ('vizier', 'dbo', 'Polissen', 'incremental','upd_dt', 'stamp17')
            ,   ('vizier', 'dbo', 'Schades', 'incremental','upd_dt', 'stamp17')
            ,   ('vizier', 'dbo', 'DnB', 'incremental','upd_dt', 'stamp17')
            ,   ('vizier', 'dbo', 'Contactmomenten', 'incremental','Upd', 'stamp17')
            ,   ('vizier', 'dbo', 'Taken', 'incremental','upd', 'stamp17')
            ,   ('vizier', 'dbo', 'Sales', 'incremental','UPD_DT', 'stamp17')
            ,   ('vizier', 'dbo', 'Retenties', 'incremental','UPD_DT', 'stamp17')
            ,   ('vizier', 'dbo', 'Adresbeeld', 'incremental','UPD_DT', 'stamp17')
            ,   ('vizier', 'dbo', 'UBO_Onderzoeken', 'incremental','UPD_DT', 'stamp17')
            ,   ('vizier', 'dbo', 'Producten', 'incremental','upd', 'stamp17')
            ,   ('vizier', 'dbo', 'Medewerkers', 'incremental','id_upd', 'stamp17')
            ,   ('vizier', 'dbo', 'Klachten', 'incremental','upd_dt', 'stamp17')
            ,   ('vizier', 'dbo', 'Verkoopkansen', 'incremental','upd', 'stamp17')
            ,   ('vizier', 'dbo', 'Interesses', 'incremental','UPD_DT', 'stamp17')
        ) a ([Bron],[schema_name],[obj_name], [load_mode],[filter_column],[kind])
    ) lm on bq.Bron = lm.Bron and bq.[schema_name] = lm.[schema_name] and bq.[obj_name] = lm.[obj_name]
    LEFT JOIN ( -- define the table partition and window size
        SELECT [Bron],[schema_name],[obj_name], [partition_column], [granularity], [lookback_months]
        FROM (
            VALUES
                ('anva_concern', 'pbi', 'Fact_PremieFacturen', 'Boek_Datum', 'month', 12)
            ,   ('geintegreerd_model', 'pbi', 'Fact_PremieFacturen', 'Boek_Datum', 'month', 12)
            ,   ('anva_meeus', 'pbi', 'Fact_PremieFacturen', 'Boek_Datum', 'month', 12)
            ,   ('ccs_level', 'pbi', 'Fact_PremieBoekingen', 'Boek_Datum', 'month', 12)
        ) a ([Bron],[schema_name],[obj_name], [partition_column], [granularity], [lookback_months])
    ) wnd on bq.Bron = wnd.Bron and bq.[schema_name] = wnd.[schema_name] and bq.[obj_name] = wnd.[obj_name]
    LEFT JOIN ( -- define the table to be excluded
        SELECT [Bron],[schema_name],[obj_name],[excluded]
        FROM (
            VALUES
                ('vizier', 'dbo', 'BO_sleutels_Wim_Verheijen', 1)
            ,   ('vizier', 'dbo', 'UMG_Historie', 1)
        ) a ([Bron],[schema_name],[obj_name], [excluded])
    ) ex on bq.Bron = ex.Bron and bq.[schema_name] = ex.[schema_name] and bq.[obj_name] = ex.[obj_name]

)

/* =========[ 3) Top-level JSON maken ]========= */
SELECT
    (
        SELECT
            (SELECT TOP (1) bron FROM TablesJson)   AS [source],
            null                                    AS [run_date_utc],
            'config/watermarks.json'                AS [watermarks_path],
            'greenhouse_sources'                     AS [base_files],

            'connection_' + (SELECT TOP (1) bron FROM TablesJson) + '_prod'  AS [connection_name],
            /* defaults object */
            JSON_QUERY(
                (
                    SELECT
                    2        AS [concurrency_large],
                    8        AS [concurrency_small],
                    15000000 AS [max_rows_per_file_large],
                    1000000  AS [max_rows_per_file_small]
                    FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
                )
            ) AS [defaults],
            /* tables array */
            JSON_QUERY(
                (
                    SELECT name, enabled, size_class, load_mode, delta_schema, delta_table, base_query--, single_file, sync_deletes
                    , CASE 
                        WHEN load_mode = 'window' AND 1=1 THEN (SELECT JSON_QUERY (
                                (
                                    SELECT
                                    [partition_column],
                                    [granularity],
                                    [lookback_months]
                                    FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
                                ))
                            ) 
                        END AS [window],
                        CASE 
                        WHEN load_mode = 'incremental' AND 1=1 THEN (SELECT JSON_QUERY (
                                (
                                    SELECT
                                    [filter_column],
                                    [kind]
                                    FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
                                ))
                            ) 
                       END AS [incremental_column]
                    FROM TablesJson 
                    WHERE 1=1
                    AND excluded = 0 
                    FOR JSON PATH
                )
            ) AS [tables]
        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER, INCLUDE_NULL_VALUES
    ) AS JsonOutput;

