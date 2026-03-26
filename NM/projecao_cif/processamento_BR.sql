use costdrivers;

with
filtered_ids_import as (
	select
		indiceprincipalID
	from tblsnapshot_indiceprincipal with (nolock)
	where paisID = 27
		and importexport = 1
		and NCM is not null
),

filtered_ids_import_needed as (
	select
		distinct(indiceprincipalID)
	from tblindiceprincipal_mes with (nolock)
	where indiceprincipalID in (select indiceprincipalID from filtered_ids_import)
		and Base = 2
		and Valor_CIF is null
),

filtered_elegibles as (
	SELECT
		B.indiceprincipalID,
		B.DataIndice,
		B.Base,
		B.Valor,
		B.Valor_cif,
		B.Valor_CIF / NULLIF(B.Valor, 0) as alpha,
		month(B.DataIndice) as mes
	FROM filtered_ids_import_needed A
    join (
		select 
			indiceprincipalID,
			DataIndice,
			Base,
			Valor,
			Valor_cif
		from tblsnapshot_indiceprincipal_mes
		WHERE DataIndice >= '2022-01-01'
		) B
		on A.indiceprincipalID = B.indiceprincipalID
),

alpha_month as (
	select 
		IndicePrincipalID,
		mes,
		AVG(alpha) as alpha_mean_month
	from filtered_elegibles
	where base = 0
		and Valor_CIF > Valor
	group by IndicePrincipalID, mes
),

forecast_data as (
	select
		A.indiceprincipalID,
		A.DataIndice,
		A.mes,
		A.Base,
		A.Valor,
		B.alpha_mean_month,
		A.Valor * B.alpha_mean_month as Valor_Cif_forecast
	from filtered_elegibles A
	join alpha_month B
		on A.IndicePrincipalID = B.IndicePrincipalID
		and A.mes = B.mes
	where A.base = 2
)

--UPDATE B
--SET B.Valor_CIF = A.Valor_CIF_forecast
SELECT *
FROM forecast_data A
JOIN tblindiceprincipal_mes B
	ON A.IndicePrincipalID = B.IndicePrincipalID
	AND A.DataIndice = B.DataIndice
WHERE (A.Valor_Cif_forecast <> B.Valor_CIF) or (B.Valor_CIF IS NULL and A.Valor_Cif_forecast IS NOT NULL);