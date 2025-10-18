{{ incremental_config('clube_mandante') }}
SELECT 
    mandante AS clube_mandante,
    COUNT(minuto_gol) AS qnt_gols
FROM {{ ref('int_gols') }}
GROUP BY mandante