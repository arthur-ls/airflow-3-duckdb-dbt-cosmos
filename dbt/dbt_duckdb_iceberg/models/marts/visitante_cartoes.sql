{{ incremental_config('clube_visitante') }}
SELECT 
    visitante AS clube_visitante,
    COUNT(minuto_cartao) AS qnt_cartoes
FROM {{ ref('int_cartoes') }}
GROUP BY visitante