{{ incremental_config('mandante') }}
SELECT
    mandante,
    SUM(chutes) AS total_chutes, 
    SUM(chutes_no_alvo) AS total_chutes_no_alvo,
    SUM(passes) AS total_passes,
    SUM(faltas) AS total_faltas,
    SUM(cartao_amarelo) AS total_cartao_amarelo, 
    SUM(cartao_vermelho) AS total_cartao_vermelho,
    SUM(impedimentos) AS total_impedimentos,
    SUM(escanteios) AS total_escanteios
FROM {{ ref('int_estatisticas') }}
GROUP BY mandante