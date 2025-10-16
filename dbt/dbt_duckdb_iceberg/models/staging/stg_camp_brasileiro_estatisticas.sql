SELECT
    partida_id, 
    rodata AS rodada, 
    clube, 
    chutes, 
    chutes_no_alvo,
    ROUND(CAST(REPLACE(posse_de_bola, '%', '') AS INTEGER)/100, 2) AS posse_de_bola, 
    passes, 
    ROUND(CAST(REPLACE(precisao_passes, '%', '') AS INTEGER)/100, 2) AS precisao_passes, 
    faltas,
    cartao_amarelo, 
    cartao_vermelho, 
    impedimentos, 
    escanteios
FROM {{ source('soccer_db', 'campeonato_brasileiro_estatisticas_full') }}