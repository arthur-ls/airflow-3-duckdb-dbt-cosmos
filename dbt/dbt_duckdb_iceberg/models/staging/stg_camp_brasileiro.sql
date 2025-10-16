SELECT 
    ID AS id, 
    rodata AS rodada, 
    strptime(data, '%d/%m/%Y') AS data_partida, 
    hora AS hora_partida, 
    mandante, 
    visitante,
    formacao_mandante, 
    formacao_visitante, 
    tecnico_mandante,
    tecnico_visitante, 
    vencedor, 
    arena, 
    mandante_Placar AS placar_mandante,
    visitante_Placar AS placar_visitante, 
    mandante_Estado AS estado_mandante, 
    visitante_Estado AS estado_visitante
FROM {{ source('soccer_db', 'campeonato_brasileiro_full') }}