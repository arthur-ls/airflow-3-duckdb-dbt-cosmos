SELECT
    partida_id, 
    rodata AS rodada, 
    clube, 
    cartao, 
    atleta, 
    CAST(num_camisa AS INTEGER) AS num_camisa,
    posicao, 
    minuto
FROM {{ source('soccer_db', 'campeonato_brasileiro_cartoes') }}