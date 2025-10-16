SELECT 
    partida_id, 
    rodata AS rodada, 
    clube, 
    atleta, 
    minuto, 
    tipo_de_gol
FROM {{ source('soccer_db', 'campeonato_brasileiro_gols') }}