{{ incremental_config('chave_unica') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(
        [ 
        'cb.id', 
        'cbc.clube',
        'cbc.cartao',
        'cbc.atleta',
        'cbc.num_camisa_atleta',
        'cbc.minuto'
        ]
    ) 
    }} AS chave_unica,
    cb.*,
    cbc.clube AS clube_cartao,
    cbc.cartao,
    cbc.atleta AS atleta_cartao,
    cbc.num_camisa_atleta AS num_camisa_atleta_cartao,
    cbc.posicao AS posicao_atleta_cartao,
    cbc.minuto AS minuto_cartao
FROM {{ ref('stg_camp_brasileiro') }} cb
LEFT JOIN {{ ref('stg_camp_brasileiro_cartoes') }} cbc
ON cb.id = cbc.partida_id