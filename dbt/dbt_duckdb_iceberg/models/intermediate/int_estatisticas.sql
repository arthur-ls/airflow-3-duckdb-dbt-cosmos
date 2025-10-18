{{ incremental_config('chave_unica') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(
        [ 
        'cb.id', 
        'cbe.clube'
        ]
    ) 
    }} AS chave_unica,
    cb.*,
    cbe.clube, 
    cbe.chutes, 
    cbe.chutes_no_alvo,
    cbe.posse_de_bola, 
    cbe.passes, 
    cbe.precisao_passes, 
    cbe.faltas,
    cbe.cartao_amarelo, 
    cbe.cartao_vermelho, 
    cbe.impedimentos, 
    cbe.escanteios
FROM {{ ref('stg_camp_brasileiro') }} cb
LEFT JOIN {{ ref('stg_camp_brasileiro_estatisticas') }} cbe
ON cb.id = cbe.partida_id