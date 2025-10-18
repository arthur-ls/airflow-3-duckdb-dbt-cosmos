{{ incremental_config('chave_unica') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(
        [ 
        'cb.id', 
        'cbg.clube',
        'cbg.atleta',
        'cbg.minuto',
        'cbg.tipo_de_gol'
        ]
    ) 
    }} AS chave_unica,
    cb.*,
    cbg.clube AS clube_atleta_gol,
    cbg.atleta AS atleta_gol,
    cbg.minuto AS minuto_gol,
    cbg.tipo_de_gol
FROM {{ ref('stg_camp_brasileiro') }} cb
LEFT JOIN {{ ref('stg_camp_brasileiro_gols') }} cbg
ON cb.id = cbg.partida_id