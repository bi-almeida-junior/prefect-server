import os
import time
import json
from typing import Optional, List, Dict, Any
from dotenv import load_dotenv

from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.cache_policies import NONE as NO_CACHE
from prefect.artifacts import create_table_artifact, create_markdown_artifact
from prefect.client.schemas.schedules import CronSchedule

# Imports dos módulos
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from shared.connections.postgresql import (  # noqa: E402
    execute_query, close_postgresql_connection, format_query_with_params
)
from shared.alerts import (  # noqa: E402
    send_flow_success_alert, send_flow_error_alert
)

# Carrega variáveis de ambiente (fallback)
load_dotenv()

# ════════════════════════════════════════════════════════════════════════════════
# BLOCKS DO PREFECT - Usando APENAS Secret
# ════════════════════════════════════════════════════════════════════════════════
#
# Configure os seguintes blocks via interface do Prefect ou via código:
#
# PostgreSQL Credentials (TODOS Secret):
#   - zapt-tech-postgres-host       (Secret)
#   - zapt-tech-postgres-port       (Secret)
#   - zapt-tech-postgres-database   (Secret)
#   - zapt-tech-postgres-user       (Secret)
#   - zapt-tech-postgres-password   (Secret)
#   - zapt-tech-postgres-schema     (Secret)
#
# API Blocks:
#   - zapt-tech-api-url             (Secret)
#   - zapt-tech-api-key             (Secret)
#
# ════════════════════════════════════════════════════════════════════════════════


# ════════════════════════════════════════════════════════════════════════════════
# CONFIGURAÇÕES ZAPT TECH
# ════════════════════════════════════════════════════════════════════════════════

# ─────────────────────────────────────────────────────────────────────────────
# Siglas válidas dos shoppings
# ─────────────────────────────────────────────────────────────────────────────
VALID_SHOPPING_SIGLAS = [
    "NK",
    "BS",
    "GS",
    "NR",
    "CS",
    "NS"
]

# ─────────────────────────────────────────────────────────────────────────────
# Organization ID do Zapt Tech
# ─────────────────────────────────────────────────────────────────────────────
ZAPT_TECH_ORGANIZATION_ID = 6291090961989632

# ─────────────────────────────────────────────────────────────────────────────
# API Endpoint (URL real do Zapt Tech)
# ─────────────────────────────────────────────────────────────────────────────
ZAPT_TECH_API_URL = "https://us-central1-zapt-backend.cloudfunctions.net/saveBulkData"

# ─────────────────────────────────────────────────────────────────────────────
# Schema dos dados conforme especificação Zapt Tech
# ─────────────────────────────────────────────────────────────────────────────
ZAPT_TECH_SCHEMA = {
    "name": "Stores_Almeida_Junior",
    "fields": [
        # Identificação da Loja
        {"name": "shopping",     "type": "string", "helperText": "Onde a loja está localizada"},
        {"name": "luc",          "type": "string", "helperText": "N° de identificação da loja"},

        # Dimensões
        {"name": "abl",          "type": "float",  "helperText": "M² da Loja"},
        {"name": "vitrine",      "type": "float",  "helperText": "O tamanho da vitrine da loja."},

        # Informações do Lojista
        {"name": "lojista",      "type": "string", "helperText": "Nome Fantasia"},
        {"name": "segmento",     "type": "string", "helperText": "Segmento em que a loja está inserida"},
        {"name": "atividade",    "type": "string", "helperText": "Atividade da empresa locada"},

        # Contrato
        {"name": "contrato",     "type": "string", "helperText": "Nº do contrato"},
        {"name": "vencimento",   "type": "string", "helperText": "Vencimento do contrato"},
        {"name": "competencia",  "type": "string", "helperText": "Mês de referência"},

        # Vendas
        {"name": "media_venda",  "type": "float",  "helperText": "Valor da venda com média nos últimos 12 meses"},
        {"name": "venda_m2",     "type": "float",  "helperText": "Venda dividida pela ABL"},

        # Aluguel Mínimo Mensal (AMM)
        {"name": "amm",          "type": "float",  "helperText": "Média do aluguel mínimo e Garantia Mínima, dos últimos 12 meses"},
        {"name": "amm_m2",       "type": "float",  "helperText": "AMM dividido pela ABL"},

        # Aluguel Variável
        {"name": "aluguel_variavel", "type": "float", "helperText": "Valor do aluguel variável"},

        # Condomínio
        {"name": "condominio",   "type": "float",  "helperText": "Apenas a linha de condomínio, média dos últimos 12 meses."},
        {"name": "condominio_m2","type": "float",  "helperText": "Condomínio dividido pela ABL"},

        # Fundo de Promoção
        {"name": "fundo_promocao",    "type": "float", "helperText": "Considerando apenas Cota Ordinária, média dos últimos 12 meses."},
        {"name": "fundo_promocao_m2", "type": "float", "helperText": "Cota Ordinária divida pela ABL"},

        # Faturamento
        {"name": "faturamento_total", "type": "float", "helperText": "Média do Faturamento dos últimos 12 meses, não considerando encargos extras"},
        {"name": "co_percentual",     "type": "float", "helperText": "Média do valor do aluguel Complementar E Variável dos últimos 12 meses"},

        # Outras Informações
        {"name": "tipo_contrato", "type": "string", "helperText": "Se é Padrão, CTO ou CTO%"},
        {"name": "cdu_m2",        "type": "float",  "helperText": ""},
        {"name": "status",        "type": "string", "helperText": "Se está locada, vaga, ou vaga-shell"}
    ]
}


# ════════════════════════════════════════════════════════════════════════════════
# SQL QUERY PADRÃO - Editável via parâmetro na interface do Prefect
# ════════════════════════════════════════════════════════════════════════════════
#
# IMPORTANTE: Use {sigla_shopping} como placeholder para a sigla do shopping
# Siglas válidas: NK, BS, GS, NR, CS, NS
#
# Esta query retorna:
#   - Lojas ocupadas com métricas (AMM, vendas, faturamento, etc)
#   - Lojas vagas
#   - Lojas vagas shell
#
# ════════════════════════════════════════════════════════════════════════════════

DEFAULT_SQL_QUERY = """
-- ============================================================================
-- CTE: COMPETENCIA - Calcula período de análise (últimos 12 meses)
-- ============================================================================
with competencia as (
    select
        TO_DATE(
            case
                when extract (day from CURRENT_DATE) <= 10 then TO_CHAR((CURRENT_DATE - INTERVAL '2 months') - INTERVAL '11 months', 'YYYYMM')
                else TO_CHAR((CURRENT_DATE - INTERVAL '1 months') - INTERVAL '11 months', 'YYYYMM')
            end, 'YYYYMM'
            )                                         as data_inicial
        ,(TO_DATE(
            case
                when extract (day from CURRENT_DATE) <= 10 then TO_CHAR(CURRENT_DATE - INTERVAL '2 months', 'YYYYMM')
                else TO_CHAR(CURRENT_DATE - INTERVAL '1 month', 'YYYYMM')
            end, 'YYYYMM'
            ) + INTERVAL '1 month - 1 day')::date     as data_final
        ,case
             when extract(day from CURRENT_DATE) <= 10 then TO_CHAR(CURRENT_DATE - INTERVAL '2 months', 'YYYYMM')
            else TO_CHAR(CURRENT_DATE - INTERVAL '1 month', 'YYYYMM')
          end                                          as  competencia_atual
),

-- ============================================================================
-- CTE: LUC - Locações Úteis Comerciais ativas do shopping
-- ============================================================================
luc as  (
    select     luc.id_shopping
            ,luc.id_luc
            ,luc.ds_subtipo_luc
            ,luc.ds_nivel
            ,luc.qt_metragem
            ,luc.vitrine
            ,id_luc_planta
    from dim_vs_luc luc
    where luc.id_status = 'A'
    and luc.ds_tipo_luc <> 'QUIOSQUE'
    and luc.dt_desativacao is null
    and id_shopping  = '{sigla_shopping}'
),

-- ============================================================================
-- CTE: OCUPACAO - Lojas ocupadas na competência atual
-- ============================================================================
ocupacao as (
    select     ocup.id_shopping
            ,ocup.id_contrato
            ,ocup.id_luc
            ,ocup.dt_competencia
            ,ocup.ds_subtipo_luc
            ,luc.vitrine
            ,to_char(dvc.dt_fim_formal, 'DD/MM/YYYY') as vencimento
            ,(
                select SUM(ocup_sub.qt_metragem)
                    from fato_ocupacao_contrato_luc ocup_sub
                    where ocup_sub.id_shopping    = ocup.id_shopping
                    and ocup_sub.id_contrato     = ocup.id_contrato
                    and ocup_sub.dt_competencia = ocup.dt_competencia
            ) abl
    from fato_ocupacao_contrato_luc ocup
    inner join luc luc
        on luc.id_shopping         = ocup.id_shopping
        and luc.id_luc              = ocup.id_luc
    inner join dim_vs_contrato dvc
        on dvc.id_contrato = ocup.id_contrato
        and dvc.id_shopping = ocup.id_shopping
    where ocup.dt_competencia    = (select competencia_atual from competencia)
        and ocup.id_tipo      = 'P'
        and ocup.id_shopping = '{sigla_shopping}'
    group by 1, 2, 3, 4, 5, 6, 7
),

-- ============================================================================
-- CTE: VENDAS - Vendas do período (últimos 12 meses)
-- ============================================================================
vendas as (
    select     ven.id_shopping
            ,ven.id_contrato
            ,ven.id_luc
            ,ven.dt_competencia
            ,SUM(ven.vl_informado_lojista) as venda_total
    from fato_venda ven
    inner join competencia comp
        on ven.dt_venda  >= comp.data_inicial
        and ven.dt_venda <= comp.data_final
        and ven.id_shopping = '{sigla_shopping}'
    group by 1, 2, 3, 4
),

-- ============================================================================
-- CTE: MEDIA_VENDAS - Média mensal de vendas por loja
-- ============================================================================
media_vendas as (
    select
        id_shopping,
        id_contrato,
        id_luc,
        COUNT(distinct dt_competencia) as qtd_competencias,
        ROUND(SUM(venda_total) / COUNT(distinct dt_competencia), 2) as media_venda
    from vendas
    where id_shopping = '{sigla_shopping}'
    group by 1, 2, 3
),

-- ============================================================================
-- CTE: FATURAMENTO - Faturamento detalhado por conta contábil
-- ============================================================================
faturamento as (
    select     fat.id_shopping
            ,fat.id_contrato
            ,fat.id_luc
            ,fat.dt_competencia
            ,fat.id_conta
            ,sum(fat.vl_faturado) as faturamento
    from fato_faturamento fat
    inner join competencia com
        on fat.dt_referencia  >= com.data_inicial
        and fat.dt_referencia <= com.data_final
        and fat.id_shopping = '{sigla_shopping}'
    group by 1, 2, 3, 4, 5
),

-- ============================================================================
-- CTE: TOTAL_COMPETENCIA - Quantidade de competências com faturamento
-- ============================================================================
total_competencia as (
    select
        id_shopping,
        id_contrato,
        id_luc,
        COUNT(distinct dt_competencia) as qtd_competencias
    from fato_faturamento fat
    inner join competencia com
        on fat.dt_referencia  >= com.data_inicial
        and fat.dt_referencia <= com.data_final
    where fat.id_shopping = '{sigla_shopping}'
    group by 1, 2, 3
)

-- ============================================================================
-- QUERY PRINCIPAL PARTE 1: LOJAS OCUPADAS
-- ============================================================================
-- Retorna todas as lojas ocupadas com suas métricas financeiras e operacionais
-- ============================================================================
select     ocup.id_shopping                                as shopping
        ,trim(coalesce(luc.id_luc_planta, luc.id_luc))    as luc
        ,ocup.abl                                        as abl
        ,ocup.vitrine                                    as vitrine
        ,con.ds_nome_fantasia                            as lojista
        ,ocup.ds_subtipo_luc                            as segmento
        ,con.ds_atividade                                as atividade
        ,ocup.id_contrato                                as contrato
        ,ocup.vencimento                                as vencimento
        ,concat(
            substring(ocup.dt_competencia, 5, 2), '/', substring(ocup.dt_competencia, 1, 4)
            )                                            as competencia
        ,ven.media_venda                                as media_venda
        ,case when ocup.abl = 0 then 0
            else round(ven.media_venda / ocup.abl, 2)
        end                                                as venda_m2
        ,round(
            sum(fat.faturamento / totcomp.qtd_competencias) filter (where fat.id_conta in ('200102', '200105', '300106', '200117', '200139')), 2)
                                                        as amm
        ,case when ocup.abl = 0 then 0
            else round(sum(fat.faturamento / totcomp.qtd_competencias) filter (where fat.id_conta in ('200102', '200105', '300106', '200117', '200139')) / ocup.abl, 2)
        end                                                as amm_m2
        ,round(
            sum(fat.faturamento / totcomp.qtd_competencias) filter (where fat.id_conta in ('300104', '300105')), 2)
                                                        as aluguel_variavel
        ,round(
            sum(fat.faturamento / totcomp.qtd_competencias) filter (where fat.id_conta in ('200001', '200015')), 2)
                                                        as condominio
        ,case when ocup.abl = 0 then 0
            else round(
                    sum(fat.faturamento / totcomp.qtd_competencias) filter (where fat.id_conta in ('200001', '200015')) / ocup.abl, 2)
        end                                                as condominio_m2
        ,round(
            sum(fat.faturamento / totcomp.qtd_competencias) filter (where fat.id_conta in ('300001', '300006')),2)
                                                        as fundo_promocao
        ,case when ocup.abl = 0 then 0
            else round(
                    sum(fat.faturamento / totcomp.qtd_competencias) filter (where fat.id_conta in ('300001', '300006')) / ocup.abl, 2)
        end                                                as fundo_promocao_m2
        ,round(
            sum(fat.faturamento / totcomp.qtd_competencias) filter (where fat.id_conta in ('200004','300101','300107','200104','200123','200124','200119','200120','200102','200117','300104','200002','200052','400001','200019','200001','400010','600010','300002','300001','200020','200029','300102','200140','200125','200121','200122','200139','300105','200028','200015','300007','200105','200024','200037','200027','200031','300006','200041','200034','200033','200032','200040','200030','200003','200009','300020','200021','300106','200005','200013','200050','200016','200158','300005','200110','200113','200160','200114','200109','200159','200162','200018','200115','300011','300003','300021','200108','200035','200012','200011','200008','200014','200023','200022','600001','600002','200006','200007','200116','200025')), 2)
                                                        as faturamento_total
        ,greatest(
          round(
            sum(fat.faturamento / totcomp.qtd_competencias)    filter (where fat.id_conta in ('3001001', '300102', '300104', '300105')), 2),
            0)
                                                        as CO_percentual
        ,con.ds_tipo_contrato                            as tipo_contrato
        ,null                                             as cdu_m2
        ,'LOCADO'                                        as status
from ocupacao ocup
inner join dim_vs_contrato con
    on con.id_shopping         = ocup.id_shopping
    and con.id_contrato     = ocup.id_contrato
full outer join media_vendas ven
    on ven.id_shopping         = ocup.id_shopping
    and ven.id_contrato     = ocup.id_contrato
    and ven.id_luc             = ocup.id_luc
inner join faturamento fat
    on fat.id_shopping         = ocup.id_shopping
    and fat.id_contrato        = ocup.id_contrato
    and fat.id_luc             = ocup.id_luc
inner join total_competencia totcomp
    on fat.id_shopping         = totcomp.id_shopping
    and fat.id_contrato        = totcomp.id_contrato
    and fat.id_luc             = totcomp.id_luc
inner join luc luc
    on ocup.id_shopping        = luc.id_shopping
    and ocup.id_luc            = luc.id_luc
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 22

UNION ALL

-- ============================================================================
-- QUERY PRINCIPAL PARTE 2: LOJAS VAGAS
-- ============================================================================
-- Retorna lojas que estavam ocupadas mas agora estão vagas
-- ============================================================================
select
        tot.shopping            as shopping
        ,tot.luc                 as luc
        ,sum(tot.abl)            as abl
        ,sum(tot.vitrine)        as vitrine
        ,tot.lojista            as lojista
        ,tot.segmento            as segmento
        ,null                    as atividade
        ,tot.contrato            as contrato
        ,null                   as vencimento
        ,tot.competencia        as competencia
        ,null::numeric            as media_venda
        ,null::numeric            as venda_m2
        ,null::numeric             as amm
        ,tab.amm_m2             as amm_m2
        ,null::numeric            as aluguel_variavel
        ,null::numeric            as condominio
        ,null::numeric            as condominio_m2
        ,null::numeric            as fundo_promocao
        ,null::numeric            as fundo_promocao_m2
        ,null::numeric            as faturamento_total
        ,null::numeric            as co_percentual
        ,null                    as tipo_contrato
        ,tab.cdu_m2             as cdu_m2
        ,'LOJA-VAGA'            as status
from (
    select  distinct
            tot.id_shopping                                    as shopping
            ,trim(coalesce(luc.id_luc_planta, luc.id_luc))    as luc
            ,luc.qt_metragem                                as abl
            ,luc.vitrine                                    as vitrine
            ,con.ds_nome_fantasia                            as lojista
            ,luc.ds_subtipo_luc                                as segmento
            ,luc.ds_nivel                                     as piso
            ,ocup3.id_contrato                                as contrato
            ,concat(
            substring(ocup2.dt_competencia, 5, 2), '/', substring(ocup2.dt_competencia, 1, 4)
            )                    as competencia
    from
    (
        select    ocup.id_shopping
                ,ocup.id_luc
                ,max(ocup.dt_competencia) as dt_competencia
        from fato_ocupacao_contrato_luc as ocup
        where ocup.ds_tipo_luc <> 'QUIOSQUE'
            and ocup.ds_subtipo_luc <> 'MALL'
        group by 1, 2) as tot
    inner join fato_ocupacao_contrato_luc as ocup2
        on ocup2.id_shopping      = tot.id_shopping
        and ocup2.id_luc          = tot.id_luc
        and ocup2.dt_competencia = tot.dt_competencia
    inner join fato_ocupacao_contrato_luc as ocup3
        on ocup3.id_shopping      = ocup2.id_shopping
        and ocup3.dt_competencia = ocup2.dt_competencia
        and ocup3.id_contrato      = ocup2.id_contrato
    inner join luc luc
        on ocup2.id_shopping      = luc.id_shopping
        and ocup2.id_luc          = luc.id_luc
    inner join dim_vs_contrato con
        on con.id_shopping          = ocup2.id_shopping
        and con.id_contrato      = ocup2.id_contrato
    where tot.dt_competencia < (case
                 when extract(day from CURRENT_DATE) <= 10 then TO_CHAR(CURRENT_DATE - INTERVAL '2 months', 'YYYYMM')
                else TO_CHAR(CURRENT_DATE - INTERVAL '1 month', 'YYYYMM')
              end)
) tot
left join tabela_comercial tab
        on tot.shopping     = tab.shopping
        and tot.segmento    = tab.categoria
        and tot.abl between tab.abl_inicial and tab.abl_final
        and tot.piso         = tab.piso
group by 1, 2, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24

UNION ALL

-- ============================================================================
-- QUERY PRINCIPAL PARTE 3: LOJAS VAGAS SHELL
-- ============================================================================
-- Retorna lojas que nunca foram ocupadas (shell/casca)
-- ============================================================================
select
    tot.shopping            as shopping
    ,tot.luc                as luc
    ,sum(tot.abl)             as abl
    ,sum(tot.vitrine)        as vitrine
    ,null                    as lojista
    ,tot.segmento            as segmento
    ,null                    as atividade
    ,null                    as contrato
    ,null                   as vencimento
    ,null                    as competencia
    ,null::numeric            as media_venda
    ,null::numeric            as venda_m2
    ,null::numeric            as amm
    ,tab.amm_m2             as amm_m2
    ,null::numeric            as aluguel_variavel
    ,null::numeric            as condominio
    ,null::numeric            as condominio_m2
    ,null::numeric            as fundo_promocao
    ,null::numeric            as fundo_promocao_m2
    ,null::numeric            as faturamento_total
    ,null::numeric            as co_percentual
    ,null                    as tipo_contrato
    ,tab.cdu_m2             as cdu_m2
    ,'LOJA-VAGA-SHELL'        as status
from (
    select    luc.id_shopping                                    as shopping
            ,trim(coalesce(luc.id_luc_planta, luc.id_luc))    as luc
            ,luc.qt_metragem                                as abl
            ,luc.vitrine                                    as vitrine
            ,luc.ds_subtipo_luc                                as segmento
            ,luc.ds_nivel                                     as piso
    from luc luc
    left join fato_ocupacao_contrato_luc ocup
        on ocup.id_shopping    = luc.id_shopping
        and ocup.id_luc     = luc.id_luc
    where ocup.id_shopping is null
        and ocup.id_luc is null
) tot
left join tabela_comercial tab
        on tot.shopping = tab.shopping
        and tot.segmento = tab.categoria
        and tot.abl between tab.abl_inicial and tab.abl_final
        and tot.piso = tab.piso
group by 1, 2, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24
"""


def validate_shopping_sigla(sigla: str) -> bool:
    """
    Valida se a sigla do shopping é válida

    Args:
        sigla: Sigla do shopping

    Returns:
        True se válida

    Raises:
        ValueError: Se sigla inválida
    """
    if sigla not in VALID_SHOPPING_SIGLAS:
        valid_list = ", ".join(VALID_SHOPPING_SIGLAS)
        raise ValueError(
            f"Sigla de shopping inválida: '{sigla}'. "
            f"Siglas válidas: {valid_list}"
        )
    return True


def format_data_for_zapt_tech_api(
        data: List[Dict[str, Any]],
        sigla_shopping: str
) -> Dict[str, Any]:
    """
    Formata os dados no formato esperado pela API Zapt Tech

    Args:
        data: Lista de dicionários com os resultados da query
        sigla_shopping: Sigla do shopping

    Returns:
        Dicionário no formato da API Zapt Tech
    """
    logger = get_run_logger()

    # Converte os resultados para o formato esperado
    items = []
    for row in data:
        # Converte valores None para string ou mantém o tipo
        item = {}
        for key, value in row.items():
            # Converte None para null explicitamente
            if value is None:
                item[key] = None
            # Converte Decimal para float
            elif hasattr(value, '__float__'):
                try:
                    item[key] = float(value)
                except:
                    item[key] = str(value)
            # Mantém strings
            elif isinstance(value, str):
                item[key] = value
            # Converte outros tipos para string
            else:
                item[key] = str(value) if value is not None else None

        items.append(item)

    # Monta o payload no formato esperado
    payload = {
        "organizationId": ZAPT_TECH_ORGANIZATION_ID,
        "schema": ZAPT_TECH_SCHEMA,
        "items": items
    }

    logger.info(f"📦 Payload formatado:")
    logger.info(f"   🏢 Organization ID: {ZAPT_TECH_ORGANIZATION_ID}")
    logger.info(f"   📋 Schema: {ZAPT_TECH_SCHEMA['name']}")
    logger.info(f"   📊 Items: {len(items)}")
    logger.info(f"   🏬 Shopping: {sigla_shopping}")

    return payload


@task(cache_policy=NO_CACHE, retries=2, retry_delay_seconds=30)
def execute_query_from_postgres(
        sql_query: str,
        sigla_shopping: str
) -> List[Dict[str, Any]]:
    """
    Conecta no PostgreSQL usando blocks nativos (Secret/String) e executa a query

    Args:
        sql_query: Query SQL a ser executada
        sigla_shopping: Sigla do shopping para substituir na query

    Returns:
        Lista de dicionários com os resultados
    """
    logger = get_run_logger()

    try:
        # Carrega credenciais usando APENAS Secret
        from prefect.blocks.system import Secret

        logger.info(f"📦 Carregando credenciais PostgreSQL via Secret blocks...")

        # Carrega credenciais individuais (TODOS Secret)
        host = Secret.load("zapt-tech-postgres-host").get()
        port = int(Secret.load("zapt-tech-postgres-port").get())
        database = Secret.load("zapt-tech-postgres-database").get()
        user = Secret.load("zapt-tech-postgres-user").get()
        password = Secret.load("zapt-tech-postgres-password").get()
        schema = Secret.load("zapt-tech-postgres-schema").get()

        logger.info(f"✅ Credenciais carregadas: {host}:{port}/{database}")
        logger.info(f"🏬 Shopping: {sigla_shopping}")

        # Substitui placeholders na query
        formatted_query = format_query_with_params(sql_query, sigla_shopping=sigla_shopping)

        logger.info(f"📝 Query formatada (primeiros 300 caracteres):")
        logger.info(f"{formatted_query[:300]}...")

        # Conecta no banco usando psycopg2
        import psycopg2
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            options=f"-c search_path={schema}"
        )

        # Executa a query
        logger.info("⚡ Executando query no PostgreSQL...")
        start_time = time.time()

        results = execute_query(conn, formatted_query)

        execution_time = time.time() - start_time

        logger.info(f"✅ Query executada com sucesso!")
        logger.info(f"   📊 Registros retornados: {len(results):,}")
        logger.info(f"   ⏱️ Tempo de execução: {execution_time:.2f}s")

        # Fecha conexão
        close_postgresql_connection(conn)

        return results

    except Exception as e:
        logger.error(f"❌ Erro ao executar query: {str(e)}")
        raise


@task(cache_policy=NO_CACHE)
def save_results_to_json(
        payload: Dict[str, Any],
        sigla_shopping: str,
        output_dir: str = "flows/zapt_tech/output"
) -> Dict[str, Any]:
    """
    Salva payload formatado em arquivo JSON local

    Args:
        payload: Payload formatado no padrão Zapt Tech (organizationId, schema, items)
        sigla_shopping: Sigla do shopping
        output_dir: Diretório de saída (padrão: flows/zapt_tech/output)

    Returns:
        Informações sobre o arquivo salvo
    """
    logger = get_run_logger()

    try:
        # Cria diretório de output se não existir
        import os
        from datetime import datetime

        os.makedirs(output_dir, exist_ok=True)

        items_count = len(payload.get("items", []))
        logger.info(f"💾 Salvando {items_count:,} registros em arquivo JSON...")
        logger.info(f"   🏢 Organization ID: {payload.get('organizationId')}")
        logger.info(f"   📋 Schema: {payload.get('schema', {}).get('name')}")
        logger.info(f"   🏬 Shopping: {sigla_shopping}")

        # Nome do arquivo com timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"zapt_tech_{sigla_shopping}_{timestamp}.json"
        filepath = os.path.join(output_dir, filename)

        # Salva JSON formatado (com indentação)
        start_time = time.time()
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)

        save_time = time.time() - start_time

        # Tamanho do arquivo
        file_size = os.path.getsize(filepath)
        file_size_mb = file_size / (1024 * 1024)

        logger.info(f"✅ Arquivo salvo com sucesso!")
        logger.info(f"   📁 Arquivo: {filepath}")
        logger.info(f"   📊 Tamanho: {file_size_mb:.2f} MB")
        logger.info(f"   ⏱️ Tempo: {save_time:.2f}s")

        # Também salva um arquivo "latest" para acesso fácil
        latest_filename = f"zapt_tech_{sigla_shopping}_latest.json"
        latest_filepath = os.path.join(output_dir, latest_filename)

        with open(latest_filepath, 'w', encoding='utf-8') as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)

        logger.info(f"   📌 Cópia salva: {latest_filepath}")

        # Cria artifact com JSON completo (sempre o mesmo nome = sobrescreve)
        try:
            from prefect.artifacts import create_markdown_artifact

            # JSON completo formatado
            full_json = json.dumps(payload, indent=2, ensure_ascii=False)

            markdown_content = f"""
# 📊 Zapt Tech - JSON Consolidado

**Total de registros:** {items_count:,}
**Tamanho:** {file_size_mb:.2f} MB
**Shopping:** {sigla_shopping}

---

## 📥 JSON Completo

```json
{full_json}
```

---

**Arquivo salvo em:** `{filepath}`
"""

            create_markdown_artifact(
                key="zapt-tech-json-data",  # 👈 SEMPRE O MESMO NOME = SOBRESCREVE
                markdown=markdown_content,
                description=f"JSON Zapt Tech - {items_count:,} registros"
            )

            logger.info("✅ Artifact criado: zapt-tech-json-data")

        except Exception as artifact_error:
            logger.warning(f"⚠️ Erro ao criar artifact: {artifact_error}")

        return {
            "status": "saved",
            "filepath": filepath,
            "latest_filepath": latest_filepath,
            "file_size_mb": file_size_mb,
            "save_time_seconds": save_time,
            "items_saved": items_count
        }

    except Exception as e:
        logger.error(f"❌ Erro ao salvar arquivo: {str(e)}")
        raise


@task(cache_policy=NO_CACHE, retries=3, retry_delay_seconds=10)
def send_results_to_api(
        api_url_block: str,
        api_key_block: str,
        payload: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Envia payload formatado para API usando blocks do Prefect

    Args:
        api_url_block: Nome do block String com URL da API
        api_key_block: Nome do block Secret com API Key (opcional)
        payload: Payload formatado no padrão Zapt Tech (organizationId, schema, items)

    Returns:
        Resposta da API
    """
    logger = get_run_logger()

    try:
        # Carrega blocks usando APENAS Secret
        from prefect.blocks.system import Secret

        logger.info(f"📦 Carregando blocks da API...")

        # Carrega URL da API (Secret)
        api_url = Secret.load(api_url_block).get()
        logger.info(f"🌐 API URL: {api_url}")

        # Carrega API Key (opcional)
        try:
            api_key = Secret.load(api_key_block).get()
        except:
            api_key = None
            logger.info("🔑 API Key não configurada (opcional)")

        items_count = len(payload.get("items", []))
        logger.info(f"📤 Enviando {items_count:,} registros para API...")
        logger.info(f"   🏢 Organization ID: {payload.get('organizationId')}")
        logger.info(f"   📋 Schema: {payload.get('schema', {}).get('name')}")

        # Prepara headers
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        # Envia para API usando requests
        import requests

        start_time = time.time()
        response = requests.post(
            api_url,
            json=payload,
            headers=headers,
            timeout=60
        )
        send_time = time.time() - start_time

        response.raise_for_status()  # Lança exceção se status code não for 2xx

        logger.info(f"✅ Dados enviados com sucesso!")
        logger.info(f"   📊 Status HTTP: {response.status_code}")
        logger.info(f"   ⏱️ Tempo de envio: {send_time:.2f}s")

        try:
            response_data = response.json()
            logger.info(f"   📥 Resposta: {json.dumps(response_data, indent=2)[:200]}...")
        except:
            response_data = response.text[:200]
            logger.info(f"   📥 Resposta (texto): {response_data}...")

        return {
            "status_code": response.status_code,
            "response": response_data,
            "send_time_seconds": send_time,
            "items_sent": items_count
        }

    except Exception as e:
        logger.error(f"❌ Erro ao enviar para API: {str(e)}")
        raise


@flow(log_prints=True, name="zapt-tech-all-shoppings")
def zapt_tech_to_api_all_shoppings(
        sql_query: str = DEFAULT_SQL_QUERY,
        api_url_block: str = "zapt-tech-api-url",
        api_key_block: str = "zapt-tech-api-key",
        send_alerts: bool = True,
        alert_group_id: Optional[str] = None
):
    """
    Flow Principal: Executa coleta para TODOS os shoppings e gera UM ÚNICO JSON

    Este flow coleta dados de todos os shoppings e consolida em um único arquivo JSON.

    Args:
        sql_query: Query SQL a ser executada para cada shopping
        api_url_block: Nome do block String com URL da API
        api_key_block: Nome do block Secret com API Key
        send_alerts: Se deve enviar alertas
        alert_group_id: ID do grupo para alertas
    """
    logger = get_run_logger()
    start_time = time.time()

    logger.info("=" * 80)
    logger.info("🚀 ZAPT TECH -> JSON CONSOLIDADO | TODOS OS SHOPPINGS")
    logger.info("=" * 80)
    logger.info(f"📋 Shoppings a processar: {', '.join(VALID_SHOPPING_SIGLAS)}")
    logger.info(f"🔢 Total de shoppings: {len(VALID_SHOPPING_SIGLAS)}")
    logger.info("=" * 80)

    # Array consolidado com items de TODOS os shoppings
    all_items = []
    results_summary = []
    success_count = 0
    error_count = 0

    # Processa cada shopping individualmente
    for sigla in VALID_SHOPPING_SIGLAS:
        logger.info(f"\n{'=' * 80}")
        logger.info(f"🏬 Processando shopping: {sigla}")
        logger.info(f"{'=' * 80}")

        try:
            # Valida sigla
            validate_shopping_sigla(sigla)

            # Executa query para o shopping
            results = execute_query_from_postgres(
                sql_query,
                sigla
            )

            # Formata dados
            payload = format_data_for_zapt_tech_api(results, sigla)

            # Adiciona items ao array consolidado
            items = payload.get("items", [])
            all_items.extend(items)

            logger.info(f"✅ Shopping {sigla}: {len(items):,} registros coletados")

            results_summary.append({
                "shopping": sigla,
                "status": "success",
                "records": len(items)
            })
            success_count += 1

        except Exception as e:
            logger.error(f"❌ Erro ao processar shopping {sigla}: {str(e)}")
            results_summary.append({
                "shopping": sigla,
                "status": "error",
                "error": str(e)
            })
            error_count += 1

    # Cria payload consolidado com TODOS os items
    logger.info(f"\n{'=' * 80}")
    logger.info("📦 CONSOLIDANDO DADOS")
    logger.info(f"{'=' * 80}")

    consolidated_payload = {
        "organizationId": ZAPT_TECH_ORGANIZATION_ID,
        "schema": ZAPT_TECH_SCHEMA,
        "items": all_items
    }

    logger.info(f"✅ Total de registros consolidados: {len(all_items):,}")
    logger.info(f"   📊 Shoppings incluídos: {success_count}")

    # Salva JSON consolidado
    logger.info(f"\n{'=' * 80}")
    logger.info("💾 SALVANDO JSON CONSOLIDADO")
    logger.info(f"{'=' * 80}")

    save_response = save_results_to_json(
        consolidated_payload,
        sigla_shopping="ALL",  # Identificador especial para arquivo consolidado
        output_dir="flows/zapt_tech/output"
    )

    # Resumo final
    logger.info(f"\n{'=' * 80}")
    logger.info("📊 RESUMO GERAL")
    logger.info(f"{'=' * 80}")

    duration = time.time() - start_time

    logger.info(f"✅ Shoppings processados com sucesso: {success_count}")
    logger.info(f"❌ Shoppings com erro: {error_count}")
    logger.info(f"📊 Total de registros: {len(all_items):,}")
    logger.info(f"💾 Arquivo JSON: {save_response['filepath']}")
    logger.info(f"📌 Acesso rápido: {save_response['latest_filepath']}")
    logger.info(f"⏱️ Duração total: {duration:.2f}s")
    logger.info(f"{'=' * 80}\n")

    # Envia alerta consolidado
    if send_alerts:
        try:
            from prefect.context import get_run_context
            try:
                context = get_run_context()
                job_id = str(context.flow_run.id) if hasattr(context, 'flow_run') else None
            except Exception:
                job_id = None

            if error_count == 0:
                # Todos os shoppings processados com sucesso
                total_records = sum(r.get("records", 0) for r in results_summary if r["status"] == "success")

                send_flow_success_alert(
                    flow_name="Zapt Tech - Todos Shoppings",
                    source="PostgreSQL",
                    destination=f"API ({api_url_block})",
                    summary={
                        "shoppings_processed": success_count,
                        "total_records": total_records,
                        "shoppings": ", ".join(VALID_SHOPPING_SIGLAS)
                    },
                    duration_seconds=duration,
                    job_id=job_id,
                    group_id=alert_group_id
                )
                logger.info("✅ Alerta consolidado de sucesso enviado")
            else:
                # Houve erros em alguns shoppings
                errors_detail = "\n".join([
                    f"- {r['shopping']}: {r.get('error', 'Erro desconhecido')}"
                    for r in results_summary if r['status'] == 'error'
                ])

                send_flow_error_alert(
                    flow_name="Zapt Tech - Todos Shoppings",
                    source="PostgreSQL",
                    destination=f"API ({api_url_block})",
                    error_message=f"{error_count} shopping(s) com erro:\n{errors_detail}",
                    duration_seconds=duration,
                    job_id=job_id,
                    group_id=alert_group_id
                )
                logger.info("✅ Alerta consolidado de erro enviado")

        except Exception as alert_error:
            logger.warning(f"⚠️ Erro ao enviar alerta consolidado: {alert_error}")

    return {
        "status": "completed",
        "shoppings_success": success_count,
        "shoppings_error": error_count,
        "duration_seconds": duration,
        "details": results_summary
    }


# ════════════════════════════════════════════════════════════════════════════════
# DEPLOYMENT DOS FLOWS
# ════════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # Deployment do flow principal (TODOS OS SHOPPINGS)
    zapt_tech_to_api_all_shoppings.from_source(
        source=".",
        entrypoint="flows/zapt_tech/zapt_tech_to_api.py:zapt_tech_to_api_all_shoppings"
    ).deploy(
        name="zapt-tech-all-shoppings",
        work_pool_name="local-pool",
        schedules=[
            CronSchedule(cron="0 8 * * *", timezone="America/Sao_Paulo")  # Diariamente às 8h
        ],
        tags=["postgresql", "api", "zapt_tech", "fact", "all-shoppings"],
        parameters={
            "sql_query": DEFAULT_SQL_QUERY,
            "api_url_block": "zapt-tech-api-url",
            "api_key_block": "zapt-tech-api-key"
        },
        description="Pipeline: Coleta dados de TODOS os shoppings (NK, BS, GS, NR, CS, NS) e envia para API Zapt Tech",
        version="1.0.0"
    )

    print("✅ Deployment criado: zapt-tech-all-shoppings")
    print(f"   📋 Shoppings: {', '.join(VALID_SHOPPING_SIGLAS)}")
    print("   ⏰ Agendamento: Diariamente às 8h (America/Sao_Paulo)")
