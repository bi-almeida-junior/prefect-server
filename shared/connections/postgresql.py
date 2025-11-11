"""
Módulo de conexão PostgreSQL para Prefect flows

Fornece funções para conectar e executar queries em bancos PostgreSQL
"""

from typing import List, Dict, Any, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
from prefect import task
from prefect.logging import get_run_logger
from prefect.cache_policies import NONE as NO_CACHE


@task(retries=3, retry_delay_seconds=10)
def connect_postgresql(
        host: str,
        database: str,
        user: str,
        password: str,
        port: int = 5432,
        schema: str = "public",
        timeout: int = 30
):
    """
    Estabelece conexão com PostgreSQL

    Args:
        host: Hostname ou IP do servidor
        database: Nome do database
        user: Usuário
        password: Senha
        port: Porta (padrão: 5432)
        schema: Schema padrão (padrão: public)
        timeout: Timeout de conexão em segundos

    Returns:
        Conexão psycopg2
    """
    logger = get_run_logger()

    try:
        logger.info(f"🔌 Conectando PostgreSQL: {host}:{port}/{database}")

        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            connect_timeout=timeout,
            options=f'-c search_path={schema}'
        )

        logger.info("✅ Conexão PostgreSQL estabelecida com sucesso")
        return conn

    except Exception as e:
        logger.error(f"❌ Erro ao conectar PostgreSQL: {str(e)}")
        raise


@task(cache_policy=NO_CACHE)
def execute_query(
        conn,
        query: str,
        params: Optional[tuple] = None,
        fetch: bool = True
) -> List[Dict[str, Any]]:
    """
    Executa query SQL e retorna resultados como lista de dicionários

    Args:
        conn: Conexão psycopg2
        query: Query SQL a ser executada
        params: Parâmetros para a query (opcional)
        fetch: Se deve fazer fetch dos resultados (padrão: True)

    Returns:
        Lista de dicionários com os resultados
    """
    logger = get_run_logger()

    try:
        logger.info(f"⚡ Executando query PostgreSQL...")
        logger.info(f"📝 Query (primeiros 200 caracteres): {query[:200]}...")

        cursor = conn.cursor(cursor_factory=RealDictCursor)

        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)

        if fetch:
            results = cursor.fetchall()
            # Converte RealDictRow para dict normal
            results = [dict(row) for row in results]

            logger.info(f"✅ Query executada com sucesso: {len(results)} linha(s) retornada(s)")
            cursor.close()
            return results
        else:
            conn.commit()
            affected_rows = cursor.rowcount
            logger.info(f"✅ Query executada: {affected_rows} linha(s) afetada(s)")
            cursor.close()
            return []

    except Exception as e:
        logger.error(f"❌ Erro ao executar query: {str(e)}")
        conn.rollback()
        raise


@task(cache_policy=NO_CACHE)
def close_postgresql_connection(conn):
    """
    Fecha conexão PostgreSQL de forma segura

    Args:
        conn: Conexão psycopg2
    """
    logger = get_run_logger()

    try:
        if conn:
            conn.close()
        logger.info("✅ Conexão PostgreSQL fechada com sucesso")
    except Exception as e:
        logger.warning(f"⚠️ Aviso ao fechar conexão: {str(e)}")


def format_query_with_params(query: str, **kwargs) -> str:
    """
    Formata query substituindo placeholders {variavel} por valores

    ATENÇÃO: Use apenas com valores confiáveis para evitar SQL injection!
    Para queries com inputs de usuários, use parâmetros do psycopg2.

    Args:
        query: Query SQL com placeholders {variavel}
        **kwargs: Variáveis para substituir

    Returns:
        Query formatada

    Example:
        query = "SELECT * FROM tabela WHERE shopping = '{shopping}'"
        formatted = format_query_with_params(query, shopping="ABC")
    """
    return query.format(**kwargs)
