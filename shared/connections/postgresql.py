from typing import List, Dict, Any, Optional
from contextlib import contextmanager
import psycopg2
from psycopg2.extras import RealDictCursor
from prefect import task
from prefect.logging import get_run_logger
from prefect.cache_policies import NONE as NO_CACHE
from prefect.blocks.system import Secret


@contextmanager
def postgresql_connection(
        host: Optional[str] = None,
        database: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        port: int = 5432,
        schema: str = "public",
        timeout: int = 30
):
    """
    Context manager para conexão PostgreSQL.
    Busca credenciais na seguinte ordem:
    1. Parâmetros fornecidos
    2. Variáveis de ambiente (.env): RPA_POSTGRES_HOST, RPA_POSTGRES_DATABASE, RPA_POSTGRES_USER, RPA_POSTGRES_PASSWORD
    3. Secrets do Prefect: rpa-postgres-host, rpa-postgres-database, rpa-postgres-user, rpa-postgres-password

    Args:
        host: Hostname ou IP do servidor
        database: Nome do database
        user: Usuário
        password: Senha
        port: Porta (padrão: 5432, ou POSTGRES_PORT do .env)
        schema: Schema padrão (padrão: public)
        timeout: Timeout de conexão em segundos

    Yields:
        Conexão psycopg2

    Example:
        # No .env:
        # RPA_POSTGRES_HOST=localhost
        # RPA_POSTGRES_DATABASE=mydb
        # RPA_POSTGRES_USER=postgres
        # RPA_POSTGRES_PASSWORD=senha123
        # RPA_POSTGRES_PORT=5432

        with postgresql_connection(schema='bronze') as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM tabela")
            conn.commit()
    """
    import os
    logger = get_run_logger()
    conn = None

    try:
        # 1. Tenta variáveis de ambiente (.env)
        if not host:
            host = os.getenv("RPA_POSTGRES_HOST")
        if not database:
            database = os.getenv("RPA_POSTGRES_DATABASE")
        if not user:
            user = os.getenv("RPA_POSTGRES_USER")
        if not password:
            password = os.getenv("RPA_POSTGRES_PASSWORD")
        if not port or port == 5432:
            port = int(os.getenv("RPA_POSTGRES_PORT", "5432"))

        # 2. Se ainda não encontrou, tenta Secrets do Prefect
        if not host:
            try:
                host = Secret.load("rpa-postgres-host").get()
            except:
                pass
        if not database:
            try:
                database = Secret.load("rpa-postgres-database").get()
            except:
                pass
        if not user:
            try:
                user = Secret.load("rpa-postgres-user").get()
            except:
                pass
        if not password:
            try:
                password = Secret.load("rpa-postgres-password").get()
            except:
                pass

        # Valida se todas as credenciais foram encontradas
        if not all([host, database, user, password]):
            raise ValueError(
                "Credenciais PostgreSQL incompletas. "
                "Configure no .env (RPA_POSTGRES_HOST, RPA_POSTGRES_DATABASE, RPA_POSTGRES_USER, RPA_POSTGRES_PASSWORD) "
                "ou nos Secrets do Prefect (rpa-postgres-host, rpa-postgres-database, rpa-postgres-user, rpa-postgres-password)"
            )

        logger.info(f"🔌 Conectando PostgreSQL: {host}:{port}/{database} (schema: {schema})")

        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            connect_timeout=timeout,
            options=f'-c search_path={schema}'
        )

        # Desabilita autocommit para controle manual de transações
        conn.autocommit = False

        logger.info("✅ Conexão PostgreSQL estabelecida com sucesso")
        yield conn

    except Exception as e:
        logger.error(f"❌ Erro ao conectar PostgreSQL: {str(e)}")
        raise

    finally:
        if conn:
            try:
                conn.close()
                logger.info("🔒 Conexão PostgreSQL fechada")
            except Exception as e:
                logger.warning(f"⚠️ Aviso ao fechar conexão: {str(e)}")


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
    Estabelece conexão com PostgreSQL (função legada - use postgresql_connection context manager)

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
