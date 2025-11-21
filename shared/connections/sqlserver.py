from typing import Optional
from contextlib import contextmanager
import pyodbc
from prefect.logging import get_run_logger
from prefect.blocks.system import Secret


@contextmanager
def sqlserver_connection(
        server: Optional[str] = None,
        database: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        driver: str = "SQL Server",
        timeout: int = 30
):
    """
    Context manager para conexão SQL Server.
    Busca credenciais na seguinte ordem:
    1. Parâmetros fornecidos
    2. Variáveis de ambiente (.env):
       - Host específico por database: SQLSERVER_HOST_{database} (ex: SQLSERVER_HOST_LUMINUS_GS)
       - Host genérico: SQLSERVER_HOST
       - SQLSERVER_DATABASE, SQLSERVER_USER, SQLSERVER_PASSWORD
    3. Secrets do Prefect:
       - Host específico: sqlserver-host-{database} (ex: sqlserver-host-luminus-gs)
       - Host genérico: sqlserver-host
       - sqlserver-database, sqlserver-user, sqlserver-password

    Args:
        server: Hostname ou IP do servidor SQL Server
        database: Nome do database
        user: Usuário
        password: Senha
        driver: Driver ODBC (padrão: "SQL Server")
        timeout: Timeout de conexão em segundos

    Yields:
        Conexão pyodbc

    Example:
        # No .env (hosts diferentes por database):
        # SQLSERVER_HOST_LUMINUS_GS=10.60.10.100
        # SQLSERVER_HOST_LUMINUS_NR=10.60.10.101
        # SQLSERVER_HOST_LUMINUS_NS=10.60.10.102
        # SQLSERVER_USER=sa
        # SQLSERVER_PASSWORD=senha123

        with sqlserver_connection(database='LUMINUS_GS') as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM tabela")
            rows = cur.fetchall()
    """
    import os
    logger = get_run_logger()
    conn = None

    try:
        # 1. Tenta variáveis de ambiente (.env)
        # Para hosts específicos por database (ex: SQLSERVER_HOST_LUMINUS_GS, SQLSERVER_HOST_LUMINUS_NR)
        if not server and database:
            server = os.getenv(f"SQLSERVER_HOST_{database}")
        if not server:
            server = os.getenv("SQLSERVER_HOST")
        if not database:
            database = os.getenv("SQLSERVER_DATABASE")
        if not user:
            user = os.getenv("SQLSERVER_USER")
        if not password:
            password = os.getenv("SQLSERVER_PASSWORD")

        # 2. Se ainda não encontrou, tenta Secrets do Prefect
        # Para hosts específicos por database (ex: sqlserver-host-luminus-gs)
        if not server and database:
            try:
                server = Secret.load(f"sqlserver-host-{database.lower().replace('_', '-')}").get()
            except:
                pass
        if not server:
            try:
                server = Secret.load("sqlserver-host").get()
            except:
                pass
        if not database:
            try:
                database = Secret.load("sqlserver-database").get()
            except:
                pass
        if not user:
            try:
                user = Secret.load("sqlserver-user").get()
            except:
                pass
        if not password:
            try:
                password = Secret.load("sqlserver-password").get()
            except:
                pass

        # Valida se todas as credenciais foram encontradas
        if not all([server, database, user, password]):
            raise ValueError(
                "Credenciais SQL Server incompletas. "
                "Configure no .env (SQLSERVER_HOST, SQLSERVER_DATABASE, SQLSERVER_USER, SQLSERVER_PASSWORD) "
                "ou nos Secrets do Prefect (sqlserver-host, sqlserver-database, sqlserver-user, sqlserver-password)"
            )

        logger.info(f"🔌 Conectando SQL Server: {server}/{database}")

        # Monta connection string
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={password};"
            f"TrustServerCertificate=yes;"
            f"Connection Timeout={timeout};"
        )

        conn = pyodbc.connect(conn_str)
        conn.autocommit = False  # Controle manual de transações

        logger.info("✅ Conexão SQL Server estabelecida com sucesso")
        yield conn

    except Exception as e:
        logger.error(f"❌ Erro ao conectar SQL Server: {str(e)}")
        raise

    finally:
        if conn:
            try:
                conn.close()
                logger.info("🔒 Conexão SQL Server fechada")
            except Exception as e:
                logger.warning(f"⚠️ Aviso ao fechar conexão: {str(e)}")