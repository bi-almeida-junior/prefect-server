"""Funções utilitárias compartilhadas."""

from datetime import datetime, timedelta
from typing import Optional
from prefect.blocks.system import Secret
from prefect import get_run_logger


def get_datetime_brasilia() -> str:
    """
    Retorna a data/hora atual ajustada para o fuso horário de Brasília (UTC-3).

    Returns:
        str: Data/hora formatada como 'YYYY-MM-DD HH:MM:SS'

    Example:
        >>> get_datetime_brasilia()
        '2025-11-18 14:30:00'
    """
    return (datetime.now() - timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S')


def load_secret(secret_name: str) -> Optional[str]:
    """
    Carrega um Secret do Prefect Blocks.

    Args:
        secret_name: Nome do secret no Prefect Blocks

    Returns:
        String com o valor do secret ou None se falhar

    Raises:
        ValueError: Se o secret não existir ou houver erro ao carregar

    Example:
        >>> api_key = load_secret("hgbrasil-weather-api-key")
        >>> db_password = load_secret("database-password")
    """
    logger = get_run_logger()
    try:
        logger.info(f"🔐 Carregando secret '{secret_name}' do Prefect Blocks...")

        secret_block = Secret.load(secret_name)
        secret_value = secret_block.get()

        logger.info(f"✅ Secret '{secret_name}' carregado com sucesso")
        return secret_value

    except Exception as e:
        logger.error(f"❌ Erro ao carregar secret '{secret_name}': {type(e).__name__}")
        logger.error(f"Certifique-se de que o secret '{secret_name}' existe no Prefect")
        raise ValueError(f"Failed to load secret '{secret_name}'") from e