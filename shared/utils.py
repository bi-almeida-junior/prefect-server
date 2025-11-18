"""Funções utilitárias compartilhadas."""

from datetime import datetime, timedelta


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