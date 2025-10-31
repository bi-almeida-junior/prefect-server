"""
Shared Connections Package

Módulos de conexão reutilizáveis para diferentes serviços.
"""

from . import sftp
from . import snowflake

__all__ = ["sftp", "snowflake"]