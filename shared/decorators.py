"""
Decoradores reutilizáveis para flows Prefect.

Automatiza tarefas repetitivas como envio de alertas de sucesso/erro.
"""

from functools import wraps
from datetime import datetime
from typing import Callable, Any, Optional, Dict
from prefect import get_run_logger

from .alerts import send_flow_success_alert, send_flow_error_alert


def flow_alerts(
    flow_name: str,
    source: str,
    destination: str,
    extract_summary: Optional[Callable[[Any], Dict[str, Any]]] = None,
    group_id: Optional[str] = None
):
    """
    Decorador que envia alertas automáticos de sucesso ou erro para flows Prefect.

    Uso:
    ```python
    @flow
    @flow_alerts(
        flow_name="Clima HGBrasil",
        source="API HGBrasil",
        destination="Snowflake",
        extract_summary=lambda result: {
            "cities_processed": result['cities'],
            "records_loaded": result['records']
        }
    )
    def my_flow():
        # seu código aqui
        return {"cities": 5, "records": 100}
    ```

    Args:
        flow_name: Nome do flow para exibir no alerta
        source: Origem dos dados (ex: "API HGBrasil", "Salesforce")
        destination: Destino dos dados (ex: "Snowflake", "PostgreSQL")
        extract_summary: Função opcional que extrai resumo do resultado do flow.
                        Deve retornar dict com campos como:
                        - records_extracted (int)
                        - records_loaded (int)
                        - bytes_processed (int)
                        - streams_processed (int)
                        Ou campos customizados que serão adicionados ao resumo.
        group_id: ID do grupo WhatsApp para enviar alertas (opcional)

    Returns:
        Decorador que envolve a função do flow
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger = get_run_logger()
            start_time = datetime.now()

            try:
                # Executa o flow
                result = func(*args, **kwargs)

                # Calcula duração
                duration_seconds = (datetime.now() - start_time).total_seconds()

                # Extrai resumo do resultado se função fornecida
                summary = {}
                if extract_summary and result is not None:
                    try:
                        summary = extract_summary(result)
                    except Exception as e:
                        logger.warning(f"Erro ao extrair resumo: {e}")
                        summary = {}

                # Envia alerta de sucesso
                try:
                    send_flow_success_alert(
                        flow_name=flow_name,
                        source=source,
                        destination=destination,
                        summary=summary,
                        duration_seconds=duration_seconds,
                        group_id=group_id
                    )
                except Exception as e:
                    logger.warning(f"Falha ao enviar alerta de sucesso: {e}")

                return result

            except Exception as e:
                # Calcula duração até o erro
                duration_seconds = (datetime.now() - start_time).total_seconds()

                # Envia alerta de erro
                try:
                    send_flow_error_alert(
                        flow_name=flow_name,
                        source=source,
                        destination=destination,
                        error_message=str(e),
                        duration_seconds=duration_seconds,
                        group_id=group_id
                    )
                except Exception as alert_error:
                    logger.warning(f"Falha ao enviar alerta de erro: {alert_error}")

                # Re-raise a exceção original
                raise

        return wrapper
    return decorator


def flow_alerts_simple(flow_name: str, source: str, destination: str):
    """
    Versão simplificada do decorador flow_alerts sem extração de resumo.

    Uso:
    ```python
    @flow
    @flow_alerts_simple(
        flow_name="Sincronização Simples",
        source="API",
        destination="Snowflake"
    )
    def my_flow():
        # seu código aqui
        pass
    ```

    Args:
        flow_name: Nome do flow
        source: Origem dos dados
        destination: Destino dos dados

    Returns:
        Decorador que envolve a função do flow
    """
    return flow_alerts(
        flow_name=flow_name,
        source=source,
        destination=destination,
        extract_summary=None
    )
