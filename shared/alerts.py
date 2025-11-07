import requests
from datetime import datetime, timedelta
from typing import Optional, Dict, Any


def send_monitoring_alert(
        message: str,
        group_id: str = "120363421261712366",
        api_url: str = "http://189.126.105.104:9002/send-group-message"
) -> bool:
    """
    Envia alerta de monitoramento para grupo via API

    Args:
        message: Mensagem a ser enviada
        group_id: ID do grupo (padrão: configurado)
        api_url: URL da API (padrão: configurado)

    Returns:
        True se enviado com sucesso, False caso contrário
    """
    body = {
        "group_id": group_id,
        "message": message
    }

    try:
        print(f"\n{'=' * 60}")
        print("📤 ENVIANDO ALERTA")
        print(f"{'=' * 60}")
        print(f"🌐 URL: {api_url}")
        print(f"👥 Group ID: {group_id}")
        print(f"📏 Tamanho da mensagem: {len(message)} caracteres")
        print(f"📝 Primeiros 100 chars: {message[:100]}")
        print(f"{'=' * 60}\n")

        response = requests.post(api_url, json=body, timeout=10, headers={'Content-Type': 'application/json'})

        print(f"📥 Status Code: {response.status_code}")
        print(f"📥 Response: {response.text}\n")

        response.raise_for_status()
        print("✅ Alerta enviado com sucesso!")
        return True
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao enviar alerta: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"📥 Status Code: {e.response.status_code}")
            print(f"📥 Response Body: {e.response.text}")
        print()
        return False


def format_duration(seconds: float) -> str:
    """
    Formata duração em segundos para formato legível

    Args:
        seconds: Duração em segundos

    Returns:
        String formatada (ex: "1 min 39 sec", "45 sec")
    """
    if seconds < 60:
        return f"{int(seconds)} sec"

    minutes = int(seconds // 60)
    remaining_seconds = int(seconds % 60)

    if minutes < 60:
        return f"{minutes} min {remaining_seconds} sec"

    hours = int(minutes // 60)
    remaining_minutes = int(minutes % 60)
    return f"{hours}h {remaining_minutes}min"


def format_bytes(bytes_size: int) -> str:
    """
    Formata tamanho em bytes para formato legível

    Args:
        bytes_size: Tamanho em bytes

    Returns:
        String formatada (ex: "203 kB", "1.5 MB")
    """
    if bytes_size < 1024:
        return f"{bytes_size} B"
    elif bytes_size < 1024 * 1024:
        return f"{bytes_size / 1024:.0f} kB"
    elif bytes_size < 1024 * 1024 * 1024:
        return f"{bytes_size / (1024 * 1024):.1f} MB"
    else:
        return f"{bytes_size / (1024 * 1024 * 1024):.1f} GB"


def send_flow_success_alert(
        flow_name: str,
        source: str,
        destination: str,
        summary: Dict[str, Any],
        duration_seconds: float,
        job_id: Optional[str] = None,
        group_id: Optional[str] = None
) -> bool:
    """
    Envia alerta de sucesso de flow

    Args:
        flow_name: Nome do flow (ex: "Sincronização Salesforce")
        source: Origem dos dados (ex: "Salesforce")
        destination: Destino dos dados (ex: "Snowflake")
        summary: Dicionário com informações do resumo
            - records_extracted (int): Registros extraídos
            - records_loaded (int): Registros carregados
            - bytes_processed (int): Bytes processados
            - streams_processed (int, opcional): Quantidade de streams
        duration_seconds: Duração em segundos
        job_id: ID do job (opcional)
        group_id: ID do grupo para enviar

    Returns:
        True se enviado com sucesso
    """
    # Formata data/hora
    now = datetime.now()
    date_str = now.strftime("%d/%m/%Y às %H:%M")

    # Monta mensagem
    message = f"""✅ SUCESSO - {flow_name}

🔄 {source} → {destination}

Resumo:"""

    # Adiciona informações do summary
    if "records_extracted" in summary:
        message += f"\n- {summary['records_extracted']:_} registros extraídos".replace('_', '.')

    if "records_loaded" in summary:
        message += f"\n- {summary['records_loaded']:_} registros carregados".replace('_', '.')

    if "bytes_processed" in summary:
        message += f"\n- {format_bytes(summary['bytes_processed'])} processados"

    if "streams_processed" in summary:
        message += f"\n- {summary['streams_processed']} stream(s) processados"

    # Adiciona duração
    message += f"\n- Duração: {format_duration(duration_seconds)}"

    # Adiciona job ID se fornecido
    if job_id:
        message += f"\n\n🆔 Job: {job_id}"

    # Adiciona data/hora
    message += f"\n📅 {date_str}"

    # Usa grupo padrão se não especificado
    if group_id is None:
        return send_monitoring_alert(message)
    else:
        return send_monitoring_alert(message, group_id=group_id)


def send_flow_error_alert(
        flow_name: str,
        source: str,
        destination: str,
        error_message: str,
        duration_seconds: float,
        job_id: Optional[str] = None,
        partial_summary: Optional[Dict[str, Any]] = None,
        group_id: Optional[str] = None
) -> bool:
    """
    Envia alerta de erro de flow

    Args:
        flow_name: Nome do flow (ex: "Sincronização Salesforce")
        source: Origem dos dados (ex: "Salesforce")
        destination: Destino dos dados (ex: "Snowflake")
        error_message: Mensagem de erro
        duration_seconds: Duração até o erro
        job_id: ID do job (opcional)
        partial_summary: Resumo parcial antes do erro (opcional)
        group_id: ID do grupo para enviar

    Returns:
        True se enviado com sucesso
    """
    # Formata data/hora
    now = datetime.now() - timedelta(hours=3)
    date_str = now.strftime("%d/%m/%Y às %H:%M")

    # Monta mensagem
    message = f"""❌ ERRO - {flow_name}

🔄 {source} → {destination}

⚠️ Erro:
{error_message[:300]}"""  # Limita tamanho do erro

    # Adiciona resumo parcial se fornecido
    if partial_summary:
        message += "\n\nProcessado até o erro:"

        if "records_extracted" in partial_summary:
            message += f"\n- {partial_summary['records_extracted']:_} registros extraídos".replace('_', '.')

        if "records_loaded" in partial_summary:
            message += f"\n- {partial_summary['records_loaded']:_} registros carregados".replace('_', '.')

    # Adiciona duração
    message += f"\n\nDuração até erro: {format_duration(duration_seconds)}"

    # Adiciona job ID se fornecido
    if job_id:
        message += f"\n\n🆔 Job: {job_id}"

    # Adiciona data/hora
    message += f"\n📅 {date_str}"

    # Usa grupo padrão se não especificado
    if group_id is None:
        return send_monitoring_alert(message)
    else:
        return send_monitoring_alert(message, group_id=group_id)


if __name__ == "__main__":
    # Teste alerta de sucesso
    send_flow_success_alert(
        flow_name="Sincronização Salesforce",
        source="Salesforce",
        destination="Snowflake - MKT",
        summary={
            "records_extracted": 470,
            "records_loaded": 470,
            "bytes_processed": 203 * 1024,  # 203 kB
            "streams_processed": 1
        },
        duration_seconds=99,  # 1 min 39 sec
        job_id="174"
    )
