import sys
import os
import time
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

import requests
import pandas as pd
from dotenv import load_dotenv
from prefect import task, flow, get_run_logger
from prefect.cache_policies import NONE
from prefect.artifacts import create_table_artifact
from prefect.client.schemas.schedules import CronSchedule

# Imports das conexões compartilhadas
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from shared.connections.snowflake import connect_snowflake, close_snowflake_connection
from shared.alerts import send_flow_success_alert, send_flow_error_alert

# Carrega variáveis de ambiente
load_dotenv()

# ====== CONFIGURAÇÕES ======
BATCH_SIZE = 5  # Quantidade de veículos por lote
FIPE_BASE_URL = "https://veiculos.fipe.org.br/api/veiculos"
REQUEST_DELAY = 1.0  # Delay entre requisições (segundos)

# STATUS 'I' (Inválido) - Usado para casos irrecuperáveis:
#   - Marca não mapeada em BRAND_CODES
#   - Modelo não encontrado na base FIPE após todas tentativas
#
# STATUS 'E' (Erro) - Usado para erros temporários/reprocessáveis:
#   - Erros HTTP ao consultar API FIPE (500, 502, 503, timeout)
#   - Erros de rede/conexão
# ================================================================

# Mapeamento completo de marcas (baseado na tabela DIM_MARCA_VEICULO)
BRAND_CODES = {
    "ACURA": "1", "AGRALE": "2", "ALFA ROMEO": "3", "ALFA": "3", "ROMEO": "3",
    "AM GEN": "4", "AM": "4", "GEN": "4", "ASIA MOTORS": "5", "ASIA": "5",
    "MOTORS": "5", "ASTON MARTIN": "189", "ASTON": "189", "MARTIN": "189",
    "AUDI": "6", "BABY": "207", "BMW": "7", "BRM": "8", "BUGRE": "123",
    "BYD": "238", "CAB MOTORS": "236", "CAB": "236", "CADILLAC": "10",
    "CAOA CHERY": "245", "CAOA": "245", "CHERY": "245", "CAOA CHERY/CHERY": "161",
    "CBT JIPE": "11", "CBT": "11", "JIPE": "11", "CHANA": "136", "CHANGAN": "182",
    "CHRYSLER": "12", "CITROËN": "13", "CITROEN": "13", "CROSS LANDER": "14",
    "CROSS": "14", "LANDER": "14", "D2D MOTORS": "241", "D2D": "241",
    "DAEWOO": "15", "DAIHATSU": "16", "DFSK": "246", "DODGE": "17", "EFFA": "147",
    "ENGESA": "18", "ENVEMO": "19", "FERRARI": "20", "FEVER": "249", "FIAT": "21",
    "FIBRAVAN": "149", "FORD": "22", "FOTON": "190", "FYBER": "170", "GAC": "254",
    "GEELY": "199", "GM - CHEVROLET": "23", "GM": "23", "CHEVROLET": "23",
    "GREAT WALL": "153", "GREAT": "153", "WALL": "153", "GURGEL": "24", "GWM": "240",
    "HAFEI": "152", "HITECH ELECTRIC": "214", "HITECH": "214", "ELECTRIC": "214",
    "HONDA": "25", "HYUNDAI": "26", "ISUZU": "27", "IVECO": "208", "JAC": "177",
    "JAECOO": "251", "JAGUAR": "28", "JEEP": "29", "JINBEI": "154", "JPX": "30",
    "KIA MOTORS": "31", "KIA": "31", "LADA": "32", "LAMBORGHINI": "171",
    "LAND ROVER": "33", "LAND": "33", "ROVER": "33", "LEXUS": "34", "LIFAN": "168",
    "LOBINI": "127", "LOTUS": "35", "MAHINDRA": "140", "MASERATI": "36",
    "MATRA": "37", "MAZDA": "38", "MCLAREN": "211", "MERCEDES-BENZ": "39",
    "MERCEDES": "39", "BENZ": "39", "MERCURY": "40", "MG": "167", "MINI": "156",
    "MITSUBISHI": "41", "MIURA": "42", "NETA": "250", "NISSAN": "43", "OMODA": "252",
    "PEUGEOT": "44", "PLYMOUTH": "45", "PONTIAC": "46", "PORSCHE": "47", "RAM": "185",
    "RELY": "186", "RENAULT": "48", "ROLLS-ROYCE": "195", "ROLLS": "195",
    "ROYCE": "195", "ROVER": "49", "SAAB": "50", "SATURN": "51", "SEAT": "52",
    "SERES": "247", "SHINERAY": "183", "SMART": "157", "SSANGYONG": "125",
    "SUBARU": "54", "SUZUKI": "55", "TAC": "165", "TOYOTA": "56", "TROLLER": "57",
    "VOLVO": "58", "VW - VOLKSWAGEN": "59", "VW": "59", "VOLKSWAGEN": "59",
    "WAKE": "163", "WALK": "120", "ZEEKR": "253"
}

# Headers HTTP necessários
FIPE_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Origin': 'https://veiculos.fipe.org.br',
    'Referer': 'https://veiculos.fipe.org.br/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin'
}


@task(name="get_current_fipe_reference_table", log_prints=True, cache_policy=NONE)
def get_current_fipe_reference_table() -> str:
    """Busca o código da tabela de referência mais recente (mês atual) da FIPE"""
    logger = get_run_logger()

    try:
        logger.info("📅 Buscando tabela de referência FIPE atual...")

        response = requests.post(
            f"{FIPE_BASE_URL}/ConsultarTabelaDeReferencia",
            headers=FIPE_HEADERS,
            timeout=30
        )
        time.sleep(REQUEST_DELAY)

        if response.status_code != 200:
            logger.warning(f"Erro HTTP {response.status_code}, usando tabela padrão 327")
            return "327"

        tables = response.json()

        if tables and len(tables) > 0:
            current_table = tables[0]
            code = str(current_table["Codigo"])
            month = current_table["Mes"]
            logger.info(f"✅ Tabela de referência: {month} (Código: {code})")
            return code
        else:
            logger.warning("Nenhuma tabela encontrada, usando padrão 327")
            return "327"

    except Exception as e:
        logger.warning(f"Erro ao buscar tabela de referência: {e}, usando padrão 327")
        return "327"


@task(name="get_pending_vehicles", log_prints=True, cache_policy=NONE)
def get_pending_vehicles(conn, database, schema, batch_size: int = 50) -> List[Dict[str, Any]]:
    """
    Busca registros com DS_STATUS = 'P', 'E' ou 'N' da tabela BRZ_03_VEICULO_CONSOLIDADO.

    Lógica:
    1. Busca até BATCH_SIZE registros com DS_STATUS IN ('P', 'E', 'N')
    2. Prioriza 'P' (processando/retry imediato), depois 'E' (erro/retry), depois 'N' (novos)
    3. Ordena por DT_INSERCAO DESC (mais novos primeiro)
    4. Retorna lista com marcas, modelos e anos dos veículos
    5. Inclui DS_MARCA e DS_MODELO da BRZ_03_VEICULO_CONSOLIDADO para preencher campos da tabela BRZ_04_VEICULO_FIPE

    Nota: Usamos a chave composta (marca + modelo + ano) para identificação

    Status:
    - P: Processando (prioridade máxima - retry imediato)
    - E: Erro (prioridade alta - retry)
    - N: Novo (prioridade normal)
    """
    logger = get_run_logger()
    cur = conn.cursor()

    try:
        logger.info(f"Buscando até {batch_size} veículos pendentes (prioridade: 'P' > 'E' > 'N')...")
        cur.execute(f"""
            SELECT
                DS_MARCA,
                DS_MODELO,
                NR_ANO_FABRICACAO,
                NR_ANO_MODELO,
                DS_STATUS
            FROM {database}.{schema}.BRZ_03_VEICULO_CONSOLIDADO
            WHERE DS_STATUS IN ('P', 'E', 'N')
            ORDER BY
                CASE DS_STATUS
                    WHEN 'P' THEN 1  -- Prioridade 1: Veículos processando (retry imediato)
                    WHEN 'E' THEN 2  -- Prioridade 2: Veículos com erro (retry)
                    WHEN 'N' THEN 3  -- Prioridade 3: Veículos novos
                END,
                DT_INSERCAO DESC
            LIMIT {batch_size}
        """)
        results = cur.fetchall()

        vehicles = [{
            "marca": row[0],  # DS_MARCA da BRZ_03_VEICULO_CONSOLIDADO
            "modelo": row[1],  # DS_MODELO da BRZ_03_VEICULO_CONSOLIDADO
            "ano_fab": row[2],
            "ano_mod": row[3],
            "marca_origem": row[0],  # Para preencher DS_MARCA na BRZ_04_VEICULO_FIPE
            "modelo_origem": row[1],  # Para preencher DS_MODELO na BRZ_04_VEICULO_FIPE
            "chave": f"{row[0]}|{row[1]}|{row[3]}"  # Chave única: marca|modelo|ano_mod
        } for row in results]

        # Conta quantos são de cada status
        processing_count = sum(1 for row in results if row[4] == 'P')
        retry_count = sum(1 for row in results if row[4] == 'E')
        new_count = sum(1 for row in results if row[4] == 'N')

        logger.info(f"Encontrados {len(vehicles)} veículos: {processing_count} processando + {retry_count} retry(s) + {new_count} novo(s)")
        return vehicles

    except Exception as e:
        logger.warning(f"Erro ao buscar veículos pendentes: {e}")
        logger.info("Tabela BRZ_03_VEICULO_CONSOLIDADO pode não existir ainda. Retornando lista vazia.")
        return []
    finally:
        cur.close()


@task(name="update_status_processing", log_prints=True, cache_policy=NONE)
def update_status_processing(conn, database, schema, vehicles: List[Dict[str, Any]]) -> int:
    """
    Atualiza DS_STATUS de 'N' ou 'E' para 'P' nos veículos selecionados.

    Args:
        conn: Conexão Snowflake
        database: Database
        schema: Schema
        vehicles: Lista de veículos a atualizar

    Returns:
        Número de registros atualizados
    """
    logger = get_run_logger()

    if not vehicles:
        logger.info("Nenhum veículo para atualizar status")
        return 0

    cur = conn.cursor()

    try:
        logger.info(f"Atualizando status 'N'/'E' → 'P' para {len(vehicles)} veículos...")

        # Atualiza cada veículo individualmente usando chave composta
        rows_updated = 0
        for v in vehicles:
            update_sql = f"""
                UPDATE {database}.{schema}.BRZ_03_VEICULO_CONSOLIDADO
                SET DS_STATUS = 'P'
                WHERE DS_MARCA = %s
                  AND DS_MODELO = %s
                  AND NR_ANO_MODELO = %s
                  AND (DS_STATUS = 'N' OR DS_STATUS = 'E')
            """
            cur.execute(update_sql, (v['marca'], v['modelo'], v['ano_mod']))
            rows_updated += cur.rowcount

        # Commit explícito
        conn.commit()

        logger.info(f"✓ {rows_updated} registros atualizados para status 'P'")
        return rows_updated

    except Exception as e:
        logger.error(f"Erro ao atualizar status para 'P': {e}")
        raise
    finally:
        cur.close()


@task(name="update_status_error", log_prints=True, cache_policy=NONE)
def update_status_error(conn, database, schema, vehicles: List[Dict[str, Any]]) -> int:
    """
    Atualiza DS_STATUS de 'P' para 'E' nos veículos que falharam.

    Args:
        conn: Conexão Snowflake
        database: Database
        schema: Schema
        vehicles: Lista de veículos que falharam

    Returns:
        Número de registros atualizados
    """
    logger = get_run_logger()

    if not vehicles:
        return 0

    cur = conn.cursor()

    try:
        logger.info(f"Atualizando status 'P' → 'E' para {len(vehicles)} veículos com erro...")

        # Atualiza cada veículo individualmente usando chave composta
        rows_updated = 0
        for v in vehicles:
            update_sql = f"""
                UPDATE {database}.{schema}.BRZ_03_VEICULO_CONSOLIDADO
                SET DS_STATUS = 'E'
                WHERE DS_MARCA = %s
                  AND DS_MODELO = %s
                  AND NR_ANO_MODELO = %s
                  AND DS_STATUS = 'P'
            """
            cur.execute(update_sql, (v['marca'], v['modelo'], v['ano_mod']))
            rows_updated += cur.rowcount

        # Commit explícito
        conn.commit()

        logger.info(f"✓ {rows_updated} registros atualizados para status 'E' (erro)")
        return rows_updated

    except Exception as e:
        logger.error(f"Erro ao atualizar status para 'E': {e}")
        raise
    finally:
        cur.close()


@task(name="update_status_invalid", log_prints=True, cache_policy=NONE)
def update_status_invalid(conn, database, schema, vehicles_with_reasons: Dict[str, str]) -> int:
    """
    Atualiza DS_STATUS de 'P' para 'I' (Inválido) nos veículos com erros irrecuperáveis.

    CASOS EXCLUSIVOS para status 'I':
    - Marca não mapeada em BRAND_CODES
    - Modelo não encontrado na base FIPE após todas tentativas

    Args:
        conn: Conexão Snowflake
        database: Database
        schema: Schema
        vehicles_with_reasons: Dict com chave_veiculo como chave e motivo como valor
                              Exemplo: {"FIAT|UNO|2020": "Modelo 'UNO' não encontrado na base FIPE"}

    Returns:
        Número de registros atualizados
    """
    logger = get_run_logger()

    if not vehicles_with_reasons:
        return 0

    cur = conn.cursor()

    try:
        logger.warning(f"⚠ Marcando {len(vehicles_with_reasons)} veículos como INVÁLIDOS (status 'I')")

        rows_updated = 0

        # Atualiza cada veículo individualmente para poder setar o motivo específico
        for chave, reason in vehicles_with_reasons.items():
            # Separa a chave composta (marca|modelo|ano)
            marca, modelo, ano = chave.split('|')

            update_sql = f"""
                UPDATE {database}.{schema}.BRZ_03_VEICULO_CONSOLIDADO
                SET DS_STATUS = 'I',
                    DS_MOTIVO_ERRO = %s
                WHERE DS_MARCA = %s
                  AND DS_MODELO = %s
                  AND NR_ANO_MODELO = %s
                  AND DS_STATUS = 'P'
            """

            cur.execute(update_sql, (reason, marca, modelo, ano))
            rows_updated += cur.rowcount

            logger.warning(f"  └─ [{chave}] Motivo: {reason}")

        # Commit explícito
        conn.commit()

        logger.warning(f"✓ {rows_updated} registros marcados como status 'I' (inválido/irrecuperável)")
        return rows_updated

    except Exception as e:
        logger.error(f"Erro ao atualizar status para 'I': {e}")
        raise
    finally:
        cur.close()


def search_model_by_year(brand_code: str, model_name: str, model_year: int,
                         available_models: List[Dict], table_code: str,
                         logger) -> tuple:
    """
    Busca modelo que tenha o ano desejado disponível.

    Args:
        brand_code: Código da marca na FIPE
        model_name: Nome do modelo procurado
        model_year: Ano do modelo procurado
        available_models: Lista de modelos disponíveis da marca
        table_code: Código da tabela de referência FIPE
        logger: Logger do Prefect

    Returns:
        Tupla (modelo_encontrado, ano_encontrado) ou (None, None)
    """
    logger.info(f"🔍 Buscando versão do {model_name} que tenha ano {model_year}...")

    # Dividir modelo em palavras-chave
    keywords = model_name.upper().split()

    for model in available_models:
        label_upper = model["Label"].upper()

        # Verificar se todas as palavras-chave estão no modelo
        if not all(keyword in label_upper for keyword in keywords):
            # Se não tem todas as palavras, tentar apenas com a primeira
            if len(keywords) > 0 and keywords[0] not in label_upper:
                continue

        # Testar se este modelo tem o ano desejado
        years_payload = {
            "codigoTipoVeiculo": "1",
            "codigoTabelaReferencia": table_code,
            "codigoMarca": brand_code,
            "codigoModelo": model["Value"]
        }

        try:
            response = requests.post(
                f"{FIPE_BASE_URL}/ConsultarAnoModelo",
                data=years_payload,
                headers=FIPE_HEADERS,
                timeout=30
            )
            time.sleep(REQUEST_DELAY)

            if response.status_code != 200:
                continue

            years = response.json()

            # Verificar se o ano existe
            for y in years:
                # Extrai apenas o ano (ex: "2020-5" -> "2020")
                if "-" in y["Value"]:
                    year_value = y["Value"].split("-")[0]
                else:
                    year_value = y["Label"].split()[0]

                if str(model_year) == year_value:
                    logger.info(f"✅ Versão encontrada: {model['Label']} - Ano: {y['Label']}")
                    return model, y
        except Exception as e:
            logger.debug(f"Erro ao testar modelo {model.get('Label', 'N/A')}: {e}")
            continue

    return None, None


@task(name="query_fipe_value", retries=0, log_prints=True, cache_policy=NONE)
def query_fipe_value(vehicle: Dict[str, Any], table_code: str) -> Optional[Dict[str, Any]]:
    """Consulta o valor FIPE de um veículo com lógica inteligente de busca"""
    logger = get_run_logger()

    # Obter código da marca
    brand_code = BRAND_CODES.get(vehicle["marca"])
    if not brand_code:
        logger.warning(f"[{vehicle['chave']}] Marca {vehicle['marca']} não encontrada no mapeamento")
        return {"_status": "I", "_motivo": f"Marca '{vehicle['marca']}' não mapeada em BRAND_CODES"}

    # Metadados de auditoria
    metadata = {
        "fl_busca_alternativa": False,
        "ds_modelo_original": None,
        "ds_anos_disponiveis": None,
        "cd_marca_fipe": brand_code,
        "cd_modelo_fipe": None,
        "cd_ano_combustivel": None
    }

    try:
        logger.info(f"[{vehicle['chave']}] Consultando {vehicle['marca']} {vehicle['modelo']} {vehicle['ano_mod']}")

        # ETAPA 1: Consultar Modelos da Marca
        models_payload = {
            "codigoTipoVeiculo": "1",
            "codigoTabelaReferencia": table_code,
            "codigoMarca": brand_code
        }

        response = requests.post(
            f"{FIPE_BASE_URL}/ConsultarModelos",
            data=models_payload,
            headers=FIPE_HEADERS,
            timeout=30
        )
        time.sleep(REQUEST_DELAY)

        if response.status_code != 200:
            logger.warning(f"[{vehicle['chave']}] Erro HTTP {response.status_code} ao consultar modelos")
            return None

        models_data = response.json()
        models = models_data.get("Modelos", [])

        # Buscar modelo correspondente - busca por palavras-chave
        keywords = vehicle["modelo"].upper().split()
        compatible_models = []

        # Buscar modelos que contenham TODAS as palavras-chave
        for m in models:
            label_upper = m["Label"].upper()
            if all(keyword in label_upper for keyword in keywords):
                compatible_models.append(m)

        # Se não encontrou com todas as palavras, tentar palavra por palavra (ordem sequencial)
        if not compatible_models and len(keywords) > 0:
            for i, keyword in enumerate(keywords):
                if not compatible_models:
                    logger.info(f"[{vehicle['chave']}] Nenhum modelo encontrado com '{vehicle['modelo']}', buscando por '{keyword}' (palavra {i + 1}/{len(keywords)})...")
                    for m in models:
                        if keyword in m["Label"].upper():
                            compatible_models.append(m)

                    if compatible_models:
                        logger.info(f"[{vehicle['chave']}] ✓ Encontrados {len(compatible_models)} modelos com a palavra '{keyword}'")
                        break

        if not compatible_models:
            logger.warning(f"[{vehicle['chave']}] Modelo {vehicle['modelo']} não encontrado em nenhuma tentativa")
            return {"_status": "I", "_motivo": f"Modelo '{vehicle['modelo']}' não encontrado na base FIPE"}

        # Se encontrou vários, mostrar quantidade e usar o ÚLTIMO (versão mais completa/cara)
        if len(compatible_models) > 1:
            logger.info(f"[{vehicle['chave']}] Encontrados {len(compatible_models)} modelos compatíveis, usando o último (versão mais completa)")
            logger.info(f"[{vehicle['chave']}] Modelos disponíveis: {[m['Label'] for m in compatible_models[:5]]}")  # Mostra primeiros 5

        found_model = compatible_models[-1]  # Usa o ÚLTIMO (-1) ao invés do primeiro (0)
        model_code = found_model["Value"]
        metadata["cd_modelo_fipe"] = model_code
        logger.info(f"[{vehicle['chave']}] Modelo selecionado: {found_model['Label']} (Código: {model_code})")

        # ETAPA 2: Consultar Anos do Modelo
        years_payload = {
            "codigoTipoVeiculo": "1",
            "codigoTabelaReferencia": table_code,
            "codigoMarca": brand_code,
            "codigoModelo": model_code
        }

        response = requests.post(
            f"{FIPE_BASE_URL}/ConsultarAnoModelo",
            data=years_payload,
            headers=FIPE_HEADERS,
            timeout=30
        )
        time.sleep(REQUEST_DELAY)

        if response.status_code != 200:
            logger.warning(f"[{vehicle['chave']}] Erro HTTP {response.status_code} ao consultar anos")
            return None

        years = response.json()

        # Buscar ano correspondente
        year_str = str(vehicle["ano_mod"])
        compatible_years = []

        for y in years:
            # Extrai apenas o ano da string (ex: "2020-5" -> "2020", "2020 Flex" -> "2020")
            if "-" in y["Value"]:
                year_value = y["Value"].split("-")[0]
            else:
                year_value = y["Label"].split()[0]

            if year_str == year_value:
                compatible_years.append(y)

        if not compatible_years:
            logger.warning(f"[{vehicle['chave']}] Ano {vehicle['ano_mod']} não encontrado para modelo {found_model['Label']}")

            # Salvar informações de auditoria
            available_years = [y['Label'] for y in years[:10]]
            logger.info(f"[{vehicle['chave']}] Anos disponíveis: {available_years}")
            metadata["ds_modelo_original"] = found_model["Label"]
            metadata["ds_anos_disponiveis"] = ", ".join(available_years)

            # ESTRATÉGIA: Buscar em TODAS as versões/variações do modelo que tenham o ano desejado
            logger.info(f"[{vehicle['chave']}] 🔄 Buscando outras versões/variações do modelo {vehicle['modelo']} com ano {vehicle['ano_mod']}...")
            logger.info(f"[{vehicle['chave']}] Verificando {len(compatible_models)} versões compatíveis do modelo...")

            new_model = None
            new_year = None

            # Percorre TODAS as versões compatíveis do modelo (não só a primeira)
            for idx, model_variant in enumerate(compatible_models):
                logger.info(f"[{vehicle['chave']}] Testando versão {idx + 1}/{len(compatible_models)}: {model_variant['Label']}")

                # Buscar anos desta variante
                variant_years_payload = {
                    "codigoTipoVeiculo": "1",
                    "codigoTabelaReferencia": table_code,
                    "codigoMarca": brand_code,
                    "codigoModelo": model_variant["Value"]
                }

                try:
                    response = requests.post(
                        f"{FIPE_BASE_URL}/ConsultarAnoModelo",
                        data=variant_years_payload,
                        headers=FIPE_HEADERS,
                        timeout=30
                    )
                    time.sleep(REQUEST_DELAY)

                    if response.status_code == 200:
                        variant_years = response.json()

                        # Verificar se tem o ano desejado
                        for y in variant_years:
                            if "-" in y["Value"]:
                                year_value = y["Value"].split("-")[0]
                            else:
                                year_value = y["Label"].split()[0]

                            if str(vehicle["ano_mod"]) == year_value:
                                logger.info(f"[{vehicle['chave']}] ✅ Ano {vehicle['ano_mod']} encontrado na versão: {model_variant['Label']}")
                                new_model = model_variant
                                new_year = y
                                break

                        if new_model:
                            break

                except Exception as e:
                    logger.debug(f"[{vehicle['chave']}] Erro ao consultar variante {model_variant['Label']}: {e}")
                    continue

            if not new_model:
                logger.warning(f"[{vehicle['chave']}] ❌ Nenhuma versão do {vehicle['modelo']} encontrada com ano {vehicle['ano_mod']}")
                logger.warning(f"[{vehicle['chave']}] Testadas {len(compatible_models)} versões sem sucesso")
                return None

            # Atualizar com o modelo e ano encontrados
            metadata["fl_busca_alternativa"] = True
            found_model = new_model
            model_code = new_model["Value"]
            metadata["cd_modelo_fipe"] = model_code
            found_year = new_year
            year_model_code = new_year["Value"]
            logger.info(f"[{vehicle['chave']}] ✅ Usando versão alternativa: {found_model['Label']}")
            logger.info(f"[{vehicle['chave']}] ✅ Ano: {found_year['Label']} (Código: {year_model_code})")
        else:
            # Se encontrou vários combustíveis para o mesmo ano, usar o primeiro
            if len(compatible_years) > 1:
                logger.info(f"[{vehicle['chave']}] Encontrados {len(compatible_years)} combustíveis para {year_str}, usando o primeiro")

            found_year = compatible_years[0]
            year_model_code = found_year["Value"]
            logger.info(f"[{vehicle['chave']}] Ano encontrado: {found_year['Label']} (Código: {year_model_code})")

        # Extrair ano e código do combustível
        if "-" in year_model_code:
            clean_year = year_model_code.split("-")[0]
            fuel_code = year_model_code.split("-")[1]
        else:
            clean_year = year_model_code
            fuel_code = "1"

        # Salvar código ano-combustível nos metadados
        metadata["cd_ano_combustivel"] = year_model_code

        # ETAPA 3: Consultar Valor Final
        value_payload = {
            "codigoTabelaReferencia": table_code,
            "codigoMarca": brand_code,
            "codigoModelo": model_code,
            "codigoTipoVeiculo": "1",
            "anoModelo": clean_year,
            "codigoTipoCombustivel": fuel_code,
            "tipoVeiculo": "carro",
            "modeloCodigoExterno": "",
            "tipoConsulta": "tradicional"
        }

        logger.info(f"[{vehicle['chave']}] Consultando valor com parâmetros: marca={brand_code}, modelo={model_code}, ano={clean_year}, combustivel={fuel_code}")

        response = requests.post(
            f"{FIPE_BASE_URL}/ConsultarValorComTodosParametros",
            data=value_payload,
            headers=FIPE_HEADERS,
            timeout=30
        )
        time.sleep(REQUEST_DELAY)
        final_result = response.json()

        # Verificar se houve erro
        if "codigo" in final_result and final_result["codigo"] == "2":
            logger.warning(f"[{vehicle['chave']}] Erro na API FIPE: {final_result.get('erro', 'Parâmetros inválidos')}")
            return None

        logger.info(f"[{vehicle['chave']}] ✅ Valor FIPE: {final_result.get('Valor')}")

        # Retornar resultado final com metadados de auditoria
        final_result["_metadata"] = metadata
        return final_result

    except Exception as e:
        logger.error(f"[{vehicle['chave']}] Erro ao consultar FIPE: {e}")
        return None


@task(name="process_vehicle_batch", log_prints=True, cache_policy=NONE)
def process_vehicle_batch(vehicles: List[Dict[str, Any]], table_code: str) -> Dict[str, Any]:
    """Processa um lote de veículos consultando valores FIPE"""
    logger = get_run_logger()

    results = []
    failed_vehicles = []
    invalid_vehicles_with_reasons = {}  # Dict: {chave: motivo}

    logger.info(f"Processando {len(vehicles)} veículos...")

    for i, vehicle in enumerate(vehicles, 1):
        if i % 10 == 0 or i == 1 or i == len(vehicles):
            logger.info(f"[{i}/{len(vehicles)}] Processando: {vehicle['chave']}")

        fipe_data = query_fipe_value(vehicle, table_code)

        if fipe_data:
            # Verificar se é status inválido (modelo não encontrado)
            if fipe_data.get("_status") == "I":
                reason = fipe_data.get("_motivo", "Erro não especificado")
                invalid_vehicles_with_reasons[vehicle['chave']] = reason
                logger.warning(f"[{vehicle['chave']}] ❌ {reason}")
                continue

            # Extrair metadados de auditoria
            metadata = fipe_data.get("_metadata", {})

            # Extrair valor numérico (remove "R$ " e substitui ponto/vírgula)
            value_text = fipe_data.get("Valor", "R$ 0,00")
            value_numeric = None
            try:
                clean_value = value_text.replace("R$", "").strip().replace(".", "").replace(",", ".")
                value_numeric = float(clean_value)
            except:
                pass

            # Extrair sigla do combustível (primeira letra)
            fuel = fipe_data.get("Combustivel", "")
            fuel_abbreviation = fuel[0] if fuel else None

            results.append({
                "DS_MARCA": vehicle["marca_origem"],  # Marca da BRZ_03_VEICULO_CONSOLIDADO
                "DS_MODELO": vehicle["modelo_origem"],  # Modelo da BRZ_03_VEICULO_CONSOLIDADO
                "NR_ANO_MODELO": vehicle["ano_mod"],  # Ano modelo da BRZ_03_VEICULO_CONSOLIDADO
                "DS_MODELO_API": fipe_data.get("Modelo").title(),  # Modelo retornado pela API FIPE
                "NR_ANO_MODELO_API": fipe_data.get("AnoModelo"),  # Ano modelo retornado pela API FIPE
                "FL_BUSCA_ALTERNATIVA": metadata.get("fl_busca_alternativa", False),
                "DS_MODELO_ORIGINAL": metadata.get("ds_modelo_original").title() if metadata.get("ds_modelo_original") else None,
                "DS_ANOS_DISPONIVEIS": metadata.get("ds_anos_disponiveis"),
                "CD_MARCA_FIPE": metadata.get("cd_marca_fipe"),
                "CD_MODELO_FIPE": metadata.get("cd_modelo_fipe"),
                "CD_ANO_COMBUSTIVEL": metadata.get("cd_ano_combustivel"),
                "DS_COMBUSTIVEL": fuel,
                "DS_SIGLA_COMBUSTIVEL": fuel_abbreviation,
                "CD_FIPE": fipe_data.get("CodigoFipe"),
                "VL_FIPE": value_text,
                "VL_FIPE_NUMERICO": value_numeric,
                "DS_MES_REFERENCIA": fipe_data.get("MesReferencia"),
                "CD_AUTENTICACAO": fipe_data.get("Autenticacao"),
                "NR_TIPO_VEICULO": 1,  # 1 = Carro
                "DT_CONSULTA_FIPE": (datetime.now() - timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S'),
                "CHAVE_VEICULO": vehicle["chave"]  # Chave composta para identificação
            })
        else:
            failed_vehicles.append(vehicle)

    logger.info("=" * 60)
    logger.info(f"RESULTADO: {len(results)}/{len(vehicles)} veículos processados")
    if invalid_vehicles_with_reasons:
        invalid_keys = list(invalid_vehicles_with_reasons.keys())
        logger.warning(f"Veículos inválidos ({len(invalid_vehicles_with_reasons)}): {', '.join(invalid_keys)}")
    if failed_vehicles:
        failed_keys = [v['chave'] for v in failed_vehicles]
        logger.warning(f"Veículos com erro ({len(failed_vehicles)}): {', '.join(failed_keys)}")
    logger.info("=" * 60)

    return {
        "df": pd.DataFrame(results) if results else pd.DataFrame(),
        "failed_vehicles": failed_vehicles,
        "invalid_vehicles_with_reasons": invalid_vehicles_with_reasons
    }


@task(name="insert_fipe_data_snowflake", log_prints=True, cache_policy=NONE)
def insert_fipe_data_snowflake(conn, database, schema, df: pd.DataFrame) -> int:
    """Insere dados FIPE no Snowflake"""
    logger = get_run_logger()

    if df.empty:
        logger.info("Nenhum dado para inserir")
        return 0

    cursor = conn.cursor()

    try:
        logger.info(f"Inserindo {len(df)} registros em {database}.{schema}.BRZ_04_VEICULO_FIPE...")

        insert_sql = f"""
        INSERT INTO {database}.{schema}.BRZ_04_VEICULO_FIPE
            (DS_MARCA, DS_MODELO, NR_ANO_MODELO, DS_MODELO_API, NR_ANO_MODELO_API,
             FL_BUSCA_ALTERNATIVA, DS_MODELO_ORIGINAL, DS_ANOS_DISPONIVEIS,
             CD_MARCA_FIPE, CD_MODELO_FIPE, CD_ANO_COMBUSTIVEL,
             DS_COMBUSTIVEL, DS_SIGLA_COMBUSTIVEL, CD_FIPE, VL_FIPE, VL_FIPE_NUMERICO,
             DS_MES_REFERENCIA, CD_AUTENTICACAO, NR_TIPO_VEICULO, DT_CONSULTA_FIPE)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Prepara registros para batch insert
        records = [
            (
                row['DS_MARCA'],
                row['DS_MODELO'],
                row['NR_ANO_MODELO'],
                row['DS_MODELO_API'],
                row['NR_ANO_MODELO_API'],
                row['FL_BUSCA_ALTERNATIVA'],
                row['DS_MODELO_ORIGINAL'],
                row['DS_ANOS_DISPONIVEIS'],
                row['CD_MARCA_FIPE'],
                row['CD_MODELO_FIPE'],
                row['CD_ANO_COMBUSTIVEL'],
                row['DS_COMBUSTIVEL'],
                row['DS_SIGLA_COMBUSTIVEL'],
                row['CD_FIPE'],
                row['VL_FIPE'],
                row['VL_FIPE_NUMERICO'],
                row['DS_MES_REFERENCIA'],
                row['CD_AUTENTICACAO'],
                row['NR_TIPO_VEICULO'],
                row['DT_CONSULTA_FIPE']
            )
            for _, row in df.iterrows()
        ]

        # Batch insert
        cursor.executemany(insert_sql, records)
        rows_inserted = cursor.rowcount

        # Commit explícito
        conn.commit()

        logger.info(f"✓ INSERT concluído: {rows_inserted} registros inseridos")
        return rows_inserted

    except Exception as e:
        logger.error(f"Erro ao inserir dados: {e}")
        raise
    finally:
        cursor.close()


@flow(name="vehicle_fipe_to_snowflake", log_prints=True)
def vehicle_fipe_to_snowflake(
        snowflake_account: Optional[str] = None,
        snowflake_user: Optional[str] = None,
        snowflake_private_key: Optional[str] = None,
        snowflake_warehouse: Optional[str] = None,
        snowflake_role: Optional[str] = None,
        batch_size: int = BATCH_SIZE
):
    """
    Flow principal: Consulta valores FIPE e insere no Snowflake.

    Args:
        snowflake_account: Conta Snowflake (padrão: .env)
        snowflake_user: Usuário Snowflake (padrão: .env)
        snowflake_private_key: Chave privada Snowflake (padrão: .env)
        snowflake_warehouse: Warehouse Snowflake (padrão: .env)
        snowflake_role: Role Snowflake (padrão: .env)
        batch_size: Tamanho do lote de processamento
    """
    logger = get_run_logger()
    start_time = datetime.now()

    logger.info("=" * 80)
    logger.info("FIPE CONSULTA: API → SNOWFLAKE")
    logger.info("=" * 80)

    # Carrega configurações do ambiente
    snowflake_account = snowflake_account or os.getenv("SNOWFLAKE_ACCOUNT")
    snowflake_user = snowflake_user or os.getenv("SNOWFLAKE_USER")
    snowflake_private_key = snowflake_private_key or os.getenv("SNOWFLAKE_PRIVATE_KEY")
    snowflake_warehouse = snowflake_warehouse or os.getenv("SNOWFLAKE_WAREHOUSE")
    snowflake_role = snowflake_role or os.getenv("SNOWFLAKE_ROLE")

    # Databases e schemas
    dest_database = "AJ_DATALAKEHOUSE_RPA"
    dest_schema = "BRONZE"

    conn = None
    try:
        # Buscar tabela de referência FIPE atual
        reference_table_code = get_current_fipe_reference_table()

        # Conexão Snowflake
        conn = connect_snowflake(
            account=snowflake_account,
            user=snowflake_user,
            private_key=snowflake_private_key,
            warehouse=snowflake_warehouse,
            database=dest_database,
            schema=dest_schema,
            role=snowflake_role
        )

        # Busca veículos pendentes (DS_STATUS = 'N' ou 'E')
        vehicles = get_pending_vehicles(conn, dest_database, dest_schema, batch_size)

        if not vehicles:
            logger.info("Nenhum veículo pendente para processar. Encerrando.")
            return

        # Atualiza status para 'P' (processando)
        update_status_processing(conn, dest_database, dest_schema, vehicles)

        # Processa veículos em lote
        total_inserted = 0
        total_errors = 0

        result = process_vehicle_batch(vehicles, reference_table_code)
        df = result["df"]
        failed_vehicles = result["failed_vehicles"]
        invalid_vehicles_with_reasons = result["invalid_vehicles_with_reasons"]

        # Insere no Snowflake e atualiza status 'S' para sucesso
        if not df.empty:
            rows_inserted = insert_fipe_data_snowflake(conn, dest_database, dest_schema, df)
            total_inserted = rows_inserted

            # Atualizar status 'P' → 'S' para veículos com sucesso
            success_vehicles = df['CHAVE_VEICULO'].tolist()
            if success_vehicles:
                cursor = conn.cursor()
                try:
                    # Atualiza cada veículo individualmente usando chave composta
                    rows_updated = 0
                    for chave in success_vehicles:
                        # Separa a chave composta (marca|modelo|ano)
                        marca, modelo, ano = chave.split('|')
                        update_success_sql = f"""
                            UPDATE {dest_database}.{dest_schema}.BRZ_03_VEICULO_CONSOLIDADO
                            SET DS_STATUS = 'S'
                            WHERE DS_MARCA = %s
                              AND DS_MODELO = %s
                              AND NR_ANO_MODELO = %s
                              AND DS_STATUS = 'P'
                        """
                        cursor.execute(update_success_sql, (marca, modelo, ano))
                        rows_updated += cursor.rowcount
                    conn.commit()
                    logger.info(f"✓ {rows_updated} veículos atualizados para status 'S' (sucesso)")
                finally:
                    cursor.close()

        # Atualiza status 'I' para veículos inválidos (modelo não encontrado na FIPE)
        if invalid_vehicles_with_reasons:
            update_status_invalid(conn, dest_database, dest_schema, invalid_vehicles_with_reasons)
            logger.warning(f"⚠️  {len(invalid_vehicles_with_reasons)} veículos marcados como inválidos (irrecuperáveis)")

        # Atualiza status 'E' para veículos que falharam na API
        if failed_vehicles:
            update_status_error(conn, dest_database, dest_schema, failed_vehicles)
            total_errors += len(failed_vehicles)

        # Resumo
        end_time = datetime.now()
        elapsed = end_time - start_time
        m, s = divmod(elapsed.total_seconds(), 60)

        logger.info("=" * 80)
        logger.info("PROCESSO CONCLUÍDO")
        logger.info("=" * 80)
        logger.info(f"Database: {dest_database}")
        logger.info(f"Schema:   {dest_schema}")
        logger.info(f"Tabela:   BRZ_04_VEICULO_FIPE")
        logger.info(f"Início:   {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Fim:      {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Duração:  {int(m)}m {int(s)}s")
        logger.info(f"Veículos processados: {len(vehicles)}")
        logger.info(f"Registros inseridos: {total_inserted}")
        logger.info(f"Erros encontrados: {total_errors}")

        # Artifact
        try:
            create_table_artifact(
                key="fipe-results",
                table=[{
                    "Métrica": "Veículos Processados",
                    "Valor": len(vehicles)
                }, {
                    "Métrica": "Registros Inseridos",
                    "Valor": total_inserted
                }, {
                    "Métrica": "Erros",
                    "Valor": total_errors
                }, {
                    "Métrica": "Duração (min)",
                    "Valor": f"{int(m)}m {int(s)}s"
                }],
                description=f"✅ {total_inserted} registros inseridos | {total_errors} erros"
            )
        except Exception as e:
            logger.warning(f"Erro criando artifact: {e}")

        # Alerta de sucesso
        try:
            send_flow_success_alert(
                flow_name="FIPE Consulta",
                source="API FIPE",
                destination="Snowflake",
                summary={
                    "records_loaded": total_inserted,
                    "vehicles_processed": len(vehicles),
                    "errors": total_errors
                },
                duration_seconds=elapsed.total_seconds()
            )
        except Exception as e:
            logger.warning(f"Falha ao enviar alerta de sucesso: {e}")

    except Exception as e:
        logger.error(f"Erro no flow: {e}")
        import traceback
        traceback.print_exc()

        # Alerta de erro
        try:
            elapsed_error = (datetime.now() - start_time).total_seconds()
            send_flow_error_alert(
                flow_name="FIPE Consulta",
                source="API FIPE",
                destination="Snowflake",
                error_message=str(e),
                duration_seconds=elapsed_error
            )
        except Exception as alert_error:
            logger.warning(f"Falha ao enviar alerta de erro: {alert_error}")

        raise

    finally:
        # Garante que a conexão seja fechada mesmo em caso de erro
        if conn is not None:
            try:
                close_snowflake_connection(conn)
                logger.info("✓ Conexão Snowflake fechada com sucesso")
            except Exception as close_error:
                logger.warning(f"Erro ao fechar conexão Snowflake: {close_error}")


if __name__ == "__main__":
    # Execução local para teste
    # vehicle_fipe_to_snowflake()

    # Deployment para execução agendada
    vehicle_fipe_to_snowflake.from_source(
        source=".",
        entrypoint="flows/vehicle/vehicle_fipe_to_snowflake.py:vehicle_fipe_to_snowflake"
    ).deploy(
        name="vehicle-fipe-to-snowflake",
        work_pool_name="local-pool",
        schedules=[
            CronSchedule(cron="0 * * * *", timezone="America/Sao_Paulo")
        ],
        tags=["rpa", "api", "snowflake", "bronze"],
        parameters={},
        description="🚗 Integração FIPE → Snowflake | Consulta valores de veículos na API da Tabela FIPE e carrega na camada Bronze. Processa veículos em lote, com busca inteligente de modelos e tratamento de status (N/P/E/S/I). Execução horária para manter dados atualizados.",
        version="1.0.0"
    )
