from datetime import datetime
from typing import Optional, Dict, Any

from pydantic import BaseModel, Field, field_validator

from shared.utils import get_datetime_brasilia


class FipeMetadata(BaseModel):
    """Metadados de auditoria da consulta FIPE."""
    alternative_search: bool = False
    original_model: Optional[str] = None
    available_years: Optional[str] = None
    brand_code: str
    model_code: str
    year_fuel_code: str


class FipeValue(BaseModel):
    """Modelo para valor FIPE retornado pela API."""
    brand: str = Field(alias='Marca')
    model: str = Field(alias='Modelo')
    model_year: int = Field(alias='AnoModelo')
    fuel: str = Field(alias='Combustivel')
    fipe_code: str = Field(alias='CodigoFipe')
    value: str = Field(alias='Valor')
    reference_month: str = Field(alias='MesReferencia')
    authentication: str = Field(alias='Autenticacao')

    class Config:
        populate_by_name = True

    @field_validator('value', mode='before')
    @classmethod
    def parse_value(cls, v):
        """Remove formatação do valor."""
        if isinstance(v, str):
            return v
        return str(v)


class VehicleRecord(BaseModel):
    """Registro de veículo para processamento."""
    brand: str
    model: str
    year_manuf: int
    year_model: int
    key: str

    @property
    def brand_upper(self) -> str:
        return self.brand.upper()

    @property
    def model_upper(self) -> str:
        return self.model.upper()


def transform_to_snowflake_row(vehicle: VehicleRecord, fipe_data: Dict[str, Any], metadata: FipeMetadata) -> Dict[str, Any]:
    """
    Transforma dados da API FIPE para formato Snowflake.

    Args:
        vehicle: Dados do veículo original
        fipe_data: Resposta da API FIPE
        metadata: Metadados de auditoria

    Returns:
        Dict pronto para inserção no Snowflake
    """
    # Extrai valor numérico
    value_text = fipe_data.get("Valor", "R$ 0,00")
    value_numeric = None
    try:
        clean_value = value_text.replace("R$", "").strip().replace(".", "").replace(",", ".")
        value_numeric = float(clean_value)
    except:
        pass

    # Extrai sigla do combustível
    fuel = fipe_data.get("Combustivel", "")
    fuel_abbr = fuel[0] if fuel else None

    # Extrai mês e ano da referência FIPE (ex: "novembro de 2025" -> mes=11, ano=2025)
    reference_month_str = fipe_data.get("MesReferencia", "")
    nr_mes_referencia = None
    nr_ano_referencia = None

    if reference_month_str:
        try:
            # Mapeamento de meses em português
            meses = {
                "janeiro": 1, "fevereiro": 2, "março": 3, "abril": 4,
                "maio": 5, "junho": 6, "julho": 7, "agosto": 8,
                "setembro": 9, "outubro": 10, "novembro": 11, "dezembro": 12
            }

            parts = reference_month_str.lower().split(" de ")
            if len(parts) == 2:
                mes_nome = parts[0].strip()
                ano_str = parts[1].strip()

                nr_mes_referencia = meses.get(mes_nome)
                nr_ano_referencia = int(ano_str)
        except:
            pass

    return {
        "DS_MARCA": vehicle.brand,
        "DS_MODELO": vehicle.model,
        "NR_ANO_MODELO": vehicle.year_model,
        "DS_MODELO_API": fipe_data.get("Modelo", "").title(),
        "NR_ANO_MODELO_API": fipe_data.get("AnoModelo"),
        "FL_BUSCA_ALTERNATIVA": metadata.alternative_search,
        "DS_MODELO_ORIGINAL": metadata.original_model.title() if metadata.original_model else None,
        "DS_ANOS_DISPONIVEIS": metadata.available_years,
        "CD_MARCA_FIPE": metadata.brand_code,
        "CD_MODELO_FIPE": metadata.model_code,
        "CD_ANO_COMBUSTIVEL": metadata.year_fuel_code,
        "DS_COMBUSTIVEL": fuel,
        "DS_SIGLA_COMBUSTIVEL": fuel_abbr,
        "CD_FIPE": fipe_data.get("CodigoFipe"),
        "VL_FIPE": value_text,
        "VL_FIPE_NUMERICO": value_numeric,
        "DS_MES_REFERENCIA": reference_month_str,
        "NR_MES_REFERENCIA": nr_mes_referencia,
        "NR_ANO_REFERENCIA": nr_ano_referencia,
        "CD_AUTENTICACAO": fipe_data.get("Autenticacao"),
        "NR_TIPO_VEICULO": 1,
        "DT_CONSULTA_FIPE": get_datetime_brasilia(),
        "CHAVE_VEICULO": vehicle.key
    }


# ===== SCHEMAS PARA PLACAS =====

class PlateRecord(BaseModel):
    """Registro de placa para processamento."""
    plate: str
    date: datetime


def _safe_int(value: Any) -> Optional[int]:
    """Converte valor para int, retorna None se inválido ou fora do range PostgreSQL INTEGER."""
    if value is None:
        print(f"  ⚠️ _safe_int: valor é None")
        return None
    try:
        # Se for string mascarada (ex: "****"), retorna None
        if isinstance(value, str):
            # Remove espaços e verifica se está vazio ou só tem asteriscos
            cleaned = value.replace("*", "").replace("-", "").strip()
            if not cleaned:
                print(f"  ⚠️ _safe_int: string vazia ou mascarada: '{value}'")
                return None

        # Se for float, valida antes de converter
        if isinstance(value, float):
            # Verifica se é infinito ou NaN
            if not (-2147483648.0 <= value <= 2147483647.0):
                print(f"  ⚠️ _safe_int: float fora do range PostgreSQL: {value}")
                return None

        num = int(value)

        # Valida range para PostgreSQL INTEGER
        if num < -2147483648 or num > 2147483647:
            print(f"  ⚠️ _safe_int: número fora do range PostgreSQL INTEGER: {num}")
            return None

        # Depois valida range razoável para anos
        if num < 1900 or num > 2100:
            print(f"  ⚠️ _safe_int: ano fora do range razoável (1900-2100): {num}")
            return None

        print(f"  ✅ _safe_int: conversão OK: {value} -> {num}")
        return num
    except (ValueError, TypeError, OverflowError) as e:
        print(f"  ❌ _safe_int: erro ao converter '{value}' (tipo: {type(value)}): {e}")
        return None


def transform_plate_to_snowflake_row(plate: str, vehicle_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transforma dados da API AnyCar para formato Snowflake.

    Args:
        plate: Placa do veículo
        vehicle_data: Resposta da API

    Returns:
        Dict pronto para inserção no Snowflake
    """
    # LOG: dados da API
    print(f"\n{'='*80}")
    print(f"🔍 TRANSFORM - PLACA: {plate}")
    print(f"📥 DADOS DA API (RAW):")
    print(f"  vehicle_data completo: {vehicle_data}")
    print(f"  ano (raw): {vehicle_data.get('ano')} | Tipo: {type(vehicle_data.get('ano'))}")
    print(f"  anoModelo (raw): {vehicle_data.get('anoModelo')} | Tipo: {type(vehicle_data.get('anoModelo'))}")
    print(f"{'='*80}\n")

    color = vehicle_data.get("cor")

    # Converte anos com validação e retorna int Python nativo (não numpy.int64)
    ano_fab = _safe_int(vehicle_data.get("ano"))
    ano_modelo = _safe_int(vehicle_data.get("anoModelo"))

    # LOG: valores convertidos
    print(f"📤 VALORES CONVERTIDOS:")
    print(f"  ano_fab: {ano_fab} | Tipo: {type(ano_fab)}")
    print(f"  ano_modelo: {ano_modelo} | Tipo: {type(ano_modelo)}")
    print(f"{'='*80}\n")

    return {
        "DS_PLACA": plate,
        "DS_MARCA": vehicle_data.get("marca"),
        "DS_MODELO": vehicle_data.get("modelo"),
        "NR_ANO_FABRICACAO": ano_fab,
        "NR_ANO_MODELO": ano_modelo,
        "DS_COR": color.upper() if color else None,
        "DT_COLETA_API": get_datetime_brasilia()
    }
