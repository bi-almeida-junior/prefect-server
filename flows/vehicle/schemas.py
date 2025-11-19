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
        "DS_MES_REFERENCIA": fipe_data.get("MesReferencia"),
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


def transform_plate_to_snowflake_row(plate: str, vehicle_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transforma dados da API Placamaster para formato Snowflake.

    Args:
        plate: Placa do veículo
        vehicle_data: Resposta da API

    Returns:
        Dict pronto para inserção no Snowflake
    """
    color = vehicle_data.get("cor")

    return {
        "DS_PLACA": plate,
        "DS_MARCA": vehicle_data.get("marca"),
        "DS_MODELO": vehicle_data.get("modelo"),
        "NR_ANO_FABRICACAO": vehicle_data.get("ano_fabricacao"),
        "NR_ANO_MODELO": vehicle_data.get("ano_modelo"),
        "DS_COR": color.upper() if color else None,
        "DT_COLETA_API": get_datetime_brasilia()
    }
