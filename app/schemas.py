from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel


class LeituraPatch(BaseModel):
    carga_tf: Optional[float] = None
    pressao_kgf_cm2: Optional[float] = None

    horario: Optional[str] = None
    tempo_estagio: Optional[float] = None
    tempo_estagio_min: Optional[float] = None
    tempo_total: Optional[str] = None

    leitura_01: Optional[float] = None
    leitura_02: Optional[float] = None
    leitura_03: Optional[float] = None
    leitura_04: Optional[float] = None

    parcial_01: Optional[float] = None
    parcial_02: Optional[float] = None
    parcial_03: Optional[float] = None
    parcial_04: Optional[float] = None

    total_01: Optional[float] = None
    total_02: Optional[float] = None
    total_03: Optional[float] = None
    total_04: Optional[float] = None

    total_media: Optional[float] = None
    estabilizado: Optional[str] = None
    porcentagem: Optional[float] = None

    grafico: Optional[str] = None
    observacao: Optional[str] = None

    obrigatoria: Optional[int] = None
    is_referencia: Optional[int] = None

    ref_override_01: Optional[int] = None
    ref_override_02: Optional[int] = None
    ref_override_03: Optional[int] = None
    ref_override_04: Optional[int] = None


class EnsaioPatch(BaseModel):
    # Cliente
    codigo_obra: Optional[str] = None
    cliente_nome: Optional[str] = None
    resp_obra: Optional[str] = None
    tec_cedro: Optional[str] = None
    endereco: Optional[str] = None
    cidade: Optional[str] = None
    data_ensaio: Optional[str] = None
    sondagem: Optional[str] = None

    # Estaca
    tipo_carregamento: Optional[str] = None
    estaca_num: Optional[str] = None
    tipo_estaca: Optional[str] = None
    diametro_cm: Optional[float] = None
    comprimento_cm: Optional[float] = None
    carga_adm_tf: Optional[float] = None
    carga_ensaio_tf: Optional[float] = None

    # Equipamentos (mapeia para equipamentos.leitura / lvdt_serieXX)
    leitura_equipamento: Optional[str] = None
    cilindro_serie: Optional[str] = None
    cilindro_area_cm2: Optional[float] = None
    celula_serie: Optional[str] = None
    extensometro_01: Optional[str] = None
    extensometro_02: Optional[str] = None
    extensometro_03: Optional[str] = None
    extensometro_04: Optional[str] = None


class CalibracaoIn(BaseModel):
    cilindro: str
    area_cm2: Optional[float] = 0.0
    carga_maxima_tf: Optional[float] = 0.0


class ClienteIn(BaseModel):
    codigo_obra: str
    data_ensaio: Optional[str] = None
    cliente_nome: Optional[str] = None
    resp_obra: Optional[str] = None
    tec_cedro: Optional[str] = None
    endereco: Optional[str] = None
    cidade: Optional[str] = None
    sondagem: Optional[str] = None


class EstacaIn(BaseModel):
    uuid: UUID
    carregamento: Optional[str] = None
    estaca_num: Optional[str] = None
    tipo_estaca: Optional[str] = None
    diametro_cm: Optional[float] = None
    profundidade_m: Optional[float] = None
    carga_adm_tf: Optional[float] = None
    carga_ensaio_tf: Optional[float] = None
    # ⚠️ não exigimos 'origem' do campo aqui (API define automaticamente)


class EquipamentoIn(BaseModel):
    leitura: Optional[str] = None
    cilindro_serie: Optional[str] = None
    cilindro_area_cm2: Optional[float] = None
    celula_serie: Optional[str] = None
    lvdt_serie01: Optional[str] = None
    lvdt_serie02: Optional[str] = None
    lvdt_serie03: Optional[str] = None
    lvdt_serie04: Optional[str] = None


class LeituraIn(BaseModel):
    estagio: str
    row_ord: int

    carga_tf: Optional[float] = None
    pressao_kgf_cm2: Optional[float] = None

    horario: Optional[str] = None
    tempo_estagio: Optional[float] = None
    tempo_estagio_min: Optional[float] = None
    tempo_total: Optional[str] = None

    leitura_01: Optional[float] = None
    leitura_02: Optional[float] = None
    leitura_03: Optional[float] = None
    leitura_04: Optional[float] = None

    parcial_01: Optional[float] = None
    parcial_02: Optional[float] = None
    parcial_03: Optional[float] = None
    parcial_04: Optional[float] = None

    total_01: Optional[float] = None
    total_02: Optional[float] = None
    total_03: Optional[float] = None
    total_04: Optional[float] = None

    total_media: Optional[float] = None
    estabilizado: Optional[str] = None
    porcentagem: Optional[float] = None

    grafico: Optional[str] = None
    observacao: Optional[str] = None

    obrigatoria: Optional[int] = None
    is_referencia: Optional[int] = None

    ref_override_01: Optional[int] = None
    ref_override_02: Optional[int] = None
    ref_override_03: Optional[int] = None
    ref_override_04: Optional[int] = None


class PushPayload(BaseModel):
    overwrite: bool = False
    cliente: ClienteIn
    estaca: EstacaIn
    equipamento: Optional[EquipamentoIn] = None
    leituras: List[LeituraIn]


# =========================
# Batch update leituras
# =========================

class LeituraBatchItem(BaseModel):
    leitura_id: int
    patch: LeituraPatch


class LeiturasBatchRequest(BaseModel):
    ensaio_uuid: UUID
    items: List[LeituraBatchItem]


class LeiturasBatchResponse(BaseModel):
    ok: bool = True
    updated: int = 0


# =========================
# NOVO: duplicar ensaio (escritório)
# =========================

class DuplicarEnsaioRequest(BaseModel):
    ensaio_uuid: UUID


class DuplicarEnsaioResponse(BaseModel):
    ok: bool = True
    original_uuid: UUID
    novo_uuid: UUID
