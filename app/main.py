from app import config  # noqa: F401  (garante load_dotenv)

from fastapi import FastAPI, HTTPException
from sqlalchemy import text

from app.db import SessionLocal
from app.schemas import PushPayload


app = FastAPI(title="PCE Sync API")



@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/sync/push")
def sync_push(payload: PushPayload):
    db = SessionLocal()
    try:
        # 1) verifica se estaca já existe
        row = db.execute(
            text("SELECT id FROM estacas WHERE uuid = :uuid"),
            {"uuid": str(payload.estaca.uuid)},
        ).fetchone()

        if row and not payload.overwrite:
            raise HTTPException(
                status_code=409,
                detail="Ensaio já existe. Confirme overwrite.",
            )

        # 2) cliente (sempre atualiza)
        cliente_id = db.execute(
            text(
                """
                INSERT INTO clientes
                    (codigo_obra, data_ensaio, cliente_nome, resp_obra,
                     tec_cedro, endereco, cidade, sondagem)
                VALUES
                    (:codigo_obra, :data_ensaio, :cliente_nome, :resp_obra,
                     :tec_cedro, :endereco, :cidade, :sondagem)
                RETURNING id
                """
            ),
            payload.cliente.model_dump(),
        ).scalar_one()

        # 3) estaca (UPSERT por uuid)
        estaca_id = db.execute(
            text(
                """
                INSERT INTO estacas
                    (cliente_id, uuid, carregamento, estaca_num, tipo_estaca,
                     diametro_cm, profundidade_m, carga_adm_tf, carga_ensaio_tf)
                VALUES
                    (:cliente_id, :uuid, :carregamento, :estaca_num, :tipo_estaca,
                     :diametro_cm, :profundidade_m, :carga_adm_tf, :carga_ensaio_tf)
                ON CONFLICT (uuid)
                DO UPDATE SET
                    cliente_id = EXCLUDED.cliente_id,
                    carregamento = EXCLUDED.carregamento,
                    estaca_num = EXCLUDED.estaca_num,
                    tipo_estaca = EXCLUDED.tipo_estaca,
                    diametro_cm = EXCLUDED.diametro_cm,
                    profundidade_m = EXCLUDED.profundidade_m,
                    carga_adm_tf = EXCLUDED.carga_adm_tf,
                    carga_ensaio_tf = EXCLUDED.carga_ensaio_tf
                RETURNING id
                """
            ),
            {
                "cliente_id": cliente_id,
                "uuid": str(payload.estaca.uuid),
                **payload.estaca.model_dump(exclude={"uuid"}),
            },
        ).scalar_one()

        # 4) equipamento (substitui último)
        if payload.equipamento:
            db.execute(
                text("DELETE FROM equipamentos WHERE estaca_id = :eid"),
                {"eid": estaca_id},
            )
            db.execute(
                text(
                    """
                    INSERT INTO equipamentos
                        (estaca_id, leitura, cilindro_serie, cilindro_area_cm2,
                         celula_serie, lvdt_serie01, lvdt_serie02, lvdt_serie03, lvdt_serie04)
                    VALUES
                        (:estaca_id, :leitura, :cilindro_serie, :cilindro_area_cm2,
                         :celula_serie, :lvdt_serie01, :lvdt_serie02, :lvdt_serie03, :lvdt_serie04)
                    """
                ),
                {"estaca_id": estaca_id, **payload.equipamento.model_dump()},
            )

        # 5) leituras (DELETE + INSERT)
        db.execute(
            text("DELETE FROM leituras WHERE estaca_id = :eid"),
            {"eid": estaca_id},
        )

        for leitura in payload.leituras:
            db.execute(
                text(
                    """
                    INSERT INTO leituras
                    (
                        estaca_id, estagio, row_ord,
                        carga_tf, pressao_kgf_cm2,
                        horario, tempo_estagio, tempo_estagio_min, tempo_total,
                        leitura_01, leitura_02, leitura_03, leitura_04,
                        parcial_01, parcial_02, parcial_03, parcial_04,
                        total_01, total_02, total_03, total_04,
                        total_media, estabilizado, porcentagem,
                        grafico, observacao,
                        obrigatoria, is_referencia,
                        ref_override_01, ref_override_02, ref_override_03, ref_override_04
                    )
                    VALUES
                    (
                        :estaca_id, :estagio, :row_ord,
                        :carga_tf, :pressao_kgf_cm2,
                        :horario, :tempo_estagio, :tempo_estagio_min, :tempo_total,
                        :leitura_01, :leitura_02, :leitura_03, :leitura_04,
                        :parcial_01, :parcial_02, :parcial_03, :parcial_04,
                        :total_01, :total_02, :total_03, :total_04,
                        :total_media, :estabilizado, :porcentagem,
                        :grafico, :observacao,
                        :obrigatoria, :is_referencia,
                        :ref_override_01, :ref_override_02, :ref_override_03, :ref_override_04
                    )
                    """
                ),
                {"estaca_id": estaca_id, **leitura.model_dump()},
            )

        db.commit()
        return {"status": "ok", "estaca_id": estaca_id}

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()
