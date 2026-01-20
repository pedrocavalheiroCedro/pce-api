from app import config  # noqa: F401 (garante load_dotenv)
from app.schemas import (
    PushPayload,
    CalibracaoIn,
    EnsaioPatch,
    LeituraIn,
    LeituraPatch,
    LeiturasBatchRequest,
    LeiturasBatchResponse,
)
import traceback
from uuid import UUID

from fastapi import FastAPI, HTTPException
from sqlalchemy import text

from app.db import SessionLocal

app = FastAPI(title="PCE Sync API")


@app.get("/health")
def health():
    return {"status": "ok"}


# ==========================
# NOVOS ENDPOINTS (ESCRITÓRIO)
# ==========================

@app.get("/ensaios")
def list_ensaios():
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT
                    e.uuid                    AS uuid,
                    c.data_ensaio             AS data_ensaio,
                    c.codigo_obra             AS codigo_obra,
                    e.estaca_num              AS estaca,
                    e.carregamento            AS tipo_carregamento,
                    e.carga_ensaio_tf         AS carga_ensaio_tf,
                    e.carga_adm_tf            AS carga_adm_tf
                FROM estacas e
                JOIN clientes c ON c.id = e.cliente_id
                ORDER BY
                    c.data_ensaio DESC NULLS LAST,
                    c.codigo_obra ASC,
                    e.estaca_num ASC
                """
            )
        ).mappings().all()

        return {"ensaios": list(rows)}
    except Exception as e:
        print("ERROR /ensaios:", repr(e), flush=True)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.get("/ensaios/{uuid}")
def get_ensaio(uuid: UUID):
    db = SessionLocal()
    try:
        estaca_row = db.execute(
            text(
                """
                SELECT
                    e.id              AS estaca_id,
                    e.uuid            AS uuid,
                    e.carregamento    AS carregamento,
                    e.estaca_num      AS estaca_num,
                    e.tipo_estaca     AS tipo_estaca,
                    e.diametro_cm     AS diametro_cm,
                    e.profundidade_m  AS profundidade_m,
                    e.carga_adm_tf    AS carga_adm_tf,
                    e.carga_ensaio_tf AS carga_ensaio_tf,

                    c.codigo_obra     AS codigo_obra,
                    c.data_ensaio     AS data_ensaio,
                    c.cliente_nome    AS cliente_nome,
                    c.resp_obra       AS resp_obra,
                    c.tec_cedro       AS tec_cedro,
                    c.endereco        AS endereco,
                    c.cidade          AS cidade,
                    c.sondagem        AS sondagem
                FROM estacas e
                JOIN clientes c ON c.id = e.cliente_id
                WHERE e.uuid = :uuid
                """
            ),
            {"uuid": str(uuid)},
        ).mappings().first()

        if not estaca_row:
            raise HTTPException(status_code=404, detail="Ensaio não encontrado")

        estaca_id = estaca_row["estaca_id"]

        equipamento_row = db.execute(
            text(
                """
                SELECT
                    leitura,
                    cilindro_serie, cilindro_area_cm2,
                    celula_serie,
                    lvdt_serie01, lvdt_serie02, lvdt_serie03, lvdt_serie04
                FROM equipamentos
                WHERE estaca_id = :eid
                ORDER BY id DESC
                LIMIT 1
                """
            ),
            {"eid": estaca_id},
        ).mappings().first()

        # patch do capmax (já aplicado no seu main.py atual)
        cal_row = None
        try:
            if equipamento_row and equipamento_row.get("cilindro_serie"):
                cal_row = db.execute(
                    text(
                        """
                        SELECT area_cm2, carga_maxima_tf
                        FROM calibracoes
                        WHERE cilindro = :cil
                        ORDER BY id DESC
                        LIMIT 1
                        """
                    ),
                    {"cil": equipamento_row["cilindro_serie"]},
                ).mappings().first()
        except Exception:
            cal_row = None

        leituras_rows = db.execute(
            text(
                """
                SELECT
                    id,
                    estagio, row_ord,
                    carga_tf, pressao_kgf_cm2,
                    horario, tempo_estagio, tempo_estagio_min, tempo_total,
                    leitura_01, leitura_02, leitura_03, leitura_04,
                    parcial_01, parcial_02, parcial_03, parcial_04,
                    total_01, total_02, total_03, total_04,
                    total_media, estabilizado, porcentagem,
                    grafico, observacao,
                    obrigatoria, is_referencia,
                    ref_override_01, ref_override_02, ref_override_03, ref_override_04
                FROM leituras
                WHERE estaca_id = :eid
                ORDER BY estagio ASC, row_ord ASC
                """
            ),
            {"eid": estaca_id},
        ).mappings().all()

        cliente = {
            "codigo_obra": estaca_row["codigo_obra"],
            "data_ensaio": estaca_row["data_ensaio"],
            "cliente_nome": estaca_row["cliente_nome"],
            "resp_obra": estaca_row["resp_obra"],
            "tec_cedro": estaca_row["tec_cedro"],
            "endereco": estaca_row["endereco"],
            "cidade": estaca_row["cidade"],
            "sondagem": estaca_row["sondagem"],
        }

        estaca = {
            "estaca_id": estaca_row["estaca_id"],
            "uuid": estaca_row["uuid"],
            "carregamento": estaca_row["carregamento"],
            "estaca_num": estaca_row["estaca_num"],
            "tipo_estaca": estaca_row["tipo_estaca"],
            "diametro_cm": estaca_row["diametro_cm"],
            "profundidade_m": estaca_row["profundidade_m"],
            "carga_adm_tf": estaca_row["carga_adm_tf"],
            "carga_ensaio_tf": estaca_row["carga_ensaio_tf"],
        }

        equip = dict(equipamento_row) if equipamento_row else None
        if equip is not None and cal_row:
            if not equip.get("cilindro_area_cm2") and cal_row.get("area_cm2") is not None:
                equip["cilindro_area_cm2"] = cal_row.get("area_cm2")
            equip["carga_maxima_tf"] = cal_row.get("carga_maxima_tf")

        return {
            "cliente": cliente,
            "estaca": estaca,
            "equipamento": equip,
            "leituras": list(leituras_rows),
        }

    finally:
        db.close()


# =====================================================
# ENDPOINTS DE LEITURAS
# =====================================================

@app.get("/leituras")
def list_leituras(estaca_id: int):
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT
                    id,
                    estaca_id,
                    estagio, row_ord,
                    carga_tf, pressao_kgf_cm2,
                    horario, tempo_estagio, tempo_estagio_min, tempo_total,
                    leitura_01, leitura_02, leitura_03, leitura_04,
                    parcial_01, parcial_02, parcial_03, parcial_04,
                    total_01, total_02, total_03, total_04,
                    total_media, estabilizado, porcentagem,
                    grafico, observacao,
                    obrigatoria, is_referencia,
                    ref_override_01, ref_override_02, ref_override_03, ref_override_04
                FROM leituras
                WHERE estaca_id = :eid
                ORDER BY estagio ASC, row_ord ASC
                """
            ),
            {"eid": estaca_id},
        ).mappings().all()
        return {"leituras": list(rows)}
    finally:
        db.close()


@app.post("/leituras", response_model=dict)
def create_leitura(payload: LeituraIn):
    db = SessionLocal()
    try:
        row = payload.model_dump()
        cols = ", ".join(row.keys())
        vals = ", ".join([f":{k}" for k in row.keys()])
        sql = text(f"INSERT INTO leituras ({cols}) VALUES ({vals}) RETURNING id")
        new_id = db.execute(sql, row).scalar_one()
        db.commit()
        return {"id": new_id}
    finally:
        db.close()


@app.patch("/leituras/{leitura_id}", response_model=dict)
def patch_leitura(leitura_id: int, payload: LeituraPatch):
    db = SessionLocal()
    try:
        data = payload.model_dump(exclude_none=True)
        if not data:
            return {"ok": True}

        set_clause = ", ".join([f"{k} = :{k}" for k in data.keys()])
        data["id"] = leitura_id
        db.execute(text(f"UPDATE leituras SET {set_clause} WHERE id = :id"), data)
        db.commit()
        return {"ok": True}
    finally:
        db.close()


@app.delete("/leituras/{leitura_id}", response_model=dict)
def delete_leitura(leitura_id: int):
    db = SessionLocal()
    try:
        db.execute(text("DELETE FROM leituras WHERE id = :id"), {"id": leitura_id})
        db.commit()
        return {"ok": True}
    finally:
        db.close()


# =====================================================
# NOVO: BATCH UPDATE DE LEITURAS POR ENSAIO UUID
# =====================================================

@app.post("/leituras/batch", response_model=LeiturasBatchResponse)
def batch_patch_leituras(payload: LeiturasBatchRequest):
    """
    Atualiza várias leituras (linhas) de um único ensaio em 1 request.
    - payload.ensaio_uuid: UUID da estaca
    - payload.items: lista {leitura_id, patch{...}}
    """
    db = SessionLocal()
    try:
        # Resolve estaca_id (uma vez)
        estaca_row = db.execute(
            text("SELECT id FROM estacas WHERE uuid = :uuid"),
            {"uuid": str(payload.ensaio_uuid)},
        ).mappings().first()

        if not estaca_row:
            raise HTTPException(status_code=404, detail="Ensaio não encontrado")

        estaca_id = estaca_row["id"]

        # Se não tem nada para atualizar, ok.
        if not payload.items:
            return LeiturasBatchResponse(ok=True, updated=0)

        # Valida que todas as leituras pertencem ao ensaio (evita update fora do ensaio)
        ids = [it.leitura_id for it in payload.items]
        rows = db.execute(
            text(
                """
                SELECT id
                FROM leituras
                WHERE estaca_id = :eid AND id = ANY(:ids)
                """
            ),
            {"eid": estaca_id, "ids": ids},
        ).mappings().all()
        allowed_ids = {r["id"] for r in rows}

        # Atualiza em transação única
        updated = 0
        for it in payload.items:
            if it.leitura_id not in allowed_ids:
                raise HTTPException(
                    status_code=400,
                    detail=f"Leitura id={it.leitura_id} não pertence ao ensaio {payload.ensaio_uuid}",
                )

            data = it.patch.model_dump(exclude_none=True)
            if not data:
                continue

            set_clause = ", ".join([f"{k} = :{k}" for k in data.keys()])
            data["id"] = it.leitura_id
            data["estaca_id"] = estaca_id

            db.execute(
                text(f"UPDATE leituras SET {set_clause} WHERE id = :id AND estaca_id = :estaca_id"),
                data,
            )
            updated += 1

        db.commit()
        return LeiturasBatchResponse(ok=True, updated=updated)

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


# =====================================================
# PATCH DO ENSAIO
# =====================================================

@app.patch("/ensaios/{uuid}")
def patch_ensaio(uuid: UUID, payload: EnsaioPatch):
    db = SessionLocal()
    try:
        estaca_row = db.execute(
            text("SELECT id, cliente_id FROM estacas WHERE uuid = :uuid"),
            {"uuid": str(uuid)},
        ).mappings().first()
        if not estaca_row:
            raise HTTPException(status_code=404, detail="Ensaio não encontrado")

        estaca_id = estaca_row["id"]
        cliente_id = estaca_row["cliente_id"]

        data = payload.model_dump(exclude_none=True)

        cliente_fields = {}
        estaca_fields = {}
        equip_fields = {}

        for k, v in data.items():
            if k in (
                "codigo_obra",
                "data_ensaio",
                "cliente_nome",
                "resp_obra",
                "tec_cedro",
                "endereco",
                "cidade",
                "sondagem",
            ):
                cliente_fields[k] = v
            elif k in (
                "tipo_carregamento",
                "estaca_num",
                "tipo_estaca",
                "diametro_cm",
                "profundidade_m",
                "carga_adm_tf",
                "carga_ensaio_tf",
            ):
                if k == "tipo_carregamento":
                    estaca_fields["carregamento"] = v
                else:
                    estaca_fields[k] = v
            elif k in (
                "leitura",
                "celula_serie",
                "cilindro_serie",
                "cilindro_area_cm2",
                "lvdt_serie01",
                "lvdt_serie02",
                "lvdt_serie03",
                "lvdt_serie04",
            ):
                equip_fields[k] = v

        if cliente_fields:
            set_clause = ", ".join([f"{k} = :{k}" for k in cliente_fields.keys()])
            cliente_fields["id"] = cliente_id
            db.execute(text(f"UPDATE clientes SET {set_clause} WHERE id = :id"), cliente_fields)

        if estaca_fields:
            set_clause = ", ".join([f"{k} = :{k}" for k in estaca_fields.keys()])
            estaca_fields["id"] = estaca_id
            db.execute(text(f"UPDATE estacas SET {set_clause} WHERE id = :id"), estaca_fields)

        if equip_fields:
            last_id = db.execute(
                text("SELECT id FROM equipamentos WHERE estaca_id = :eid ORDER BY id DESC LIMIT 1"),
                {"eid": estaca_id},
            ).scalar()
            if last_id:
                set_clause = ", ".join([f"{k} = :{k}" for k in equip_fields.keys()])
                equip_fields["id"] = last_id
                db.execute(text(f"UPDATE equipamentos SET {set_clause} WHERE id = :id"), equip_fields)
            else:
                equip_fields["estaca_id"] = estaca_id
                cols = ", ".join(equip_fields.keys())
                vals = ", ".join([f":{k}" for k in equip_fields.keys()])
                db.execute(text(f"INSERT INTO equipamentos ({cols}) VALUES ({vals})"), equip_fields)

        db.commit()
        return {"ok": True}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


# =====================================================
# CALIBRACOES / PUSH (iguais ao seu arquivo atual)
# =====================================================

@app.get("/calibracoes")
def list_calibracoes():
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT
                    id,
                    cilindro,
                    area_cm2,
                    carga_maxima_tf
                FROM calibracoes
                ORDER BY id ASC
                """
            )
        ).mappings().all()
        return {"calibracoes": list(rows)}
    except Exception as e:
        # importante para você enxergar o motivo real no response/log
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()



@app.post("/calibracoes")
def create_calibracao(payload: CalibracaoIn):
    db = SessionLocal()
    try:
        data = payload.model_dump()
        cols = ", ".join(data.keys())
        vals = ", ".join([f":{k}" for k in data.keys()])
        new_id = db.execute(text(f"INSERT INTO calibracoes ({cols}) VALUES ({vals}) RETURNING id"), data).scalar_one()
        db.commit()
        return {"id": new_id}
    finally:
        db.close()


@app.patch("/calibracoes/{cal_id}")
def patch_calibracao(cal_id: int, payload: CalibracaoIn):
    db = SessionLocal()
    try:
        data = payload.model_dump(exclude_none=True)
        if not data:
            return {"ok": True}
        set_clause = ", ".join([f"{k} = :{k}" for k in data.keys()])
        data["id"] = cal_id
        db.execute(text(f"UPDATE calibracoes SET {set_clause} WHERE id = :id"), data)
        db.commit()
        return {"ok": True}
    finally:
        db.close()


@app.delete("/calibracoes/{cal_id}")
def delete_calibracao(cal_id: int):
    db = SessionLocal()
    try:
        db.execute(text("DELETE FROM calibracoes WHERE id = :id"), {"id": cal_id})
        db.commit()
        return {"ok": True}
    finally:
        db.close()


@app.post("/push")
def push(payload: PushPayload):
    db = SessionLocal()
    try:
        cli = payload.cliente.model_dump()
        codigo_obra = cli.get("codigo_obra")
        data_ensaio = cli.get("data_ensaio")

        row_cli = db.execute(
            text(
                """
                SELECT id
                FROM clientes
                WHERE codigo_obra = :codigo_obra AND data_ensaio = :data_ensaio
                LIMIT 1
                """
            ),
            {"codigo_obra": codigo_obra, "data_ensaio": data_ensaio},
        ).mappings().first()

        if row_cli:
            cliente_id = row_cli["id"]
            set_clause = ", ".join([f"{k} = :{k}" for k in cli.keys()])
            cli["id"] = cliente_id
            db.execute(text(f"UPDATE clientes SET {set_clause} WHERE id = :id"), cli)
        else:
            cols = ", ".join(cli.keys())
            vals = ", ".join([f":{k}" for k in cli.keys()])
            cliente_id = db.execute(
                text(f"INSERT INTO clientes ({cols}) VALUES ({vals}) RETURNING id"),
                cli,
            ).scalar_one()

        est = payload.estaca.model_dump()
        est_uuid = str(est.get("uuid"))

        row_est = db.execute(
            text("SELECT id FROM estacas WHERE uuid = :uuid LIMIT 1"),
            {"uuid": est_uuid},
        ).mappings().first()
        if row_est:
            estaca_id = row_est["id"]
            est["cliente_id"] = cliente_id
            set_clause = ", ".join([f"{k} = :{k}" for k in est.keys() if k != "uuid"])
            params = {k: v for k, v in est.items() if k != "uuid"}
            params["id"] = estaca_id
            db.execute(text(f"UPDATE estacas SET {set_clause} WHERE id = :id"), params)
        else:
            est["cliente_id"] = cliente_id
            cols = ", ".join(est.keys())
            vals = ", ".join([f":{k}" for k in est.keys()])
            estaca_id = db.execute(
                text(f"INSERT INTO estacas ({cols}) VALUES ({vals}) RETURNING id"),
                est,
            ).scalar_one()

        eq = payload.equipamento.model_dump()
        eq["estaca_id"] = estaca_id
        cols = ", ".join(eq.keys())
        vals = ", ".join([f":{k}" for k in eq.keys()])
        db.execute(text(f"INSERT INTO equipamentos ({cols}) VALUES ({vals})"), eq)

        db.execute(text("DELETE FROM leituras WHERE estaca_id = :eid"), {"eid": estaca_id})

        for leitura in payload.leituras:
            row = leitura.model_dump()
            row["estaca_id"] = estaca_id
            cols = ", ".join(row.keys())
            vals = ", ".join([f":{k}" for k in row.keys()])
            db.execute(text(f"INSERT INTO leituras ({cols}) VALUES ({vals})"), row)

        db.commit()
        return {"ok": True, "uuid": est_uuid}

    except Exception as e:
        db.rollback()
        print("ERROR /push:", repr(e), flush=True)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()
