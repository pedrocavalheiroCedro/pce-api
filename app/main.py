from app import config  # noqa: F401

import traceback
import re
from uuid import UUID, uuid4

from fastapi import FastAPI, HTTPException
from sqlalchemy import text

from app.db import SessionLocal
from app.schemas import (
    PushPayload,
    CalibracaoIn,
    DuplicarEnsaioRequest,
    DuplicarEnsaioResponse,
)

app = FastAPI(title="PCE Sync API")


@app.get("/health")
def health():
    return {"status": "ok"}


# ==========================
# ENSAIOS (ESCRITÓRIO)
# ==========================

@app.get("/ensaios")
def list_ensaios():
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT
                    e.uuid            AS uuid,
                    e.uuid_origem     AS uuid_origem,
                    e.origem          AS origem,

                    c.data_ensaio     AS data_ensaio,
                    c.codigo_obra     AS codigo_obra,

                    e.estaca_num      AS estaca,
                    e.carregamento    AS tipo_carregamento,
                    e.carga_ensaio_tf AS carga_ensaio_tf,
                    e.carga_adm_tf    AS carga_adm_tf
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
                    e.uuid_origem     AS uuid_origem,
                    e.origem          AS origem,

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

        # ✅ Recupera calibracao por cilindro_serie (para area e carga_maxima_tf)
        cal_row = None
        if equipamento_row and equipamento_row.get("cilindro_serie"):
            try:
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
            "uuid_origem": estaca_row["uuid_origem"],
            "origem": estaca_row["origem"],
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
            # ✅ se area não veio no equipamento, usa calibracao
            if (equip.get("cilindro_area_cm2") is None or str(equip.get("cilindro_area_cm2")) == "") and cal_row.get("area_cm2") is not None:
                equip["cilindro_area_cm2"] = cal_row.get("area_cm2")

            # ✅ SEMPRE devolve carga_maxima_tf para o novo_page
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
# PUSH (CAMPO) - regra 409 exists + BULK INSERT
# =====================================================

def _find_estaca_by_codigo_estaca(db, codigo_obra: str, estaca_num: str):
    if not codigo_obra or not estaca_num:
        return None

    return db.execute(
        text(
            """
            SELECT
                e.id AS id,
                e.uuid AS uuid,
                e.cliente_id AS cliente_id
            FROM estacas e
            JOIN clientes c ON c.id = e.cliente_id
            WHERE c.codigo_obra = :codigo_obra
              AND e.estaca_num = :estaca_num
            ORDER BY e.id DESC
            LIMIT 1
            """
        ),
        {"codigo_obra": codigo_obra, "estaca_num": estaca_num},
    ).mappings().first()


def _push_impl(payload: PushPayload):
    db = SessionLocal()
    try:
        overwrite = bool(getattr(payload, "overwrite", False))

        # -------- Cliente --------
        cli = payload.cliente.model_dump()
        codigo_obra = (cli.get("codigo_obra") or "").strip()
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

        # -------- Estaca --------
        est = payload.estaca.model_dump()
        est_uuid = str(est.get("uuid"))
        estaca_num = (est.get("estaca_num") or "").strip()

        row_est_by_uuid = db.execute(
            text("SELECT id, origem, uuid_origem FROM estacas WHERE uuid = :uuid LIMIT 1"),
            {"uuid": est_uuid},
        ).mappings().first()

        if row_est_by_uuid:
            estaca_id = row_est_by_uuid["id"]
            origem_atual = row_est_by_uuid.get("origem")
            uuid_origem_atual = row_est_by_uuid.get("uuid_origem")

            est["cliente_id"] = cliente_id
            params = {k: v for k, v in est.items() if k != "uuid"}
            set_clause = ", ".join([f"{k} = :{k}" for k in params.keys()])
            params["id"] = estaca_id
            db.execute(text(f"UPDATE estacas SET {set_clause} WHERE id = :id"), params)

            if not origem_atual:
                db.execute(text("UPDATE estacas SET origem = 'campo' WHERE id = :id"), {"id": estaca_id})
            if not uuid_origem_atual:
                db.execute(text("UPDATE estacas SET uuid_origem = uuid WHERE id = :id"), {"id": estaca_id})

        else:
            row_exist = _find_estaca_by_codigo_estaca(db, codigo_obra, estaca_num)

            if row_exist:
                if not overwrite:
                    raise HTTPException(
                        status_code=409,
                        detail={
                            "reason": "exists",
                            "by": "codigo_obra+estaca_num",
                            "codigo_obra": codigo_obra,
                            "estaca_num": estaca_num,
                            "existing_uuid": str(row_exist.get("uuid") or ""),
                        },
                    )

                estaca_id = int(row_exist["id"])

                est_update = {k: v for k, v in est.items() if k != "uuid"}
                est_update["cliente_id"] = cliente_id
                est_update["uuid"] = est_uuid
                est_update["origem"] = "campo"
                est_update["uuid_origem"] = est_uuid

                set_clause = ", ".join([f"{k} = :{k}" for k in est_update.keys()])
                est_update["id"] = estaca_id
                db.execute(text(f"UPDATE estacas SET {set_clause} WHERE id = :id"), est_update)

            else:
                est["cliente_id"] = cliente_id
                est["origem"] = "campo"
                est["uuid_origem"] = est_uuid

                cols = ", ".join(est.keys())
                vals = ", ".join([f":{k}" for k in est.keys()])
                estaca_id = db.execute(
                    text(f"INSERT INTO estacas ({cols}) VALUES ({vals}) RETURNING id"),
                    est,
                ).scalar_one()

        # -------- Equipamentos --------
        eq = payload.equipamento.model_dump() if payload.equipamento else {}
        if eq:
            eq["estaca_id"] = estaca_id
            cols = ", ".join(eq.keys())
            vals = ", ".join([f":{k}" for k in eq.keys()])
            db.execute(text(f"INSERT INTO equipamentos ({cols}) VALUES ({vals})"), eq)

        # -------- Leituras (BULK INSERT) --------
        db.execute(text("DELETE FROM leituras WHERE estaca_id = :eid"), {"eid": estaca_id})

        rows = []
        for leitura in payload.leituras:
            d = leitura.model_dump()
            d["estaca_id"] = estaca_id
            rows.append(d)

        if rows:
            cols = list(rows[0].keys())
            cols_sql = ", ".join(cols)
            vals_sql = ", ".join([f":{c}" for c in cols])

            insert_sql = text(f"INSERT INTO leituras ({cols_sql}) VALUES ({vals_sql})")
            db.execute(insert_sql, rows)

        db.commit()
        return {"ok": True, "uuid": est_uuid}

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        print("ERROR push:", repr(e), flush=True)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.post("/push")
def push(payload: PushPayload):
    return _push_impl(payload)


@app.post("/upload")
def upload(payload: PushPayload):
    return _push_impl(payload)


@app.post("/sync/push")
def sync_push(payload: PushPayload):
    return _push_impl(payload)


@app.post("/sync/upload")
def sync_upload(payload: PushPayload):
    return _push_impl(payload)


# =====================================================
# DUPLICAR ENSAIO (ESCRITÓRIO) - VERSIONAMENTO PERFEITO
# =====================================================

def _next_escritorio_label(db, uuid_origem: str) -> str:
    rows = db.execute(
        text("SELECT origem FROM estacas WHERE uuid_origem = :u"),
        {"u": uuid_origem},
    ).mappings().all()

    max_n = -1
    for r in rows:
        o = str(r.get("origem") or "")
        m = re.search(r"(\d+)$", o.strip())
        if m:
            try:
                max_n = max(max_n, int(m.group(1)))
            except Exception:
                pass

    return f"Escritorio {max_n + 1:02d}"


@app.post("/ensaios/duplicar", response_model=DuplicarEnsaioResponse)
def duplicar_ensaio(payload: DuplicarEnsaioRequest):
    db = SessionLocal()
    try:
        original_uuid = str(payload.ensaio_uuid)

        est_row = db.execute(
            text("SELECT * FROM estacas WHERE uuid = :uuid LIMIT 1"),
            {"uuid": original_uuid},
        ).mappings().first()
        if not est_row:
            raise HTTPException(status_code=404, detail="Ensaio não encontrado")

        estaca_id_old = int(est_row["id"])
        cliente_id_old = int(est_row["cliente_id"])

        uuid_origem = str(est_row.get("uuid_origem") or original_uuid)
        origem_label = _next_escritorio_label(db, uuid_origem)

        cli_row = db.execute(
            text("SELECT * FROM clientes WHERE id = :id LIMIT 1"),
            {"id": cliente_id_old},
        ).mappings().first()
        if not cli_row:
            raise HTTPException(status_code=500, detail="Cliente do ensaio não encontrado")

        cli_data = dict(cli_row)
        cli_data.pop("id", None)

        cli_cols = ", ".join(cli_data.keys())
        cli_vals = ", ".join([f":{k}" for k in cli_data.keys()])
        cliente_id_new = db.execute(
            text(f"INSERT INTO clientes ({cli_cols}) VALUES ({cli_vals}) RETURNING id"),
            cli_data,
        ).scalar_one()

        new_uuid = str(uuid4())
        est_data = dict(est_row)
        est_data.pop("id", None)

        est_data["uuid"] = new_uuid
        est_data["cliente_id"] = int(cliente_id_new)
        est_data["uuid_origem"] = uuid_origem
        est_data["origem"] = origem_label

        est_cols = ", ".join(est_data.keys())
        est_vals = ", ".join([f":{k}" for k in est_data.keys()])
        estaca_id_new = db.execute(
            text(f"INSERT INTO estacas ({est_cols}) VALUES ({est_vals}) RETURNING id"),
            est_data,
        ).scalar_one()

        eq_rows = db.execute(
            text("SELECT * FROM equipamentos WHERE estaca_id = :eid ORDER BY id ASC"),
            {"eid": estaca_id_old},
        ).mappings().all()

        for eq in eq_rows:
            eq_data = dict(eq)
            eq_data.pop("id", None)
            eq_data["estaca_id"] = int(estaca_id_new)
            eq_cols = ", ".join(eq_data.keys())
            eq_vals = ", ".join([f":{k}" for k in eq_data.keys()])
            db.execute(text(f"INSERT INTO equipamentos ({eq_cols}) VALUES ({eq_vals})"), eq_data)

        lt_rows = db.execute(
            text("SELECT * FROM leituras WHERE estaca_id = :eid ORDER BY estagio ASC, row_ord ASC"),
            {"eid": estaca_id_old},
        ).mappings().all()

        for lt in lt_rows:
            lt_data = dict(lt)
            lt_data.pop("id", None)
            lt_data["estaca_id"] = int(estaca_id_new)
            lt_cols = ", ".join(lt_data.keys())
            lt_vals = ", ".join([f":{k}" for k in lt_data.keys()])
            db.execute(text(f"INSERT INTO leituras ({lt_cols}) VALUES ({lt_vals})"), lt_data)

        db.commit()
        return DuplicarEnsaioResponse(
            ok=True,
            original_uuid=payload.ensaio_uuid,
            novo_uuid=UUID(new_uuid),
            origem=origem_label,
        )

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        print("ERROR /ensaios/duplicar:", repr(e), flush=True)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


# =====================================================
# CALIBRACOES (voltando endpoints que o escritório usa)
# =====================================================

@app.get("/calibracoes")
def list_calibracoes():
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT id, cilindro, area_cm2, carga_maxima_tf
                FROM calibracoes
                ORDER BY cilindro ASC, id ASC
                """
            )
        ).mappings().all()
        return {"calibracoes": list(rows)}
    except Exception as e:
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
        new_id = db.execute(
            text(f"INSERT INTO calibracoes ({cols}) VALUES ({vals}) RETURNING id"),
            data,
        ).scalar_one()
        db.commit()
        return {"id": new_id}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.patch("/calibracoes/{cal_id}")
def patch_calibracao(cal_id: int, payload: CalibracaoIn):
    db = SessionLocal()
    try:
        data = payload.model_dump(exclude_none=True)

        # aceita PATCH parcial, mas mantém compatibilidade do seu frontend:
        # ele envia cilindro/area/carga sempre.
        if not data:
            return {"ok": True}

        set_clause = ", ".join([f"{k} = :{k}" for k in data.keys()])
        data["id"] = cal_id
        db.execute(text(f"UPDATE calibracoes SET {set_clause} WHERE id = :id"), data)
        db.commit()
        return {"ok": True}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.delete("/calibracoes/{cal_id}")
def delete_calibracao(cal_id: int):
    db = SessionLocal()
    try:
        db.execute(text("DELETE FROM calibracoes WHERE id = :id"), {"id": cal_id})
        db.commit()
        return {"ok": True}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


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
            {"eid": int(estaca_id)},
        ).mappings().all()

        # ✅ o grafico_page espera "data"
        return {"data": list(rows)}
    finally:
        db.close()
