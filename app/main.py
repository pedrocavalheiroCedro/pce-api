from app import config  # noqa: F401  (garante load_dotenv)
from app.schemas import PushPayload, CalibracaoIn, EnsaioPatch, LeituraIn, LeituraPatch
from typing import List, Union
from fastapi import Body
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
    """
    Lista ensaios (um por estaca.uuid) em formato de resumo para a tela Arquivos do PCE_Escritório.
    """
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
    """
    Retorna o ensaio completo (cliente + estaca + equipamento + leituras),
    para abrir no PCE_Escritório.
    """
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
            "estaca_id": estaca_row["estaca_id"],  # ✅ ADICIONE ISTO
            "uuid": estaca_row["uuid"],
            "carregamento": estaca_row["carregamento"],
            "estaca_num": estaca_row["estaca_num"],
            "tipo_estaca": estaca_row["tipo_estaca"],
            "diametro_cm": estaca_row["diametro_cm"],
            "profundidade_m": estaca_row["profundidade_m"],
            "carga_adm_tf": estaca_row["carga_adm_tf"],
            "carga_ensaio_tf": estaca_row["carga_ensaio_tf"],
        }

        return {
            "cliente": cliente,
            "estaca": estaca,
            "equipamento": dict(equipamento_row) if equipamento_row else None,
            "leituras": list(leituras_rows),
        }

    finally:
        db.close()


# =====================================================
# ENDPOINTS DE LEITURAS (para leituras_*_page do escritório)
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

        return {"data": list(rows)}

    except Exception as e:
        # ✅ melhoria 1: tratamento de erro + log
        print("ERROR /leituras:", repr(e), flush=True)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        db.close()


@app.post("/leituras")
def create_leituras(payload: Union[dict, List[dict]] = Body(...)):
    """
    Aceita 1 leitura (dict) ou lista de leituras (bulk).
    Necessita estaca_id dentro de cada item.
    """
    db = SessionLocal()
    try:
        items = payload if isinstance(payload, list) else [payload]

        created = []
        for raw in items:
            estaca_id = raw.get("estaca_id")
            if not estaca_id:
                raise HTTPException(status_code=400, detail="estaca_id é obrigatório")

            # valida apenas os campos do schema (campos extras são ignorados aqui)
            leitura = LeituraIn(**{k: raw.get(k) for k in LeituraIn.model_fields.keys()})

            new_id = db.execute(
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
                    RETURNING id
                    """
                ),
                {"estaca_id": int(estaca_id), **leitura.model_dump()},
            ).scalar_one()

            created.append({"id": new_id, "estaca_id": int(estaca_id), **leitura.model_dump()})

        db.commit()
        return {"data": created}

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        print("ERROR POST /leituras:", repr(e), flush=True)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.patch("/leituras/{leitura_id}")
def patch_leitura(leitura_id: int, body: LeituraPatch):
    db = SessionLocal()
    try:
        data = body.model_dump(exclude_unset=True)

        ALLOWED = {
+           "row_ord",
            "carga_tf", "pressao_kgf_cm2",
            "horario", "tempo_estagio", "tempo_estagio_min", "tempo_total",
            "leitura_01", "leitura_02", "leitura_03", "leitura_04",
            "parcial_01", "parcial_02", "parcial_03", "parcial_04",
            "total_01", "total_02", "total_03", "total_04",
            "total_media", "estabilizado", "porcentagem",
            "grafico", "observacao",
            "obrigatoria", "is_referencia",
            "ref_override_01", "ref_override_02", "ref_override_03", "ref_override_04",
        }
        data = {k: v for k, v in data.items() if k in ALLOWED}

        if not data:
            return {"status": "ok"}

        sets = [f"{k} = :{k}" for k in data.keys()]
        data["id"] = leitura_id

        res = db.execute(
            text(f"UPDATE leituras SET {', '.join(sets)} WHERE id = :id"),
            data,
        )

        if getattr(res, "rowcount", None) == 0:
            raise HTTPException(status_code=404, detail="Leitura não encontrada")

        db.commit()
        return {"status": "ok"}

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        print("ERROR PATCH /leituras:", repr(e), flush=True)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.delete("/ensaios/{uuid}")
def delete_ensaio(uuid: UUID):
    """
    Exclui ensaio do banco on-line.
    Como o /sync/push cria um cliente "por ensaio", removemos também o cliente associado.
    """
    db = SessionLocal()
    try:
        row = db.execute(
            text("SELECT id, cliente_id FROM estacas WHERE uuid = :uuid"),
            {"uuid": str(uuid)},
        ).fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Ensaio não encontrado")

        estaca_id, cliente_id = row[0], row[1]

        db.execute(text("DELETE FROM leituras WHERE estaca_id = :eid"), {"eid": estaca_id})
        db.execute(text("DELETE FROM equipamentos WHERE estaca_id = :eid"), {"eid": estaca_id})
        db.execute(text("DELETE FROM estacas WHERE id = :eid"), {"eid": estaca_id})
        db.execute(text("DELETE FROM clientes WHERE id = :cid"), {"cid": cliente_id})

        db.commit()
        return {"status": "ok"}

    except HTTPException:
        db.rollback()
        raise

    except Exception as e:
        db.rollback()
        print("ERROR DELETE /ensaios:", repr(e), flush=True)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        db.close()



@app.patch("/ensaios/{uuid}")
def patch_ensaio(uuid: UUID, body: EnsaioPatch):
    db = SessionLocal()
    try:
        # 1) localizar estaca e cliente
        estaca_row = db.execute(
            text("SELECT id, cliente_id FROM estacas WHERE uuid = :uuid"),
            {"uuid": str(uuid)},
        ).mappings().first()

        if not estaca_row:
            raise HTTPException(status_code=404, detail="Ensaio não encontrado.")

        estaca_id = estaca_row["id"]
        cliente_id = estaca_row["cliente_id"]

        data = body.model_dump(exclude_unset=True)

        # 2) UPDATE CLIENTE (só o que veio no PATCH)
        cliente_fields_map = {
            "codigo_obra": "codigo_obra",
            "cliente_nome": "cliente_nome",
            "resp_obra": "resp_obra",
            "tec_cedro": "tec_cedro",
            "endereco": "endereco",
            "cidade": "cidade",
            "data_ensaio": "data_ensaio",
            "sondagem": "sondagem",
        }

        sets = []
        params = {"cid": cliente_id}
        for k, col in cliente_fields_map.items():
            if k in data:
                sets.append(f"{col} = :{k}")
                params[k] = data[k]

        if sets:
            db.execute(
                text(f"UPDATE clientes SET {', '.join(sets)} WHERE id = :cid"),
                params,
            )

        # 3) UPDATE ESTACA
        estaca_sets = []
        estaca_params = {"uuid": str(uuid)}

        if "tipo_carregamento" in data:
            estaca_sets.append("carregamento = :tipo_carregamento")
            estaca_params["tipo_carregamento"] = data["tipo_carregamento"]

        if "estaca_num" in data:
            estaca_sets.append("estaca_num = :estaca_num")
            estaca_params["estaca_num"] = data["estaca_num"]

        if "tipo_estaca" in data:
            estaca_sets.append("tipo_estaca = :tipo_estaca")
            estaca_params["tipo_estaca"] = data["tipo_estaca"]

        if "diametro_cm" in data:
            estaca_sets.append("diametro_cm = :diametro_cm")
            estaca_params["diametro_cm"] = data["diametro_cm"]

        if "comprimento_cm" in data:
            # banco usa profundidade_m
            try:
                profundidade_m = float(data["comprimento_cm"]) / 100.0
            except Exception:
                profundidade_m = None
            estaca_sets.append("profundidade_m = :profundidade_m")
            estaca_params["profundidade_m"] = profundidade_m

        if "carga_adm_tf" in data:
            estaca_sets.append("carga_adm_tf = :carga_adm_tf")
            estaca_params["carga_adm_tf"] = data["carga_adm_tf"]

        if "carga_ensaio_tf" in data:
            estaca_sets.append("carga_ensaio_tf = :carga_ensaio_tf")
            estaca_params["carga_ensaio_tf"] = data["carga_ensaio_tf"]

        if estaca_sets:
            db.execute(
                text(f"UPDATE estacas SET {', '.join(estaca_sets)} WHERE uuid = :uuid"),
                estaca_params,
            )

        # 4) UPDATE/REPLACE EQUIPAMENTOS (mescla com o que já existe)
        equip_keys = {
            "leitura_equipamento": "leitura",
            "cilindro_serie": "cilindro_serie",
            "cilindro_area_cm2": "cilindro_area_cm2",
            "celula_serie": "celula_serie",
            "extensometro_01": "lvdt_serie01",
            "extensometro_02": "lvdt_serie02",
            "extensometro_03": "lvdt_serie03",
            "extensometro_04": "lvdt_serie04",
        }

        if any(k in data for k in equip_keys):
            current = db.execute(
                text("SELECT * FROM equipamentos WHERE estaca_id = :eid ORDER BY id DESC LIMIT 1"),
                {"eid": estaca_id},
            ).mappings().first()
            current = dict(current) if current else {}

            merged = {
                "leitura": current.get("leitura"),
                "cilindro_serie": current.get("cilindro_serie"),
                "cilindro_area_cm2": current.get("cilindro_area_cm2"),
                "celula_serie": current.get("celula_serie"),
                "lvdt_serie01": current.get("lvdt_serie01"),
                "lvdt_serie02": current.get("lvdt_serie02"),
                "lvdt_serie03": current.get("lvdt_serie03"),
                "lvdt_serie04": current.get("lvdt_serie04"),
            }

            for k, col in equip_keys.items():
                if k in data:
                    merged[col] = data[k]

            db.execute(text("DELETE FROM equipamentos WHERE estaca_id = :eid"), {"eid": estaca_id})
            db.execute(
                text(
                    """
                    INSERT INTO equipamentos (
                        estaca_id, leitura, cilindro_serie, cilindro_area_cm2, celula_serie,
                        lvdt_serie01, lvdt_serie02, lvdt_serie03, lvdt_serie04
                    ) VALUES (
                        :estaca_id, :leitura, :cilindro_serie, :cilindro_area_cm2, :celula_serie,
                        :lvdt_serie01, :lvdt_serie02, :lvdt_serie03, :lvdt_serie04
                    )
                    """
                ),
                {"estaca_id": estaca_id, **merged},
            )

        db.commit()
        return {"status": "ok"}

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()




# ==========================
# ENDPOINTS EXISTENTES (CAMPO)
# ==========================

@app.get("/calibracoes")
def get_calibracoes():
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT cilindro, area_cm2, carga_maxima_tf
                FROM calibracoes
                ORDER BY cilindro
                """
            )
        ).mappings().all()

        return {"calibracoes": list(rows)}
    finally:
        db.close()



@app.put("/calibracoes/{cilindro}")
def upsert_calibracao(cilindro: str, payload: CalibracaoIn):
    """
    Cria/atualiza calibração por cilindro (UPSERT).
    Isso resolve o "Novo" do escritório e também as edições na tabela.
    """
    db = SessionLocal()
    try:
        cil = cilindro.strip()

        if not cil:
            raise HTTPException(status_code=400, detail="cilindro inválido")

        area = payload.area_cm2 or 0.0
        carga = payload.carga_maxima_tf or 0.0

        db.execute(
            text(
                """
                INSERT INTO calibracoes (cilindro, area_cm2, carga_maxima_tf)
                VALUES (:cilindro, :area_cm2, :carga_maxima_tf)
                ON CONFLICT (cilindro)
                DO UPDATE SET
                    area_cm2 = EXCLUDED.area_cm2,
                    carga_maxima_tf = EXCLUDED.carga_maxima_tf
                """
            ),
            {"cilindro": cil, "area_cm2": area, "carga_maxima_tf": carga},
        )

        db.commit()
        return {"status": "ok", "cilindro": cil}

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        print("ERROR PUT /calibracoes:", repr(e), flush=True)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.delete("/calibracoes/{cilindro}")
def delete_calibracao(cilindro: str):
    """
    Exclui calibração por cilindro.
    """
    db = SessionLocal()
    try:
        cil = cilindro.strip()
        if not cil:
            raise HTTPException(status_code=400, detail="cilindro inválido")

        res = db.execute(
            text("DELETE FROM calibracoes WHERE cilindro = :cilindro"),
            {"cilindro": cil},
        )

        # res.rowcount funciona na maioria dos drivers; se vier None, tudo bem.
        if getattr(res, "rowcount", None) == 0:
            raise HTTPException(status_code=404, detail="Não encontrado")

        db.commit()
        return {"status": "ok"}

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        print("ERROR DELETE /calibracoes:", repr(e), flush=True)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()






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
            raise HTTPException(status_code=409, detail="Ensaio já existe. Confirme overwrite.")

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
            db.execute(text("DELETE FROM equipamentos WHERE estaca_id = :eid"), {"eid": estaca_id})
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
        db.execute(text("DELETE FROM leituras WHERE estaca_id = :eid"), {"eid": estaca_id})

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
        # >>> isto é o que faz aparecer o erro completo no Render <<<
        print("ERROR /sync/push:", repr(e), flush=True)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        db.close()
