# generators.py
from collections import defaultdict
import re

import sql_access as SA  # para quote_ident
# NOTA: no usamos conexión aquí; solo generamos SQL a partir del resultado de análisis y la estructura CSV.

# -------- utilidades sobre la estructura CSV --------
def tipos_por_tabla(estructura_rows):
    """
    -> {tabla: {col: tipo_sql}}
    """
    out = defaultdict(dict)
    for r in estructura_rows:
        t = (r.get("tabla") or "").strip()
        a = (r.get("atributo") or "").strip()
        ty = (r.get("tipo") or "").strip()
        if t and a:
            out[t][a] = ty or "NVARCHAR(255)"
    return out

def roles_por_tabla(estructura_rows):
    """
    -> {tabla: {col: set(['PK','FK',...])}}
    Admite combinaciones separadas por ; o ,
    """
    out = defaultdict(lambda: defaultdict(set))
    for r in estructura_rows:
        t = (r.get("tabla") or "").strip()
        a = (r.get("atributo") or "").strip()
        roles_cell = (r.get("llave") or "").strip()
        if not t or not a or not roles_cell:
            continue
        for tok in re.split(r"[;,]\s*", roles_cell):
            if tok:
                out[t][a].add(tok.strip().upper())
    return out

def _fallback_type(col, tipos_map, default="NVARCHAR(255)"):
    ty = tipos_map.get(col)
    return ty if ty else default

def _safe_name(name):
    # nombre de tabla/constraint amigable
    return re.sub(r"[^A-Za-z0-9_]", "_", name)

# -------- planificador de normalización --------
def build_plan(schema, table_sql, tabla_csv, estructura_rows, analysis_result):
    """
    Construye un plan de nuevas tablas y cambios a partir del 'analysis_result' de analysis.analyze_table().
    Retorna un dict con:
      {
        "new_tables": [
            {
              "source": "1NF"|"2NF"|"3FN-decl"|"3FN-infer",
              "name": "<nuevo_nombre>",
              "cols": [("ColName","TipoSQL"), ...],
              "pk": ["col", ...],
              "fk_from_original": {  # si corresponde (3FN típico)
                 "from_table": table_sql,
                 "from_cols": ["col", ...],
                 "to_table": "<nuevo_nombre>",
                 "to_cols": ["col", ...],
              },
              "move_from_original": ["colA","colB",...],  # columnas a mover
              "comment": "texto"
            },
            ...
        ],
        "drop_candidates": ["ColA", "ColB"],  # a eliminar del original (comentado en SQL)
        "notes": ["..."]
      }
    """
    tipos_tabla = tipos_por_tabla(estructura_rows).get(tabla_csv, {})
    pk_cols = analysis_result.get("pk_cols", []) or []
    name_groups = analysis_result.get("one_nf", {}).get("name_groups", []) or []
    two_nf = analysis_result.get("two_nf", []) or []
    three_nf = analysis_result.get("three_nf", []) or []

    plan = {"new_tables": [], "drop_candidates": [], "notes": []}

    # --- 1NF: grupos Telefono1/Telefono2...
    # Generamos una tabla hija por cada base detectada.
    for base, cols in name_groups:
        base_clean = _safe_name(base)
        new_name = f"{table_sql}_{base_clean}"
        # Elegimos tipo del primer miembro del grupo
        first_ty = _fallback_type(cols[0], tipos_tabla)
        # PK propuesta: PK original + 'n' (ordinal)
        cols_def = []
        for c in pk_cols:
            cols_def.append((c, _fallback_type(c, tipos_tabla)))
        cols_def.append(("n", "INT"))
        # una sola columna de valor para el grupo
        cols_def.append((base.rstrip("_") or "valor", first_ty))

        plan["new_tables"].append({
            "source": "1NF",
            "name": new_name,
            "cols": cols_def,
            "pk": pk_cols + ["n"],
            "fk_from_original": None,
            "move_from_original": cols,  # se van del original
            "comment": "Grupos repetidos por nombre; requiere UNPIVOT o inserciones por filas (n)."
        })
        plan["drop_candidates"].extend([c for c in cols if c not in plan["drop_candidates"]])

    # --- 2FN: agrupar por subset determinante -> atributos dependientes
    # Two_nf items: {"subset": [...], "attr": "..."}
    group_2fn = defaultdict(list)
    for item in two_nf:
        subset = tuple(item.get("subset", []))
        attr = item.get("attr")
        if subset and attr:
            group_2fn[subset].append(attr)

    for subset, attrs in group_2fn.items():
        name = f"{table_sql}_" + "_".join(_safe_name(s) for s in subset) + "_info"
        cols_def = []
        for c in subset:
            cols_def.append((c, _fallback_type(c, tipos_tabla)))
        # añadir atributos que dependen solo del subset
        for a in sorted(set(attrs)):
            cols_def.append((a, _fallback_type(a, tipos_tabla)))

        plan["new_tables"].append({
            "source": "2NF",
            "name": name,
            "cols": cols_def,
            "pk": list(subset),
            "fk_from_original": None,  # en general no podemos referenciar si subset no es UNIQUE en original
            "move_from_original": sorted(set(attrs)),
            "comment": "Dependencias parciales detectadas (2FN). Considere UNIQUE en el subset para habilitar FK si aplica."
        })
        for a in attrs:
            if a not in plan["drop_candidates"]:
                plan["drop_candidates"].append(a)

    # --- 3FN: de analysis_result.three_nf
    # Diferenciamos por el 'reason'
    #   - FD declarada: chain = "A+B -> y"
    #   - inferida:     chain = "PK1+PK2 -> a -> b"
    group_3fn_decl = defaultdict(list)  # lhs tuple -> [rhs...]
    group_3fn_infer = defaultdict(list) # 'a' -> [b...]

    for v in three_nf:
        chain = v.get("chain", "")
        reason = (v.get("reason") or "").lower()
        parts = [p.strip() for p in chain.split("->")]
        if "declarada" in reason:
            if len(parts) == 2:
                lhs_raw, rhs_raw = parts
                lhs = [c.strip() for c in lhs_raw.split("+") if c.strip()]
                rhs = [c.strip() for c in rhs_raw.split("+") if c.strip()]
                if lhs and rhs:
                    group_3fn_decl[tuple(lhs)].extend(rhs)
        else:
            # inferida (esperamos PK... -> a -> b)
            if len(parts) == 3:
                a = parts[1].strip()
                b = parts[2].strip()
                if a and b:
                    group_3fn_infer[a].append(b)

    # 3FN declaradas: una tabla por determinante LHS
    for lhs, rhs_list in group_3fn_decl.items():
        lhs_tuple = tuple(lhs)
        name = f"{table_sql}_" + "_".join(_safe_name(s) for s in lhs_tuple) + "_dim"
        cols_def = []
        for c in lhs_tuple:
            cols_def.append((c, _fallback_type(c, tipos_tabla)))
        for y in sorted(set(rhs_list)):
            cols_def.append((y, _fallback_type(y, tipos_tabla)))

        plan["new_tables"].append({
            "source": "3FN-decl",
            "name": name,
            "cols": cols_def,
            "pk": list(lhs_tuple),
            "fk_from_original": {
                "from_table": table_sql,
                "from_cols": list(lhs_tuple),
                "to_table": name,
                "to_cols": list(lhs_tuple),
            },
            "move_from_original": sorted(set(rhs_list)),
            "comment": "FD declarada con determinante no-superclave."
        })
        for y in rhs_list:
            if y not in plan["drop_candidates"]:
                plan["drop_candidates"].append(y)

    # 3FN inferida (PK -> a -> b): una tabla por 'a'
    for a, bs in group_3fn_infer.items():
        name = f"{table_sql}_{_safe_name(a)}_dim"
        cols_def = [(a, _fallback_type(a, tipos_tabla))]
        for b in sorted(set(bs)):
            cols_def.append((b, _fallback_type(b, tipos_tabla)))

        plan["new_tables"].append({
            "source": "3FN-infer",
            "name": name,
            "cols": cols_def,
            "pk": [a],
            "fk_from_original": {
                "from_table": table_sql,
                "from_cols": [a],
                "to_table": name,
                "to_cols": [a],
            },
            "move_from_original": sorted(set(bs)),
            "comment": "Dependencia transitiva inferida A->B (1-col)."
        })
        for b in bs:
            if b not in plan["drop_candidates"]:
                plan["drop_candidates"].append(b)

    # Notas útiles
    if not plan["new_tables"]:
        plan["notes"].append("No se detectaron violaciones que requieran nuevas tablas.")

    # ⬅️ NUEVO: guarda las columnas originales (de la tabla CSV) para validar INSERTs
    plan["orig_cols"] = sorted(tipos_tabla.keys())

    return plan

# -------- Render SQL --------
def render_sql(schema, table_sql, plan):
    """
    Retorna un string con el script SQL (CREATE/INSERT/ALTER) para aplicar la normalización propuesta.
    No ejecuta nada.
    """
    sb = []
    qsch = SA.quote_ident(schema)
    qorig = f"{qsch}.{SA.quote_ident(table_sql)}"

    # nombres sin corchetes para OBJECT_ID
    orig_unquoted = f"{schema}.{table_sql}"
    orig_cols = set(plan.get("orig_cols", []))  # columnas reales de la tabla original (según CSV)

    sb.append("-- =====================================================")
    sb.append(f"-- Normalización propuesta para {schema}.{table_sql}")
    sb.append("-- Generado automáticamente — revise y ajuste según su modelo")
    sb.append("-- =====================================================")
    sb.append("BEGIN TRAN;")
    sb.append("")

    for nt in plan["new_tables"]:
        name = nt["name"]
        cols = nt["cols"]                 # [(col, tipo), ...]
        pk = nt["pk"] or []
        fk = nt.get("fk_from_original")
        moved = nt.get("move_from_original", [])
        comment = nt.get("comment", "")

        qnew = f"{qsch}.{SA.quote_ident(name)}"
        new_unquoted = f"{schema}.{name}"

        sb.append(f"\n-- ---------- {nt['source']} ----------")
        if comment:
            sb.append(f"-- {comment}")

        # CREATE TABLE (OBJECT_ID usando nombre sin corchetes)
        sb.append(f"IF OBJECT_ID(N'{new_unquoted}', N'U') IS NULL")
        sb.append("BEGIN")
        cols_lines = []
        for c, ty in cols:
            cols_lines.append(f"    {SA.quote_ident(c)} {ty}")
        if pk:
            cols_lines.append(
                f"    ,CONSTRAINT {_safe_name('PK_' + name)} PRIMARY KEY ({', '.join(SA.quote_ident(c) for c in pk)})"
            )
        sb.append(f"  CREATE TABLE {qnew} (\n" + ",\n".join(cols_lines) + "\n  );")
        sb.append("END;")

        # INSERT
        if nt["source"] == "1NF":
            # 1NF requiere UNPIVOT / CROSS APPLY; dejamos plantilla clara
            sb.append(f"-- INSERT en {qnew}: requiere UNPIVOT/CROSS APPLY de columnas {', '.join(SA.quote_ident(c) for c in moved)}")
            sb.append(f"-- Ejemplo (ajuste nombres):")
            sb.append(f"-- INSERT INTO {qnew} ({', '.join(SA.quote_ident(c) for c, _ in cols)})")
            sb.append(f"-- SELECT {', '.join(SA.quote_ident(c) for c in pk)}, v.n, v.valor")
            sb.append(f"-- FROM {qorig}")
            sb.append(f"-- CROSS APPLY (")
            sb.append(f"--   VALUES (1, {SA.quote_ident(moved[0])})" if moved else "--   VALUES (1, NULL)")
            if len(moved) > 1:
                for i, c in enumerate(moved[1:], start=2):
                    sb.append(f"--        ,({i}, {SA.quote_ident(c)})")
            sb.append(f"-- ) AS v(n, valor);")
        else:
            # Verifica que TODAS las columnas de la nueva tabla existan en la tabla original
            cols_in_orig = [c for c, _ in cols if c in orig_cols]
            if len(cols_in_orig) == len(cols):
                # INSERT idempotente simple (puedes cambiar a MERGE si lo prefieres)
                sb.append(f"INSERT INTO {qnew} ({', '.join(SA.quote_ident(c) for c, _ in cols)})")
                sb.append(f"SELECT DISTINCT {', '.join(SA.quote_ident(c) for c in cols_in_orig)}")
                sb.append(f"FROM {qorig};")
            else:
                sb.append(f"-- No se pudo generar INSERT automático para {qnew}:")
                sb.append(f"-- Columnas no presentes en {orig_unquoted}: " +
                          ", ".join(SA.quote_ident(c) for c, _ in cols if c not in orig_cols))
                sb.append(f"-- Ajuste manual del INSERT.")

        # FK desde original hacia la nueva (solo si se definió en el plan)
        if fk:
            fk_name = _safe_name(f"FK_{table_sql}_{name}")
            from_cols = fk["from_cols"]
            to_cols = fk["to_cols"]
            sb.append(f"IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'{fk_name}')")
            sb.append("BEGIN")
            sb.append(f"  ALTER TABLE {qorig}")
            sb.append(f"  ADD CONSTRAINT {fk_name} FOREIGN KEY ({', '.join(SA.quote_ident(c) for c in from_cols)})")
            sb.append(f"      REFERENCES {qnew} ({', '.join(SA.quote_ident(c) for c in to_cols)});")
            sb.append("END;")

        # Sugerencia de DROP en original (comentada)
        if moved:
            sb.append(f"-- Sugerencia: eliminar columnas movidas del original (revise dependencias):")
            for c in moved:
                sb.append(f"-- ALTER TABLE {qorig} DROP COLUMN {SA.quote_ident(c)};")

        sb.append("")

    if plan.get("drop_candidates"):
        sb.append("-- Columnas candidatas a eliminar del original (ya listadas arriba por bloque):")
        sb.append("-- " + ", ".join(SA.quote_ident(c) for c in plan['drop_candidates']))
        sb.append("")

    if plan.get("notes"):
        for n in plan["notes"]:
            sb.append(f"-- Nota: {n}")

    sb.append("\n-- Si todo es correcto:")
    sb.append("-- COMMIT;")
    sb.append("-- Si hay problemas:")
    sb.append("ROLLBACK;")
    sb.append("")
    return "\n".join(sb)


# -------- API principal --------
def generate_sql(schema, table_sql, tabla_csv, estructura_rows, analysis_result):
    plan = build_plan(schema, table_sql, tabla_csv, estructura_rows, analysis_result)
    sql = render_sql(schema, table_sql, plan)
    return sql, plan
