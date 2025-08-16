# generators.py - Versión Simplificada (DROP/CREATE/INSERT only)
from collections import defaultdict
import re
import sql_access as SA  # para quote_ident

# -------- Utilidades sobre la estructura CSV --------
def tipos_por_tabla(estructura_rows):
    out = defaultdict(dict)
    for r in estructura_rows:
        t = (r.get("tabla") or "").strip()
        a = (r.get("atributo") or "").strip()
        ty = (r.get("tipo") or "").strip()
        if t and a:
            out[t][a] = ty or "NVARCHAR(255)"
    return out

def roles_por_tabla(estructura_rows):
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

def _safe_name(name):
    return re.sub(r"[^A-Za-z0-9_]", "_", name)

def _fallback_type(col, tipos_map, default="NVARCHAR(255)"):
    return tipos_map.get(col, default)

# -------- Planificador simplificado --------
def build_simple_plan(schema, table_sql, tabla_csv, estructura_rows, analysis_result):
    tipos_tabla = tipos_por_tabla(estructura_rows).get(tabla_csv, {})
    roles_tabla = roles_por_tabla(estructura_rows).get(tabla_csv, {})
    plan = {
        "original_table": {"name": table_sql, "columns": tipos_tabla, "roles": roles_tabla},
        "new_tables": [],
        "notes": []
    }

    pk_cols = analysis_result.get("pk_cols", [])
    name_groups = analysis_result.get("one_nf", {}).get("name_groups", [])
    two_nf = analysis_result.get("two_nf", [])
    three_nf = analysis_result.get("three_nf", [])

    # 1NF: grupos repetidos -> tabla detalle (requiere UNPIVOT al insertar)
    for base, cols in name_groups:
        base_clean = _safe_name(base)
        new_table_name = f"{table_sql}_{base_clean}_detalle"
        new_columns = {}
        for pk_col in pk_cols:
            new_columns[pk_col] = _fallback_type(pk_col, tipos_tabla)
        new_columns["secuencia"] = "INT"
        valor_col = base.rstrip("_") or "valor"
        new_columns[valor_col] = _fallback_type(cols[0], tipos_tabla)
        plan["new_tables"].append({
            "type": "1NF",
            "name": new_table_name,
            "columns": new_columns,
            "pk_columns": pk_cols + ["secuencia"],
            "source_columns": cols,
            "comment": f"1NF: separar columnas repetidas {', '.join(cols)}"
        })

    # 2NF: dependencias parciales
    group_2nf = defaultdict(list)
    for item in two_nf:
        subset = tuple(item.get("subset", []))
        attr = item.get("attr")
        if subset and attr:
            group_2nf[subset].append(attr)

    for subset, attrs in group_2nf.items():
        subset_name = "_".join(_safe_name(s) for s in subset)
        new_table_name = f"{table_sql}_{subset_name}_dim"
        new_columns = {}
        for col in subset:
            new_columns[col] = _fallback_type(col, tipos_tabla)
        for attr in sorted(set(attrs)):
            new_columns[attr] = _fallback_type(attr, tipos_tabla)
        plan["new_tables"].append({
            "type": "2NF",
            "name": new_table_name,
            "columns": new_columns,
            "pk_columns": list(subset),
            "source_columns": sorted(set(attrs)),
            "comment": f"2NF: parcial de {', '.join(subset)}"
        })

    # 3NF: dependencias transitivas
    for violation in three_nf:
        chain = violation.get("chain", "")
        parts = [p.strip() for p in chain.split("->")]
        if len(parts) >= 2:
            determinant = parts[-2] if len(parts) > 2 else parts[0].split("+")[-1]
            dependent = parts[-1]
            new_table_name = f"{table_sql}_{_safe_name(determinant)}_dim"
            new_columns = {
                determinant: _fallback_type(determinant, tipos_tabla),
                dependent: _fallback_type(dependent, tipos_tabla)
            }
            plan["new_tables"].append({
                "type": "3NF",
                "name": new_table_name,
                "columns": new_columns,
                "pk_columns": [determinant],
                "source_columns": [dependent],
                "comment": f"3NF: {chain}"
            })

    if not plan["new_tables"]:
        plan["notes"].append("La tabla ya está en forma normal - no requiere normalización")

    return plan

# -------- Generador DROP/CREATE/INSERT --------
def render_simple_sql(schema, plan):
    """
    Genera SOLO:
      - respaldo con SELECT INTO __OLD
      - DROP TABLE original
      - CREATE TABLE original (estructura corregida)
      - INSERT a original desde __OLD (solo columnas que permanecen)
      - CREATE & INSERT de tablas nuevas (2NF/3NF); 1NF deja plantilla UNPIVOT
      - DROP TABLE __OLD
    Sin transacciones, sin IF EXISTS, sin PRINT.
    """
    sql = []
    schema_q = SA.quote_ident(schema)
    base_name = plan["original_table"]["name"]
    base_q = SA.quote_ident(base_name)
    orig_full = f"{schema_q}.{base_q}"
    old_full = f"{schema_q}.{SA.quote_ident(base_name + '__OLD')}"

    # Columnas originales (según template) y roles
    tipos_map = plan["original_table"]["columns"] or {}
    roles_map = plan["original_table"]["roles"] or {}
    pk_cols = [c for c, roles in roles_map.items() if 'PK' in roles]

    # Columnas movidas a tablas nuevas (2NF/3NF) -> no se quedan en la base
    moved = set()
    for tinfo in plan["new_tables"]:
        if tinfo["type"] in ("2NF", "3NF"):
            moved.update(tinfo.get("source_columns", []))

    # Columnas que permanecen en la tabla base (mantener PK siempre)
    stay_cols = []
    for c in tipos_map.keys():
        if c in pk_cols or c not in moved:
            stay_cols.append(c)

    # Respaldo de datos
    sql.append(f"SELECT * INTO {old_full} FROM {orig_full};")
    # Eliminar tabla original
    sql.append(f"DROP TABLE {orig_full};")

    # Crear tabla original con estructura corregida
    col_defs = []
    for col in stay_cols:
        c_q = SA.quote_ident(col)
        ty = tipos_map.get(col, "NVARCHAR(255)")
        not_null = " NOT NULL" if col in pk_cols else ""
        col_defs.append(f"    {c_q} {ty}{not_null}")
    if pk_cols:
        pkname = f"PK_{_safe_name(base_name)}"
        pk_cols_q = ", ".join(SA.quote_ident(c) for c in pk_cols if c in stay_cols)
        if pk_cols_q:
            col_defs.append(f"    CONSTRAINT {pkname} PRIMARY KEY ({pk_cols_q})")

    sql.append(f"CREATE TABLE {orig_full} (\n" + ",\n".join(col_defs) + "\n);")

    # Insertar datos a la tabla base desde __OLD
    if stay_cols:
        cols_q = ", ".join(SA.quote_ident(c) for c in stay_cols)
        sql.append(f"INSERT INTO {orig_full} ({cols_q})")
        sql.append(f"SELECT DISTINCT {cols_q} FROM {old_full};")

    # Crear e insertar en tablas nuevas
    for tinfo in plan["new_tables"]:
        tname = tinfo["name"]
        t_full = f"{schema_q}.{SA.quote_ident(tname)}"
        cols = tinfo["columns"]
        pkc = tinfo.get("pk_columns", [])
        src = tinfo.get("source_columns", [])

        # CREATE
        t_defs = []
        for c, ty in cols.items():
            c_q = SA.quote_ident(c)
            not_null = " NOT NULL" if c in pkc else ""
            t_defs.append(f"    {c_q} {ty}{not_null}")
        if pkc:
            pkname = f"PK_{_safe_name(tname)}"
            pk_cols_q = ", ".join(SA.quote_ident(c) for c in pkc)
            t_defs.append(f"    CONSTRAINT {pkname} PRIMARY KEY ({pk_cols_q})")
        sql.append(f"CREATE TABLE {t_full} (\n" + ",\n".join(t_defs) + "\n);")

        # INSERT (2NF/3NF directos; 1NF plantilla UNPIVOT)
        if tinfo["type"] == "1NF":
            # Plantilla mínima para que el script sea DROP/CREATE/INSERT-only
            # Reemplazar UNPIVOT por el patrón real.
            valor_col = [c for c in cols.keys() if c not in (pkc + ["secuencia"])][-1] if cols else "valor"
            pk_sel = ", ".join(SA.quote_ident(c) for c in pkc if c in tipos_map)
            # Comentario y plantilla
            sql.append(f"-- TODO UNPIVOT para {t_full}: ajustar según columnas repetidas ({', '.join(src)})")
            sql.append(f"-- INSERT INTO {t_full} ({pk_sel}, secuencia, {SA.quote_ident(valor_col)})")
            sql.append(f"-- SELECT {pk_sel}, seq AS secuencia, val AS {SA.quote_ident(valor_col)}")
            sql.append(f"-- FROM (")
            sql.append(f"--   SELECT {pk_sel}, {', '.join(SA.quote_ident(c) for c in src)} FROM {old_full}")
            sql.append(f"-- ) AS s")
            sql.append(f"-- UNPIVOT (val FOR col IN ({', '.join(SA.quote_ident(c) for c in src)})) AS u")
            sql.append(f"-- CROSS APPLY (SELECT ROW_NUMBER() OVER (PARTITION BY {pk_sel} ORDER BY (SELECT 1)) AS seq) AS x;")
        else:
            # 2NF/3NF: seleccionar determinantes + dependientes desde __OLD
            cols_in_old = [c for c in cols.keys()]
            # Evitar insertar columnas que no existían en __OLD (p.ej. columnas nuevas calculadas)
            insertable = [c for c in cols_in_old if c in tipos_map or c in pkc]
            if insertable:
                cols_q2 = ", ".join(SA.quote_ident(c) for c in insertable)
                sql.append(f"INSERT INTO {t_full} ({cols_q2})")
                sql.append(f"SELECT DISTINCT {cols_q2} FROM {old_full}")
                # filtros básicos para no traer filas vacías en dependientes
                # (si hay dependientes declarados)
                dep_cols = [c for c in tinfo.get("source_columns", []) if c in insertable]
                if dep_cols:
                    conds = " OR ".join(f"{SA.quote_ident(c)} IS NOT NULL" for c in dep_cols)
                    sql[-1] += f" WHERE {conds}"
                sql.append(";")

    # Eliminar respaldo
    sql.append(f"DROP TABLE {old_full};")

    return "\n".join(sql)

# -------- API principal --------
def generate_simple_sql(schema, table_sql, tabla_csv, estructura_rows, analysis_result):
    plan = build_simple_plan(schema, table_sql, tabla_csv, estructura_rows, analysis_result)
    sql = render_simple_sql(schema, plan)
    return sql, plan

def generate_sql(schema, table_sql, tabla_csv, estructura_rows, analysis_result):
    return generate_simple_sql(schema, table_sql, tabla_csv, estructura_rows, analysis_result)
