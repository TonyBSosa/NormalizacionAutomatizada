import re
from itertools import combinations

import sql_access as SA
import validators as V

# ------------------ Estructura desde CSV ------------------
def estructura_por_tabla(rows):
    """
    Devuelve:
      - attrs_por_tabla: {tabla: [atributos_en_orden]}
      - fds_declaradas:  {tabla: list[(lhs_cols, rhs_cols)]}
      - llaves:          {tabla: {col: "PK"/"PK(part)"/"FK"...}}
    """
    attrs = {}
    fds = {}
    llaves = {}
    for r in rows:
        t = (r.get("tabla") or "").strip()
        a = (r.get("atributo") or "").strip()
        l = (r.get("llave") or "").strip()
        df_cell = (r.get("dependencia_funcional") or "").strip()
        if not t or not a:
            continue
        attrs.setdefault(t, []).append(a)
        if l:
            llaves.setdefault(t, {})[a] = l
        if df_cell:
            for piece in [p.strip() for p in df_cell.split(";") if p.strip()]:
                lhs, rhs = V._parse_fd(piece)
                fds.setdefault(t, []).append((lhs, rhs))
    return attrs, fds, llaves

# ------------------ 1FN Heurística ------------------
SEP_CHARS = [",", ";", "/", "|", "[", "]"]

def es_valor_atomico(val):
    if val is None:
        return True
    s = str(val)
    if any(sep in s for sep in SEP_CHARS):
        return False
    if s.strip().startswith("{") or s.strip().startswith("["):
        return False
    return True

def detectar_repetidos_nombrado(attrs):
    """
    Telefono1, Telefono2 ... => sugiere tabla hija.
    """
    groups = {}
    for a in attrs:
        m = re.match(r"^(.*?)(\d+)$", a)
        if m:
            base = m.group(1).strip().lower()
            groups.setdefault(base, []).append(a)
    return [(b, cols) for b, cols in groups.items() if len(cols) > 1]

# ------------------ 2FN/3FN helpers SQL ------------------
def has_fd_sql(conn, schema, table, lhs_cols, rhs_col, ignore_null_rhs=True):
    """
    LHS -> RHS: no debe existir un grupo de LHS con más de 1 valor distinto de RHS.
    """
    lhs_list = [SA.quote_ident(c) for c in lhs_cols]
    rhs = SA.quote_ident(rhs_col)
    where = f"WHERE {rhs} IS NOT NULL" if ignore_null_rhs else ""
    q = f"""
    SELECT TOP 1 1
    FROM (
        SELECT {', '.join(lhs_list)}, COUNT(DISTINCT {rhs}) AS d
        FROM {SA.quote_ident(schema)}.{SA.quote_ident(table)}
        {where}
        GROUP BY {', '.join(lhs_list)}
        HAVING COUNT(DISTINCT {rhs}) > 1
    ) z;
    """
    with conn.cursor() as cur:
        cur.execute(q)
        row = cur.fetchone()
        return row is None  # True si NO hubo violación -> FD probable

def is_unique_sql(conn, schema, table, cols):
    cols_q = ", ".join(SA.quote_ident(c) for c in cols)
    q = f"""
    SELECT TOP 1 1
    FROM {SA.quote_ident(schema)}.{SA.quote_ident(table)}
    GROUP BY {cols_q}
    HAVING COUNT(*) > 1;
    """
    with conn.cursor() as cur:
        cur.execute(q)
        row = cur.fetchone()
        return row is None

# ------------------ Análisis principal ------------------
def analyze_table(conn, schema, table, tabla_csv, estructura_rows, cfg):
    attrs_por_tabla, fds_decl, llaves = estructura_por_tabla(estructura_rows)
    attrs = attrs_por_tabla.get(tabla_csv, [])
    fds_declaradas = fds_decl.get(tabla_csv, [])

    cols_sql = SA.get_columns(conn, schema, table)
    pk_cols  = SA.get_pk_columns(conn, schema, table)
    unique_sets = SA.get_unique_sets(conn, schema, table)

    # Atributos prime = PK + UNIQUE
    prime_cols = set(pk_cols)
    for s in unique_sets:
        for c in s:
            prime_cols.add(c)

    # 1FN: atomocidad y grupos repetidos
    atomic_issues = []
    name_groups = detectar_repetidos_nombrado(attrs)

    cols_to_check = [c for c in attrs if c in cols_sql]
    if cols_to_check:
        sample = SA.fetch_sample_rows(conn, schema, table, cols_to_check, limit=cfg["sample_rows"])
        col_index = {c: i for i, c in enumerate(cols_to_check)}
        for c in cols_to_check:
            idx = col_index[c]
            for row in sample:
                val = row[idx]
                if not es_valor_atomico(val):
                    atomic_issues.append((c, str(val)))
                    break

    # 2FN: solo si PK compuesta
    violations_2fn = []
    if len(pk_cols) > 1:
        non_key_cols = [c for c in cols_to_check if c not in pk_cols]
        for r in range(1, len(pk_cols)):
            for subset in combinations(pk_cols, r):
                for c in non_key_cols:
                    holds = has_fd_sql(conn, schema, table, list(subset), c, ignore_null_rhs=not cfg["fd_check_nulls"])
                    if holds:
                        violations_2fn.append({
                            "subset": list(subset),
                            "attr": c,
                            "explain": f"{'+'.join(subset)} -> {c} (dependencia parcial de la clave compuesta)"
                        })

    # 3FN:
    violations_3fn = []

    # (a) Declaradas
    for lhs, rhs in fds_declaradas:
        is_superkey = False
        if set(lhs) == set(pk_cols):
            is_superkey = True
        if not is_superkey:
            for uset in unique_sets:
                if set(lhs) == set(uset):
                    is_superkey = True
                    break
        if not is_superkey:
            for y in rhs:
                if y not in prime_cols:
                    violations_3fn.append({
                        "chain": f"{'+'.join(lhs)} -> {y}",
                        "reason": "Determinante no es superclave y RHS no primo (FD declarada)"
                    })

    # (b) Inferencia simple A->B (1-col determinante)
    if cfg["infer_singlecol_fds"]:
        non_key_cols = [c for c in cols_to_check if c not in prime_cols]
        for a in non_key_cols:
            if is_unique_sql(conn, schema, table, [a]):
                continue  # sería clave candidata
            for b in non_key_cols:
                if b == a:
                    continue
                if has_fd_sql(conn, schema, table, [a], b, ignore_null_rhs=True):
                    if a not in pk_cols and b not in prime_cols:
                        violations_3fn.append({
                            "chain": f"{'+'.join(pk_cols)} -> {a} -> {b}",
                            "reason": "Dependencia transitiva inferida (1-col)"
                        })

    return {
        "schema": schema,
        "table": table,
        "attrs_csv": attrs,
        "cols_sql": cols_sql,
        "pk_cols": pk_cols,
        "unique_sets": unique_sets,
        "prime_cols": sorted(prime_cols),
        "one_nf": {
            "atomic_issues": atomic_issues,
            "name_groups": name_groups,
        },
        "two_nf": violations_2fn,
        "three_nf": violations_3fn,
    }
