import re, csv, io

# ------------------ Reglas / Regex ------------------
RE_VARCHAR = re.compile(r"^(VAR)?CHAR\(\s*\d+\s*\)$", re.IGNORECASE)     # CHAR(n) o VARCHAR(n)
RE_NVARCHAR = re.compile(r"^N(VAR)?CHAR\(\s*\d+\s*\)$", re.IGNORECASE)   # NCHAR(n) o NVARCHAR(n)
RE_DECIMAL  = re.compile(r"^(DECIMAL|NUMERIC)\(\s*\d+\s*,\s*\d+\s*\)$", re.IGNORECASE)
RE_FK       = re.compile(r"^FK\(\s*([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)\s*\)$")  # FK(Tabla.Col)

BASIC_TYPES = {
    "INT","BIGINT","SMALLINT","TINYINT",
    "DATE","DATETIME","DATETIME2","SMALLDATETIME","TIME",
    "FLOAT","REAL","BIT","MONEY","SMALLMONEY",
    "TEXT","NTEXT","UNIQUEIDENTIFIER"
}

REQUIRED_COLS = ["tabla", "atributo", "tipo", "llave"]

# ------------------ CSV ------------------
def parse_csv(file_like):
    """
    Lee CSV UTF-8 (admite BOM) con encabezados.
    file_like: objeto con .read() que retorna bytes o str
    """
    raw_bytes = file_like.read()
    if isinstance(raw_bytes, bytes):
        raw = raw_bytes.decode("utf-8-sig")
    else:
        raw = raw_bytes
    reader = csv.DictReader(io.StringIO(raw))
    rows = [{(k or "").strip(): (v or "").strip() for k, v in r.items()} for r in reader]
    headers = [(h or "").strip() for h in (reader.fieldnames or [])]
    return headers, rows

def normalize_headers(headers):
    return [h.strip().lower() for h in headers]

def validate_headers(headers_norm):
    errors = []
    missing = [c for c in REQUIRED_COLS if c not in headers_norm]
    if missing:
        errors.append(f"Faltan columnas obligatorias: {', '.join(missing)}")
    return errors

# ------------------ Tipos / Llaves ------------------
def validate_type(t: str):
    """Valida tipo SQL esperado."""
    if not t:
        return False, "Tipo vacío"
    t_up = t.upper().strip()

    # Tipos con longitud
    if RE_VARCHAR.match(t_up) or RE_NVARCHAR.match(t_up):
        return True, ""

    # DECIMAL/NUMERIC(p,s)
    if RE_DECIMAL.match(t_up):
        return True, ""

    base = t_up.split("(", 1)[0]
    if base in BASIC_TYPES:
        return True, ""

    # Errores comunes y mensaje claro
    if base in {"VARCHAR","CHAR","NVARCHAR","NCHAR"}:
        return False, f"Especifica longitud: usa {base}(n), p.ej. {base}(100)"
    if base in {"DECIMAL","NUMERIC"}:
        return False, f"Especifica precisión y escala: usa {base}(p,s), p.ej. {base}(10,2)"
    return False, f"Tipo no reconocido: {t}"

def validate_key(k: str):
    """
    Acepta combinaciones separadas por ; o ,:
    PK, PK(part), FK, FK(Tabla.Col), NK, UNIQUE
    """
    if not k:
        return True, ""
    tokens = re.split(r"[;,]\s*", k.strip())

    for tok in tokens:
        ku = tok.strip().upper()
        if ku in {"PK", "PK(PART)", "NK", "UNIQUE"}:
            continue
        if ku == "FK":
            # permitido; se decide si es error/advertencia en validate_rows(...)
            continue
        if ku.startswith("FK("):
            if RE_FK.match(tok):
                continue
            return False, "FK inválida. Formato correcto: FK(Tabla.Col)"
        return False, f"Valor de 'llave' no soportado: {tok}"
    return True, ""

# ------------------ FDs helpers ------------------
def _split_list(s: str):
    return [p.strip() for p in s.split(",") if p.strip()]

def _parse_fd(fd_raw: str):
    """
    fd_raw: 'A,B->C,D'  ->  (['A','B'], ['C','D'])
    """
    if "->" not in fd_raw:
        raise ValueError("falta '->'")
    lhs, rhs = [p.strip() for p in fd_raw.split("->", 1)]
    if not lhs or not rhs:
        raise ValueError("faltan lados LHS/RHS")
    lhs_cols = _split_list(lhs)
    rhs_cols = _split_list(rhs)
    if not lhs_cols or not rhs_cols:
        raise ValueError("LHS o RHS vacíos")
    return lhs_cols, rhs_cols

def _collect_table_columns(rows):
    """
    Construye dict: {tabla_lower: set(col_lower, ...)} a partir del CSV.
    """
    mapping = {}
    for r in rows:
        t = (r.get("tabla","") or "").strip()
        a = (r.get("atributo","") or "").strip()
        if not t or not a:
            continue
        key = t.lower()
        mapping.setdefault(key, set()).add(a.lower())
    return mapping

def _collect_fk_targets(rows):
    """
    Retorna set de pares (tabla_lower, col_lower) existentes en el CSV,
    para validar que FK(Tabla.Col) apunte a columnas declaradas.
    """
    targets = set()
    for r in rows:
        t = (r.get("tabla","") or "").strip().lower()
        a = (r.get("atributo","") or "").strip().lower()
        if t and a:
            targets.add((t, a))
    return targets

# ------------------ Validación de filas ------------------
def validate_rows(rows, require_fk_target_in_csv: bool = False):
    errors, warnings = [], []
    seen = set()

    table_cols = _collect_table_columns(rows)  # tabla -> {cols}
    fk_targets = _collect_fk_targets(rows)     # {(tabla,col), ...}

    for i, r in enumerate(rows, start=2):  # header en línea 1
        tabla = (r.get("tabla") or "").strip()
        atributo = (r.get("atributo") or "").strip()
        tipo = (r.get("tipo") or "").strip()
        llave = (r.get("llave") or "").strip()
        df_cell = (r.get("dependencia_funcional") or "").strip()

        # Vacíos obligatorios
        if not tabla:
            errors.append(f"Línea {i}: 'tabla' vacío")
        if not atributo:
            errors.append(f"Línea {i}: 'atributo' vacío")

        # Duplicados
        key = (tabla.lower(), atributo.lower())
        if key in seen:
            errors.append(f"Línea {i}: atributo duplicado '{tabla}.{atributo}'")
        else:
            seen.add(key)

        # Tipo
        ok, msg = validate_type(tipo)
        if not ok:
            errors.append(f"Línea {i}: tipo inválido '{tipo}'. {msg}")

        # Llave
        ok, msg = validate_key(llave)
        if not ok:
            errors.append(f"Línea {i}: llave inválida '{llave}'. {msg}")
        else:
            tokens = re.split(r"[;,]\s*", llave.strip()) if llave else []
            has_fk_plain = any(t.strip().upper() == "FK" for t in tokens)

            if has_fk_plain:
                if require_fk_target_in_csv:
                    errors.append(
                        f"Línea {i}: '{tabla}.{atributo}' declara 'FK' sin destino en CSV (usa FK(Tabla.Col) o desactiva la opción)."
                    )
                else:
                    warnings.append(
                        f"Línea {i}: '{tabla}.{atributo}' declara FK sin destino en CSV; se resolverá desde SQL."
                    )

            # Validar FK(Tabla.Col) si aparece
            for tok in tokens:
                ts = tok.strip()
                if ts.upper().startswith("FK("):
                    m = RE_FK.match(ts)
                    if m:
                        ref_tabla = m.group(1).strip().lower()
                        ref_col   = m.group(2).strip().lower()
                        if (ref_tabla, ref_col) not in fk_targets:
                            errors.append(
                                f"Línea {i}: FK apunta a columna inexistente en plantilla: {ref_tabla}.{ref_col}"
                            )

        # Dependencias funcionales (A,B->C,D ; separadas por ;)
        if df_cell:
            tabla_key = tabla.lower()
            cols_in_tabla = table_cols.get(tabla_key, set())
            raw_fds = [p.strip() for p in df_cell.split(";") if p.strip()]
            if not raw_fds:
                errors.append(f"Línea {i}: dependencia_funcional vacía/inválida '{df_cell}'")
            else:
                for fd in raw_fds:
                    try:
                        lhs_cols, rhs_cols = _parse_fd(fd)
                    except ValueError as ex:
                        errors.append(f"Línea {i}: dependencia_funcional inválida '{fd}' ({ex}). Formato: A,B->C,D")
                        continue

                    for c in lhs_cols:
                        if c.lower() not in cols_in_tabla:
                            errors.append(f"Línea {i}: DF '{fd}' refiere columna inexistente en {tabla}: '{c}'")
                    for c in rhs_cols:
                        if c.lower() not in cols_in_tabla:
                            errors.append(f"Línea {i}: DF '{fd}' refiere columna inexistente en {tabla}: '{c}'")

                    if any(c.strip() == atributo for c in rhs_cols):
                        warnings.append(f"Línea {i}: DF '{fd}' incluye '{atributo}' en RHS (verifica redundancia).")

    return errors, warnings
