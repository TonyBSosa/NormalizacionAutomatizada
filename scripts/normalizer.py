# scripts/normalizer.py
import pandas as pd
import re
from itertools import combinations

MULTI_SEPARATORS = [",", ";", "/", "|"]


def _clean_value(v):
    if pd.isna(v):
        return None
    if isinstance(v, str):
        v = v.strip()
        v = re.sub(r"^[\[\(\{]\s*|\s*[\]\)\}]$", "", v)  # quita [] () {}
        return v if v != "" else None
    return v


def split_multivalue(value):
    """Devuelve lista de valores si detecta separadores, si no, lista de 1 valor."""
    if pd.isna(value):
        return []
    if not isinstance(value, str):
        return [value]
    raw = _clean_value(value)
    if raw is None:
        return []
    for sep in MULTI_SEPARATORS:
        if sep in raw:
            parts = [p.strip() for p in raw.split(sep)]
            parts = [p for p in parts if p != ""]
            if len(parts) > 1:
                return parts
    return [raw]


def detect_tables(estructura_df: pd.DataFrame):
    """
    Espera columnas: tabla, atributo, tipo (opcional), llave (PK, FK:Tabla(Col), vacío)
    Devuelve un dict con metadatos por tabla.
    """
    estructura_df = estructura_df.copy()
    estructura_df["llave"] = estructura_df.get("llave", pd.Series([""] * len(estructura_df))).astype(str)

    tables = {}
    for t, chunk in estructura_df.groupby('tabla'):
        attrs = chunk['atributo'].tolist()
        tipos = dict(zip(
            chunk['atributo'],
            chunk.get('tipo', pd.Series(["NVARCHAR(255)"] * len(chunk))).fillna('NVARCHAR(255)')
        ))
        pks = chunk[chunk['llave'].str.contains('PK', case=False, na=False)]['atributo'].tolist()

        fks_map = []
        fk_rows = chunk[chunk['llave'].str.contains('FK', case=False, na=False)]
        for _, row in fk_rows.iterrows():
            # Formatos: "FK:Clientes(idCliente)" o "FK Clientes(idCliente)"
            m = re.search(r'FK\s*:?\s*([A-Za-z0-9_]+)\s*\(\s*([A-Za-z0-9_]+)\s*\)', str(row['llave']))
            if m:
                fks_map.append((row['atributo'], m.group(1), m.group(2)))

        tables[t] = {'attrs': attrs, 'types': tipos, 'pk': pks, 'fks': fks_map}
    return tables


def proper_subsets(pk):
    """Subconjuntos propios no vacíos de la clave compuesta."""
    out = []
    for r in range(1, len(pk)):
        out += [list(c) for c in combinations(pk, r)]
    return out


def depends_on(df: pd.DataFrame, determinant_cols, target):
    """
    Heurística: target depende funcionalmente de determinant_cols si por cada combinación
    de determinant_cols solo hay un valor de target.
    """
    if any(col not in df.columns for col in determinant_cols + [target]):
        return False
    g = df.groupby(determinant_cols)[target].nunique(dropna=False)
    return (g.max() == 1)


def normalize_1NF(table_name, meta, df):
    """
    1FN: separa atributos multivaluados en tablas hijas, desmontando listas A,B,C.
    - Mantiene la tabla base sin esos atributos (deduplicada por PK si existe).
    - Crea tablas hijas {tabla}_{atributo} con PK = PK_base + atributo
    """
    result_tables = {}
    schema = {}

    base_cols = [c for c in meta['attrs'] if c in df.columns]
    base_df = df[base_cols].copy()

    # Detectar multivaluados
    multival_cols = []
    for col in base_cols:
        if base_df[col].apply(lambda x: len(split_multivalue(x))).max() > 1:
            multival_cols.append(col)

    # Construye tablas hijas
    for mv in multival_cols:
        child_name = f"{table_name}_{mv}"
        pk_cols = meta['pk'][:] if len(meta['pk']) > 0 else [f"{table_name}_id_auto"]
        expanded_rows = []
        for idx, r in base_df.iterrows():
            values = split_multivalue(r[mv])
            r = r.copy()
            if len(meta['pk']) == 0 and f"{table_name}_id_auto" not in r:
                r[f"{table_name}_id_auto"] = idx
            for v in values:
                new_row = {pk: r.get(pk) for pk in pk_cols}
                new_row[mv] = _clean_value(v)
                expanded_rows.append(new_row)
        child_df = pd.DataFrame(expanded_rows).drop_duplicates()

        # tipos
        types = {}
        for c in child_df.columns:
            types[c] = meta['types'].get(c, 'NVARCHAR(255)')

        # PK de hija: pk base + mv
        child_pk = pk_cols + [mv]
        schema[child_name] = {
            'attrs': list(child_df.columns),
            'types': types,
            'pk': child_pk,
            'fks': [(pk, table_name, pk) for pk in pk_cols]
        }
        result_tables[child_name] = child_df

    # Quitar columnas multivaluadas de la base
    base_df = base_df.drop(columns=multival_cols, errors='ignore').drop_duplicates()

    # Si no había PK y se generó {tabla}_id_auto, lo trasladamos a base
    if len(meta['pk']) == 0 and f"{table_name}_id_auto" in base_df.columns:
        meta_pk = [f"{table_name}_id_auto"]
    else:
        meta_pk = meta['pk']

    schema[table_name] = {
        'attrs': list(base_df.columns),
        'types': {c: meta['types'].get(c, 'NVARCHAR(255)') for c in base_df.columns},
        'pk': meta_pk,
        'fks': meta['fks'][:]  # FKs definidas en estructura
    }
    result_tables[table_name] = base_df

    return schema, result_tables, multival_cols


def normalize_2NF(table_name, schema, tables_data):
    """
    2FN: Para tablas con PK compuesta, separa atributos que dependen de parte de la PK.
    Crea tablas determinantes por cada subconjunto y hace que la TABLA BASE
    tenga FK -> TABLA_DETERMINANTE (NO al revés).
    """
    meta = schema[table_name]
    df = tables_data[table_name].copy()

    pk = meta['pk']
    if len(pk) < 2:
        return schema, tables_data, []  # nada que hacer

    created = []
    for sub in proper_subsets(pk):
        dependents = []
        for col in df.columns:
            if col in pk:
                continue
            if depends_on(df, sub, col):
                dependents.append(col)

        if dependents:
            new_name = f"{table_name}_{'_'.join(sub)}_det"
            cols = sub + dependents
            new_df = df[cols].drop_duplicates().copy()

            # esquema de la tabla determinante (sin FK hacia la base)
            types = {c: meta['types'].get(c, 'NVARCHAR(255)') for c in cols}
            schema[new_name] = {
                'attrs': cols,
                'types': types,
                'pk': sub[:],
                'fks': []
            }
            tables_data[new_name] = new_df

            # En la base, quitamos los dependientes y agregamos FK(sub) -> new_name(sub)
            df = df.drop(columns=dependents, errors='ignore')
            base_fks = schema[table_name].get('fks', [])
            for s in sub:
                if (s, new_name, s) not in base_fks:
                    base_fks.append((s, new_name, s))
            schema[table_name]['fks'] = base_fks

            created.append((new_name, sub, dependents))

    # Actualizar datos y schema de la tabla base
    df = df.drop_duplicates()
    tables_data[table_name] = df
    schema[table_name]['attrs'] = list(df.columns)
    schema[table_name]['types'] = {
        c: schema[table_name]['types'].get(c, meta['types'].get(c, 'NVARCHAR(255)'))
        for c in df.columns
    }
    return schema, tables_data, created


def normalize_3NF(table_name, schema, tables_data):
    """
    3FN: Detecta dependencias transitivas entre NO claves (A→B, con A no clave).
    Crea tabla dimensión {tabla}_dim_{A} con PK = A y columnas dependientes.
    """
    meta = schema[table_name]
    df = tables_data[table_name].copy()
    pk = set(meta['pk'])

    non_keys = [c for c in df.columns if c not in pk]
    created = []

    used_as_det = set()  # evita crear múltiples veces para el mismo determinante
    for det in non_keys:
        if det in used_as_det:
            continue
        dependents = []
        for target in non_keys:
            if target == det:
                continue
            if depends_on(df, [det], target):
                dependents.append(target)

        if dependents:
            new_name = f"{table_name}_dim_{det}"
            cols = [det] + dependents
            new_df = df[cols].drop_duplicates().copy()

            types = {c: meta['types'].get(c, 'NVARCHAR(255)') for c in cols}
            schema[new_name] = {
                'attrs': cols,
                'types': types,
                'pk': [det],
                'fks': []
            }
            tables_data[new_name] = new_df
            created.append((new_name, det, dependents))

            # En la tabla base, retiramos los dependents; mantenemos 'det' como FK a la dimensión
            df = df.drop(columns=dependents, errors='ignore')
            base_fks = schema[table_name].get('fks', [])
            if (det, new_name, det) not in base_fks:
                base_fks.append((det, new_name, det))
            schema[table_name]['fks'] = base_fks
            used_as_det.add(det)

    df = df.drop_duplicates()
    tables_data[table_name] = df
    schema[table_name]['attrs'] = list(df.columns)
    schema[table_name]['types'] = {
        c: schema[table_name]['types'].get(c, meta['types'].get(c, 'NVARCHAR(255)'))
        for c in df.columns
    }

    return schema, tables_data, created


def merge_schemas(base_schema, add_schema):
    out = dict(base_schema)
    for t, meta in add_schema.items():
        out[t] = meta
    return out


def merge_tables(base_tables, add_tables):
    out = dict(base_tables)
    for t, df in add_tables.items():
        out[t] = df
    return out


def map_sql_type(t: str):
    s = str(t).upper()
    if any(x in s for x in ["INT", "BIGINT", "SMALLINT", "TINYINT"]):
        return s
    if any(x in s for x in ["DECIMAL", "NUMERIC", "FLOAT", "REAL", "MONEY"]):
        return s
    if "DATE" in s or "TIME" in s:
        return s
    if "BIT" in s or "BOOL" in s:
        return "BIT"
    # por defecto
    return "NVARCHAR(255)"


def generate_mermaid(schema):
    """
    ER con Mermaid (client-side). Relación simple para cada FK.
    """
    lines = ["erDiagram"]
    # entidades
    for t, meta in schema.items():
        lines.append(f"  {t} {{")
        for col in meta['attrs']:
            typ = meta['types'].get(col, 'NVARCHAR(255)').lower()
            if "int" in typ:
                mtyp = "int"
            elif any(x in typ for x in ["decimal", "numeric", "float", "real", "money"]):
                mtyp = "float"
            elif "date" in typ or "time" in typ:
                mtyp = "date"
            elif "bit" in typ or "bool" in typ:
                mtyp = "boolean"
            else:
                mtyp = "string"
            lines.append(f"    {mtyp} {col}")
        lines.append("  }")

    # relaciones
    for t, meta in schema.items():
        for (fk_col, ref_table, ref_col) in meta.get('fks', []):
            # }o--||  (FK lado muchos/0-1 hacia lado 1 obligatorio)
            lines.append(
                f'  {t} }}o--|| {ref_table} : "{fk_col}->{ref_table}.{ref_col}"'
            )

    return "\n".join(lines)


def generate_sql(schema):
    stmts = []
    for t, meta in schema.items():
        cols_defs = []
        for col in meta['attrs']:
            col_type = map_sql_type(meta['types'].get(col, 'NVARCHAR(255)'))
            nullability = "NOT NULL" if col in meta['pk'] else "NULL"
            cols_defs.append(f"    [{col}] {col_type} {nullability}")
        pk = meta['pk']
        pk_stmt = f",\n    CONSTRAINT [PK_{t}] PRIMARY KEY ({', '.join('['+c+']' for c in pk)})" if pk else ""
        table_stmt = f"CREATE TABLE [{t}] (\n" + ",\n".join(cols_defs) + pk_stmt + "\n);\n"
        stmts.append(table_stmt)

    # FKs (una por columna determinante; opcional: consolidar en multicolumna)
    for t, meta in schema.items():
        for i, (fk_col, ref_table, ref_col) in enumerate(meta.get('fks', []), start=1):
            fk_name = f"FK_{t}_{fk_col}_{i}"
            stmts.append(
                f"ALTER TABLE [{t}] ADD CONSTRAINT [{fk_name}] FOREIGN KEY ([{fk_col}]) "
                f"REFERENCES [{ref_table}]([{ref_col}]);\n"
            )

    return "\n".join(stmts)


def generate_description(schema):
    lines = []
    for t, meta in schema.items():
        lines.append(f"Entidad: {t}")
        lines.append(f"  Atributos: {', '.join(meta['attrs'])}")
        if meta['pk']:
            lines.append(f"  Clave primaria: {', '.join(meta['pk'])}")
        if meta.get('fks'):
            for fk_col, ref_t, ref_c in meta['fks']:
                lines.append(f"  FK: {fk_col} → {ref_t}({ref_c})")
        lines.append("")
    return "\n".join(lines)


def normalizar_pipeline(estructura_df: pd.DataFrame, datos_df: pd.DataFrame):
    """
    Ejecuta 1FN → 2FN → 3FN por cada tabla detectada en 'estructura_df' usando 'datos_df'.
    Devuelve:
      - schema_final (dict)
      - tablas_data (dict[str, DataFrame])
      - mermaid (str)
      - sql_script (str)
      - descripcion (str)
      - resumen_acciones (dict por tabla con detalles de lo hecho)
    """
    base_tables = detect_tables(estructura_df)

    schema_final = {}
    tablas_data = {}
    resumen_acciones = {}

    # Si 'datos_df' contiene columnas de múltiples tablas, extraemos por tabla.
    for t, meta in base_tables.items():
        cols = [c for c in meta['attrs'] if c in datos_df.columns]
        if not cols:
            continue
        df_t = datos_df[cols].copy()

        # 1FN
        s1, td1, mv_cols = normalize_1NF(t, meta, df_t)
        schema_final = merge_schemas(schema_final, s1)
        tablas_data = merge_tables(tablas_data, td1)

        # 2FN (sobre la tabla base t)
        s2, td2, created_2fn = normalize_2NF(t, schema_final, tablas_data)
        schema_final, tablas_data = s2, td2

        # 3FN (sobre la tabla base t)
        s3, td3, created_3fn = normalize_3NF(t, schema_final, tablas_data)
        schema_final, tablas_data = s3, td3

        resumen_acciones[t] = {
            '1FN_multivaluados_separados': mv_cols,
            '2FN_dependencias_parciales': created_2fn,
            '3FN_dependencias_transitivas': created_3fn
        }

    mermaid = generate_mermaid(schema_final)
    sql_script = generate_sql(schema_final)
    descripcion = generate_description(schema_final)

    return schema_final, tablas_data, mermaid, sql_script, descripcion, resumen_acciones
