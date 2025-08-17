# scripts/sql_utils.py
import os
import socket
import pyodbc
import pandas as pd

DEFAULT_DRIVER = "ODBC Driver 17 for SQL Server"

# =========================
# Helpers de conexión
# =========================
def _normalize_server_name(server: str | None) -> str:
    """
    Limpia y normaliza un nombre de servidor ingresado por el usuario.
    Acepta formatos como:
      - localhost
      - .   / (local)
      - HOST
      - HOST\SQLEXPRESS
    """
    if not server:
        return "localhost"
    s = str(server).strip()
    # Alias comunes
    if s in {".", "(local)"}:
        return "localhost"
    # Evitar espacios raros
    while "  " in s:
        s = s.replace("  ", " ")
    return s

def connect_sql_server(
    server: str = "localhost",
    database: str | None = None,
    driver: str = DEFAULT_DRIVER,
    trusted: bool = True,
    trust_server_certificate: bool = True,
):
    """
    Conexión a SQL Server con pyodbc usando autenticación integrada (Windows).
    Por defecto usa Driver 17. Con Driver 18 añadimos TrustServerCertificate=yes.
    """
    server = _normalize_server_name(server)

    parts = [f"DRIVER={{{driver}}}", f"SERVER={server}"]
    if database:
        parts.append(f"DATABASE={database}")

    # Para este proyecto mantenemos Trusted_Connection
    if trusted:
        parts.append("Trusted_Connection=yes")
    else:
        # Si quisieras usuario/clave, aquí sería el lugar. Por ahora, seguimos con trusted.
        parts.append("Trusted_Connection=yes")

    # Driver 18 suele pedir certificado
    if "ODBC Driver 18" in driver and trust_server_certificate:
        parts.append("TrustServerCertificate=yes")

    conn_str = ";".join(parts) + ";"
    return pyodbc.connect(conn_str)

# =========================
# Listados y lecturas
# =========================
def list_databases(conn) -> list[str]:
    q = "SELECT name FROM sys.databases WHERE database_id > 4 ORDER BY name;"
    return [r[0] for r in conn.cursor().execute(q).fetchall()]

def list_tables_grouped(conn) -> dict:
    """
    Dict {schema: [table, ...]} ordenado.
    """
    q = """
    SELECT s.name AS schema_name, t.name AS table_name
    FROM sys.tables t
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    ORDER BY s.name, t.name;
    """
    data: dict[str, list[str]] = {}
    for s, t in conn.cursor().execute(q).fetchall():
        data.setdefault(s, []).append(t)
    return data

def fetch_table_df(conn, schema: str, table: str) -> pd.DataFrame:
    sql = f"SELECT * FROM [{schema}].[{table}];"
    return pd.read_sql(sql, conn)

def fetch_query_df(conn, sql_query: str) -> pd.DataFrame:
    return pd.read_sql(sql_query, conn)

def get_table_structure_df(conn, schema: str, table: str) -> pd.DataFrame:
    """
    Devuelve DataFrame con columnas: tabla, atributo, tipo, llave (PK, FK...).
    """
    cols_q = """
    SELECT 
        TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE,
        COALESCE(CHARACTER_MAXIMUM_LENGTH, 0) AS LEN
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    ORDER BY ORDINAL_POSITION;
    """
    cols = pd.read_sql(cols_q, conn, params=[schema, table])
    cols["tipo"] = cols.apply(
        lambda r: (
            f"{r['DATA_TYPE'].upper()}({int(r['LEN'])})"
            if r["LEN"]
            and r["LEN"] > 0
            and r["DATA_TYPE"] not in ("text", "ntext", "image")
            else r["DATA_TYPE"].upper()
        ),
        axis=1,
    )

    pk_q = """
    SELECT kcu.COLUMN_NAME
    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
    JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
      ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
     AND kcu.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
    WHERE tc.TABLE_SCHEMA = ? AND tc.TABLE_NAME = ?
      AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY';
    """
    pk = pd.read_sql(pk_q, conn, params=[schema, table])["COLUMN_NAME"].tolist()

    fk_q = """
    SELECT 
        cu.COLUMN_NAME AS FK_COLUMN,
        pk.TABLE_SCHEMA AS PK_SCHEMA,
        pk.TABLE_NAME AS PK_TABLE,
        pku.COLUMN_NAME AS PK_COLUMN
    FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
    JOIN INFORMATION_SCHEMA.CONSTRAINT_TABLE_USAGE fk
      ON fk.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
    JOIN INFORMATION_SCHEMA.CONSTRAINT_TABLE_USAGE pk
      ON pk.CONSTRAINT_NAME = rc.UNIQUE_CONSTRAINT_NAME
    JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE cu
      ON cu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
    JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE pku
      ON pku.CONSTRAINT_NAME = rc.UNIQUE_CONSTRAINT_NAME
     AND pku.ORDINAL_POSITION = cu.ORDINAL_POSITION
    WHERE fk.TABLE_SCHEMA = ? AND fk.TABLE_NAME = ?;
    """
    fks = pd.read_sql(fk_q, conn, params=[schema, table])

    data = []
    for _, r in cols.iterrows():
        col = r["COLUMN_NAME"]
        llave = ""
        if col in pk:
            llave = "PK"
        fk_rows = fks[fks["FK_COLUMN"] == col]
        for __, fr in fk_rows.iterrows():
            llave_fk = f"FK:{fr['PK_TABLE']}({fr['PK_COLUMN']})"
            llave = llave + ("; " if llave and llave.strip() else "") + llave_fk
        data.append(
            {"tabla": f"{schema}.{table}", "atributo": col, "tipo": r["tipo"], "llave": llave}
        )
    return pd.DataFrame(data)

# =========================
# Autodetección & Manual
# =========================
def guess_local_servers() -> list[str]:
    """
    Devuelve una lista de candidatos de servidor locales frecuentes.
    No garantiza que existan; se usan para prueba rápida.
    """
    host = socket.gethostname()
    cand = [
        ".", "(local)", "localhost",
        host,
        os.getenv("COMPUTERNAME") or host,
        "localhost\\SQLEXPRESS",
        f"{host}\\SQLEXPRESS",
    ]
    seen, out = set(), []
    for s in cand:
        s = _normalize_server_name(s)
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out

def safe_connect_autodetect(
    database: str | None = None,
    driver: str = DEFAULT_DRIVER,
    trusted: bool = True,
):
    """
    Recorre candidatos locales y devuelve (conn, server) del primero que responda.
    Si no conecta a ninguno, devuelve (None, None).
    """
    for srv in guess_local_servers():
        try:
            conn = connect_sql_server(server=srv, database=database, driver=driver, trusted=trusted)
            conn.cursor().execute("SELECT 1;").fetchone()
            return conn, srv
        except Exception:
            continue
    return None, None

def safe_connect_manual(
    server: str,
    database: str | None = None,
    driver: str = DEFAULT_DRIVER,
    trusted: bool = True,
):
    """
    Intenta conectar al servidor proporcionado por el usuario.
    Lanza excepción con mensaje claro si falla.
    """
    srv = _normalize_server_name(server)
    try:
        conn = connect_sql_server(server=srv, database=database, driver=driver, trusted=trusted)
        conn.cursor().execute("SELECT 1;").fetchone()
        return conn, srv
    except Exception as e:
        # Incluimos el servidor en el mensaje para depurar más fácil.
        raise RuntimeError(f"No se pudo conectar al servidor '{srv}'. Detalle: {e}")

def probe_databases(
    server: str | None = None,
    driver: str = DEFAULT_DRIVER,
    trusted: bool = True,
) -> dict:
    """
    Lista bases de datos. Si 'server' viene, se usa ese; si no, se autodetecta.
    Devuelve dict con claves:
      { 'server': <nombre>, 'databases': [..] }
    """
    if server:
        conn, srv = None, _normalize_server_name(server)
        try:
            conn = connect_sql_server(server=srv, driver=driver, trusted=trusted)
            dbs = list_databases(conn)
            conn.close()
            return {"server": srv, "databases": dbs}
        except Exception as e:
            raise RuntimeError(f"No se pudo listar bases en '{srv}'. Detalle: {e}")
    else:
        conn, srv = safe_connect_autodetect(driver=driver, trusted=trusted)
        if not conn:
            return {"server": None, "databases": []}
        try:
            dbs = list_databases(conn)
            conn.close()
            return {"server": srv, "databases": dbs}
        except Exception as e:
            raise RuntimeError(f"No se pudo listar bases en '{srv}'. Detalle: {e}")

# =========================
# API surface recomendado
# =========================
__all__ = [
    "DEFAULT_DRIVER",
    "connect_sql_server",
    "list_databases",
    "list_tables_grouped",
    "fetch_table_df",
    "fetch_query_df",
    "get_table_structure_df",
    "guess_local_servers",
    "safe_connect_autodetect",
    "safe_connect_manual",
    "probe_databases",
]
