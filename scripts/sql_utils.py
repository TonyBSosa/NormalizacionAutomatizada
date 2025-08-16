# scripts/sql_utils.py
import os
import socket
import pyodbc
import pandas as pd

DEFAULT_DRIVER = "ODBC Driver 17 for SQL Server"

def connect_sql_server(
    server="localhost",
    database=None,
    driver=DEFAULT_DRIVER,
    trusted=True,
    trust_server_certificate=True,
):
    """
    Conexión a SQL Server con pyodbc.
    Por defecto usa Trusted_Connection (Windows) y Driver 17.
    """
    parts = [f"DRIVER={{{driver}}}", f"SERVER={server}"]
    if database:
        parts.append(f"DATABASE={database}")
    if trusted:
        parts.append("Trusted_Connection=yes")
    else:
        # Para este proyecto no exponemos usuario/clave en UI
        parts.append("Trusted_Connection=yes")

    # Driver 18 suele pedir certificado; añadimos TrustServerCertificate si aplica
    if "ODBC Driver 18" in driver and trust_server_certificate:
        parts.append("TrustServerCertificate=yes")

    conn_str = ";".join(parts) + ";"
    return pyodbc.connect(conn_str)

def list_databases(conn):
    q = "SELECT name FROM sys.databases WHERE database_id > 4 ORDER BY name;"
    return [r[0] for r in conn.cursor().execute(q).fetchall()]

def list_tables_grouped(conn):
    """
    Dict {schema: [table, ...]} ordenado.
    """
    q = """
    SELECT s.name AS schema_name, t.name AS table_name
    FROM sys.tables t
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    ORDER BY s.name, t.name;
    """
    data = {}
    for s, t in conn.cursor().execute(q).fetchall():
        data.setdefault(s, []).append(t)
    return data

def fetch_table_df(conn, schema, table):
    sql = f"SELECT * FROM [{schema}].[{table}];"
    return pd.read_sql(sql, conn)

def fetch_query_df(conn, sql_query):
    return pd.read_sql(sql_query, conn)

def get_table_structure_df(conn, schema, table):
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
        lambda r: f"{r['DATA_TYPE'].upper()}({int(r['LEN'])})"
                  if r["LEN"] and r["LEN"] > 0 and r["DATA_TYPE"] not in ("text", "ntext", "image")
                  else r["DATA_TYPE"].upper(),
        axis=1
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
        data.append({
            "tabla": f"{schema}.{table}",
            "atributo": col,
            "tipo": r["tipo"],
            "llave": llave
        })
    return pd.DataFrame(data)

# --- Autodetección de servidor local ---
def guess_local_servers():
    host = socket.gethostname()
    cand = [
        ".", "(local)", "localhost", host, os.getenv("COMPUTERNAME") or host,
        "localhost\\SQLEXPRESS", f"{host}\\SQLEXPRESS"
    ]
    seen, out = set(), []
    for s in cand:
        if s and s not in seen:
            seen.add(s); out.append(s)
    return out

def safe_connect_autodetect(database=None, driver=DEFAULT_DRIVER, trusted=True):
    for srv in guess_local_servers():
        try:
            conn = connect_sql_server(server=srv, database=database, driver=driver, trusted=trusted)
            conn.cursor().execute("SELECT 1;").fetchone()
            return conn, srv
        except Exception:
            continue
    return None, None
