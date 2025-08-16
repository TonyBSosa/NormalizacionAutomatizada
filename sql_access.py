import pyodbc

def build_conn_str(server, database, auth_mode, uid=None, pwd=None, driver="{ODBC Driver 17 for SQL Server}"):
    if auth_mode == "windows":
        return f"DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
    return f"DRIVER={driver};SERVER={server};DATABASE={database};UID={uid};PWD={pwd};"

def test_connection(conn_str):
    with pyodbc.connect(conn_str, timeout=5) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()

def quote_ident(name: str) -> str:
    return f"[{name.replace(']', ']]')}]"

def get_pk_columns(conn, schema, table):
    sql = """
    SELECT kcu.COLUMN_NAME
    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
    JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
      ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
     AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
     AND tc.TABLE_NAME   = kcu.TABLE_NAME
    WHERE tc.TABLE_SCHEMA=? AND tc.TABLE_NAME=? AND tc.CONSTRAINT_TYPE='PRIMARY KEY'
    ORDER BY kcu.ORDINAL_POSITION;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (schema, table))
        return [r[0] for r in cur.fetchall()]

def get_unique_sets(conn, schema, table):
    """
    Retorna lista de conjuntos de columnas con índice/constraint UNIQUE (excluye PK).
    """
    sql = """
    SELECT i.name, STRING_AGG(c.name, ',') WITHIN GROUP (ORDER BY ic.key_ordinal) AS cols
    FROM sys.indexes i
    JOIN sys.index_columns ic ON ic.object_id=i.object_id AND ic.index_id=i.index_id
    JOIN sys.columns c ON c.object_id=ic.object_id AND c.column_id=ic.column_id
    JOIN sys.tables t ON t.object_id=i.object_id
    JOIN sys.schemas s ON s.schema_id=t.schema_id
    WHERE s.name=? AND t.name=? AND i.is_unique=1 AND i.is_primary_key=0
    GROUP BY i.name
    """
    with conn.cursor() as cur:
        cur.execute(sql, (schema, table))
        res = []
        for r in cur.fetchall():
            cols = [x.strip() for x in (r[1] or "").split(",") if x.strip()]
            if cols:
                res.append(cols)
        return res

def get_columns(conn, schema, table):
    sql = """
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA=? AND TABLE_NAME=?
    ORDER BY ORDINAL_POSITION;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (schema, table))
        return [r[0] for r in cur.fetchall()]

def fetch_sample_rows(conn, schema, table, cols, limit=None):
    cols_sql = ", ".join(quote_ident(c) for c in cols)
    top = f"TOP {int(limit)} " if limit else ""
    q = f"SELECT {top}{cols_sql} FROM {quote_ident(schema)}.{quote_ident(table)}"
    with conn.cursor() as cur:
        cur.execute(q)
        return [tuple(row) for row in cur.fetchall()]
