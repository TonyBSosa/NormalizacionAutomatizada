from flask import Flask, render_template, request, jsonify
import pandas as pd
import os
from itertools import combinations

from scripts.normalizer import normalizar_pipeline
from scripts.sql_utils import (
    connect_sql_server,
    get_table_structure_df,
    fetch_table_df,
    list_databases,
    list_tables_grouped,
    safe_connect_autodetect,
)
import pyodbc

# IMPORTANTE: tu carpeta es "Templates" con T mayúscula
app = Flask(__name__, template_folder="Templates")
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


# ---------- Utilidades ----------
def proper_subsets(pk_cols):
    for r in range(1, len(pk_cols)):
        for combo in combinations(pk_cols, r):
            yield list(combo)

def es_valor_atomico(valor):
    if pd.isna(valor):
        return True
    if isinstance(valor, str) and any(sep in valor for sep in [",", ";", "/", "|", "[", "]"]):
        return False
    return True

def verificar_1FN(df: pd.DataFrame):
    for col in df.columns:
        for val in df[col]:
            if not es_valor_atomico(val):
                return False, col, val
    return True, None, None

def verificar_2FN(estructura_df: pd.DataFrame, datos_df: pd.DataFrame):
    mensajes = []
    estructura_df = estructura_df.copy()
    estructura_df['llave'] = estructura_df.get('llave', pd.Series([""] * len(estructura_df))).astype(str)

    for tabla, chunk in estructura_df.groupby('tabla'):
        pk = chunk[chunk['llave'].str.contains('PK', case=False, na=False)]['atributo'].tolist()
        if len(pk) < 2:
            continue
        attrs_tabla = [a for a in chunk['atributo'] if a in datos_df.columns]
        if not attrs_tabla:
            continue
        df_t = datos_df[attrs_tabla].copy()
        for sub in proper_subsets(pk):
            candidatos = [c for c in attrs_tabla if c not in pk]
            for col in candidatos:
                try:
                    g = df_t.groupby(sub)[col].nunique(dropna=True)
                except Exception:
                    continue
                if g.index.nunique() >= 2 and g.max() == 1:
                    mensajes.append(
                        f"❌ '{col}' depende solo de parte de la clave compuesta {sub} en la tabla '{tabla}'"
                    )
    if not mensajes:
        mensajes.append("✅ Los datos cumplen con la Segunda Forma Normal (2FN).")
    return mensajes

def verificar_3FN(datos_df: pd.DataFrame, estructura_df: pd.DataFrame | None = None):
    pk = set()
    if estructura_df is not None and 'llave' in estructura_df.columns:
        pk = set(
            estructura_df[estructura_df['llave'].astype(str).str.contains('PK', case=False, na=False)]['atributo'].tolist()
        )
    cols = [c for c in datos_df.columns if c != "__tabla"]
    non_keys = [c for c in cols if c not in pk]
    mensajes = []
    for det in non_keys:
        counts = datos_df[det].value_counts(dropna=True)
        if counts.empty or counts.max() < 2:
            continue
        for target in non_keys:
            if target == det:
                continue
            try:
                g = datos_df.groupby(det)[target].nunique(dropna=True)
            except Exception:
                continue
            if g.max() == 1:
                mensajes.append(f"❌ Existe una dependencia transitoria: '{target}' depende de '{det}'")
    if not mensajes:
        mensajes.append("✅ Los datos cumplen con la Tercera Forma Normal (3FN).")
    return mensajes


# ---------- Home ----------
@app.route('/', methods=['GET', 'POST'])
def index():
    try:
        estructura_df = None
        datos_df = None
        resultado_1fn = None
        resultado_2fn = None
        resultado_3fn = None
        diagram_mermaid = None
        sql_script = None
        descripcion = None
        tablas_result_html = []
        resumen_acciones = None

        if request.method == 'POST':
            source = request.form.get('source', 'csv')

            if source == 'csv':
                estructura_file = request.files.get('estructura')
                datos_file = request.files.get('datos')
                if estructura_file and estructura_file.filename.lower().endswith('.csv'):
                    p = os.path.join(app.config['UPLOAD_FOLDER'], estructura_file.filename)
                    estructura_file.save(p)
                    estructura_df = pd.read_csv(p)
                if datos_file and datos_file.filename.lower().endswith('.csv'):
                    p = os.path.join(app.config['UPLOAD_FOLDER'], datos_file.filename)
                    datos_file.save(p)
                    datos_df = pd.read_csv(p)

            elif source == 'sql':
                # CONEXIÓN 100% AUTOMÁTICA (sin servidor en UI)
                database = request.form.get('sql_db') or None
                schema_name = request.form.get('sql_schema') or 'dbo'
                table = request.form.get('sql_table') or None

                conn = None
                try:
                    conn, _used_server = safe_connect_autodetect(database=database)
                    if conn is None:
                        raise RuntimeError("No se pudo establecer conexión automática con SQL Server.")
                    if not (database and table):
                        raise ValueError("Selecciona Base de datos y Tabla.")

                    datos_df = fetch_table_df(conn, schema_name, table)
                    estructura_df = get_table_structure_df(conn, schema_name, table)
                finally:
                    try:
                        if conn: conn.close()
                    except Exception:
                        pass

        # ----- Evaluación + pipeline -----
        if datos_df is not None:
            ok1, col, val = verificar_1FN(datos_df)
            resultado_1fn = "✅ Cumple con la Primera Forma Normal (1FN)." if ok1 else \
                            f"❌ No cumple con 1FN. Columna '{col}' tiene valor no atómico: '{val}'"
            if estructura_df is not None:
                resultado_2fn = verificar_2FN(estructura_df, datos_df)
            resultado_3fn = verificar_3FN(datos_df, estructura_df)

            if estructura_df is not None:
                (
                    schema_final,
                    tablas_data,
                    diagram_mermaid,
                    sql_script,
                    descripcion,
                    resumen_acciones,
                ) = normalizar_pipeline(estructura_df, datos_df)

                for name, df in tablas_data.items():
                    tablas_result_html.append((
                        name,
                        df.head(50).to_html(classes='table table-sm table-striped', index=False)
                    ))

        return render_template(
            'index.html',
            estructura=estructura_df.to_html(classes='table table-bordered', index=False) if estructura_df is not None else None,
            datos=datos_df.to_html(classes='table table-striped', index=False) if datos_df is not None else None,
            resultado_1fn=resultado_1fn,
            resultado_2fn=resultado_2fn,
            resultado_3fn=resultado_3fn,
            diagram_mermaid=diagram_mermaid,
            sql_script=sql_script,
            descripcion=descripcion,
            tablas_result_html=tablas_result_html,
            resumen_acciones=resumen_acciones
        )
    except Exception as e:
        app.logger.exception("Error en '/'")
        return f"Error en la aplicación: {e}", 500


# ===== API: Autodetectar y listar bases (sin servidor) =====
@app.post("/api/sql/probe")
def api_sql_probe():
    try:
        payload = request.get_json(silent=True) or {}
        database = payload.get("database")  # opcional; normalmente None aquí

        # Conexión automática
        conn, used = safe_connect_autodetect(database=database)
        if conn is None:
            return jsonify({"ok": False, "error": "No fue posible conectar automáticamente."}), 400

        try:
            dbs = list_databases(conn)
            return jsonify({"ok": True, "server": used, "databases": dbs})
        finally:
            try: conn.close()
            except Exception: pass
    except Exception as e:
        app.logger.exception("Fallo en /api/sql/probe")
        return jsonify({"ok": False, "error": str(e)}), 500


# ===== API: Listar tablas por esquema (sin servidor) =====
@app.post("/api/sql/tables")
def api_sql_tables():
    try:
        payload = request.get_json(silent=True) or {}
        database = payload.get("database")
        if not database:
            return jsonify({"ok": False, "error": "Falta 'database'."}), 400

        # Conexión automática
        conn, used_server = safe_connect_autodetect(database=database)
        if conn is None:
            return jsonify({"ok": False, "error": "No se pudo conectar."}), 400

        try:
            grouped = list_tables_grouped(conn)
            return jsonify({"ok": True, "server": used_server, "schemas": grouped})
        finally:
            try: conn.close()
            except Exception: pass
    except Exception as e:
        app.logger.exception("Fallo en /api/sql/tables")
        return jsonify({"ok": False, "error": str(e)}), 500


if __name__ == '__main__':
    import webbrowser
    import threading

    def open_browser():
        webbrowser.open("http://127.0.0.1:5000")

    threading.Timer(1.0, open_browser).start()
    
    app.run(debug=True, host='127.0.0.1', port=5000)
