from flask import Flask, render_template, request, redirect, url_for, flash, session
from werkzeug.utils import secure_filename
import pyodbc  # Mover import al inicio
import os, io, json
from contextlib import contextmanager

from config import Config, ANALYSIS_CFG
import validators as V
import sql_access as SA
import generators as GN
import analysis as AZ

# ------------------ App ------------------
app = Flask(__name__)
app.config.from_object(Config)
os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)

# ------------------ Utils ------------------
def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in app.config["ALLOWED_EXT"]

def set_estructura_in_session(rows):
    session["estructura_rows_json"] = json.dumps(rows, ensure_ascii=False)

def get_estructura_from_session():
    raw = session.get("estructura_rows_json")
    return json.loads(raw) if raw else None

def validate_positive_int(value, default=5, max_value=100):
    """Valida que el valor sea un entero positivo dentro de límites."""
    try:
        val = int(value)
        if val <= 0:
            return default
        return min(val, max_value)
    except (ValueError, TypeError):
        return default

@contextmanager
def get_db_connection():
    """Context manager para manejo seguro de conexiones."""
    conn_str = session.get("conn_str")
    if not conn_str:
        raise RuntimeError("No hay conexión activa. Ve a /conectar_sql")
    
    conn = None
    try:
        conn = pyodbc.connect(conn_str, timeout=30)
        yield conn
    except pyodbc.Error as e:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

# Inyecta defaults para que los templates nunca fallen por variables faltantes
@app.context_processor
def inject_defaults():
    return {
        "result": None,
        "saved": session.get("conn_saved", {})
    }

# ------------------ Routes ------------------
@app.route("/")
def index():
    has_csv = bool(session.get("estructura_rows_json"))
    has_conn = bool(session.get("conn_str"))
    return render_template(
        "index.html",
        has_csv=has_csv,
        has_conn=has_conn,
        saved=session.get("conn_saved", {})
    )

@app.route("/upload", methods=["POST"])
def upload():
    file = request.files.get("estructura")
    if not file or file.filename == "":
        flash("Selecciona un archivo .csv", "error")
        return redirect(url_for("index"))

    # Solo CSV
    if not allowed_file(file.filename):
        flash("Extensión no permitida. Usa .csv", "error")
        return redirect(url_for("index"))

    try:
        # Guardar copia física (útil para depurar)
        filename = secure_filename(file.filename)
        path = os.path.join(app.config["UPLOAD_FOLDER"], filename)
        file.stream.seek(0)
        file.save(path)

        # Leer y validar usando validators.py
        with open(path, "rb") as f:
            headers, rows = V.parse_csv(io.BytesIO(f.read()))

        if not headers:
            flash("No se pudieron leer encabezados. Verifica el CSV.", "error")
            return redirect(url_for("index"))

        head_errors = V.validate_headers(V.normalize_headers(headers))
        row_errors, row_warnings = V.validate_rows(
            rows,
            require_fk_target_in_csv=app.config.get("REQUIRE_FK_TARGET_IN_CSV", False)
        )
        errors = head_errors + row_errors

        # Guarda estructura en sesión (aunque haya errores, para previsualizar)
        set_estructura_in_session(rows)

        if errors:
            return render_template(
                "resultado.html",
                ok=False,
                errors=errors,
                warnings=row_warnings,
                filename=filename,
                rows_preview=rows[:20],
                has_conn=bool(session.get("conn_str")),
                saved=session.get("conn_saved", {})
            )

        flash("Archivo de estructura válido ✅", "success")
        return render_template(
            "resultado.html",
            ok=True,
            errors=[],
            warnings=row_warnings,
            filename=filename,
            rows_preview=rows[:20],
            has_conn=bool(session.get("conn_str")),
            saved=session.get("conn_saved", {})
        )
    
    except Exception as e:
        flash(f"Error procesando el archivo: {str(e)}", "error")
        return redirect(url_for("index"))

# ------ Conexión a SQL ------
@app.route("/conectar_sql", methods=["GET", "POST"])
def conectar_sql():
    if request.method == "GET":
        return render_template("conectar_sql.html", saved=session.get("conn_saved", {}))

    try:
        server = request.form.get("server", "").strip()
        database = request.form.get("database", "").strip()
        auth_mode = request.form.get("auth_mode", "windows")
        uid = request.form.get("uid", "").strip()
        pwd = request.form.get("pwd", "").strip()
        driver = request.form.get("driver", "{ODBC Driver 17 for SQL Server}").strip() or "{ODBC Driver 17 for SQL Server}"

        if not server or not database:
            flash("Servidor/instancia y base de datos son obligatorios.", "error")
            return redirect(url_for("conectar_sql"))
        
        if auth_mode == "sql" and (not uid or not pwd):
            flash("Usuario y contraseña son obligatorios para autenticación SQL.", "error")
            return redirect(url_for("conectar_sql"))

        conn_str = SA.build_conn_str(server, database, auth_mode, uid=uid, pwd=pwd, driver=driver)

        SA.test_connection(conn_str)
        
        session["conn_str"] = conn_str
        session["conn_saved"] = {
            "server": server, "database": database, "auth_mode": auth_mode, "driver": driver,
            "uid": uid if auth_mode == "sql" else ""
        }
        flash("✅ Conexión exitosa", "success")
        return redirect(url_for("tablas"))
        
    except Exception as e:
        flash(f"❌ No se pudo conectar: {e}", "error")
        return redirect(url_for("conectar_sql"))

@app.route("/tablas")
def tablas():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT TABLE_SCHEMA, TABLE_NAME
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_TYPE = 'BASE TABLE'
                    ORDER BY TABLE_SCHEMA, TABLE_NAME
                """)
                tables = [{"schema": r[0], "name": r[1]} for r in cur.fetchall()]
    except Exception as e:
        flash(f"Error al listar tablas: {e}", "error")
        return redirect(url_for("conectar_sql"))

    return render_template("tablas.html", tables=tables, saved=session.get("conn_saved", {}))

@app.route("/preview")
def preview():
    schema = (request.args.get("schema") or "dbo").strip()
    table = (request.args.get("table") or "").strip()
    top = validate_positive_int(request.args.get("top", "5"), default=5, max_value=50)

    if not table:
        flash("Selecciona una tabla.", "error")
        return redirect(url_for("tablas"))

    rows, cols, err = [], [], None
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Obtener columnas
                cur.execute("""
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                    ORDER BY ORDINAL_POSITION
                """, (schema, table))
                cols = [r[0] for r in cur.fetchall()]
                
                if not cols:
                    raise RuntimeError("No se encontraron columnas para la tabla seleccionada.")

                # Obtener datos - usar parámetros para prevenir inyección SQL
                cur.execute(f"""
                    SELECT TOP (?) * 
                    FROM [{schema}].[{table}]
                """, (top,))
                rows = [list("" if x is None else str(x) for x in r) for r in cur.fetchall()]
                
    except Exception as e:
        err = str(e)

    return render_template(
        "preview.html", 
        schema=schema, table=table, cols=cols, rows=rows, 
        err=err, top=top, saved=session.get("conn_saved", {})
    )

# ------ Análisis ------
@app.route("/analizar")
def analizar():
    rows = get_estructura_from_session()
    if not rows:
        flash("Primero sube un archivo de estructura CSV.", "error")
        return redirect(url_for("index"))

    try:
        attrs_por_tabla, _, _ = AZ.estructura_por_tabla(rows)
        tablas_csv = set(attrs_por_tabla.keys())

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT TABLE_SCHEMA, TABLE_NAME
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_TYPE = 'BASE TABLE'
                """)
                tablas_sql = {(r[0], r[1]) for r in cur.fetchall()}

        presentes = []
        for t in sorted(tablas_csv):
            pair = None
            for sch, tb in tablas_sql:
                if tb.lower() == t.lower():
                    pair = (sch, tb)
                    break
            presentes.append({"csv": t, "sql": pair})

        return render_template("analizar.html", presentes=presentes, saved=session.get("conn_saved", {}))
        
    except Exception as e:
        flash(f"Error durante el análisis: {e}", "error")
        return redirect(url_for("index"))

@app.route("/analizar_tabla")
def analizar_tabla():
    rows = get_estructura_from_session()
    if not rows:
        flash("Primero sube un archivo de estructura CSV.", "error")
        return redirect(url_for("index"))

    tabla_csv = (request.args.get("csv_table") or "").strip()
    schema = (request.args.get("schema") or "dbo").strip()
    table = (request.args.get("table") or "").strip()
    
    if not tabla_csv or not table:
        flash("Faltan parámetros de tabla.", "error")
        return redirect(url_for("analizar"))

    try:
        with get_db_connection() as conn:
            result = AZ.analyze_table(conn, schema, table, tabla_csv, rows, ANALYSIS_CFG)
    except Exception as e:
        flash(f"Error analizando la tabla: {e}", "error")
        return redirect(url_for("analizar"))

    return render_template(
        "analizar_tabla.html",
        csv_table=tabla_csv,
        result=result,
        has_conn=bool(session.get("conn_str")),
        saved=session.get("conn_saved", {})
    )

@app.route("/generar_scripts")
def generar_scripts():
    rows = get_estructura_from_session()
    if not rows:
        flash("Primero sube un archivo de estructura CSV.", "error")
        return redirect(url_for("index"))

    if not session.get("conn_str"):
        flash("Conéctate a SQL Server para generar scripts.", "error")
        return redirect(url_for("conectar_sql"))

    tabla_csv = (request.args.get("csv_table") or "").strip()
    schema = (request.args.get("schema") or "dbo").strip()
    table = (request.args.get("table") or "").strip()
    
    if not tabla_csv or not table:
        flash("Faltan parámetros de tabla.", "error")
        return redirect(url_for("analizar"))

    try:
        with get_db_connection() as conn:
            result = AZ.analyze_table(conn, schema, table, tabla_csv, rows, ANALYSIS_CFG)
        sql_text, plan = GN.generate_sql(schema, table, tabla_csv, rows, result)
    except Exception as e:
        flash(f"Error generando scripts: {e}", "error")
        return redirect(url_for("analizar"))

    return render_template(
        "generar_scripts.html",
        schema=schema, table=table, csv_table=tabla_csv,
        sql_text=sql_text, plan=plan,
        has_conn=True,
        saved=session.get("conn_saved", {})
    )

@app.route("/analizar_todo")
def analizar_todo():
    rows = get_estructura_from_session()
    if not rows:
        flash("Primero sube un archivo de estructura CSV.", "error")
        return redirect(url_for("index"))

    try:
        # Reusa la lógica de /analizar para emparejar CSV ↔ SQL
        attrs_por_tabla, _, _ = AZ.estructura_por_tabla(rows)
        tablas_csv = sorted(set(attrs_por_tabla.keys()))

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT TABLE_SCHEMA, TABLE_NAME
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_TYPE = 'BASE TABLE'
                """)
                tablas_sql = {(r[0], r[1]) for r in cur.fetchall()}

        # Emparejar
        pairs = []
        for t in tablas_csv:
            sql_pair = None
            for sch, tb in tablas_sql:
                if tb.lower() == t.lower():
                    sql_pair = (sch, tb)
                    break
            pairs.append((t, sql_pair))

        # Analizar todas
        resultados = []
        for csv_name, sql_pair in pairs:
            if not sql_pair:
                resultados.append({
                    "csv": csv_name, "schema": None, "table": None,
                    "status": "sin_sql", "msg": "No encontrada en SQL", 
                    "issues": [], "ok": False
                })
                continue

            schema, table = sql_pair
            try:
                with get_db_connection() as conn:
                    res = AZ.analyze_table(conn, schema, table, csv_name, rows, ANALYSIS_CFG)

                # Resumen/estado
                one = res.get("one_nf", {}) or {}
                n_atomic = len(one.get("atomic_issues", []) or [])
                n_groups = len(one.get("name_groups", []) or [])
                n2 = len(res.get("two_nf", []) or [])
                n3 = len(res.get("three_nf", []) or [])

                ok = (n_atomic == 0 and n_groups == 0 and n2 == 0 and n3 == 0)

                issues = []
                if n_atomic: 
                    issues.append(f"1FN: {n_atomic} valores no atómicos")
                if n_groups: 
                    issues.append(f"1FN: {n_groups} grupo(s) de columnas repetidas")
                if n2:      
                    issues.append(f"2FN: {n2} dependencia(s) parcial(es)")
                if n3:      
                    issues.append(f"3FN: {n3} dependencia(s) transitiva(s)")

                msg = "OK" if ok else "Con problemas"
                resultados.append({
                    "csv": csv_name, "schema": schema, "table": table,
                    "status": "ok" if ok else "problemas",
                    "msg": msg, "issues": issues, "ok": ok, "result": res
                })

            except Exception as e:
                resultados.append({
                    "csv": csv_name, "schema": schema, "table": table,
                    "status": "error", "msg": f"Error: {e}", 
                    "issues": [], "ok": False
                })

        return render_template("analizar_todo.html", resultados=resultados, saved=session.get("conn_saved", {}))
        
    except Exception as e:
        flash(f"Error en análisis completo: {e}", "error")
        return redirect(url_for("index"))

if __name__ == "__main__":
    app.run(debug=True)