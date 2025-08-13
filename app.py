from flask import Flask, render_template, request
import pandas as pd
import os

app = Flask(__name__)
UPLOAD_FOLDER = 'uploads'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# ---------- 1FN ----------
def es_valor_atomico(valor):
    if pd.isna(valor):
        return True
    if isinstance(valor, str) and any(sep in valor for sep in [",", ";", "/", "|", "[", "]"]):
        return False
    return True

def verificar_1FN(df):
    for col in df.columns:
        for val in df[col]:
            if not es_valor_atomico(val):
                return False, col, val
    return True, None, None

# ---------- 2FN ----------
def verificar_2FN(estructura_df, datos_df):
    mensajes = []
    claves = estructura_df[estructura_df['llave'].str.contains('PK', na=False)]
    tablas = estructura_df['tabla'].unique()

    for tabla in tablas:
        tabla_pk = claves[claves['tabla'] == tabla]['atributo'].tolist()
        if len(tabla_pk) < 2:
            continue  # Solo analizamos claves compuestas

        columnas = estructura_df[estructura_df['tabla'] == tabla]['atributo'].tolist()
        comunes = [c for c in tabla_pk if c in datos_df.columns]

        if not all(col in datos_df.columns for col in tabla_pk):
            continue

        for col in datos_df.columns:
            if col not in tabla_pk:
                df_temp = datos_df.groupby(comunes)[col].nunique().reset_index()
                if df_temp[col].max() == 1:
                    mensajes.append(
                        f"❌ '{col}' depende solo de parte de la clave compuesta {comunes} en la tabla '{tabla}'"
                    )

    if not mensajes:
        mensajes.append("✅ Los datos cumplen con la Segunda Forma Normal (2FN).")

    return mensajes

# ---------- 3FN ----------
def verificar_3FN(datos_df):
    mensajes = []
    for col in datos_df.columns:
        for other_col in datos_df.columns:
            if col != other_col:
                df_temp = datos_df.groupby(col)[other_col].nunique().reset_index()
                if df_temp[other_col].max() == 1:
                    mensajes.append(
                        f"❌ Existe una dependencia transitoria: '{other_col}' depende de '{col}'"
                    )
                    return mensajes
    mensajes.append("✅ Los datos cumplen con la Tercera Forma Normal (3FN).")
    return mensajes

# ---------- FLASK ----------
@app.route('/', methods=['GET', 'POST'])
def index():
    estructura_df = None
    datos_df = None
    resultado_1fn = None
    resultado_2fn = None
    resultado_3fn = None

    if request.method == 'POST':
        estructura_file = request.files['estructura']
        datos_file = request.files['datos']

        if estructura_file and estructura_file.filename.endswith('.csv'):
            estructura_path = os.path.join(app.config['UPLOAD_FOLDER'], estructura_file.filename)
            estructura_file.save(estructura_path)
            estructura_df = pd.read_csv(estructura_path)

        if datos_file and datos_file.filename.endswith('.csv'):
            datos_path = os.path.join(app.config['UPLOAD_FOLDER'], datos_file.filename)
            datos_file.save(datos_path)
            datos_df = pd.read_csv(datos_path)

            # 1FN
            cumple_1fn, col, val = verificar_1FN(datos_df)
            if cumple_1fn:
                resultado_1fn = "✅ Cumple con la Primera Forma Normal (1FN)."
            else:
                resultado_1fn = f"❌ No cumple con 1FN. Columna '{col}' tiene valor no atómico: '{val}'"

            # 2FN
            if cumple_1fn and estructura_df is not None:
                resultado_2fn = verificar_2FN(estructura_df, datos_df)

            # 3FN
            if cumple_1fn:
                resultado_3fn = verificar_3FN(datos_df)

    return render_template(
        'index.html',
        estructura=estructura_df.to_html(classes='table table-bordered', index=False) if estructura_df is not None else None,
        datos=datos_df.to_html(classes='table table-striped', index=False) if datos_df is not None else None,
        resultado_1fn=resultado_1fn,
        resultado_2fn=resultado_2fn,
        resultado_3fn=resultado_3fn
    )

if __name__ == '__main__':
    app.run(debug=True)
