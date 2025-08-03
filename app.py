from flask import Flask, render_template, request
import pandas as pd
import os

app = Flask(__name__)
UPLOAD_FOLDER = 'uploads'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

@app.route('/', methods=['GET', 'POST'])
def index():
    table_data = None
    if request.method == 'POST':
        file = request.files['archivo']
        if file and file.filename.endswith('.csv'):
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
            file.save(filepath)
            df = pd.read_csv(filepath)
            table_data = df.to_html(classes='table table-bordered', index=False)
    return render_template('index.html', table=table_data)

if __name__ == '__main__':
    app.run(debug=True)
