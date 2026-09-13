from flask import Flask, render_template, request, jsonify
import os

app = Flask(__name__)

# Ruta principal de la aplicación P2Ppredict
@app.route('/')
def index():
    return render_template('index.html')

# API de estado y salud del servidor
@app.route('/api/health', methods=['GET'])
def health_check():
    return jsonify({"status": "online", "app": "P2Ppredict", "version": "2.0"})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
