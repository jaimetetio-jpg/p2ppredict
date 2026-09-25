from collections import defaultdict
from datetime import datetime
import os
import random
import time
from flask import Flask, jsonify, render_template, request, session
from flask_cors import CORS
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
import requests
from werkzeug.security import check_password_hash, generate_password_hash

app = Flask(__name__)

# ================= APARTADO DE VALIDACIÓN - KEY TXT =================
VALIDATION_KEY_TXT = "8c73ed3c39ffc42821ce971267c7b58d01487ed71624c57309cc6079dd976f5f8f462164ffbdc8424ba6d641e94332ef1d8b17e8789cb1385717cceaecf6eb79"
# =====================================================================

CORS(app, resources={r"/api/*": {"origins": "*"}}, supports_credentials=True)
app.secret_key = os.environ.get(
    "FLASK_SECRET_KEY", "p2ppredict_secret_key_ultra_segura_2026"
)

RAW_ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "Anthony*2023")
ADMIN_PASSWORD_HASH = generate_password_hash(RAW_ADMIN_PASSWORD)
PI_API_KEY = os.environ.get("PI_API_KEY", "")
DATABASE_URL = os.environ.get("DATABASE_URL")

# ================= SISTEMA DE RATE LIMITING EN MEMORIA =================
request_records = defaultdict(list)


def check_rate_limit(limit=25, window=60):
    ip = request.remote_addr or "127.0.0.1"
    now = time.time()
    request_records[ip] = [t for t in request_records[ip] if now - t < window]
    if len(request_records[ip]) >= limit:
        return False
    request_records[ip].append(now)
    return True


# ================= CONFIGURACIÓN DE POOL DE CONEXIONES Y BASE DE DATOS =================
db_pool = None
if DATABASE_URL:
    try:
        db_pool = pool.ThreadedConnectionPool(1, 25, DATABASE_URL)
    except Exception:
        db_pool = None


class PooledConnectionWrapper:

    def __init__(self, conn, p):
        self.conn = conn
        self.pool = p

    def cursor(self, *args, **kwargs):
        if "cursor_factory" not in kwargs and not args:
            kwargs["cursor_factory"] = RealDictCursor
        return self.conn.cursor(*args, **kwargs)

    def commit(self):
        return self.conn.commit()

    def rollback(self):
        return self.conn.rollback()

    def close(self):
        if self.pool:
            try:
                self.pool.putconn(self.conn)
            except Exception:
                try:
                    self.conn.close()
                except:
                    pass
        else:
            try:
                self.conn.close()
            except:
                pass


def obtener_conexion():
    if DATABASE_URL and db_pool:
        try:
            conn = db_pool.getconn()
            return PooledConnectionWrapper(conn, db_pool)
        except Exception:
            pass
    if DATABASE_URL:
        conn = psycopg2.connect(
            DATABASE_URL, cursor_factory=RealDictCursor, connect_timeout=10
        )
        return conn
    else:
        import sqlite3

        conn = sqlite3.connect("p2ppredict.db", timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        return conn


def actualizar_esquema_db():
    if not DATABASE_URL:
        return
    conn = obtener_conexion()
    cur = conn.cursor()
    try:
        cur.execute("""
            ALTER TABLE usuarios 
            ADD COLUMN IF NOT EXISTS is_frozen BOOLEAN DEFAULT FALSE;
        """)
        cur.execute("""
            ALTER TABLE orders 
            ALTER COLUMN evento_id TYPE TEXT USING evento_id::TEXT;
        """)
        conn.commit()
    except Exception as e:
        conn.rollback()
    finally:
        cur.close()
        conn.close()


def inicializar_bd():
    actualizar_esquema_db()
    conn = obtener_conexion()
    c = conn.cursor()
    if DATABASE_URL:
        c.execute("""CREATE TABLE IF NOT EXISTS usuarios (
            username TEXT PRIMARY KEY, 
            saldo_disponible DOUBLE PRECISION DEFAULT 0.0, 
            is_frozen BOOLEAN DEFAULT FALSE
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS transacciones (
            id SERIAL PRIMARY KEY, 
            username TEXT, 
            tipo TEXT, 
            monto DOUBLE PRECISION, 
            txid TEXT, 
            fecha TEXT
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS historial_apuestas (
            id SERIAL PRIMARY KEY, 
            username TEXT, 
            titulo_evento TEXT, 
            opcion_elegida TEXT, 
            monto DOUBLE PRECISION, 
            estado TEXT
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS orders (
            id SERIAL PRIMARY KEY, 
            username TEXT, 
            evento_id TEXT, 
            opcion_id INTEGER, 
            tipo_orden TEXT, 
            accion TEXT, 
            precio DOUBLE PRECISION, 
            cantidad DOUBLE PRECISION, 
            estado TEXT DEFAULT 'activa', 
            fecha TEXT
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS posiciones_activas (
            id TEXT PRIMARY KEY, 
            market_id TEXT NOT NULL, 
            handle TEXT NOT NULL, 
            titulo TEXT NOT NULL, 
            opcion TEXT NOT NULL, 
            contratos INTEGER NOT NULL, 
            invertido NUMERIC NOT NULL, 
            payout NUMERIC NOT NULL, 
            created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS historial_transacciones (
            id TEXT PRIMARY KEY, 
            titulo TEXT NOT NULL, 
            tipo TEXT NOT NULL, 
            monto NUMERIC NOT NULL, 
            detalle TEXT NOT NULL, 
            created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS eventos (
            id SERIAL PRIMARY KEY, 
            titulo TEXT, 
            categoria TEXT, 
            estado TEXT DEFAULT 'activo', 
            fecha_cierre TEXT, 
            ganador_id INTEGER
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS opciones_evento (
            id SERIAL PRIMARY KEY, 
            evento_id INTEGER, 
            nombre TEXT, 
            pozo DOUBLE PRECISION DEFAULT 0.0
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS admin_logs (
            id SERIAL PRIMARY KEY, 
            ip TEXT, 
            accion TEXT, 
            detalles TEXT, 
            fecha TEXT
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS admin_balance_audit (
            id SERIAL PRIMARY KEY, 
            admin_user TEXT, 
            target_user TEXT, 
            monto_anterior DOUBLE PRECISION, 
            monto_nuevo DOUBLE PRECISION, 
            razon TEXT, 
            fecha TEXT
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS admin_audit_logs (
            id SERIAL PRIMARY KEY, 
            admin_id TEXT, 
            action_type TEXT, 
            target_id TEXT, 
            ip_address TEXT, 
            user_agent TEXT, 
            payload_snapshot TEXT, 
            created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS admin_pending_actions (
            id SERIAL PRIMARY KEY, 
            admin_creator TEXT, 
            action_type TEXT, 
            target_id TEXT, 
            payload TEXT, 
            status TEXT DEFAULT 'PENDING', 
            created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS anuncios_globales (
            id SERIAL PRIMARY KEY, 
            titulo TEXT NOT NULL, 
            contenido TEXT NOT NULL, 
            tipo TEXT DEFAULT 'info', 
            activo BOOLEAN DEFAULT TRUE, 
            fecha TEXT
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS pi_wallet_events (
            id SERIAL PRIMARY KEY, 
            username TEXT, 
            evento_tipo TEXT, 
            monto DOUBLE PRECISION, 
            balance_total_plataforma DOUBLE PRECISION, 
            txid TEXT, 
            fecha TEXT
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS global_audit_logs (
            id SERIAL PRIMARY KEY, 
            username TEXT, 
            accion TEXT, 
            detalle TEXT, 
            created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS support_tickets (
            id SERIAL PRIMARY KEY, 
            username TEXT, 
            mensaje TEXT, 
            fecha TEXT
        )""")
        c.execute(
            "CREATE INDEX IF NOT EXISTS idx_global_audit_username ON global_audit_logs(username);"
        )
    else:
        c.execute("""CREATE TABLE IF NOT EXISTS usuarios (
            username TEXT PRIMARY KEY, 
            saldo_disponible REAL DEFAULT 0.0, 
            is_frozen INTEGER DEFAULT 0
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS transacciones (
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            username TEXT, 
            tipo TEXT, 
            monto REAL, 
            txid TEXT, 
            fecha TEXT
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS historial_apuestas (
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            username TEXT, 
            titulo_evento TEXT, 
            opcion_elegida TEXT, 
            monto REAL, 
            estado TEXT
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            username TEXT, 
            evento_id TEXT, 
            opcion_id INTEGER, 
            tipo_orden TEXT, 
            accion TEXT, 
            precio REAL, 
            cantidad REAL, 
            estado TEXT DEFAULT 'activa', 
            fecha TEXT
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS posiciones_activas (
            id TEXT PRIMARY KEY, 
            market_id TEXT NOT NULL, 
            handle TEXT NOT NULL, 
            titulo TEXT NOT NULL, 
            opcion TEXT NOT NULL, 
            contratos INTEGER NOT NULL, 
            invertido REAL NOT NULL, 
            payout REAL NOT NULL, 
            created_at TEXT
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS historial_transacciones (
            id TEXT PRIMARY KEY, 
            titulo TEXT NOT NULL, 
            tipo TEXT NOT NULL, 
            monto REAL NOT NULL, 
            detalle TEXT NOT NULL, 
            created_at TEXT
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS eventos (
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            titulo TEXT, 
            categoria TEXT, 
            estado TEXT DEFAULT 'activo', 
            fecha_cierre TEXT, 
            ganador_id INTEGER
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS opciones_evento (
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            evento_id INTEGER, 
            nombre TEXT, 
            pozo REAL DEFAULT 0.0
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS admin_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            ip TEXT, 
            accion TEXT, 
            detalles TEXT, 
            fecha TEXT
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS admin_balance_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            admin_user TEXT, 
            target_user TEXT, 
            monto_anterior REAL, 
            monto_nuevo REAL, 
            razon TEXT, 
            fecha TEXT
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS admin_audit_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            admin_id TEXT, 
            action_type TEXT, 
            target_id TEXT, 
            ip_address TEXT, 
            user_agent TEXT, 
            payload_snapshot TEXT, 
            created_at TEXT
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS admin_pending_actions (
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            admin_creator TEXT, 
            action_type TEXT, 
            target_id TEXT, 
            payload TEXT, 
            status TEXT DEFAULT 'PENDING', 
            created_at TEXT
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS anuncios_globales (
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            titulo TEXT NOT NULL, 
            contenido TEXT NOT NULL, 
            tipo TEXT DEFAULT 'info', 
            activo INTEGER DEFAULT 1, 
            fecha TEXT
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS pi_wallet_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            username TEXT, 
            evento_tipo TEXT, 
            monto REAL, 
            balance_total_plataforma REAL, 
            txid TEXT, 
            fecha TEXT
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS global_audit_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            username TEXT, 
            accion TEXT, 
            detalle TEXT, 
            created_at TEXT
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS support_tickets (
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            username TEXT, 
            mensaje TEXT, 
            fecha TEXT
        )""")
        c.execute(
            "CREATE INDEX IF NOT EXISTS idx_global_audit_username ON global_audit_logs(username);"
        )

    # Se eliminaron los mercados estáticos de BTC y Pi Network de la inicialización para trabajar puramente con datos dinámicos.
    conn.commit()
    conn.close()


inicializar_bd()


def registrar_log_admin(accion, detalles):
    try:
        conn = obtener_conexion()
        c = conn.cursor()
        ip = request.remote_addr or "127.0.0.1"
        fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if DATABASE_URL:
            c.execute(
                "INSERT INTO admin_logs (ip, accion, detalles, fecha) VALUES"
                " (%s, %s, %s, %s)",
                (ip, accion, detalles, fecha),
            )
        else:
            c.execute(
                "INSERT INTO admin_logs (ip, accion, detalles, fecha) VALUES"
                " (?, ?, ?, ?)",
                (ip, accion, detalles, fecha),
            )
        conn.commit()
        conn.close()
    except Exception:
        pass


def registrar_audit_log(admin_id, action_type, target_id, payload_snapshot):
    try:
        conn = obtener_conexion()
        c = conn.cursor()
        ip = request.remote_addr or "127.0.0.1"
        ua = request.user_agent.string or "Desconocido"
        if DATABASE_URL:
            c.execute(
                "INSERT INTO admin_audit_logs (admin_id, action_type,"
                " target_id, ip_address, user_agent, payload_snapshot) VALUES"
                " (%s, %s, %s, %s, %s, %s)",
                (
                    admin_id,
                    action_type,
                    target_id,
                    ip,
                    ua,
                    str(payload_snapshot),
                ),
            )
        else:
            fecha_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            c.execute(
                "INSERT INTO admin_audit_logs (admin_id, action_type,"
                " target_id, ip_address, user_agent, payload_snapshot,"
                " created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    admin_id,
                    action_type,
                    target_id,
                    ip,
                    ua,
                    str(payload_snapshot),
                    fecha_str,
                ),
            )
        conn.commit()
        conn.close()
    except Exception:
        pass


def registrar_global_audit(username, accion, detalle):
    try:
        conn = obtener_conexion()
        c = conn.cursor()
        fecha_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if DATABASE_URL:
            c.execute(
                "INSERT INTO global_audit_logs (username, accion, detalle)"
                " VALUES (%s, %s, %s)",
                (username, accion, detalle),
            )
        else:
            c.execute(
                "INSERT INTO global_audit_logs (username, accion, detalle,"
                " created_at) VALUES (?, ?, ?, ?)",
                (username, accion, detalle, fecha_str),
            )
        conn.commit()
        conn.close()
    except Exception:
        pass


@app.after_request
def agregar_cabeceras_seguridad(response):
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Strict-Transport-Security"] = (
        "max-age=31536000; includeSubDomains"
    )
    response.headers["Cross-Origin-Embedder-Policy"] = "unsafe-none"
    response.headers["Cross-Origin-Opener-Policy"] = "unsafe-none"
    
    if request.path.startswith('/api/'):
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        
    return response


@app.route("/")
def home():
    return render_template("index.html")


@app.route('/validation-key.txt')
def validation_key():
    return "1b9ee5cdf565585e21f8bd18899df2e7026cb"


@app.route('/healthz')
def healthz():
    return "OK", 200


@app.route("/api/saldo/<username>", methods=["GET"])
def obtener_saldo(username):
    limite = int(request.args.get("limit", 20))
    offset = int(request.args.get("offset", 0))
    filtro_tipo = request.args.get("tipo", "").strip()

    conn = obtener_conexion()
    c = conn.cursor()

    if DATABASE_URL:
        c.execute(
            "SELECT saldo_disponible, is_frozen FROM usuarios WHERE username ="
            " %s",
            (username,),
        )
    else:
        c.execute(
            "SELECT saldo_disponible, is_frozen FROM usuarios WHERE username ="
            " ?",
            (username,),
        )
    row = c.fetchone()

    if not row:
        saldo_inicial = (
            0.10 if username.lower() in ["@jaimetetio", "jaimetetio"] else 0.0
        )
        if DATABASE_URL:
            c.execute(
                "INSERT INTO usuarios (username, saldo_disponible, is_frozen)"
                " VALUES (%s, %s, FALSE)",
                (username, saldo_inicial),
            )
            if saldo_inicial > 0:
                txid = (
                    f"CREDITO_INICIAL_{datetime.now().strftime('%Y%m%d%H%M%S')}"
                )
                fecha = datetime.now().strftime("%Y-%m-%d %H:%M")
                c.execute(
                    "INSERT INTO transacciones (username, tipo, monto, txid,"
                    " fecha) VALUES (%s, %s, %s, %s, %s)",
                    (username, "Crédito Inicial", saldo_inicial, txid, fecha),
                )
        else:
            c.execute(
                "INSERT INTO usuarios (username, saldo_disponible, is_frozen)"
                " VALUES (?, ?, 0)",
                (username, saldo_inicial),
            )
            if saldo_inicial > 0:
                txid = (
                    f"CREDITO_INICIAL_{datetime.now().strftime('%Y%m%d%H%M%S')}"
                )
                fecha = datetime.now().strftime("%Y-%m-%d %H:%M")
                c.execute(
                    "INSERT INTO transacciones (username, tipo, monto, txid,"
                    " fecha) VALUES (?, ?, ?, ?, ?)",
                    (username, "Crédito Inicial", saldo_inicial, txid, fecha),
                )
        conn.commit()
        saldo = saldo_inicial
        is_frozen = False
    else:
        row_dict = dict(row)
        saldo = row_dict.get("saldo_disponible", 0.0)
        is_frozen = bool(row_dict.get("is_frozen", 0))

    if DATABASE_URL:
        c.execute(
            "SELECT * FROM historial_apuestas WHERE username = %s ORDER BY id"
            " DESC LIMIT %s OFFSET %s",
            (username, limite, offset),
        )
    else:
        c.execute(
            "SELECT * FROM historial_apuestas WHERE username = ? ORDER BY id"
            " DESC LIMIT ? OFFSET ?",
            (username, limite, offset),
        )
    historial = [dict(row) for row in c.fetchall()]

    if filtro_tipo:
        if DATABASE_URL:
            c.execute(
                "SELECT * FROM transacciones WHERE username = %s AND tipo ILIKE"
                " %s ORDER BY id DESC LIMIT %s OFFSET %s",
                (username, f"%{filtro_tipo}%", limite, offset),
            )
        else:
            c.execute(
                "SELECT * FROM transacciones WHERE username = ? AND tipo LIKE ?"
                " ORDER BY id DESC LIMIT ? OFFSET ?",
                (username, f"%{filtro_tipo}%", limite, offset),
            )
    else:
        if DATABASE_URL:
            c.execute(
                "SELECT * FROM transacciones WHERE username = %s ORDER BY id"
                " DESC LIMIT %s OFFSET %s",
                (username, limite, offset),
            )
        else:
            c.execute(
                "SELECT * FROM transacciones WHERE username = ? ORDER BY id"
                " DESC LIMIT ? OFFSET ?",
                (username, limite, offset),
            )
    transacciones = [dict(row) for row in c.fetchall()]
    conn.close()

    return jsonify({
        "success": True,
        "saldo_disponible": saldo,
        "is_frozen": is_frozen,
        "historial": historial,
        "transacciones": transacciones,
    })


@app.route("/api/eventos", methods=["GET"])
def obtener_eventos():
    conn = obtener_conexion()
    c = conn.cursor()
    c.execute("SELECT * FROM eventos ORDER BY id ASC")
    eventos_db = c.fetchall()
    lista_final = []
    for ev in eventos_db:
        ev_dict = dict(ev)
        if DATABASE_URL:
            c.execute(
                "SELECT id, nombre, pozo FROM opciones_evento WHERE evento_id ="
                " %s",
                (ev_dict["id"],),
            )
        else:
            c.execute(
                "SELECT id, nombre, pozo FROM opciones_evento WHERE evento_id ="
                " ?",
                (ev_dict["id"],),
            )
        opciones = [dict(op) for op in c.fetchall()]
        ev_dict["opciones"] = opciones
        lista_final.append(ev_dict)
    conn.close()
    return jsonify(lista_final)


@app.route("/api/participar", methods=["POST"])
def participar():
    if not check_rate_limit(limit=25, window=60):
        return jsonify({
            "success": False,
            "error": "Demasiadas peticiones. Por favor, espera un momento.",
        }), 429

    data = request.json or {}
    username = data.get("username", "Invitado")
    evento_id = data.get("evento_id")
    opcion_id = data.get("opcion_id")
    try:
        monto = float(data.get("monto", 0))
    except (ValueError, TypeError):
        return jsonify({"success": False, "error": "Monto inválido"}), 400

    if monto <= 0:
        return jsonify({"success": False, "error": "El monto debe ser mayor a 0"}), 400

    conn = obtener_conexion()
    c = conn.cursor()
    try:
        if DATABASE_URL:
            c.execute(
                "SELECT saldo_disponible, is_frozen FROM usuarios WHERE"
                " username = %s FOR UPDATE",
                (username,),
            )
        else:
            c.execute(
                "SELECT saldo_disponible, is_frozen FROM usuarios WHERE"
                " username = ?",
                (username,),
            )
        row = c.fetchone()
        row_dict = dict(row) if row else {}
        if row_dict and row_dict.get("is_frozen"):
            conn.rollback()
            return jsonify({
                "success": False,
                "error": "Tu cuenta se encuentra suspendida temporalmente.",
            }), 403

        saldo_actual = row_dict.get("saldo_disponible", 0) if row else 0
        if not row or saldo_actual < monto:
            conn.rollback()
            return jsonify({"success": False, "error": "Saldo insuficiente"}), 400

        try:
            ev_id_int = int(evento_id)
        except (ValueError, TypeError):
            ev_id_int = None

        if ev_id_int is not None:
            if DATABASE_URL:
                c.execute("SELECT * FROM eventos WHERE id = %s", (ev_id_int,))
            else:
                c.execute("SELECT * FROM eventos WHERE id = ?", (ev_id_int,))
            evento = c.fetchone()
        else:
            evento = None

        evento_dict = dict(evento) if evento else {}
        if not evento or evento_dict.get("estado") != "activo":
            conn.rollback()
            return jsonify({"success": False, "error": "Mercado no disponible"}), 400

        if DATABASE_URL:
            c.execute(
                "SELECT * FROM opciones_evento WHERE id = %s AND evento_id = %s",
                (opcion_id, ev_id_int),
            )
        else:
            c.execute(
                "SELECT * FROM opciones_evento WHERE id = ? AND evento_id = ?",
                (opcion_id, ev_id_int),
            )
        opcion = c.fetchone()
        opcion_dict = dict(opcion) if opcion else {}
        if not opcion:
            conn.rollback()
            return jsonify({"success": False, "error": "Opción inválida"}), 400

        nuevo_saldo = saldo_actual - monto
        if DATABASE_URL:
            c.execute(
                "UPDATE usuarios SET saldo_disponible = %s WHERE username = %s",
                (nuevo_saldo, username),
            )
            c.execute(
                "UPDATE opciones_evento SET pozo = pozo + %s WHERE id = %s",
                (monto, opcion_id),
            )
            c.execute(
                "INSERT INTO historial_apuestas (username, titulo_evento,"
                " opcion_elegida, monto, estado) VALUES (%s, %s, %s, %s, %s)",
                (username, evento_dict.get("titulo"), opcion_dict.get("nombre"), monto, "Activo"),
            )
            c.execute(
                "INSERT INTO transacciones (username, tipo, monto, txid, fecha)"
                " VALUES (%s, %s, %s, %s, %s)",
                (
                    username,
                    "Apuesta",
                    -monto,
                    f"BET_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                    datetime.now().strftime("%Y-%m-%d %H:%M"),
                ),
            )
        else:
            c.execute(
                "UPDATE usuarios SET saldo_disponible = ? WHERE username = ?",
                (nuevo_saldo, username),
            )
            c.execute(
                "UPDATE opciones_evento SET pozo = pozo + ? WHERE id = ?",
                (monto, opcion_id),
            )
            c.execute(
                "INSERT INTO historial_apuestas (username, titulo_evento,"
                " opcion_elegida, monto, estado) VALUES (?, ?, ?, ?, ?)",
                (username, evento_dict.get("titulo"), opcion_dict.get("nombre"), monto, "Activo"),
            )
            c.execute(
                "INSERT INTO transacciones (username, tipo, monto, txid, fecha)"
                " VALUES (?, ?, ?, ?, ?)",
                (
                    username,
                    "Apuesta",
                    -monto,
                    f"BET_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                    datetime.now().strftime("%Y-%m-%d %H:%M"),
                ),
            )
        conn.commit()
        registrar_global_audit(
            username,
            "PARTICIPAR_APUESTA",
            f"Apuesta de {monto} en '{evento_dict.get('titulo')}' por '{opcion_dict.get('nombre')}'",
        )
        return jsonify({
            "success": True,
            "nuevo_saldo": nuevo_saldo,
            "mensaje": "¡Apuesta registrada con éxito!",
        })
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route("/api/clob/ordenes", methods=["GET"])
def obtener_ordenes_clob():
    evento_id = request.args.get("evento_id")
    conn = obtener_conexion()
    c = conn.cursor()
    if evento_id:
        if DATABASE_URL:
            c.execute(
                "SELECT * FROM orders WHERE evento_id::text = %s AND estado = 'activa'"
                " ORDER BY precio DESC",
                (str(evento_id),),
            )
        else:
            c.execute(
                "SELECT * FROM orders WHERE evento_id = ? AND estado = 'activa'"
                " ORDER BY precio DESC",
                (str(evento_id),),
            )
    else:
        c.execute(
            "SELECT * FROM orders WHERE estado = 'activa' ORDER BY id DESC"
            " LIMIT 50"
        )
    ordenes = [dict(row) for row in c.fetchall()]
    conn.close()
    return jsonify({"success": True, "ordenes": ordenes})


@app.route("/api/clob/actualizar-dinamico", methods=["GET"])
def actualizar_ordenes_dinamico():
    conn = obtener_conexion()
    c = conn.cursor()
    try:
        if DATABASE_URL:
            c.execute(
                "SELECT * FROM orders WHERE estado = 'activa' ORDER BY RANDOM()"
                " LIMIT 1"
            )
        else:
            c.execute(
                "SELECT * FROM orders WHERE estado = 'activa' ORDER BY RANDOM()"
                " LIMIT 1"
            )
        orden_azar = c.fetchone()
        orden_azar_dict = dict(orden_azar) if orden_azar else {}
        if orden_azar:
            variacion = round(random.uniform(-0.01, 0.01), 3)
            nuevo_precio = max(0.01, round(orden_azar_dict.get("precio", 0.0) + variacion, 3))
            if DATABASE_URL:
                c.execute(
                    "UPDATE orders SET precio = %s WHERE id = %s",
                    (nuevo_precio, orden_azar_dict.get("id")),
                )
            else:
                c.execute(
                    "UPDATE orders SET precio = ? WHERE id = ?",
                    (nuevo_precio, orden_azar_dict.get("id")),
                )
            conn.commit()

        if DATABASE_URL:
            c.execute(
                "SELECT * FROM orders WHERE estado = 'activa' ORDER BY precio"
                " DESC LIMIT 50"
            )
        else:
            c.execute(
                "SELECT * FROM orders WHERE estado = 'activa' ORDER BY precio"
                " DESC LIMIT 50"
            )
        ordenes = [dict(row) for row in c.fetchall()]
        conn.close()
        return jsonify({"success": True, "ordenes": ordenes, "timestamp": time.time()})
    except Exception as e:
        if DATABASE_URL and conn:
            conn.rollback()
        if conn:
            conn.close()
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/clob/orden", methods=["POST"])
def crear_orden_clob():
    data = request.json or {}
    username = data.get("username")
    evento_id = str(data.get("evento_id", ""))
    opcion_id = data.get("opcion_id")
    tipo_orden = data.get("tipo_orden", "limit")
    accion = data.get("accion")
    try:
        precio_ingresado = float(data.get("precio", 0))
        cantidad = float(data.get("cantidad", 0))
    except (ValueError, TypeError):
        return jsonify({"success": False, "error": "Valores numéricos inválidos"}), 400

    if cantidad <= 0 or accion not in ["comprar", "vender"]:
        return jsonify({"success": False, "error": "Parámetros de orden incorrectos"}), 400

    conn = obtener_conexion()
    c = conn.cursor()
    try:
        if DATABASE_URL:
            c.execute(
                "SELECT saldo_disponible, is_frozen FROM usuarios WHERE username = %s FOR UPDATE",
                (username,),
            )
        else:
            c.execute(
                "SELECT saldo_disponible, is_frozen FROM usuarios WHERE username = ?",
                (username,),
            )
        row_user = c.fetchone()
        row_user_dict = dict(row_user) if row_user else {}
        if row_user_dict and row_user_dict.get("is_frozen"):
            conn.rollback()
            return jsonify({
                "success": False,
                "error": "Tu cuenta se encuentra suspendida temporalmente.",
            }), 403

        if not row_user:
            saldo_inicial = 150.00 if username.lower() in ["@jaimetetio", "jaimetetio"] else 0.0
            if DATABASE_URL:
                c.execute(
                    "INSERT INTO usuarios (username, saldo_disponible, is_frozen) VALUES (%s, %s, FALSE)",
                    (username, saldo_inicial),
                )
            else:
                c.execute(
                    "INSERT INTO usuarios (username, saldo_disponible, is_frozen) VALUES (?, ?, 0)",
                    (username, saldo_inicial),
                )
            conn.commit()
            if DATABASE_URL:
                c.execute(
                    "SELECT saldo_disponible, is_frozen FROM usuarios WHERE username = %s FOR UPDATE",
                    (username,),
                )
            else:
                c.execute(
                    "SELECT saldo_disponible, is_frozen FROM usuarios WHERE username = ?",
                    (username,),
                )
            row_user = c.fetchone()
            row_user_dict = dict(row_user) if row_user else {}

        if accion == "comprar":
            precio_eval = precio_ingresado if tipo_orden == "limit" or precio_ingresado > 0 else 1.0
            costo_inicial = precio_eval * cantidad
            if row_user_dict.get("saldo_disponible", 0.0) < costo_inicial:
                conn.rollback()
                return jsonify({
                    "success": False,
                    "error": "Saldo insuficiente para colocar la orden de compra",
                }), 400
        elif accion == "vender":
            pass

        nuevo_saldo_creador = row_user_dict.get("saldo_disponible", 0.0)
        fecha_str = datetime.now().strftime("%Y-%m-%d %H:%M")

        titulo_ev = "Mercado P2P Dinámico"
        try:
            ev_id_int = int(evento_id)
            if DATABASE_URL:
                c.execute("SELECT titulo FROM eventos WHERE id = %s", (ev_id_int,))
            else:
                c.execute("SELECT titulo FROM eventos WHERE id = ?", (ev_id_int,))
            ev_row = c.fetchone()
            if ev_row:
                ev_row_dict = dict(ev_row)
                titulo_ev = ev_row_dict.get("titulo", titulo_ev)
        except (ValueError, TypeError):
            titulo_ev = f"Mercado Dinámico ({evento_id})"

        nombre_op = "Opción"
        try:
            op_id_int = int(opcion_id)
            if DATABASE_URL:
                c.execute("SELECT nombre FROM opciones_evento WHERE id = %s", (op_id_int,))
            else:
                c.execute("SELECT nombre FROM opciones_evento WHERE id = ?", (op_id_int,))
            op_row = c.fetchone()
            if op_row:
                op_row_dict = dict(op_row)
                nombre_op = op_row_dict.get("nombre", nombre_op)
        except (ValueError, TypeError):
            nombre_op = str(opcion_id)

        cantidad_restante = cantidad
        precio_objetivo = precio_ingresado

        if accion == "comprar":
            if DATABASE_URL:
                if tipo_orden == "limit":
                    c.execute(
                        """SELECT * FROM orders WHERE evento_id::text = %s AND opcion_id = %s AND accion = 'vender' AND estado = 'activa' AND username != %s AND precio <= %s ORDER BY precio ASC, id ASC FOR UPDATE""",
                        (evento_id, opcion_id, username, precio_ingresado),
                    )
                else:
                    c.execute(
                        """SELECT * FROM orders WHERE evento_id::text = %s AND opcion_id = %s AND accion = 'vender' AND estado = 'activa' AND username != %s ORDER BY precio ASC, id ASC FOR UPDATE""",
                        (evento_id, opcion_id, username),
                    )
            else:
                if tipo_orden == "limit":
                    c.execute(
                        """SELECT * FROM orders WHERE evento_id = ? AND opcion_id = ? AND accion = 'vender' AND estado = 'activa' AND username != ? AND precio <= ? ORDER BY precio ASC, id ASC""",
                        (evento_id, opcion_id, username, precio_ingresado),
                    )
                else:
                    c.execute(
                        """SELECT * FROM orders WHERE evento_id = ? AND opcion_id = ? AND accion = 'vender' AND estado = 'activa' AND username != ? ORDER BY precio ASC, id ASC""",
                        (evento_id, opcion_id, username),
                    )

            contra_ordenes = c.fetchall()

            for contra in contra_ordenes:
                contra_dict = dict(contra)
                if cantidad_restante <= 0:
                    break
                match_cant = min(cantidad_restante, contra_dict.get("cantidad", 0.0))
                match_precio = contra_dict.get("precio", 0.0)
                precio_objetivo = match_precio

                costo_match = match_precio * match_cant
                if nuevo_saldo_creador < costo_match:
                    match_cant = nuevo_saldo_creador / match_precio
                    if match_cant <= 0:
                        break
                    costo_match = match_precio * match_cant

                nuevo_saldo_creador -= costo_match
                if DATABASE_URL:
                    c.execute(
                        "UPDATE usuarios SET saldo_disponible = %s WHERE username = %s",
                        (nuevo_saldo_creador, username),
                    )
                else:
                    c.execute(
                        "UPDATE usuarios SET saldo_disponible = ? WHERE username = ?",
                        (nuevo_saldo_creador, username),
                    )

                if DATABASE_URL:
                    c.execute(
                        "SELECT saldo_disponible FROM usuarios WHERE username = %s FOR UPDATE",
                        (contra_dict.get("username"),),
                    )
                else:
                    c.execute(
                        "SELECT saldo_disponible FROM usuarios WHERE username = ?",
                        (contra_dict.get("username"),),
                    )
                v_row = c.fetchone()
                v_row_dict = dict(v_row) if v_row else {}
                if v_row:
                    nuevo_vendedor_saldo = v_row_dict.get("saldo_disponible", 0.0) + costo_match
                    if DATABASE_URL:
                        c.execute(
                            "UPDATE usuarios SET saldo_disponible = %s WHERE username = %s",
                            (nuevo_vendedor_saldo, contra_dict.get("username")),
                        )
                    else:
                        c.execute(
                            "UPDATE usuarios SET saldo_disponible = ? WHERE username = ?",
                            (nuevo_vendedor_saldo, contra_dict.get("username")),
                        )

                if DATABASE_URL:
                    c.execute(
                        "INSERT INTO historial_apuestas (username, titulo_evento, opcion_elegida, monto, estado) VALUES (%s, %s, %s, %s, 'Activo')",
                        (username, titulo_ev, nombre_op, match_cant),
                    )
                else:
                    c.execute(
                        "INSERT INTO historial_apuestas (username, titulo_evento, opcion_elegida, monto, estado) VALUES (?, ?, ?, ?, 'Activo')",
                        (username, titulo_ev, nombre_op, match_cant),
                    )

                nueva_contra_cant = contra_dict.get("cantidad", 0.0) - match_cant
                nuevo_estado_contra = "completada" if nueva_contra_cant <= 0 else "activa"
                if DATABASE_URL:
                    c.execute(
                        "UPDATE orders SET cantidad = %s, estado = %s WHERE id = %s",
                        (nueva_contra_cant, nuevo_estado_contra, contra_dict.get("id")),
                    )
                else:
                    c.execute(
                        "UPDATE orders SET cantidad = ?, estado = ? WHERE id = ?",
                        (nueva_contra_cant, nuevo_estado_contra, contra_dict.get("id")),
                    )
                cantidad_restante -= match_cant

        else:
            if tipo_orden == "limit":
                if DATABASE_URL:
                    c.execute(
                        """SELECT * FROM orders WHERE evento_id::text = %s AND opcion_id = %s AND accion = 'comprar' AND estado = 'activa' AND username != %s AND precio >= %s ORDER BY precio DESC, id ASC FOR UPDATE""",
                        (evento_id, opcion_id, username, precio_ingresado),
                    )
                else:
                    c.execute(
                        """SELECT * FROM orders WHERE evento_id = ? AND opcion_id = ? AND accion = 'comprar' AND estado = 'activa' AND username != ? AND precio >= ? ORDER BY precio DESC, id ASC""",
                        (evento_id, opcion_id, username, precio_ingresado),
                    )
            else:
                if DATABASE_URL:
                    c.execute(
                        """SELECT * FROM orders WHERE evento_id::text = %s AND opcion_id = %s AND accion = 'comprar' AND estado = 'activa' AND username != %s ORDER BY precio DESC, id ASC FOR UPDATE""",
                        (evento_id, opcion_id, username),
                    )
                else:
                    c.execute(
                        """SELECT * FROM orders WHERE evento_id = ? AND opcion_id = ? AND accion = 'comprar' AND estado = 'activa' AND username != ? ORDER BY precio DESC, id ASC""",
                        (evento_id, opcion_id, username),
                    )

            contra_ordenes = c.fetchall()

            for contra in contra_ordenes:
                contra_dict = dict(contra)
                if cantidad_restante <= 0:
                    break
                match_cant = min(cantidad_restante, contra_dict.get("cantidad", 0.0))
                match_precio = contra_dict.get("precio", 0.0)
                precio_objetivo = match_precio

                monto_transaccion = match_precio * match_cant
                nuevo_saldo_creador += monto_transaccion
                if DATABASE_URL:
                    c.execute(
                        "UPDATE usuarios SET saldo_disponible = %s WHERE username = %s",
                        (nuevo_saldo_creador, username),
                    )
                    c.execute(
                        "INSERT INTO historial_apuestas (username, titulo_evento, opcion_elegida, monto, estado) VALUES (%s, %s, %s, %s, 'Activo')",
                        (contra_dict.get("username"), titulo_ev, nombre_op, match_cant),
                    )
                else:
                    c.execute(
                        "UPDATE usuarios SET saldo_disponible = ? WHERE username = ?",
                        (nuevo_saldo_creador, username),
                    )
                    c.execute(
                        "INSERT INTO historial_apuestas (username, titulo_evento, opcion_elegida, monto, estado) VALUES (?, ?, ?, ?, 'Activo')",
                        (contra_dict.get("username"), titulo_ev, nombre_op, match_cant),
                    )

                nueva_contra_cant = contra_dict.get("cantidad", 0.0) - match_cant
                nuevo_estado_contra = "completada" if nueva_contra_cant <= 0 else "activa"
                if DATABASE_URL:
                    c.execute(
                        "UPDATE orders SET cantidad = %s, estado = %s WHERE id = %s",
                        (nueva_contra_cant, nuevo_estado_contra, contra_dict.get("id")),
                    )
                else:
                    c.execute(
                        "UPDATE orders SET cantidad = ?, estado = ? WHERE id = ?",
                        (nueva_contra_cant, nuevo_estado_contra, contra_dict.get("id")),
                    )
                cantidad_restante -= match_cant

        if cantidad_restante > 0:
            precio_para_libro = precio_objetivo if precio_objetivo > 0 else 0.50
            if accion == "comprar":
                costo_remanente = precio_para_libro * cantidad_restante
                if nuevo_saldo_creador >= costo_remanente:
                    nuevo_saldo_creador -= costo_remanente
                    if DATABASE_URL:
                        c.execute(
                            "UPDATE usuarios SET saldo_disponible = %s WHERE username = %s",
                            (nuevo_saldo_creador, username),
                        )
                    else:
                        c.execute(
                            "UPDATE usuarios SET saldo_disponible = ? WHERE username = ?",
                            (nuevo_saldo_creador, username),
                        )
                else:
                    cantidad_restante = nuevo_saldo_creador / precio_para_libro
                    costo_remanente = nuevo_saldo_creador
                    nuevo_saldo_creador = 0.0
                    if DATABASE_URL:
                        c.execute(
                            "UPDATE usuarios SET saldo_disponible = 0.0 WHERE username = %s",
                            (username,),
                        )
                    else:
                        c.execute(
                            "UPDATE usuarios SET saldo_disponible = 0.0 WHERE username = ?",
                            (username,),
                        )

            if cantidad_restante > 0:
                if DATABASE_URL:
                    c.execute(
                        "INSERT INTO orders (username, evento_id, opcion_id, tipo_orden, accion, precio, cantidad, estado, fecha) VALUES (%s, %s, %s, 'limit', %s, %s, %s, 'activa', %s)",
                        (username, str(evento_id), opcion_id, accion, precio_para_libro, cantidad_restante, fecha_str),
                    )
                else:
                    c.execute(
                        "INSERT INTO orders (username, evento_id, opcion_id, tipo_orden, accion, precio, cantidad, estado, fecha) VALUES (?, ?, ?, 'limit', ?, ?, ?, 'activa', ?)",
                        (username, str(evento_id), opcion_id, accion, precio_para_libro, cantidad_restante, fecha_str),
                    )

        monto_registrado = (precio_ingresado * cantidad if accion == "comprar" else cantidad)
        if DATABASE_URL:
            c.execute(
                "INSERT INTO historial_apuestas (username, titulo_evento, opcion_elegida, monto, estado) VALUES (%s, %s, %s, %s, %s)",
                (
                    username,
                    titulo_ev,
                    f"CLOB {accion.capitalize()} ({cantidad})",
                    monto_registrado,
                    ("Completada" if cantidad_restante == 0 else "Parcial / En Libro"),
                ),
            )
            c.execute(
                "INSERT INTO transacciones (username, tipo, monto, txid, fecha) VALUES (%s, %s, %s, %s, %s)",
                (
                    username,
                    f"CLOB Orden ({accion})",
                    -(precio_ingresado * (cantidad - cantidad_restante) if accion == "comprar" else 0),
                    f"CLOB_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                    fecha_str,
                ),
            )
        else:
            c.execute(
                "INSERT INTO historial_apuestas (username, titulo_evento, opcion_elegida, monto, estado) VALUES (?, ?, ?, ?, ?)",
                (
                    username,
                    titulo_ev,
                    f"CLOB {accion.capitalize()} ({cantidad})",
                    monto_registrado,
                    ("Completada" if cantidad_restante == 0 else "Parcial / En Libro"),
                ),
            )
            c.execute(
                "INSERT INTO transacciones (username, tipo, monto, txid, fecha) VALUES (?, ?, ?, ?, ?)",
                (
                    username,
                    f"CLOB Orden ({accion})",
                    -(precio_ingresado * (cantidad - cantidad_restante) if accion == "comprar" else 0),
                    f"CLOB_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                    fecha_str,
                ),
            )

        conn.commit()
        registrar_global_audit(
            username,
            "CLOB_ORDEN_MARKET_TO_LIMIT",
            f"Acción: {accion} | Total: {cantidad} | Remanente en libro: {cantidad_restante}",
        )

        cantidad_inicial = float(cantidad)
        cantidad_ejecutada = cantidad_inicial - cantidad_restante

        if cantidad_ejecutada == 0:
            mensaje_respuesta = "Orden límite publicada en el Order Book. Esperando contraparte."
            estado_orden = "abierta"
        elif cantidad_restante == 0:
            mensaje_respuesta = "¡Orden ejecutada con éxito en el mercado!"
            estado_orden = "completada"
        else:
            mensaje_respuesta = "Orden ejecutada parcialmente. El remanente se colocó en el Order Book."
            estado_orden = "parcial"

        return jsonify({
            "success": True,
            "message": mensaje_respuesta,
            "status": estado_orden,
            "executed": cantidad_ejecutada,
            "remaining": cantidad_restante,
            "nuevo_saldo": nuevo_saldo_creador
        }), 200

    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route("/api/crear-orden", methods=["POST"])
def crear_orden():
    data = request.get_json(silent=True) or request.form

    username = data.get("username")
    evento_id = data.get("evento_id")
    opcion_id = data.get("opcion_id")
    tipo_orden = data.get("tipo_orden")
    accion = data.get("accion")
    precio = data.get("precio")
    
    cantidad_contratos = data.get("contracts") or data.get("cantidad")

    if not username or not evento_id or not cantidad_contratos:
        return jsonify({
            "success": False, 
            "error": "Faltan datos obligatorios, asegúrate de indicar la cantidad de contratos y el evento."
        }), 400

    try:
        cantidad = float(cantidad_contratos)
        precio_num = float(precio) if precio else 0.0
        
        if cantidad <= 0:
            return jsonify({
                "success": False, 
                "error": "La cantidad de contratos debe ser mayor a cero."
            }), 400
            
    except (ValueError, TypeError):
        return jsonify({
            "success": False, 
            "error": "El formato de la cantidad o el precio no es válido."
        }), 400

    conn = obtener_conexion()
    c = conn.cursor()
    fecha_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        if DATABASE_URL:
            c.execute(
                """
                INSERT INTO orders (username, evento_id, opcion_id, tipo_orden, accion, precio, cantidad, estado, fecha) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'activa', %s)
                """,
                (username, evento_id, opcion_id, tipo_orden, accion, precio_num, cantidad, fecha_actual)
            )
        else:
            c.execute(
                """
                INSERT INTO orders (username, evento_id, opcion_id, tipo_orden, accion, precio, cantidad, estado, fecha) 
                VALUES (?, ?, ?, ?, ?, ?, ?, 'activa', ?)
                """,
                (username, evento_id, opcion_id, tipo_orden, accion, precio_num, cantidad, fecha_actual)
            )
        
        conn.commit()
        return jsonify({"success": True, "message": "Orden creada exitosamente."}), 200

    except Exception as e:
        conn.rollback()
        return jsonify({"success": False, "error": f"Error interno en la base de datos: {str(e)}"}), 500
    finally:
        c.close()
        conn.close()


@app.route("/api/pi/aprobar-pago", methods=["POST"])
def aprobar_pago():
    data = request.json or {}
    payment_id = data.get("paymentId")
    if DATABASE_URL and not PI_API_KEY:
        pass
    if not PI_API_KEY:
        return jsonify({"success": False, "error": "PI_API_KEY no configurada"}), 500
    headers = {"Authorization": f"Key {PI_API_KEY}"}
    try:
        response = requests.post(
            f"https://api.minepi.com/v2/payments/{payment_id}/approve",
            headers=headers,
            timeout=10,
        )
        if response.status_code == 200:
            return jsonify({"success": True})
    except requests.exceptions.RequestException:
        return jsonify({"success": False, "error": "Error de red con Pi Network"}), 504
    return jsonify({"success": False, "error": "No se pudo aprobar el pago"}), 400


@app.route("/api/pi/completar-pago", methods=["POST"])
def completar_pago():
    data = request.json or {}
    username = data.get("username")
    try:
        monto = float(data.get("monto", 0))
    except (ValueError, TypeError):
        return jsonify({"success": False, "error": "Monto inválido"}), 400

    payment_id = data.get("paymentId")
    txid = data.get("txid")

    if PI_API_KEY:
        headers = {"Authorization": f"Key {PI_API_KEY}"}
        try:
            response = requests.post(
                f"https://api.minepi.com/v2/payments/{payment_id}/complete",
                headers=headers,
                json={"txid": txid},
                timeout=10,
            )
            if response.status_code != 200:
                return jsonify({
                    "success": False,
                    "error": "Error al completar el pago en Pi",
                }), 400
        except requests.exceptions.RequestException:
            return jsonify({
                "success": False,
                "error": "Error de red con Pi Network",
            }), 504

    conn = obtener_conexion()
    c = conn.cursor()
    try:
        if DATABASE_URL:
            c.execute(
                "SELECT saldo_disponible, is_frozen FROM usuarios WHERE"
                " username = %s FOR UPDATE",
                (username,),
            )
        else:
            c.execute(
                "SELECT saldo_disponible, is_frozen FROM usuarios WHERE"
                " username = ?",
                (username,),
            )
        row = c.fetchone()
        row_dict = dict(row) if row else {}
        if row_dict and row_dict.get("is_frozen"):
            conn.rollback()
            return jsonify({
                "success": False,
                "error": "Tu cuenta se encuentra suspendida temporalmente.",
            }), 403

        if not row:
            nuevo_saldo = monto
            if DATABASE_URL:
                c.execute(
                    "INSERT INTO usuarios (username, saldo_disponible,"
                    " is_frozen) VALUES (%s, %s, FALSE)",
                    (username, nuevo_saldo),
                )
            else:
                c.execute(
                    "INSERT INTO usuarios (username, saldo_disponible,"
                    " is_frozen) VALUES (?, ?, 0)",
                    (username, nuevo_saldo),
                )
        else:
            nuevo_saldo = row_dict.get("saldo_disponible", 0.0) + monto
            if DATABASE_URL:
                c.execute(
                    "UPDATE usuarios SET saldo_disponible = %s WHERE username ="
                    " %s",
                    (nuevo_saldo, username),
                )
            else:
                c.execute(
                    "UPDATE usuarios SET saldo_disponible = ? WHERE username ="
                    " ?",
                    (nuevo_saldo, username),
                )

        fecha = datetime.now().strftime("%Y-%m-%d %H:%M")
        if DATABASE_URL:
            c.execute(
                "INSERT INTO transacciones (username, tipo, monto, txid, fecha)"
                " VALUES (%s, %s, %s, %s, %s)",
                (username, "Recarga Pi Real", monto, txid or payment_id, fecha),
            )
            c.execute("SELECT SUM(saldo_disponible) as total FROM usuarios")
            res_tot = c.fetchone()
            res_tot_dict = dict(res_tot) if res_tot else {}
            balance_total_plataforma = (
                res_tot_dict.get("total") if res_tot_dict and res_tot_dict.get("total") else 0.0
            )
            c.execute(
                "INSERT INTO pi_wallet_events (username, evento_tipo, monto,"
                " balance_total_plataforma, txid, fecha) VALUES (%s, %s, %s, %s,"
                " %s, %s)",
                (
                    username,
                    "COMPLETAR_PAGO",
                    monto,
                    balance_total_plataforma,
                    txid or payment_id,
                    fecha,
                ),
            )
        else:
            c.execute(
                "INSERT INTO transacciones (username, tipo, monto, txid, fecha)"
                " VALUES (?, ?, ?, ?, ?)",
                (username, "Recarga Pi Real", monto, txid or payment_id, fecha),
            )
            c.execute("SELECT SUM(saldo_disponible) as total FROM usuarios")
            res_tot = c.fetchone()
            res_tot_dict = dict(res_tot) if res_tot else {}
            balance_total_plataforma = (
                res_tot_dict.get("total") if res_tot_dict and res_tot_dict.get("total") else 0.0
            )
            c.execute(
                "INSERT INTO pi_wallet_events (username, evento_tipo, monto,"
                " balance_total_plataforma, txid, fecha) VALUES (?, ?, ?, ?, ?,"
                " ?)",
                (
                    username,
                    "COMPLETAR_PAGO",
                    monto,
                    balance_total_plataforma,
                    txid or payment_id,
                    fecha,
                ),
            )

        conn.commit()
        registrar_global_audit(
            username,
            "RECARGA_PI",
            f"Recarga completada de {monto} Pi (TxID: {txid or payment_id})",
        )
        return jsonify({
            "success": True,
            "nuevo_saldo": nuevo_saldo,
            "balance_total_plataforma": balance_total_plataforma,
            "mensaje": f"Recarga de {monto} Pi acreditada con éxito.",
        })
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route("/api/pi/retirar", methods=["POST"])
def solicitar_retiro():
    if not check_rate_limit(limit=10, window=60):
        return jsonify({
            "success": False,
            "error": "Demasiadas peticiones de retiro. Intente más tarde.",
        }), 429

    data = request.json or {}
    username = data.get("username")
    try:
        monto = float(data.get("monto", 0))
    except (ValueError, TypeError):
        return jsonify({"success": False, "error": "Monto inválido"}), 400

    wallet_destino = str(data.get("wallet_address", "")).strip()
    if monto < 1.0:
        return jsonify({"success": False, "error": "El monto mínimo de retiro es de 1.0 Pi"}), 400
    if not wallet_destino or len(wallet_destino) < 10:
        return jsonify({
            "success": False,
            "error": "La dirección de la billetera de destino no es válida",
        }), 400
    if not PI_API_KEY:
        return jsonify({
            "success": False,
            "error": "PI_API_KEY no configurada en el servidor",
        }), 500

    conn = obtener_conexion()
    c = conn.cursor()
    try:
        if DATABASE_URL:
            c.execute(
                "SELECT saldo_disponible, is_frozen FROM usuarios WHERE"
                " username = %s FOR UPDATE",
                (username,),
            )
        else:
            c.execute(
                "SELECT saldo_disponible, is_frozen FROM usuarios WHERE"
                " username = ?",
                (username,),
            )
        row = c.fetchone()
        row_dict = dict(row) if row else {}
        if row_dict and row_dict.get("is_frozen"):
            conn.rollback()
            return jsonify({
                "success": False,
                "error": "Tu cuenta se encuentra suspendida temporalmente.",
            }), 403

        saldo_actual = row_dict.get("saldo_disponible", 0.0)
        if not row or saldo_actual < monto:
            conn.rollback()
            return jsonify({
                "success": False,
                "error": "Saldo insuficiente para procesar el retiro",
            }), 400

        nuevo_saldo = saldo_actual - monto
        if DATABASE_URL:
            c.execute(
                "UPDATE usuarios SET saldo_disponible = %s WHERE username = %s",
                (nuevo_saldo, username),
            )
        else:
            c.execute(
                "UPDATE usuarios SET saldo_disponible = ? WHERE username = ?",
                (nuevo_saldo, username),
            )

        headers = {
            "Authorization": f"Key {PI_API_KEY}",
            "Content-Type": "application/json",
        }
        payload = {
            "amount": monto,
            "uid": username,
            "memo": f"Retiro automático desde P2PPredict hacia {wallet_destino}",
            "metadata": {"wallet": wallet_destino},
        }
        pi_response = requests.post(
            "https://api.minepi.com/v2/payments",
            json=payload,
            headers=headers,
            timeout=10,
        )
        if pi_response.status_code not in [200, 201]:
            conn.rollback()
            return jsonify({
                "success": False,
                "error": "La pasarela de Pi Network rechazó el desembolso",
            }), 400

        pi_data = pi_response.json()
        txid = pi_data.get(
            "txid", f"RETIRO_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        )
        fecha = datetime.now().strftime("%Y-%m-%d %H:%M")

        if DATABASE_URL:
            c.execute(
                "INSERT INTO transacciones (username, tipo, monto, txid, fecha)"
                " VALUES (%s, %s, %s, %s, %s)",
                (username, "Retiro Pi Blockchain", -monto, txid, fecha),
            )
            c.execute("SELECT SUM(saldo_disponible) as total FROM usuarios")
            res_tot = c.fetchone()
            res_tot_dict = dict(res_tot) if res_tot else {}
            balance_total_plataforma = (
                res_tot_dict.get("total") if res_tot_dict and res_tot_dict.get("total") else 0.0
            )
            c.execute(
                "INSERT INTO pi_wallet_events (username, evento_tipo, monto,"
                " balance_total_plataforma, txid, fecha) VALUES (%s, %s, %s, %s,"
                " %s, %s)",
                (
                    username,
                    "SOLICITAR_RETIRO",
                    -monto,
                    balance_total_plataforma,
                    txid,
                    fecha,
                ),
            )
        else:
            c.execute(
                "INSERT INTO transacciones (username, tipo, monto, txid, fecha)"
                " VALUES (?, ?, ?, ?, ?)",
                (username, "Retiro Pi Blockchain", -monto, txid, fecha),
            )
            c.execute("SELECT SUM(saldo_disponible) as total FROM usuarios")
            res_tot = c.fetchone()
            res_tot_dict = dict(res_tot) if res_tot else {}
            balance_total_plataforma = (
                res_tot_dict.get("total") if res_tot_dict and res_tot_dict.get("total") else 0.0
            )
            c.execute(
                "INSERT INTO pi_wallet_events (username, evento_tipo, monto,"
                " balance_total_plataforma, txid, fecha) VALUES (?, ?, ?, ?, ?,"
                " ?)",
                (
                    username,
                    "SOLICITAR_RETIRO",
                    -monto,
                    balance_total_plataforma,
                    txid,
                    fecha,
                ),
            )

        conn.commit()
        registrar_global_audit(
            username,
            "RETIRO_PI",
            f"Retiro de {monto} Pi a la billetera {wallet_destino} (TxID: {txid})",
        )
        return jsonify({
            "success": True,
            "nuevo_saldo": nuevo_saldo,
            "balance_total_plataforma": balance_total_plataforma,
            "txid": txid,
            "mensaje": f"Retiro de {monto} Pi procesado con éxito.",
        })
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route("/api/pi/balance-plataforma", methods=["GET"])
def obtener_balance_plataforma():
    conn = obtener_conexion()
    c = conn.cursor()
    try:
        if DATABASE_URL:
            c.execute("SELECT SUM(saldo_disponible) as total_circulante FROM usuarios")
        else:
            c.execute("SELECT SUM(saldo_disponible) as total_circulante FROM usuarios")
        row = c.fetchone()
        row_dict = dict(row) if row else {}
        total_circulante = (
            row_dict.get("total_circulante") if row_dict and row_dict.get("total_circulante") else 0.0
        )
        if DATABASE_URL:
            c.execute("SELECT * FROM pi_wallet_events ORDER BY id DESC LIMIT 20")
        else:
            c.execute("SELECT * FROM pi_wallet_events ORDER BY id DESC LIMIT 20")
        eventos = [dict(r) for r in c.fetchall()]
        conn.close()
        return jsonify({
            "success": True,
            "balance_total_pi": total_circulante,
            "ultimos_eventos_wallet": eventos,
        })
    except Exception as e:
        if conn:
            conn.close()
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/pi/approve", methods=["POST"])
def approve_pi_payment():
    data = request.json or {}
    payment_id = data.get("paymentId")
    return jsonify({"status": "success", "message": "Pago aprobado por el servidor"}), 200


@app.route("/api/pi/complete", methods=["POST"])
def complete_pi_payment():
    data = request.json or {}
    payment_id = data.get("paymentId")
    txid = data.get("txid")
    return jsonify({"status": "success", "message": "Pago completado y registrado"}), 200


@app.route("/api/admin/login", methods=["POST"])
def admin_login():
    if not check_rate_limit(limit=5, window=60):
        registrar_log_admin(
            "LOGIN_FALLIDO_RATE_LIMIT",
            "Demasiados intentos de acceso bloqueados por seguridad.",
        )
        return jsonify({
            "success": False,
            "error": "Demasiados intentos fallidos. Inténtelo más tarde.",
        }), 429

    data = request.json or {}
    password = data.get("password", "")
    if check_password_hash(ADMIN_PASSWORD_HASH, password):
        session.clear()
        session.regenerate = True
        session["is_admin"] = True
        registrar_log_admin("LOGIN_EXITOSO", "Administrador inició sesión correctamente.")
        return jsonify({"success": True, "message": "Acceso autorizado"})

    registrar_log_admin("LOGIN_FALLIDO", "Intento de acceso con contraseña incorrecta.")
    return jsonify({"success": False, "error": "Credenciales inválidas"}), 401


@app.route("/api/admin/verificar-sesion", methods=["GET"])
def admin_verificar_sesion():
    if session.get("is_admin"):
        return jsonify({"success": True, "is_admin": True})
    return jsonify({"success": True, "is_admin": False}), 403


@app.route("/api/ranking", methods=["GET"])
def obtener_ranking():
    conn = obtener_conexion()
    c = conn.cursor()
    if DATABASE_URL:
        c.execute(
            "SELECT username, saldo_disponible FROM usuarios ORDER BY"
            " saldo_disponible DESC LIMIT 10"
        )
    else:
        c.execute(
            "SELECT username, saldo_disponible FROM usuarios ORDER BY"
            " saldo_disponible DESC LIMIT 10"
        )
    ranking = [dict(row) for row in c.fetchall()]
    conn.close()
    return jsonify({"success": True, "ranking": ranking})


@app.route("/api/cobrar/<int:apuesta_id>", methods=["POST"])
def cobrar_prediccion(apuesta_id):
    data = request.json or {}
    username = data.get("username")
    if not username:
        return jsonify({"success": False, "error": "Usuario no especificado"}), 400

    conn = obtener_conexion()
    c = conn.cursor()
    try:
        if DATABASE_URL:
            c.execute("SELECT is_frozen FROM usuarios WHERE username = %s", (username,))
        else:
            c.execute("SELECT is_frozen FROM usuarios WHERE username = ?", (username,))
        u_check = c.fetchone()
        u_check_dict = dict(u_check) if u_check else {}
        if u_check_dict and u_check_dict.get("is_frozen"):
            conn.rollback()
            return jsonify({
                "success": False,
                "error": "Tu cuenta se encuentra suspendida temporalmente.",
            }), 403

        if DATABASE_URL:
            c.execute(
                "SELECT * FROM historial_apuestas WHERE id = %s AND username = %s",
                (apuesta_id, username),
            )
        else:
            c.execute(
                "SELECT * FROM historial_apuestas WHERE id = ? AND username = ?",
                (apuesta_id, username),
            )
        apuesta = c.fetchone()
        apuesta_dict = dict(apuesta) if apuesta else {}
        if not apuesta:
            conn.rollback()
            return jsonify({"success": False, "error": "Apuesta no encontrada"}), 404

        if apuesta_dict.get("estado") != "Ganada":
            conn.rollback()
            return jsonify({
                "success": False,
                "error": "Esta apuesta no está marcada como ganadora o ya fue cobrada",
            }), 400

        premio = apuesta_dict.get("monto", 0.0) * 2.0
        if DATABASE_URL:
            c.execute(
                "SELECT saldo_disponible FROM usuarios WHERE username = %s FOR UPDATE",
                (username,),
            )
        else:
            c.execute(
                "SELECT saldo_disponible FROM usuarios WHERE username = ?",
                (username,),
            )
        u_row = c.fetchone()
        u_row_dict = dict(u_row) if u_row else {}
        if not u_row:
            conn.rollback()
            return jsonify({"success": False, "error": "Usuario no existe"}), 400

        nuevo_saldo = u_row_dict.get("saldo_disponible", 0.0) + premio
        if DATABASE_URL:
            c.execute(
                "UPDATE usuarios SET saldo_disponible = %s WHERE username = %s",
                (nuevo_saldo, username),
            )
            c.execute(
                "UPDATE historial_apuestas SET estado = 'Cobrada' WHERE id = %s",
                (apuesta_id,),
            )
            c.execute(
                "INSERT INTO transacciones (username, tipo, monto, txid, fecha)"
                " VALUES (%s, %s, %s, %s, %s)",
                (
                    username,
                    "Cobro de Predicción",
                    premio,
                    f"COBRO_{apuesta_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                    datetime.now().strftime("%Y-%m-%d %H:%M"),
                ),
            )
        else:
            c.execute(
                "UPDATE usuarios SET saldo_disponible = ? WHERE username = ?",
                (nuevo_saldo, username),
            )
            c.execute(
                "UPDATE historial_apuestas SET estado = 'Cobrada' WHERE id = ?",
                (apuesta_id,),
            )
            c.execute(
                "INSERT INTO transacciones (username, tipo, monto, txid, fecha)"
                " VALUES (?, ?, ?, ?, ?)",
                (
                    username,
                    "Cobro de Predicción",
                    premio,
                    f"COBRO_{apuesta_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                    datetime.now().strftime("%Y-%m-%d %H:%M"),
                ),
            )
        conn.commit()
        registrar_global_audit(
            username,
            "COBRO_PREMIO",
            f"Cobro exitoso de premio por {premio} (Apuesta ID: {apuesta_id})",
        )
        return jsonify({
            "success": True,
            "nuevo_saldo": nuevo_saldo,
            "mensaje": f"¡Premio de {premio} cobrado con éxito!",
        })
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route("/api/admin/crear-evento", methods=["POST"])
def admin_crear_evento():
    if not session.get("is_admin"):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    data = request.json or {}
    titulo = data.get("titulo")
    categoria = data.get("categoria", "General")
    fecha_cierre = data.get("fecha_cierre", datetime.now().strftime("%Y-%m-%d"))
    opciones = data.get("opciones", [])
    if not titulo or not opciones or len(opciones) < 2:
        return jsonify({
            "success": False,
            "error": "Título y al menos 2 opciones son obligatorios",
        }), 400

    conn = obtener_conexion()
    c = conn.cursor()
    try:
        if DATABASE_URL:
            c.execute(
                "INSERT INTO eventos (titulo, categoria, estado, fecha_cierre) VALUES"
                " (%s, %s, 'activo', %s) RETURNING id",
                (titulo, categoria, fecha_cierre),
            )
            ev_row = c.fetchone()
            ev_row_dict = dict(ev_row) if ev_row else {}
            ev_id = ev_row_dict.get("id")
            for opt in opciones:
                c.execute(
                    "INSERT INTO opciones_evento (evento_id, nombre, pozo) VALUES (%s,"
                    " %s, 0.0)",
                    (ev_id, opt),
                )
        else:
            c.execute(
                "INSERT INTO eventos (titulo, categoria, estado, fecha_cierre) VALUES"
                " (?, ?, 'activo', ?)",
                (titulo, categoria, fecha_cierre),
            )
            ev_id = c.lastrowid
            for opt in opciones:
                c.execute(
                    "INSERT INTO opciones_evento (evento_id, nombre, pozo) VALUES (?,"
                    " ?, 0.0)",
                    (ev_id, opt),
                )
        conn.commit()
        registrar_log_admin("CREAR_EVENTO", f"Creado evento ID {ev_id}: {titulo}")
        registrar_audit_log(
            "Admin",
            "CREAR_EVENTO",
            str(ev_id),
            {"titulo": titulo, "opciones": opciones},
        )
        return jsonify({"success": True, "mensaje": "Mercado/Evento creado con éxito"})
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route("/api/admin/cerrar-evento", methods=["POST"])
def admin_cerrar_evento():
    if not session.get("is_admin"):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    data = request.json or {}
    evento_id = data.get("evento_id")
    ganador_id = data.get("ganador_id")
    if not evento_id or not ganador_id:
        return jsonify({"success": False, "error": "Faltan parámetros de cierre"}), 400

    conn = obtener_conexion()
    c = conn.cursor()
    try:
        if DATABASE_URL:
            c.execute("SELECT * FROM eventos WHERE id = %s", (evento_id,))
        else:
            c.execute("SELECT * FROM eventos WHERE id = ?", (evento_id,))
        evento = c.fetchone()
        evento_dict = dict(evento) if evento else {}
        if not evento or evento_dict.get("estado") == "cerrado":
            conn.rollback()
            return jsonify({
                "success": False,
                "error": "El evento no existe o ya está cerrado",
            }), 400

        if DATABASE_URL:
            c.execute("SELECT nombre FROM opciones_evento WHERE id = %s", (ganador_id,))
        else:
            c.execute("SELECT * FROM opciones_evento WHERE id = ?", (ganador_id,))
        opcion_ganadora = c.fetchone()
        opcion_ganadora_dict = dict(opcion_ganadora) if opcion_ganadora else {}
        if not opcion_ganadora:
            conn.rollback()
            return jsonify({"success": False, "error": "Opción ganadora inválida"}), 400

        nombre_ganador = opcion_ganadora_dict.get("nombre")
        titulo_evento = evento_dict.get("titulo")

        if DATABASE_URL:
            c.execute(
                "UPDATE eventos SET estado = 'cerrado', ganador_id = %s WHERE id = %s",
                (ganador_id, evento_id),
            )
        else:
            c.execute(
                "UPDATE eventos SET estado = 'cerrado', ganador_id = ? WHERE id = ?",
                (ganador_id, evento_id),
            )

        if DATABASE_URL:
            c.execute(
                "SELECT * FROM orders WHERE evento_id::text = %s AND estado = 'activa'",
                (str(evento_id),),
            )
        else:
            c.execute(
                "SELECT * FROM orders WHERE evento_id = ? AND estado = 'activa'",
                (str(evento_id),),
            )
        ordenes_activas_residuales = c.fetchall()

        for orden in ordenes_activas_residuales:
            orden_dict = dict(orden)
            usr = orden_dict.get("username")
            cant_residual = orden_dict.get("cantidad", 0.0)
            accion_orden = orden_dict.get("accion")
            precio_orden = orden_dict.get("precio", 0.0)
            op_id = orden_dict.get("opcion_id")

            if DATABASE_URL:
                c.execute(
                    "SELECT nombre FROM opciones_evento WHERE id = %s", (op_id,)
                )
            else:
                c.execute(
                    "SELECT nombre FROM opciones_evento WHERE id = ?", (op_id,)
                )
            op_data = c.fetchone()
            op_data_dict = dict(op_data) if op_data else {}
            nombre_op_residual = op_data_dict.get("nombre", "Opción")

            if accion_orden == "comprar":
                monto_a_devolver = precio_orden * cant_residual
                if DATABASE_URL:
                    c.execute(
                        "SELECT saldo_disponible FROM usuarios WHERE username = %s FOR UPDATE",
                        (usr,),
                    )
                else:
                    c.execute(
                        "SELECT saldo_disponible FROM usuarios WHERE username = ?",
                        (usr,),
                    )
                u_s = c.fetchone()
                u_s_dict = dict(u_s) if u_s else {}
                if u_s:
                    nuevo_s_compra = u_s_dict.get("saldo_disponible", 0.0) + monto_a_devolver
                    if DATABASE_URL:
                        c.execute(
                            "UPDATE usuarios SET saldo_disponible = %s WHERE username = %s",
                            (nuevo_s_compra, usr),
                        )
                        c.execute(
                            "INSERT INTO transacciones (username, tipo, monto, txid, fecha) VALUES (%s, %s, %s, %s, %s)",
                            (
                                usr,
                                "Devolución Orden No Ejecutada",
                                monto_a_devolver,
                                f"DEV_{orden_dict.get('id')}_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                                datetime.now().strftime("%Y-%m-%d %H:%M"),
                            ),
                        )
                    else:
                        c.execute(
                            "UPDATE usuarios SET saldo_disponible = ? WHERE username = ?",
                            (nuevo_s_compra, usr),
                        )
                        c.execute(
                            "INSERT INTO transacciones (username, tipo, monto, txid, fecha) VALUES (?, ?, ?, ?, ?)",
                            (
                                usr,
                                "Devolución Orden No Ejecutada",
                                monto_a_devolver,
                                f"DEV_{orden_dict.get('id')}_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                                datetime.now().strftime("%Y-%m-%d %H:%M"),
                            ),
                        )
            elif accion_orden == "vender":
                if DATABASE_URL:
                    c.execute(
                        "INSERT INTO historial_apuestas (username, titulo_evento, opcion_elegida, monto, estado) VALUES (%s, %s, %s, %s, 'Cancelada')",
                        (
                            usr,
                            titulo_evento,
                            nombre_op_residual,
                            cant_residual,
                        ),
                    )
                else:
                    c.execute(
                        "INSERT INTO historial_apuestas (username, titulo_evento, opcion_elegida, monto, estado) VALUES (?, ?, ?, ?, 'Cancelada')",
                        (
                            usr,
                            titulo_evento,
                            nombre_op_residual,
                            cant_residual,
                        ),
                    )

            if DATABASE_URL:
                c.execute(
                    "UPDATE orders SET estado = 'cancelada_cierre' WHERE id = %s",
                    (orden_dict.get("id"),),
                )
            else:
                c.execute(
                    "UPDATE orders SET estado = 'cancelada_cierre' WHERE id = ?",
                    (orden_dict.get("id"),),
                )

        if DATABASE_URL:
            c.execute(
                "SELECT * FROM historial_apuestas WHERE titulo_evento = %s AND"
                " opcion_elegida = %s AND estado = 'Activo'",
                (titulo_evento, nombre_ganador),
            )
        else:
            c.execute(
                "SELECT * FROM historial_apuestas WHERE titulo_evento = ? AND"
                " opcion_elegida = ? AND estado = 'Activo'",
                (titulo_evento, nombre_ganador),
            )
        apuestas_ganadoras = c.fetchall()

        for ap in apuestas_ganadoras:
            ap_dict = dict(ap)
            usr = ap_dict.get("username")
            premio = ap_dict.get("monto", 0.0) * 2.0
            if DATABASE_URL:
                c.execute(
                    "SELECT saldo_disponible FROM usuarios WHERE username = %s FOR"
                    " UPDATE",
                    (usr,),
                )
            else:
                c.execute(
                    "SELECT saldo_disponible FROM usuarios WHERE username = ?", (usr,)
                )
            u_row = c.fetchone()
            u_row_dict = dict(u_row) if u_row else {}
            if u_row:
                nuevo_saldo = u_row_dict.get("saldo_disponible", 0.0) + premio
                if DATABASE_URL:
                    c.execute(
                        "UPDATE usuarios SET saldo_disponible = %s WHERE username = %s",
                        (nuevo_saldo, usr),
                    )
                    c.execute(
                        "INSERT INTO transacciones (username, tipo, monto, txid, fecha)"
                        " VALUES (%s, %s, %s, %s, %s)",
                        (
                            usr,
                            "Premio Automático",
                            premio,
                            (
                                f"AUTO_WIN_{ap_dict.get('id')}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
                            ),
                            datetime.now().strftime("%Y-%m-%d %H:%M"),
                        ),
                    )
                else:
                    c.execute(
                        "UPDATE usuarios SET saldo_disponible = ? WHERE username = ?",
                        (nuevo_saldo, usr),
                    )
                    c.execute(
                        "INSERT INTO transacciones (username, tipo, monto, txid, fecha)"
                        " VALUES (?, ?, ?, ?, ?)",
                        (
                            usr,
                            "Premio Automático",
                            premio,
                            (
                                f"AUTO_WIN_{ap_dict.get('id')}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
                            ),
                            datetime.now().strftime("%Y-%m-%d %H:%M"),
                        ),
                    )

        if DATABASE_URL:
            c.execute(
                "UPDATE historial_apuestas SET estado = 'Ganada' WHERE titulo_evento ="
                " %s AND opcion_elegida = %s AND estado = 'Activo'",
                (titulo_evento, nombre_ganador),
            )
            c.execute(
                "UPDATE historial_apuestas SET estado = 'Perdida' WHERE titulo_evento ="
                " %s AND opcion_elegida != %s AND estado = 'Activo'",
                (titulo_evento, nombre_ganador),
            )
        else:
            c.execute(
                "UPDATE historial_apuestas SET estado = 'Ganada' WHERE titulo_evento ="
                " ? AND opcion_elegida = ? AND estado = 'Activo'",
                (titulo_evento, nombre_ganador),
            )
            c.execute(
                "UPDATE historial_apuestas SET estado = 'Perdida' WHERE titulo_evento ="
                " ? AND opcion_elegida != ? AND estado = 'Activo'",
                (titulo_evento, nombre_ganador),
            )

        conn.commit()
        registrar_log_admin(
            "CERRAR_EVENTO",
            f"Cerrado evento ID {evento_id}. Ganador: {nombre_ganador}. Pagos"
            " acreditados automáticamente y órdenes residuales gestionadas.",
        )
        registrar_audit_log("Admin", "CERRAR_EVENTO", str(evento_id), {"ganador": nombre_ganador})
        return jsonify({
            "success": True,
            "mensaje": (
                f"Evento cerrado, órdenes residuales procesadas y premios acreditados automáticamente. Ganador:"
                f" {nombre_ganador}"
            ),
        })
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route("/api/admin/toggle-freeze", methods=["POST"])
def admin_toggle_freeze():
    if not session.get("is_admin"):
        return jsonify({"success": False, "error": "No autorizado"}), 401
    data = request.json or {}
    username = data.get("username")
    if not username:
        return jsonify({"success": False, "error": "Usuario no especificado"}), 400

    conn = obtener_conexion()
    c = conn.cursor()
    try:
        if DATABASE_URL:
            c.execute("SELECT is_frozen FROM usuarios WHERE username = %s", (username,))
        else:
            c.execute("SELECT is_frozen FROM usuarios WHERE username = ?", (username,))
        row = c.fetchone()
        row_dict = dict(row) if row else {}
        if not row:
            conn.close()
            return jsonify({"success": False, "error": "Usuario no encontrado"}), 404

        nuevo_estado = not bool(row_dict.get("is_frozen", 0))
        if DATABASE_URL:
            c.execute(
                "UPDATE usuarios SET is_frozen = %s WHERE username = %s",
                (nuevo_estado, username),
            )
        else:
            c.execute(
                "UPDATE usuarios SET is_frozen = ? WHERE username = ?",
                (1 if nuevo_estado else 0, username),
            )
        conn.commit()
        accion_desc = "Congelado" if nuevo_estado else "Descongelado"
        registrar_log_admin("TOGGLE_FREEZE", f"Usuario {username} ha sido {accion_desc}")
        registrar_audit_log("Admin", "TOGGLE_FREEZE", username, {"is_frozen": nuevo_estado})
        return jsonify({
            "success": True,
            "mensaje": f"Usuario {username} ha sido {accion_desc.lower()} exitosamente.",
            "is_frozen": nuevo_estado,
        })
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
