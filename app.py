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

CORS(app, resources={r"/api/*": {"origins": "*"}})
app.secret_key = os.environ.get(
    "FLASK_SECRET_KEY", "p2ppredict_secret_key_ultra_segura_2026"
)

# Configuración de contraseña de administrador robusta vía variable de entorno o por defecto con hash seguro
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


def inicializar_bd():
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
    c.execute("""CREATE TABLE IF NOT EXISTS ordenes_clob (
                        id SERIAL PRIMARY KEY,
                        username TEXT,
                        evento_id INTEGER,
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
    c.execute("""CREATE TABLE IF NOT EXISTS ordenes_clob (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, 
                    username TEXT, 
                    evento_id INTEGER, 
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

  conn.commit()

  # REQUISITO 2: Asegurar que solo exista la predicción de la Mainnet de Pi abierta inicialmente si la tabla está vacía
  c.execute("SELECT COUNT(*) as total FROM eventos")
  row = c.fetchone()
  total_evs = row["total"] if row else 0

  if total_evs == 0:
    eventos_iniciales = [
        {
            "titulo": "¿Pi Network lanzará su Mainnet abierta global este año?",
            "categoria": "Pi Ecosystem",
            "fecha_cierre": "2026-11-30",
            "opciones": [("Sí", 0.0), ("No", 0.0)],
        },
    ]
    for ev in eventos_iniciales:
      if DATABASE_URL:
        c.execute(
            "INSERT INTO eventos (titulo, categoria, estado, fecha_cierre)"
            " VALUES (%s, %s, 'activo', %s) RETURNING id",
            (ev["titulo"], ev["categoria"], ev["fecha_cierre"]),
        )
        res_ev = c.fetchone()
        ev_id = res_ev["id"]
        for opt_nombre, opt_pozo in ev["opciones"]:
          c.execute(
              "INSERT INTO opciones_evento (evento_id, nombre, pozo) VALUES"
              " (%s, %s, %s)",
              (ev_id, opt_nombre, opt_pozo),
          )
      else:
        c.execute(
            "INSERT INTO eventos (titulo, categoria, estado, fecha_cierre)"
            " VALUES (?, ?, 'activo', ?)",
            (ev["titulo"], ev["categoria"], ev["fecha_cierre"]),
        )
        ev_id = c.lastrowid
        for opt_nombre, opt_pozo in ev["opciones"]:
          c.execute(
              "INSERT INTO opciones_evento (evento_id, nombre, pozo) VALUES"
              " (?, ?, ?)",
              (ev_id, opt_nombre, opt_pozo),
          )
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
          "INSERT INTO admin_logs (ip, accion, detalles, fecha) VALUES (%s,"
          " %s, %s, %s)",
          (ip, accion, detalles, fecha),
      )
    else:
      c.execute(
          "INSERT INTO admin_logs (ip, accion, detalles, fecha) VALUES (?, ?,"
          " ?, ?)",
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
          "INSERT INTO admin_audit_logs (admin_id, action_type, target_id,"
          " ip_address, user_agent, payload_snapshot) VALUES (%s, %s, %s, %s,"
          " %s, %s)",
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
          "INSERT INTO admin_audit_logs (admin_id, action_type, target_id,"
          " ip_address, user_agent, payload_snapshot, created_at) VALUES (?, ?,"
          " ?, ?, ?, ?, ?)",
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


@app.after_request
def agregar_cabeceras_seguridad(response):
  response.headers["X-Content-Type-Options"] = "nosniff"
  response.headers["X-Frame-Options"] = "ALLOWALL"
  response.headers["X-XSS-Protection"] = "1; mode=block"
  response.headers["Strict-Transport-Security"] = (
      "max-age=31536000; includeSubDomains"
  )
  response.headers["Access-Control-Allow-Origin"] = "*"
  response.headers["Access-Control-Allow-Headers"] = (
      "Content-Type,Authorization"
  )
  response.headers["Access-Control-Allow-Methods"] = "GET,POST,PUT,DELETE,OPTIONS"
  response.headers["Cross-Origin-Embedder-Policy"] = "unsafe-none"
  response.headers["Cross-Origin-Opener-Policy"] = "unsafe-none"
  return response


@app.route("/")
def home():
  return render_template("index.html")


@app.route("/api/saldo/<username>", methods=["GET"])
def obtener_saldo(username):
  limite = int(request.args.get("limit", 20))
  offset = int(request.args.get("offset", 0))
  filtro_tipo = request.args.get("tipo", "").strip()

  conn = obtener_conexion()
  c = conn.cursor()

  if DATABASE_URL:
    c.execute(
        "SELECT saldo_disponible, is_frozen FROM usuarios WHERE username = %s",
        (username,),
    )
  else:
    c.execute(
        "SELECT saldo_disponible, is_frozen FROM usuarios WHERE username = ?",
        (username,),
    )
  row = c.fetchone()

  if not row:
    # REQUISITO 1: Únicamente @jaimetetio queda con saldo inicial de 0.1 Pi. Los demás en 0.0
    saldo_inicial = (
        0.10 if username.lower() in ["@jaimetetio", "jaimetetio"] else 0.0
    )
    if DATABASE_URL:
      c.execute(
          "INSERT INTO usuarios (username, saldo_disponible, is_frozen) VALUES"
          " (%s, %s, FALSE)",
          (username, saldo_inicial),
      )
      if saldo_inicial > 0:
        txid = f"CREDITO_INICIAL_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        fecha = datetime.now().strftime("%Y-%m-%d %H:%M")
        c.execute(
            "INSERT INTO transacciones (username, tipo, monto, txid, fecha)"
            " VALUES (%s, %s, %s, %s, %s)",
            (username, "Crédito Inicial", saldo_inicial, txid, fecha),
        )
    else:
      c.execute(
          "INSERT INTO usuarios (username, saldo_disponible, is_frozen) VALUES"
          " (?, ?, 0)",
          (username, saldo_inicial),
      )
      if saldo_inicial > 0:
        txid = f"CREDITO_INICIAL_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        fecha = datetime.now().strftime("%Y-%m-%d %H:%M")
        c.execute(
            "INSERT INTO transacciones (username, tipo, monto, txid, fecha)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (username, "Crédito Inicial", saldo_inicial, txid, fecha),
        )
    conn.commit()
    saldo = saldo_inicial
    is_frozen = False
  else:
    saldo = row["saldo_disponible"]
    is_frozen = bool(row["is_frozen"])

  if DATABASE_URL:
    c.execute(
        "SELECT * FROM historial_apuestas WHERE username = %s ORDER BY id DESC"
        " LIMIT %s OFFSET %s",
        (username, limite, offset),
    )
  else:
    c.execute(
        "SELECT * FROM historial_apuestas WHERE username = ? ORDER BY id DESC"
        " LIMIT ? OFFSET ?",
        (username, limite, offset),
    )
  historial = [dict(row) for row in c.fetchall()]

  if filtro_tipo:
    if DATABASE_URL:
      c.execute(
          "SELECT * FROM transacciones WHERE username = %s AND tipo ILIKE %s"
          " ORDER BY id DESC LIMIT %s OFFSET %s",
          (username, f"%{filtro_tipo}%", limite, offset),
      )
    else:
      c.execute(
          "SELECT * FROM transacciones WHERE username = ? AND tipo LIKE ? ORDER"
          " BY id DESC LIMIT ? OFFSET ?",
          (username, f"%{filtro_tipo}%", limite, offset),
      )
  else:
    if DATABASE_URL:
      c.execute(
          "SELECT * FROM transacciones WHERE username = %s ORDER BY id DESC"
          " LIMIT %s OFFSET %s",
          (username, limite, offset),
      )
    else:
      c.execute(
          "SELECT * FROM transacciones WHERE username = ? ORDER BY id DESC LIMIT"
          " ? OFFSET ?",
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
          "SELECT id, nombre, pozo FROM opciones_evento WHERE evento_id = %s",
          (ev_dict["id"],),
      )
    else:
      c.execute(
          "SELECT id, nombre, pozo FROM opciones_evento WHERE evento_id = ?",
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
          "SELECT saldo_disponible, is_frozen FROM usuarios WHERE username = %s"
          " FOR UPDATE",
          (username,),
      )
    else:
      c.execute(
          "SELECT saldo_disponible, is_frozen FROM usuarios WHERE username = ?",
          (username,),
      )

    row = c.fetchone()
    if row and row.get("is_frozen"):
      conn.rollback()
      conn.close()
      return jsonify({
          "success": False,
          "error": "Tu cuenta se encuentra suspendida temporalmente.",
      }), 403

    saldo_actual = row["saldo_disponible"] if row else 0
    if not row or saldo_actual < monto:
      conn.rollback()
      conn.close()
      return jsonify({"success": False, "error": "Saldo insuficiente"})

    if DATABASE_URL:
      c.execute("SELECT * FROM eventos WHERE id = %s", (evento_id,))
    else:
      c.execute("SELECT * FROM eventos WHERE id = ?", (evento_id,))
    evento = c.fetchone()

    if not evento or evento["estado"] != "activo":
      conn.rollback()
      conn.close()
      return jsonify({"success": False, "error": "Mercado no disponible"})

    if DATABASE_URL:
      c.execute(
          "SELECT * FROM opciones_evento WHERE id = %s AND evento_id = %s",
          (opcion_id, evento_id),
      )
    else:
      c.execute(
          "SELECT * FROM opciones_evento WHERE id = ? AND evento_id = ?",
          (opcion_id, evento_id),
      )
    opcion = c.fetchone()

    if not opcion:
      conn.rollback()
      conn.close()
      return jsonify({"success": False, "error": "Opción inválida"})

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
          (username, evento["titulo"], opcion["nombre"], monto, "Activo"),
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
          (username, evento["titulo"], opcion["nombre"], monto, "Activo"),
      )
      c.execute(
          "INSERT INTO transacciones (username, tipo, monto, txid, fecha)"
          " VALUES (?, ?, ?, ?, ?, ?)",
          (
              username,
              "Apuesta",
              -monto,
              f"BET_{datetime.now().strftime('%Y%m%d%H%M%S')}",
              datetime.now().strftime("%Y-%m-%d %H:%M"),
          ),
      )

    conn.commit()
    return jsonify({
        "success": True,
        "nuevo_saldo": nuevo_saldo,
        "mensaje": "¡Apuesta registrada con éxito!",
    })
  except Exception as e:
    conn.rollback()
    return jsonify({"success": False, "error": str(e)}), 500
  finally:
    conn.close()


@app.route("/api/clob/ordenes", methods=["GET"])
def obtener_ordenes_clob():
  evento_id = request.args.get("evento_id")
  conn = obtener_conexion()
  c = conn.cursor()
  if evento_id:
    if DATABASE_URL:
      c.execute(
          "SELECT * FROM ordenes_clob WHERE evento_id = %s AND estado = 'activa'"
          " ORDER BY precio DESC",
          (evento_id,),
      )
    else:
      c.execute(
          "SELECT * FROM ordenes_clob WHERE evento_id = ? AND estado = 'activa'"
          " ORDER BY precio DESC",
          (evento_id,),
      )
  else:
    c.execute(
        "SELECT * FROM ordenes_clob WHERE estado = 'activa' ORDER BY id DESC"
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
          "SELECT * FROM ordenes_clob WHERE estado = 'activa' ORDER BY RANDOM()"
          " LIMIT 1"
      )
    else:
      c.execute(
          "SELECT * FROM ordenes_clob WHERE estado = 'activa' ORDER BY RANDOM()"
          " LIMIT 1"
      )

    orden_azar = c.fetchone()
    if orden_azar:
      variacion = round(random.uniform(-0.01, 0.01), 3)
      nuevo_precio = max(0.01, round(orden_azar["precio"] + variacion, 3))
      if DATABASE_URL:
        c.execute(
            "UPDATE ordenes_clob SET precio = %s WHERE id = %s",
            (nuevo_precio, orden_azar["id"]),
        )
      else:
        c.execute(
            "UPDATE ordenes_clob SET precio = ? WHERE id = ?",
            (nuevo_precio, orden_azar["id"]),
        )
      conn.commit()

    if DATABASE_URL:
      c.execute(
          "SELECT * FROM ordenes_clob WHERE estado = 'activa' ORDER BY precio"
          " DESC LIMIT 50"
      )
    else:
      c.execute(
          "SELECT * FROM ordenes_clob WHERE estado = 'activa' ORDER BY precio"
          " DESC LIMIT 50"
      )
    ordenes = [dict(row) for row in c.fetchall()]
    conn.close()
    return jsonify({"success": True, "ordenes": ordenes, "timestamp": time.time()})
  except Exception as e:
    conn.rollback()
    conn.close()
    return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/clob/orden", methods=["POST"])
def crear_orden_clob():
  data = request.json or {}
  username = data.get("username")
  evento_id = data.get("evento_id")
  opcion_id = data.get("opcion_id")
  tipo_orden = data.get("tipo_orden", "limit")
  accion = data.get("accion")

  try:
    precio = float(data.get("precio", 0))
    cantidad = float(data.get("cantidad", 0))
  except (ValueError, TypeError):
    return jsonify({"success": False, "error": "Valores numéricos inválidos"}), 400

  if precio <= 0 or cantidad <= 0 or accion not in ["comprar", "vender"]:
    return jsonify({"success": False, "error": "Parámetros de orden incorrectos"}), 400

  conn = obtener_conexion()
  c = conn.cursor()
  try:
    if DATABASE_URL:
      c.execute(
          "SELECT saldo_disponible, is_frozen FROM usuarios WHERE username = %s"
          " FOR UPDATE",
          (username,),
      )
    else:
      c.execute(
          "SELECT saldo_disponible, is_frozen FROM usuarios WHERE username = ?",
          (username,),
      )
    row_user = c.fetchone()

    if row_user and row_user.get("is_frozen"):
      conn.rollback()
      conn.close()
      return jsonify({
          "success": False,
          "error": "Tu cuenta se encuentra suspendida temporalmente.",
      }), 403

    costo_inicial = precio * cantidad if accion == "comprar" else cantidad
    if not row_user or row_user["saldo_disponible"] < costo_inicial:
      conn.rollback()
      conn.close()
      return jsonify({
          "success": False,
          "error": "Saldo insuficiente para colocar la orden",
      })

    nuevo_saldo_creador = row_user["saldo_disponible"] - costo_inicial
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

    cantidad_restante = cantidad
    fecha_str = datetime.now().strftime("%Y-%m-%d %H:%M")

    if DATABASE_URL:
      c.execute("SELECT titulo FROM eventos WHERE id = %s", (evento_id,))
      ev_row = c.fetchone()
      c.execute("SELECT nombre FROM opciones_evento WHERE id = %s", (opcion_id,))
      op_row = c.fetchone()
    else:
      c.execute("SELECT titulo FROM eventos WHERE id = ?", (evento_id,))
      ev_row = c.fetchone()
      c.execute("SELECT nombre FROM opciones_evento WHERE id = ?", (opcion_id,))
      op_row = c.fetchone()

    titulo_ev = ev_row["titulo"] if ev_row else "Mercado P2P"
    nombre_op = op_row["nombre"] if op_row else "Opción"

    if accion == "comprar":
      if DATABASE_URL:
        c.execute(
            "SELECT * FROM ordenes_clob WHERE evento_id = %s AND opcion_id = %s"
            " AND accion = 'vender' AND estado = 'activa' AND precio <= %s"
            " ORDER BY precio ASC, id ASC FOR UPDATE",
            (evento_id, opcion_id, precio),
        )
      else:
        c.execute(
            "SELECT * FROM ordenes_clob WHERE evento_id = ? AND opcion_id = ?"
            " AND accion = 'vender' AND estado = 'activa' AND precio <= ?"
            " ORDER BY precio ASC, id ASC",
            (evento_id, opcion_id, precio),
        )
      contra_ordenes = c.fetchall()

      for contra in contra_ordenes:
        if cantidad_restante <= 0:
          break

        match_cant = min(cantidad_restante, contra["cantidad"])
        match_precio = contra["precio"]

        diferencia_precio = (precio - match_precio) * match_cant
        if diferencia_precio > 0:
          nuevo_saldo_creador += diferencia_precio
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

        monto_vendedor = match_precio * match_cant
        if DATABASE_URL:
          c.execute(
              "SELECT saldo_disponible FROM usuarios WHERE username = %s FOR"
              " UPDATE",
              (contra["username"],),
          )
        else:
          c.execute(
              "SELECT saldo_disponible FROM usuarios WHERE username = ?",
              (contra["username"],),
          )
        v_row = c.fetchone()
        if v_row:
          nuevo_saldo_vendedor = v_row["saldo_disponible"] + monto_vendedor
          if DATABASE_URL:
            c.execute(
                "UPDATE usuarios SET saldo_disponible = %s WHERE username = %s",
                (nuevo_saldo_vendedor, contra["username"]),
            )
          else:
            c.execute(
                "UPDATE usuarios SET saldo_disponible = ? WHERE username = ?",
                (nuevo_saldo_vendedor, contra["username"]),
            )

        if DATABASE_URL:
          c.execute(
              "INSERT INTO historial_apuestas (username, titulo_evento,"
              " opcion_elegida, monto, estado) VALUES (%s, %s, %s, %s, %s)",
              (
                  username,
                  titulo_ev,
                  nombre_op,
                  match_precio * match_cant,
                  "Activo",
              ),
          )
        else:
          c.execute(
              "INSERT INTO historial_apuestas (username, titulo_evento,"
              " opcion_elegida, monto, estado) VALUES (?, ?, ?, ?, ?)",
              (
                  username,
                  titulo_ev,
                  nombre_op,
                  match_precio * match_cant,
                  "Activo",
              ),
          )

        nueva_contra_cant = contra["cantidad"] - match_cant
        nuevo_estado_contra = (
            "completada" if nueva_contra_cant <= 0 else "activa"
        )
        if DATABASE_URL:
          c.execute(
              "UPDATE ordenes_clob SET cantidad = %s, estado = %s WHERE id = %s",
              (nueva_contra_cant, nuevo_estado_contra, contra["id"]),
          )
        else:
          c.execute(
              "UPDATE ordenes_clob SET cantidad = ?, estado = ? WHERE id = ?",
              (nueva_contra_cant, nuevo_estado_contra, contra["id"]),
          )

        cantidad_restante -= match_cant

    else:
      if DATABASE_URL:
        c.execute(
            "SELECT * FROM ordenes_clob WHERE evento_id = %s AND opcion_id = %s"
            " AND accion = 'comprar' AND estado = 'activa' AND precio >= %s"
            " ORDER BY precio DESC, id ASC FOR UPDATE",
            (evento_id, opcion_id, precio),
        )
      else:
        c.execute(
            "SELECT * FROM ordenes_clob WHERE evento_id = ? AND opcion_id = ?"
            " AND accion = 'comprar' AND estado = 'activa' AND precio >= ?"
            " ORDER BY precio DESC, id ASC",
            (evento_id, opcion_id, precio),
        )
      contra_ordenes = c.fetchall()

      for contra in contra_ordenes:
        if cantidad_restante <= 0:
          break

        match_cant = min(cantidad_restante, contra["cantidad"])
        match_precio = contra["precio"]

        monto_venta = match_precio * match_cant

        nuevo_saldo_creador += monto_venta
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
              "INSERT INTO historial_apuestas (username, titulo_evento,"
              " opcion_elegida, monto, estado) VALUES (%s, %s, %s, %s, %s)",
              (
                  contra["username"],
                  titulo_ev,
                  nombre_op,
                  match_precio * match_cant,
                  "Activo",
              ),
          )
        else:
          c.execute(
              "INSERT INTO historial_apuestas (username, titulo_evento,"
              " opcion_elegida, monto, estado) VALUES (?, ?, ?, ?, ?)",
              (
                  contra["username"],
                  titulo_ev,
                  nombre_op,
                  match_precio * match_cant,
                  "Activo",
              ),
          )

        nueva_contra_cant = contra["cantidad"] - match_cant
        nuevo_estado_contra = (
            "completada" if nueva_contra_cant <= 0 else "activa"
        )
        if DATABASE_URL:
          c.execute(
              "UPDATE ordenes_clob SET cantidad = %s, estado = %s WHERE id = %s",
              (nueva_contra_cant, nuevo_estado_contra, contra["id"]),
          )
        else:
          c.execute(
              "UPDATE ordenes_clob SET cantidad = ?, estado = ? WHERE id = ?",
              (nueva_contra_cant, nuevo_estado_contra, contra["id"]),
          )

        cantidad_restante -= match_cant

    estado_final_orden = "activa" if cantidad_restante > 0 else "completada"
    if cantidad_restante > 0:
      if DATABASE_URL:
        c.execute(
            "INSERT INTO ordenes_clob (username, evento_id, opcion_id,"
            " tipo_orden, accion, precio, cantidad, estado, fecha) VALUES (%s,"
            " %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                username,
                evento_id,
                opcion_id,
                tipo_orden,
                accion,
                precio,
                cantidad_restante,
                estado_final_orden,
                fecha_str,
            ),
        )
      else:
        c.execute(
            "INSERT INTO ordenes_clob (username, evento_id, opcion_id,"
            " tipo_orden, accion, precio, cantidad, estado, fecha) VALUES (?, ?,"
            " ?, ?, ?, ?, ?, ?, ?)",
            (
                username,
                evento_id,
                opcion_id,
                tipo_orden,
                accion,
                precio,
                cantidad_restante,
                estado_final_orden,
                fecha_str,
            ),
        )

    if DATABASE_URL:
      c.execute(
          "INSERT INTO historial_apuestas (username, titulo_evento,"
          " opcion_elegida, monto, estado) VALUES (%s, %s, %s, %s, %s)",
          (
              username,
              titulo_ev,
              f"CLOB {accion.capitalize()} ({cantidad} a {precio})",
              costo_inicial,
              (
                  "Vendida"
                  if accion == "vender"
                  else "Completada/Ordenada"
              ),
          ),
      )
      c.execute(
          "INSERT INTO transacciones (username, tipo, monto, txid, fecha)"
          " VALUES (%s, %s, %s, %s, %s)",
          (
              username,
              f"CLOB Orden ({accion})",
              -costo_inicial + (precio * (cantidad - cantidad_restante)),
              f"CLOB_{datetime.now().strftime('%Y%m%d%H%M%S')}",
              fecha_str,
          ),
      )
    else:
      c.execute(
          "INSERT INTO historial_apuestas (username, titulo_evento,"
          " opcion_elegida, monto, estado) VALUES (?, ?, ?, ?, ?)",
          (
              username,
              titulo_ev,
              f"CLOB {accion.capitalize()} ({cantidad} a {precio})",
              costo_inicial,
              (
                  "Vendida"
                  if accion == "vender"
                  else "Completada/Ordenada"
              ),
          ),
      )
      c.execute(
          "INSERT INTO transacciones (username, tipo, monto, txid, fecha)"
          " VALUES (?, ?, ?, ?, ?, ?)",
          (
              username,
              f"CLOB Orden ({accion})",
              -costo_inicial + (precio * (cantidad - cantidad_restante)),
              f"CLOB_{datetime.now().strftime('%Y%m%d%H%M%S')}",
              fecha_str,
          ),
      )

    conn.commit()
    return jsonify({
        "success": True,
        "nuevo_saldo": nuevo_saldo_creador,
        "mensaje": (
            f"Orden procesada. Ejecutado: {cantidad - cantidad_restante} /"
            f" Colocado en libro: {cantidad_restante}"
        ),
    })
  except Exception as e:
    conn.rollback()
    return jsonify({"success": False, "error": str(e)}), 500
  finally:
    conn.close()


@app.route("/api/pi/aprobar-pago", methods=["POST"])
def aprobar_pago():
  data = request.json or {}
  payment_id = data.get("paymentId")
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
    return jsonify(
        {"success": False, "error": "Error de red con Pi Network"}
    ), 504

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
      return jsonify(
          {"success": False, "error": "Error de red con Pi Network"}
      ), 504

  conn = obtener_conexion()
  c = conn.cursor()

  try:
    if DATABASE_URL:
      c.execute(
          "SELECT saldo_disponible, is_frozen FROM usuarios WHERE username = %s"
          " FOR UPDATE",
          (username,),
      )
    else:
      c.execute(
          "SELECT saldo_disponible, is_frozen FROM usuarios WHERE username = ?",
          (username,),
      )
    row = c.fetchone()

    if row and row.get("is_frozen"):
      conn.rollback()
      conn.close()
      return jsonify({
          "success": False,
          "error": "Tu cuenta se encuentra suspendida temporalmente.",
      }), 403

    if not row:
      nuevo_saldo = monto
      if DATABASE_URL:
        c.execute(
            "INSERT INTO usuarios (username, saldo_disponible, is_frozen) VALUES"
            " (%s, %s, FALSE)",
            (username, nuevo_saldo),
        )
      else:
        c.execute(
            "INSERT INTO usuarios (username, saldo_disponible, is_frozen) VALUES"
            " (?, ?, 0)",
            (username, nuevo_saldo),
        )
    else:
      nuevo_saldo = row["saldo_disponible"] + monto
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

    fecha = datetime.now().strftime("%Y-%m-%d %H:%M")
    if DATABASE_URL:
      c.execute(
          "INSERT INTO transacciones (username, tipo, monto, txid, fecha)"
          " VALUES (%s, %s, %s, %s, %s)",
          (username, "Recarga Pi Real", monto, txid or payment_id, fecha),
      )
    else:
      c.execute(
          "INSERT INTO transacciones (username, tipo, monto, txid, fecha)"
          " VALUES (?, ?, ?, ?, ?, ?)",
          (username, "Recarga Pi Real", monto, txid or payment_id, fecha),
      )

    if DATABASE_URL:
      c.execute("SELECT SUM(saldo_disponible) as total FROM usuarios")
      res_tot = c.fetchone()
      balance_total_plataforma = (
          res_tot["total"] if res_tot and res_tot["total"] else 0.0
      )
      c.execute(
          "INSERT INTO pi_wallet_events (username, evento_tipo, monto,"
          " balance_total_plataforma, txid, fecha) VALUES (%s, %s, %s, %s, %s,"
          " %s)",
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
      c.execute("SELECT SUM(saldo_disponible) as total FROM usuarios")
      res_tot = c.fetchone()
      balance_total_plataforma = (
          res_tot["total"] if res_tot and res_tot["total"] else 0.0
      )
      c.execute(
          "INSERT INTO pi_wallet_events (username, evento_tipo, monto,"
          " balance_total_plataforma, txid, fecha) VALUES (?, ?, ?, ?, ?, ?)",
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
    return jsonify({
        "success": True,
        "nuevo_saldo": nuevo_saldo,
        "balance_total_plataforma": balance_total_plataforma,
        "mensaje": f"Recarga de {monto} Pi acreditada con éxito.",
    })
  except Exception as e:
    conn.rollback()
    return jsonify({"success": False, "error": str(e)}), 500
  finally:
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
    return jsonify(
        {"success": False, "error": "El monto mínimo de retiro es de 1.0 Pi"}
    ), 400

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
          "SELECT saldo_disponible, is_frozen FROM usuarios WHERE username = %s"
          " FOR UPDATE",
          (username,),
      )
    else:
      c.execute(
          "SELECT saldo_disponible, is_frozen FROM usuarios WHERE username = ?",
          (username,),
      )
    row = c.fetchone()

    if row and row.get("is_frozen"):
      conn.rollback()
      conn.close()
      return jsonify({
          "success": False,
          "error": "Tu cuenta se encuentra suspendida temporalmente.",
      }), 403

    if not row or row["saldo_disponible"] < monto:
      conn.rollback()
      conn.close()
      return jsonify({
          "success": False,
          "error": "Saldo insuficiente para procesar el retiro",
      })

    saldo_actual = row["saldo_disponible"]
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
      conn.close()
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
    else:
      c.execute(
          "INSERT INTO transacciones (username, tipo, monto, txid, fecha)"
          " VALUES (?, ?, ?, ?, ?, ?)",
          (username, "Retiro Pi Blockchain", -monto, txid, fecha),
      )

    if DATABASE_URL:
      c.execute("SELECT SUM(saldo_disponible) as total FROM usuarios")
      res_tot = c.fetchone()
      balance_total_plataforma = (
          res_tot["total"] if res_tot and res_tot["total"] else 0.0
      )
      c.execute(
          "INSERT INTO pi_wallet_events (username, evento_tipo, monto,"
          " balance_total_plataforma, txid, fecha) VALUES (%s, %s, %s, %s, %s,"
          " %s)",
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
      c.execute("SELECT SUM(saldo_disponible) as total FROM usuarios")
      res_tot = c.fetchone()
      balance_total_plataforma = (
          res_tot["total"] if res_tot and res_tot["total"] else 0.0
      )
      c.execute(
          "INSERT INTO pi_wallet_events (username, evento_tipo, monto,"
          " balance_total_plataforma, txid, fecha) VALUES (?, ?, ?, ?, ?, ?)",
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
    return jsonify({
        "success": True,
        "nuevo_saldo": nuevo_saldo,
        "balance_total_plataforma": balance_total_plataforma,
        "txid": txid,
        "mensaje": f"Retiro de {monto} Pi procesado con éxito.",
    })
  except Exception as e:
    conn.rollback()
    return jsonify({"success": False, "error": str(e)}), 500
  finally:
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
    total_circulante = (
        row["total_circulante"] if row and row["total_circulante"] else 0.0
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
    conn.close()
    return jsonify({"success": False, "error": str(e)}), 500


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
    session["is_admin"] = True
    registrar_log_admin(
        "LOGIN_EXITOSO", "Administrador inició sesión correctamente."
    )
    return jsonify({"success": True, "message": "Acceso autorizado"})

  registrar_log_admin(
      "LOGIN_FALLIDO", "Intento de acceso con contraseña incorrecta."
  )
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
    if u_check and u_check.get("is_frozen"):
      conn.rollback()
      conn.close()
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
    if not apuesta:
      conn.rollback()
      conn.close()
      return jsonify({"success": False, "error": "Apuesta no encontrada"}), 404

    if apuesta["estado"] != "Ganada":
      conn.rollback()
      conn.close()
      return jsonify({
          "success": False,
          "error": (
              "Esta apuesta no está marcada como ganadora o ya fue cobrada"
          ),
      }), 400

    premio = apuesta["monto"] * 2.0

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
    if not u_row:
      conn.rollback()
      conn.close()
      return jsonify({"success": False, "error": "Usuario no existe"}), 400

    nuevo_saldo = u_row["saldo_disponible"] + premio

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
              (
                  f"COBRO_{apuesta_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
              ),
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
          " VALUES (?, ?, ?, ?, ?, ?)",
          (
              username,
              "Cobro de Predicción",
              premio,
              (
                  f"COBRO_{apuesta_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
              ),
              datetime.now().strftime("%Y-%m-%d %H:%M"),
          ),
      )

    conn.commit()
    return jsonify({
        "success": True,
        "nuevo_saldo": nuevo_saldo,
        "mensaje": f"¡Premio de {premio} cobrado con éxito!",
    })
  except Exception as e:
    conn.rollback()
    return jsonify({"success": False, "error": str(e)}), 500
  finally:
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
      ev_id = c.fetchone()["id"]
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
    registrar_log_admin(
        "CREAR_EVENTO", f"Creado evento ID {ev_id}: {titulo}"
    )
    registrar_audit_log(
        "Admin",
        "CREAR_EVENTO",
        str(ev_id),
        {"titulo": titulo, "opciones": opciones},
    )
    return jsonify({"success": True, "mensaje": "Mercado/Evento creado con éxito"})
  except Exception as e:
    conn.rollback()
    return jsonify({"success": False, "error": str(e)}), 500
  finally:
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

    if not evento or evento["estado"] == "cerrado":
      conn.rollback()
      conn.close()
      return jsonify({
          "success": False,
          "error": "El evento no existe o ya está cerrado",
      })

    if DATABASE_URL:
      c.execute("SELECT nombre FROM opciones_evento WHERE id = %s", (ganador_id,))
    else:
      c.execute("SELECT * FROM opciones_evento WHERE id = ?", (ganador_id,))
    opcion_ganadora = c.fetchone()

    if not opcion_ganadora:
      conn.rollback()
      conn.close()
      return jsonify({"success": False, "error": "Opción ganadora inválida"})

    nombre_ganador = opcion_ganadora["nombre"]
    titulo_evento = evento["titulo"]

    if DATABASE_URL:
      c.execute(
          "UPDATE eventos SET estado = 'cerrado', ganador_id = %s WHERE id = %s",
          (ganador_id, evento_id),
      )
      c.execute(
          "SELECT * FROM historial_apuestas WHERE titulo_evento = %s AND"
          " opcion_elegida = %s AND estado = 'Activo'",
          (titulo_evento, nombre_ganador),
      )
    else:
      c.execute(
          "UPDATE eventos SET estado = 'cerrado', ganador_id = ? WHERE id = ?",
          (ganador_id, evento_id),
      )
      c.execute(
          "SELECT * FROM historial_apuestas WHERE titulo_evento = ? AND"
          " opcion_elegida = ? AND estado = 'Activo'",
          (titulo_evento, nombre_ganador),
      )

    apuestas_ganadoras = c.fetchall()

    for ap in apuestas_ganadoras:
      usr = ap["username"]
      premio = ap["monto"] * 2.0

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
      if u_row:
        nuevo_saldo = u_row["saldo_disponible"] + premio
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
                      f"AUTO_WIN_{ap['id']}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
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
              " VALUES (?, ?, ?, ?, ?, ?)",
              (
                  usr,
                  "Premio Automático",
                  premio,
                  (
                      f"AUTO_WIN_{ap['id']}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
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
        " acreditados automáticamente.",
    )
    registrar_audit_log(
        "Admin", "CERRAR_EVENTO", str(evento_id), {"ganador": nombre_ganador}
    )
    return jsonify({
        "success": True,
        "mensaje": (
            f"Evento cerrado y premios acreditados automáticamente. Ganador:"
            f" {nombre_ganador}"
        ),
    })
  except Exception as e:
    conn.rollback()
    return jsonify({"success": False, "error": str(e)}), 500
  finally:
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

    if not row:
      conn.close()
      return jsonify({"success": False, "error": "Usuario no encontrado"}), 404

    nuevo_estado = not bool(row["is_frozen"])

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
    registrar_log_admin(
        "TOGGLE_FREEZE", f"Usuario {username} ha sido {accion_desc}."
    )
    registrar_audit_log(
        "Admin", "TOGGLE_FREEZE", username, {"is_frozen": nuevo_estado}
    )
    conn.close()
    return jsonify({
        "success": True,
        "is_frozen": nuevo_estado,
        "mensaje": f"Cuenta de {username} {accion_desc} exitosamente.",
    })
  except Exception as e:
    conn.rollback()
    conn.close()
    return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/admin/ajustar-balance", methods=["POST"])
def admin_ajustar_balance():
  if not session.get("is_admin"):
    return jsonify({"success": False, "error": "No autorizado"}), 401

  data = request.json or {}
  username = data.get("username")
  razon = str(data.get("razon", "")).strip()

  try:
    monto_cambio = float(data.get("monto", 0))
  except (ValueError, TypeError):
    return jsonify({"success": False, "error": "Monto inválido"}), 400

  if not username:
    return jsonify({"success": False, "error": "Usuario no especificado"}), 400

  if not razon:
    return jsonify({
        "success": False,
        "error": (
            "Es obligatorio dejar una nota o razón para el ajuste de balance"
        ),
    }), 400

  conn = obtener_conexion()
  c = conn.cursor()
  try:
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
    row = c.fetchone()

    if not row:
      conn.close()
      return jsonify({"success": False, "error": "Usuario no encontrado"}), 404

    monto_anterior = row["saldo_disponible"]
    monto_nuevo = monto_anterior + monto_cambio

    if monto_nuevo < 0:
      conn.close()
      return jsonify({
          "success": False,
          "error": "El ajuste dejaría al usuario con saldo negativo",
      }), 400

    if abs(monto_cambio) >= 100.0:
      payload_str = str({
          "username": username,
          "monto_cambio": monto_cambio,
          "razon": razon,
          "monto_anterior": monto_anterior,
      })
      if DATABASE_URL:
        c.execute(
            "INSERT INTO admin_pending_actions (admin_creator, action_type,"
            " target_id, payload, status) VALUES (%s, %s, %s, %s, 'PENDING')",
            ("Admin", "AJUSTE_BALANCE", username, payload_str),
        )
      else:
        fecha_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute(
            "INSERT INTO admin_pending_actions (admin_creator, action_type,"
            " target_id, payload, status, created_at) VALUES (?, ?, ?, ?,"
            " 'PENDING', ?)",
            ("Admin", "AJUSTE_BALANCE", username, payload_str, fecha_str),
        )
      conn.commit()
      conn.close()
      return jsonify({
          "success": True,
          "pending": True,
          "mensaje": (
              "Ajuste crítico detectado. Solicitud retenida en estado PENDING"
              " para aprobación dual de un segundo administrador."
          ),
      })

    if DATABASE_URL:
      c.execute(
          "UPDATE usuarios SET saldo_disponible = %s WHERE username = %s",
          (monto_nuevo, username),
      )
      fecha_str = datetime.now().strftime("%Y-%m-%d %H:%M")
      c.execute(
          "INSERT INTO transacciones (username, tipo, monto, txid, fecha)"
          " VALUES (%s, %s, %s, %s, %s)",
          (
              username,
              "Ajuste Admin",
              monto_cambio,
              f"ADMIN_ADJUST_{datetime.now().strftime('%Y%m%d%H%M%S')}",
              fecha_str,
          ),
      )
      c.execute(
          "INSERT INTO admin_balance_audit (admin_user, target_user,"
          " monto_anterior, monto_nuevo, razon, fecha) VALUES (%s, %s, %s, %s,"
          " %s, %s)",
          (
              "Admin",
              username,
              monto_anterior,
              monto_nuevo,
              razon,
              fecha_str,
          ),
      )
    else:
      c.execute(
          "UPDATE usuarios SET saldo_disponible = ? WHERE username = ?",
          (monto_nuevo, username),
      )
      fecha_str = datetime.now().strftime("%Y-%m-%d %H:%M")
      c.execute(
          "INSERT INTO transacciones (username, tipo, monto, txid, fecha)"
          " VALUES (?, ?, ?, ?, ?, ?)",
          (
              username,
              "Ajuste Admin",
              monto_cambio,
              f"ADMIN_ADJUST_{datetime.now().strftime('%Y%m%d%H%M%S')}",
              fecha_str,
          ),
      )
      c.execute(
          "INSERT INTO admin_balance_audit (admin_user, target_user,"
          " monto_anterior, monto_nuevo, razon, fecha) VALUES (?, ?, ?, ?, ?, ?)",
          (
              "Admin",
              username,
              monto_anterior,
              monto_nuevo,
              razon,
              fecha_str,
          ),
      )

    conn.commit()
    registrar_log_admin(
        "AJUSTE_BALANCE",
        f"Ajuste a {username}: Cambio de {monto_cambio}. Razón: {razon}",
    )
    registrar_audit_log(
        "Admin",
        "AJUSTE_BALANCE",
        username,
        {
            "monto_anterior": monto_anterior,
            "monto_nuevo": monto_nuevo,
            "razon": razon,
        },
    )
    conn.close()
    return jsonify({
        "success": True,
        "saldo_disponible": monto_nuevo,
        "mensaje": (
            f"Balance ajustado correctamente. Nuevo saldo: {monto_nuevo}"
        ),
    })
  except Exception as e:
    conn.rollback()
    conn.close()
    return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/admin/usuario/<username>/detalle", methods=["GET"])
def admin_obtener_usuario_detalle(username):
  if not session.get("is_admin"):
    return jsonify({"success": False, "error": "No autorizado"}), 401

  conn = obtener_conexion()
  c = conn.cursor()
  try:
    if DATABASE_URL:
      c.execute(
          "SELECT username, saldo_disponible, is_frozen FROM usuarios WHERE"
          " username = %s",
          (username,),
      )
    else:
      c.execute(
          "SELECT username, saldo_disponible, is_frozen FROM usuarios WHERE"
          " username = ?",
          (username,),
      )
    user_row = c.fetchone()

    if not user_row:
      conn.close()
      return jsonify({"success": False, "error": "Usuario no encontrado"}), 404

    if DATABASE_URL:
      c.execute(
          "SELECT * FROM transacciones WHERE username = %s ORDER BY id DESC",
          (username,),
      )
    else:
      c.execute(
          "SELECT * FROM transacciones WHERE username = ? ORDER BY id DESC",
          (username,),
      )
    transacciones = [dict(r) for r in c.fetchall()]

    if DATABASE_URL:
      c.execute(
          "SELECT * FROM historial_apuestas WHERE username = %s ORDER BY id"
          " DESC",
          (username,),
      )
    else:
      c.execute(
          "SELECT * FROM historial_apuestas WHERE username = ? ORDER BY id"
          " DESC",
          (username,),
      )
    historial_apuestas = [dict(r) for r in c.fetchall()]

    conn.close()
    return jsonify({
        "success": True,
        "usuario": {
            "username": user_row["username"],
            "saldo_disponible": user_row["saldo_disponible"],
            "is_frozen": bool(user_row["is_frozen"]),
        },
        "transacciones": transacciones,
        "historial_apuestas": historial_apuestas,
    })
  except Exception as e:
    conn.close()
    return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/admin/anuncios", methods=["GET", "POST"])
def admin_anuncios():
  conn = obtener_conexion()
  c = conn.cursor()

  if request.method == "POST":
    if not session.get("is_admin"):
      conn.close()
      return jsonify({"success": False, "error": "No autorizado"}), 401

    data = request.json or {}
    titulo = data.get("titulo")
    contenido = data.get("contenido")
    tipo = data.get("tipo", "info")
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M")

    if not titulo or not contenido:
      conn.close()
      return (
          jsonify({
              "success": False,
              "error": "Título y contenido son obligatorios",
          }),
          400,
      )

    try:
      if DATABASE_URL:
        c.execute(
            "INSERT INTO anuncios_globales (titulo, contenido, tipo, activo,"
            " fecha) VALUES (%s, %s, %s, TRUE, %s)",
            (titulo, contenido, tipo, fecha),
        )
      else:
        c.execute(
            "INSERT INTO anuncios_globales (titulo, contenido, tipo, activo,"
            " fecha) VALUES (?, ?, ?, 1, ?)",
            (titulo, contenido, tipo, fecha),
        )
      conn.commit()
      registrar_log_admin(
          "CREAR_ANUNCIO", f"Publicado anuncio global: {titulo}"
      )
      conn.close()
      return jsonify({"success": True, "mensaje": "Anuncio publicado con éxito"})
    except Exception as e:
      conn.rollback()
      conn.close()
      return jsonify({"success": False, "error": str(e)}), 500

  try:
    c.execute("SELECT * FROM anuncios_globales ORDER BY id DESC LIMIT 10")
    anuncios = [dict(r) for r in c.fetchall()]
    conn.close()
    return jsonify({"success": True, "anuncios": anuncios})
  except Exception as e:
    conn.close()
    return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/admin/metricas-temporales", methods=["GET"])
def admin_metricas_temporales():
  if not session.get("is_admin"):
    return jsonify({"success": False, "error": "No autorizado"}), 401

  conn = obtener_conexion()
  c = conn.cursor()
  try:
    c.execute("SELECT COUNT(*) as total FROM usuarios")
    total_usuarios = c.fetchone()["total"]

    c.execute(
        "SELECT SUM(saldo_disponible) as circulante_total FROM usuarios"
    )
    res_circulante = c.fetchone()
    circulante_total = res_circulante["circulante_total"] or 0.0

    c.execute("SELECT SUM(precio * cantidad) as volumen_clob FROM ordenes_clob")
    res_vol = c.fetchone()
    volumen_clob = res_vol["volumen_clob"] or 0.0

    conn.close()
    return jsonify({
        "success": True,
        "metricas": {
            "total_usuarios": total_usuarios,
            "circulante_total": circulante_total,
            "volumen_clob": volumen_clob,
        },
    })
  except Exception as e:
    conn.close()
    return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/posiciones-activas/<username>", methods=["GET"])
def obtener_posiciones_activas(username):
  conn = obtener_conexion()
  c = conn.cursor()
  try:
    if DATABASE_URL:
      c.execute(
          "SELECT * FROM historial_apuestas WHERE username = %s AND estado ="
          " 'Activo' ORDER BY id DESC",
          (username,),
      )
    else:
      c.execute(
          "SELECT * FROM historial_apuestas WHERE username = ? AND estado ="
          " 'Activo' ORDER BY id DESC",
          (username,),
      )

    posiciones = [dict(row) for row in c.fetchall()]
    conn.close()
    return jsonify({"success": True, "posiciones_activas": posiciones})
  except Exception as e:
    conn.close()
    return jsonify({"success": False, "error": str(e)}), 500


if __name__ == "__main__":
  app.run(
      host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=True
  )
