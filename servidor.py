"""
servidor.py — PFO 3: Sistema Distribuido Cliente-Servidor
IFTS N° 29 — Programación Sobre Redes — Jorge Kalas

Arquitectura simulada:
  - Servidor principal: acepta conexiones de clientes via socket TCP
  - Pool de Workers: hilos que procesan tareas en paralelo
  - Cola de tareas: queue.Queue (equivalente funcional a RabbitMQ)
  - Almacenamiento: SQLite (equivalente funcional a PostgreSQL)
"""

import socket
import threading
import queue
import sqlite3
import json
import time
import os
from datetime import datetime

# ─── Configuración del servidor ───────────────────────────────────────────────
HOST = "127.0.0.1"
PORT = 9000
NUM_WORKERS = 4          # Cantidad de hilos en el pool de workers
DB_NAME = "tareas.db"    # Base de datos SQLite (equivalente a PostgreSQL)

# Cola de mensajes compartida entre el servidor y los workers
# Equivalente funcional a RabbitMQ en esta implementación académica
cola_de_tareas = queue.Queue()

# Lock para operaciones seguras sobre SQLite con múltiples hilos
db_lock = threading.Lock()


# ─── Base de datos ────────────────────────────────────────────────────────────

def inicializar_base_de_datos():
    """
    Crea la tabla 'tareas' si no existe.
    Representa el rol de PostgreSQL en la arquitectura objetivo de producción.
    """
    with db_lock:
        conexion = sqlite3.connect(DB_NAME)
        cursor = conexion.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tareas (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                cliente_ip  TEXT    NOT NULL,
                tarea       TEXT    NOT NULL,
                resultado   TEXT,
                estado      TEXT    DEFAULT 'pendiente',
                fecha       TEXT    NOT NULL
            )
        """)
        conexion.commit()
        conexion.close()
    print(f"[DB] Base de datos '{DB_NAME}' inicializada correctamente.")


def guardar_tarea(cliente_ip, tarea, resultado, estado):
    """Persiste una tarea y su resultado en SQLite."""
    with db_lock:
        conexion = sqlite3.connect(DB_NAME)
        cursor = conexion.cursor()
        cursor.execute(
            "INSERT INTO tareas (cliente_ip, tarea, resultado, estado, fecha) VALUES (?, ?, ?, ?, ?)",
            (cliente_ip, tarea, resultado, estado, datetime.now().isoformat())
        )
        conexion.commit()
        conexion.close()


# ─── Procesamiento de tareas ──────────────────────────────────────────────────

def procesar_tarea(tarea):
    """
    Simula el procesamiento de una tarea recibida del cliente.
    En producción, aquí se ejecutaría lógica de negocio real.
    """
    time.sleep(0.5)  # Simula tiempo de procesamiento
    tarea_lower = tarea.lower()

    if tarea_lower.startswith("eco:"):
        return f"ECO >> {tarea[4:].strip()}"
    elif tarea_lower.startswith("mayusculas:"):
        return tarea[11:].strip().upper()
    elif tarea_lower.startswith("minusculas:"):
        return tarea[11:].strip().lower()
    elif tarea_lower.startswith("invertir:"):
        return tarea[9:].strip()[::-1]
    elif tarea_lower.startswith("contar:"):
        texto = tarea[7:].strip()
        return f"Caracteres: {len(texto)} | Palabras: {len(texto.split())}"
    else:
        return f"Tarea procesada: '{tarea}' — [Timestamp: {datetime.now().strftime('%H:%M:%S')}]"


# ─── Workers ──────────────────────────────────────────────────────────────────

def worker(worker_id):
    """
    Hilo worker que consume tareas de la cola y las procesa.
    Cada worker opera de forma independiente (pool de hilos).
    Equivalente a un servidor worker en la arquitectura distribuida real.
    """
    print(f"[Worker-{worker_id}] Iniciado y esperando tareas...")
    while True:
        try:
            # Espera bloqueante hasta que haya una tarea disponible
            item = cola_de_tareas.get(timeout=1)
            if item is None:
                # Señal de apagado
                print(f"[Worker-{worker_id}] Recibió señal de apagado.")
                break

            conn_cliente, addr, tarea = item
            print(f"[Worker-{worker_id}] Procesando tarea de {addr}: '{tarea}'")

            try:
                resultado = procesar_tarea(tarea)
                estado = "completada"
            except Exception as e:
                resultado = f"Error al procesar: {str(e)}"
                estado = "error"

            # Persistir en base de datos
            guardar_tarea(addr[0], tarea, resultado, estado)

            # Enviar resultado al cliente
            respuesta = json.dumps({
                "worker": worker_id,
                "tarea": tarea,
                "resultado": resultado,
                "estado": estado,
                "timestamp": datetime.now().isoformat()
            })
            try:
                conn_cliente.sendall((respuesta + "\n").encode("utf-8"))
            except Exception:
                pass  # El cliente puede haberse desconectado
            finally:
                conn_cliente.close()

            cola_de_tareas.task_done()

        except queue.Empty:
            continue
        except Exception as e:
            print(f"[Worker-{worker_id}] Error inesperado: {e}")


# ─── Servidor principal ───────────────────────────────────────────────────────

def aceptar_conexiones(servidor_socket):
    """
    Acepta conexiones entrantes de clientes y encola sus tareas.
    Actúa como balanceador/dispatcher hacia la cola de mensajes.
    """
    print(f"[Servidor] Escuchando en {HOST}:{PORT} con {NUM_WORKERS} workers activos.")
    while True:
        try:
            conn, addr = servidor_socket.accept()
            print(f"[Servidor] Nueva conexión desde {addr}")
            datos = conn.recv(4096).decode("utf-8").strip()
            if datos:
                # Encolar la tarea para que un worker la procese
                cola_de_tareas.put((conn, addr, datos))
                print(f"[Servidor] Tarea encolada desde {addr}: '{datos}'")
            else:
                conn.close()
        except OSError:
            break


def iniciar_servidor():
    """
    Punto de entrada del servidor.
    1. Inicializa la base de datos.
    2. Levanta el pool de workers.
    3. Abre el socket y comienza a aceptar clientes.
    """
    inicializar_base_de_datos()

    # Iniciar pool de workers (hilos independientes)
    workers = []
    for i in range(1, NUM_WORKERS + 1):
        t = threading.Thread(target=worker, args=(i,), daemon=True)
        t.start()
        workers.append(t)
    print(f"[Servidor] Pool de {NUM_WORKERS} workers iniciado.")

    # Configuración del socket TCP/IP
    servidor_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        servidor_socket.bind((HOST, PORT))
        servidor_socket.listen(10)
        aceptar_conexiones(servidor_socket)
    except OSError as e:
        print(f"[Servidor] Error al iniciar: {e}")
    finally:
        # Enviar señales de apagado a los workers
        for _ in workers:
            cola_de_tareas.put(None)
        servidor_socket.close()
        print("[Servidor] Apagado correctamente.")


if __name__ == "__main__":
    iniciar_servidor()
