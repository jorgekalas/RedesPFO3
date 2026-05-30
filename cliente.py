"""
cliente.py — PFO 3: Sistema Distribuido Cliente-Servidor
IFTS N° 29 — Programación Sobre Redes — Jorge Kalas

El cliente se conecta al servidor, envía tareas y recibe
el resultado procesado por uno de los workers del pool.
"""

import socket
import json

# ─── Configuración de conexión ────────────────────────────────────────────────
HOST = "127.0.0.1"
PORT = 9000


def enviar_tarea(tarea):
    """
    Abre una conexión TCP con el servidor, envía la tarea
    y retorna la respuesta del worker que la procesó.
    Cada tarea usa una conexión independiente (stateless).
    """
    try:
        # Crear socket TCP/IP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((HOST, PORT))
            s.sendall((tarea + "\n").encode("utf-8"))

            # Esperar respuesta del worker
            respuesta_raw = b""
            while True:
                fragmento = s.recv(4096)
                if not fragmento:
                    break
                respuesta_raw += fragmento
                if b"\n" in respuesta_raw:
                    break

        respuesta = json.loads(respuesta_raw.decode("utf-8").strip())
        return respuesta

    except ConnectionRefusedError:
        print(f"[Cliente] Error: No se pudo conectar a {HOST}:{PORT}.")
        print("[Cliente] Asegurate de que el servidor esté corriendo.")
        return None
    except Exception as e:
        print(f"[Cliente] Error inesperado: {e}")
        return None


def mostrar_ayuda():
    """Muestra los comandos disponibles."""
    print("\n  Comandos disponibles:")
    print("  ─────────────────────────────────────────────")
    print("  eco:<texto>         → Devuelve el texto tal cual")
    print("  mayusculas:<texto>  → Convierte a MAYÚSCULAS")
    print("  minusculas:<texto>  → Convierte a minúsculas")
    print("  invertir:<texto>    → Invierte el texto")
    print("  contar:<texto>      → Cuenta caracteres y palabras")
    print("  <cualquier texto>   → Tarea genérica procesada por un worker")
    print("  ayuda               → Muestra este menú")
    print("  salir               → Cierra el cliente")
    print("  ─────────────────────────────────────────────\n")


def ejecutar_cliente():
    """
    Bucle principal del cliente.
    El usuario puede enviar múltiples tareas hasta escribir 'salir'.
    """
    print("=" * 55)
    print("  Cliente — Sistema Distribuido de Tareas")
    print(f"  Conectando a {HOST}:{PORT}")
    print("=" * 55)
    mostrar_ayuda()

    while True:
        try:
            tarea = input("Tarea > ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\n[Cliente] Conexión cerrada por el usuario.")
            break

        if not tarea:
            continue

        if tarea.lower() == "salir":
            print("[Cliente] Cerrando cliente. ¡Hasta luego!")
            break

        if tarea.lower() == "ayuda":
            mostrar_ayuda()
            continue

        print(f"[Cliente] Enviando tarea al servidor...")
        respuesta = enviar_tarea(tarea)

        if respuesta:
            print("\n  ── Respuesta del servidor ──────────────────")
            print(f"  Worker asignado : Worker-{respuesta.get('worker')}")
            print(f"  Tarea enviada   : {respuesta.get('tarea')}")
            print(f"  Resultado       : {respuesta.get('resultado')}")
            print(f"  Estado          : {respuesta.get('estado')}")
            print(f"  Timestamp       : {respuesta.get('timestamp')}")
            print("  ────────────────────────────────────────────\n")


if __name__ == "__main__":
    ejecutar_cliente()
