# PFO 3 — Sistema Distribuido Cliente-Servidor

Programacion Sobre Redes — IFTS N° 29  
Autor: Jorge Kalas  
Docente: German Rios  
Primer Semestre 2026

---

## Descripcion

Sistema distribuido implementado en Python que demuestra una arquitectura cliente-servidor con pool de workers y cola de mensajes. El servidor acepta conexiones TCP, encola las tareas recibidas y las distribuye a cuatro workers que las procesan en paralelo. Cada resultado queda persistido en una base de datos SQLite.

## Arquitectura

```
Clientes  ->  Balanceador (Nginx/HAProxy)  ->  Servidor (dispatcher)
                                                      |
                                               Cola de mensajes
                                            /    |    |    \
                                        W-1  W-2  W-3  W-4  (workers)
                                                      |
                                              PostgreSQL / S3
```

## Requisitos

- Python 3.8 o superior
- No requiere dependencias externas (usa solo biblioteca estandar)

## Ejecucion

**1. Iniciar el servidor** (primera terminal):

```
python servidor.py
```

El servidor inicializa la base de datos, levanta 4 workers y escucha en `127.0.0.1:9000`.

**2. Iniciar el cliente** (segunda terminal):

```
python cliente.py
```

## Comandos disponibles desde el cliente

| Comando              | Resultado                              |
|----------------------|----------------------------------------|
| eco:<texto>          | Devuelve el texto tal cual             |
| mayusculas:<texto>   | Convierte a MAYUSCULAS                 |
| minusculas:<texto>   | Convierte a minusculas                 |
| invertir:<texto>     | Invierte el texto                      |
| contar:<texto>       | Cuenta caracteres y palabras           |
| <cualquier texto>    | Tarea generica procesada por un worker |
| salir                | Cierra el cliente                      |

## Archivos

```
RedesPFO3/
├── servidor.py          # Servidor con pool de workers y cola de mensajes
├── cliente.py           # Cliente interactivo de consola
├── tareas.db            # Base de datos SQLite (se genera al ejecutar)
├── diagrama_pfo3.xml    # Diagrama de arquitectura (importable en draw.io)
└── README.md
```

## Base de datos

La tabla `tareas` registra cada tarea procesada:

| Campo      | Tipo    | Descripcion                        |
|------------|---------|------------------------------------|
| id         | INTEGER | Clave primaria autoincremental     |
| cliente_ip | TEXT    | IP del cliente que envio la tarea  |
| tarea      | TEXT    | Texto de la tarea recibida         |
| resultado  | TEXT    | Resultado devuelto por el worker   |
| estado     | TEXT    | completada / error                 |
| fecha      | TEXT    | Timestamp ISO de procesamiento     |

Para consultar los registros:

```
python -c "import sqlite3; conn=sqlite3.connect('tareas.db'); [print(r) for r in conn.execute('SELECT * FROM tareas')]; conn.close()"
```
