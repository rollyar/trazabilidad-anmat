#!/usr/bin/env python3
"""Test rápido de conexión a Firebird usando la configuración del .env"""

import sys
from config import (
    FIREBIRD_HOST, FIREBIRD_PORT, FIREBIRD_DB,
    FIREBIRD_USER, FIREBIRD_PASSWORD, FIREBIRD_ROLE,
    FIREBIRD_CONN_STRING,
)
from firebird.driver import connect as fb_connect

print(f"Host    : {FIREBIRD_HOST}:{FIREBIRD_PORT}")
print(f"Base    : {FIREBIRD_DB}")
print(f"Usuario : {FIREBIRD_USER}")
print(f"Rol     : {FIREBIRD_ROLE}")
print(f"Cadena  : {FIREBIRD_CONN_STRING}")
print()

try:
    conn = fb_connect(
        FIREBIRD_CONN_STRING,
        user=FIREBIRD_USER,
        password=FIREBIRD_PASSWORD,
        role=FIREBIRD_ROLE,
        charset="UTF8",
    )
    print("Conexion establecida OK")

    cur = conn.cursor()

    # 1) Ping básico
    cur.execute("SELECT 1 FROM RDB$DATABASE")
    print(f"Ping         : {cur.fetchone()[0]}")

    # 2) Versión del servidor
    cur.execute("SELECT RDB$GET_CONTEXT('SYSTEM', 'ENGINE_VERSION') FROM RDB$DATABASE")
    print(f"Versión FB   : {cur.fetchone()[0]}")

    # 3) Fecha/hora del servidor
    cur.execute("SELECT CURRENT_TIMESTAMP FROM RDB$DATABASE")
    print(f"Timestamp DB : {cur.fetchone()[0]}")

    # 4) Primeras 3 sucursales con GLN
    cur.execute(
        "SELECT FIRST 3 id_sucursal, sucursal, gln FROM sucursal WHERE gln IS NOT NULL ORDER BY id_sucursal"
    )
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    print(f"\nSucursales con GLN (primeras {len(rows)}):")
    print("  " + " | ".join(cols))
    for row in rows:
        print("  " + " | ".join(str(v) for v in row))

    cur.close()
    conn.close()
    print("\nConexion cerrada correctamente.")
    sys.exit(0)

except Exception as e:
    print(f"\nERROR: {e}", file=sys.stderr)
    sys.exit(1)
