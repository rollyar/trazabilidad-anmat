#!/usr/bin/env python3
"""Test de conexión al WebService ANMAT (TrazaMed) sin usar Firebird.

Usa las credenciales de prueba del .env:
  - App  : ANMAT_USUARIO / ANMAT_PASSWORD  (pruebasws / Clave1234)
  - WSSE : testwservice / testwservicepsw  (fijos por especificación ANMAT)
"""

import sys
import socket
from unittest.mock import MagicMock

# Workaround Python 3.12+: 'imp' fue removido pero pyafipws aún lo importa
sys.modules['imp'] = MagicMock()

import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import ANMAT_WSDL, ANMAT_USUARIO, ANMAT_PASSWORD

try:
    from pyafipws.trazamed import TrazaMed
except ImportError as e:
    print(f"ERROR: No se pudo importar TrazaMed de pyafipws: {e}")
    sys.exit(1)

TIMEOUT = 45
WSS_USER = "testwservice"
WSS_PASS = "testwservicepsw"

print("=" * 60)
print("TEST CONEXION ANMAT (TrazaMed)")
print("=" * 60)
print(f"WSDL     : {ANMAT_WSDL}")
print(f"App user : {ANMAT_USUARIO}")
print()

ws = TrazaMed()
ws.Username = WSS_USER
ws.Password = WSS_PASS
ws.LanzarExcepciones = False

# 1) Conectar al WSDL
print("1) Conectando al WebService...")
socket.setdefaulttimeout(TIMEOUT)
ok = ws.Conectar(cache='', wsdl=ANMAT_WSDL, timeout=TIMEOUT)

if not ok:
    print(f"   FALLO: {ws.Excepcion or ws.Errores}")
    sys.exit(1)

print("   OK - conexion establecida\n")

# 2) GetTransaccionesNoConfirmadas (consulta de solo lectura)
print("2) GetTransaccionesNoConfirmadas (lectura de transacciones pendientes)...")
ok = ws.GetTransaccionesNoConfirmadas(
    usuario=ANMAT_USUARIO,
    password=ANMAT_PASSWORD,
)

if not ok:
    print(f"   Error WS: {ws.Errores or ws.Excepcion}")
else:
    transacciones = []
    while ws.LeerTransaccion():
        transacciones.append({
            'id'     : ws.GetParametro("_id_transaccion"),
            'gtin'   : ws.GetParametro("_gtin"),
            'lote'   : ws.GetParametro("_lote"),
            'serie'  : ws.GetParametro("_numero_serial"),
            'origen' : ws.GetParametro("_gln_origen"),
            'destino': ws.GetParametro("_gln_destino"),
        })
    print(f"   Transacciones no confirmadas: {len(transacciones)}")
    for t in transacciones[:5]:
        print(f"   -> id={t['id']} | gtin={t['gtin']} | lote={t['lote']} | origen={t['origen']} -> destino={t['destino']}")
    if len(transacciones) > 5:
        print(f"   ... y {len(transacciones) - 5} más")

print()

# 3) GetTransaccionesWS: búsqueda por un GTIN de prueba (ficticio)
print("3) GetTransaccionesWS (busqueda por GTIN de prueba)...")
ok = ws.GetTransaccionesWS(
    usuario=ANMAT_USUARIO,
    password=ANMAT_PASSWORD,
    id_medicamento='07790001999990',   # GTIN de ejemplo para entorno de pruebas
)

if not ok:
    raw = ws.Errores or ws.Excepcion
    print(f"   Respuesta WS: {raw}")
else:
    resultados = []
    while ws.LeerTransaccion():
        resultados.append({
            'id'    : ws.GetParametro("_id_transaccion"),
            'gtin'  : ws.GetParametro("_gtin"),
            'estado': ws.GetParametro("_id_estado"),
        })
    print(f"   Resultados: {len(resultados)}")
    for r in resultados[:3]:
        print(f"   -> id={r['id']} | gtin={r['gtin']} | estado={r['estado']}")

print()
print("Test finalizado.")
