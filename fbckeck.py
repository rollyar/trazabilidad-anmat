"""
Diagnóstico Firebird 5 - ASPURC / SIIA
Requiere: pip install firebird-driver --break-system-packages

Uso:
    python fb_diagnostico.py

Ajustar DSN/credenciales abajo.
"""

import subprocess
from firebird.driver import connect, driver_config

# --- Configuración ---
DB_PATH = "siia"   # ajustar
HOST = "192.168.1.2"  # "localhost"
USER = "SYSDBA"
PASSWORD = "2019@betulo"          # ajustar
GSTAT_BIN = "gstat"             # o ruta completa, ej: /opt/firebird/bin/gstat


def run_gstat():
    print("=" * 60)
    print("GSTAT -h (header / OIT-OAT-Next)")
    print("=" * 60)
    try:
        out = subprocess.run(
            [GSTAT_BIN, "-h", "-user", USER, "-password", PASSWORD, DB_PATH],
            capture_output=True, text=True, timeout=30
        )
        print(out.stdout or out.stderr)
    except Exception as e:
        print(f"No se pudo correr gstat: {e}")
        print("Corré manualmente: gstat -h -user SYSDBA -password <pass> <ruta.fdb>")


def run_mon_queries(con):
    cur = con.cursor()

    print("\n" + "=" * 60)
    print("Conexiones activas y su actividad")
    print("=" * 60)
    cur.execute("""
        SELECT
            a.MON$ATTACHMENT_ID,
            a.MON$USER,
            a.MON$REMOTE_ADDRESS,
            a.MON$STATE,
            a.MON$TIMESTAMP
        FROM MON$ATTACHMENTS a
        WHERE a.MON$ATTACHMENT_ID <> CURRENT_CONNECTION
        ORDER BY a.MON$TIMESTAMP
    """)
    for row in cur.fetchall():
        print(row)

    print("\n" + "=" * 60)
    print("Statements en ejecución (posibles queries colgados)")
    print("=" * 60)
    cur.execute("""
        SELECT
            s.MON$STATEMENT_ID,
            s.MON$ATTACHMENT_ID,
            s.MON$STATE,
            s.MON$TIMESTAMP,
            s.MON$SQL_TEXT
        FROM MON$STATEMENTS s
        WHERE s.MON$STATE = 1
    """)
    rows = cur.fetchall()
    if not rows:
        print("Ninguno activo en este momento.")
    for row in rows:
        print(row)

    print("\n" + "=" * 60)
    print("IO / Records stats por conexión (top consumo)")
    print("=" * 60)
    cur.execute("""
        SELECT
            io.MON$ATTACHMENT_ID,
            io.MON$PAGE_READS,
            io.MON$PAGE_WRITES,
            io.MON$PAGE_FETCHES,
            io.MON$PAGE_MARKS
        FROM MON$IO_STATS io
        ORDER BY io.MON$PAGE_READS DESC
    """)
    for row in cur.fetchall():
        print(row)

    print("\n" + "=" * 60)
    print("Record stats (lecturas secuenciales vs indexadas)")
    print("=" * 60)
    cur.execute("""
        SELECT
            r.MON$STAT_ID,
            r.MON$RECORD_SEQ_READS,
            r.MON$RECORD_IDX_READS,
            r.MON$RECORD_UPDATES,
            r.MON$RECORD_DELETES,
            r.MON$RECORD_BACKOUTS
        FROM MON$RECORD_STATS r
    """)
    for row in cur.fetchall():
        print(row)

    print("\n" + "=" * 60)
    print("Transacciones abiertas hace más tiempo (posible bloqueo GC)")
    print("=" * 60)
    cur.execute("""
        SELECT
            t.MON$TRANSACTION_ID,
            t.MON$ATTACHMENT_ID,
            t.MON$STATE,
            t.MON$TIMESTAMP,
            t.MON$ISOLATION_MODE
        FROM MON$TRANSACTIONS t
        ORDER BY t.MON$TIMESTAMP
    """)
    for row in cur.fetchall():
        print(row)

    cur.close()


def main():
    run_gstat()

    con = connect(f"{HOST}:{DB_PATH}", user=USER, password=PASSWORD)
    try:
        run_mon_queries(con)
    finally:
        con.close()


if __name__ == "__main__":
    main()
