#!/bin/bash
#
# Script de inicio para trazabilidad_anmat en Linux
# Debe ejecutarse como root o con sudo
#

# CONFIGURACIÓN
APP_DIR="/opt/trazabilidad_anmat"
APP_USER="trazabilidad"
PORT="${PORT:-5001}"
LOG_FILE="/var/log/trazabilidad_anmat.log"
PID_FILE="/var/run/trazabilidad_anmat.pid"

# Activar venv
source "$APP_DIR/venv/bin/activate"

# Iniciar la aplicación
cd "$APP_DIR"

case "$1" in
    start)
        echo "Iniciando trazabilidad_anmat..."
        nohup python app.py > "$LOG_FILE" 2>&1 &
        echo $! > "$PID_FILE"
        echo "trazabilidad_anmat iniciado (PID: $(cat $PID_FILE))"
        ;;
    stop)
        if [ -f "$PID_FILE" ]; then
            PID=$(cat "$PID_FILE")
            echo "Deteniendo trazabilidad_anmat (PID: $PID)..."
            kill $PID
            rm "$PID_FILE"
            echo "Detenido"
        else
            echo "No se encontró el archivo PID"
        fi
        ;;
    restart)
        $0 stop
        sleep 2
        $0 start
        ;;
    status)
        if [ -f "$PID_FILE" ]; then
            PID=$(cat "$PID_FILE")
            if ps -p $PID > /dev/null; then
                echo "trazabilidad_anmat está corriendo (PID: $PID)"
            else
                echo "El proceso no está corriendo pero existe el archivo PID"
            fi
        else
            echo "trazabilidad_anmat no está corriendo"
        fi
        ;;
    *)
        echo "Uso: $0 {start|stop|restart|status}"
        exit 1
        ;;
esac
