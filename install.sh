#!/bin/bash
#
# Script de instalación/deploy para Trazabilidad ANMAT
#
# Uso:
#   bash install.sh                    # Instalación completa
#   bash install.sh --quick            # Solo dependencias (skip infra)
#   bash install.sh --venv-only        # Solo crear/actualizar venv
#
# Requisitos:
#   - Python 3.11+
#   - Firebird client libraries
#   - systemd (Linux) o launchd (macOS)
#   - Acceso a la base de datos Firebird
#   - Conexión a Internet (para WSDL)
#

set -e

# --- Configuración ---
INSTALL_DIR="${INSTALL_DIR:-/opt/trazabilidad_anmat}"
APP_USER="${APP_USER:-trazabilidad}"
APP_GROUP="${APP_GROUP:-trazabilidad}"
PORT="${PORT:-5001}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info()  { echo -e "${GREEN}[✓]${NC} $1"; }
warn()  { echo -e "${YELLOW}[!]${NC} $1"; }
error() { echo -e "${RED}[✗]${NC} $1"; }

# --- Checks ---
check_python() {
    if command -v python3 &>/dev/null; then
        PY=python3
    elif command -v python &>/dev/null; then
        PY=python
    else
        error "Python no encontrado. Instalar Python 3.11+"
        exit 1
    fi
    ver=$($PY --version 2>&1 | grep -oP '\d+\.\d+')
    info "Python $ver encontrado"
}

check_firebird() {
    if ldconfig -p 2>/dev/null | grep -q libfbclient; then
        info "Firebird client libraries OK"
    elif [ -f /usr/lib/x86_64-linux-gnu/libfbclient.so ]; then
        info "Firebird client libraries OK"
    elif [ -f /usr/local/lib/libfbclient.dylib ]; then
        info "Firebird client libraries OK"
    else
        warn "Firebird client libs no detectadas (puede no ser necesario si usás firebird-driver puro)"
    fi
}

# --- Instalación ---
setup_venv() {
    info "Creando/actualizando virtualenv..."
    if [ ! -d "venv" ]; then
        $PY -m venv venv
    fi
    source venv/bin/activate
    pip install --upgrade pip setuptools wheel

    info "Instalando dependencias..."
    pip install -r requirements.txt

    # Parchar pysimplesoap para compatibilidad Python 3.12
    _patch_pysimplesoap

    info "Dependencias instaladas"
}

_patch_pysimplesoap() {
    SITE_PKG=$(python -c "import site; print(site.getsitepackages()[0])" 2>/dev/null || true)
    [ -z "$SITE_PKG" ] && SITE_PKG=$(python -c "import sysconfig; print(sysconfig.get_paths()['purelib'])")

    HELPERS="$SITE_PKG/pysimplesoap/helpers.py"
    CLIENT="$SITE_PKG/pysimplesoap/client.py"
    SIMPLEXML="$SITE_PKG/pysimplesoap/simplexml.py"
    TRANSPORT="$SITE_PKG/pysimplesoap/transport.py"

    if [ -f "$HELPERS" ]; then
        sed -i 's/hashlib.md5(url)/hashlib.md5(url.encode("utf8"))/g' "$HELPERS" 2>/dev/null || true
        info "helpers.py parchado"
    fi
    if [ -f "$CLIENT" ]; then
        sed -i 's/basestring/str/g' "$CLIENT" 2>/dev/null || true
        info "client.py parchado"
    fi
    if [ -f "$SIMPLEXML" ]; then
        sed -i 's/self.__document = xml.dom.minidom.parseString(text)/if isinstance(text, bytes):\n                    text = text.decode("utf-8")\n                self.__document = xml.dom.minidom.parseString(text)/g' "$SIMPLEXML" 2>/dev/null || true
        info "simplexml.py parchado"
    fi
    if [ -f "$TRANSPORT" ]; then
        sed -i 's/inspect.getargspec/inspect.getfullargspec/g' "$TRANSPORT" 2>/dev/null || true
        info "transport.py parchado"
    fi
}

setup_config() {
    if [ ! -f ".env" ]; then
        if [ -f ".env.example" ]; then
            cp .env.example .env
            warn "Archivo .env creado desde .env.example — EDITAR CON CREDENCIALES REALES"
        else
            error "No existe .env ni .env.example"
            exit 1
        fi
    else
        info ".env ya existe"
    fi
}

setup_systemd() {
    if [ ! -d /etc/systemd/system ]; then
        warn "systemd no disponible — omitiendo servicio"
        return
    fi

    SERVICE_SRC="$SCRIPT_DIR/trazabilidad_anmat.service"
    SERVICE_DST="/etc/systemd/system/trazabilidad_anmat.service"

    if [ -f "$SERVICE_DST" ]; then
        info "Servicio systemd ya instalado"
        return
    fi

    if [ "$INSTALL_DIR" != "$SCRIPT_DIR" ]; then
        # Necesitamos copiar archivos al directorio de instalación
        info "Copiando archivos a $INSTALL_DIR..."
        mkdir -p "$INSTALL_DIR"
        cp -r "$SCRIPT_DIR"/* "$INSTALL_DIR/"
        chown -R "$APP_USER:$APP_GROUP" "$INSTALL_DIR"
    fi

    if [ -f "$SERVICE_SRC" ]; then
        cp "$SERVICE_SRC" "$SERVICE_DST"
        # Ajustar directorio de trabajo en el service file si es necesario
        sed -i "s|WorkingDirectory=.*|WorkingDirectory=$INSTALL_DIR|" "$SERVICE_DST"
        sed -i "s|Environment=.*|Environment=PATH=$INSTALL_DIR/venv/bin|" "$SERVICE_DST"
        sed -i "s|ExecStart=.*|ExecStart=$INSTALL_DIR/venv/bin/python app.py|" "$SERVICE_DST"
        systemctl daemon-reload
        systemctl enable trazabilidad_anmat
        info "Servicio systemd instalado y habilitado"
    fi
}

create_user() {
    if id "$APP_USER" &>/dev/null 2>&1; then
        info "Usuario $APP_USER ya existe"
    else
        useradd -r -s /bin/false -m -d "$INSTALL_DIR" "$APP_USER" 2>/dev/null || \
        adduser --system --group --no-create-home "$APP_USER" 2>/dev/null || \
        warn "No se pudo crear usuario $APP_USER (crear manualmente)"
        info "Usuario $APP_USER creado"
    fi
}

test_connection() {
    info "Probando conexión Firebird..."
    source venv/bin/activate
    python test_firebird.py 2>&1 | head -5 || warn "test_firebird falló — verificar .env"

    info "Probando conexión ANMAT..."
    python test_anmat.py 2>&1 | head -10 || warn "test_anmat falló — verificar .env"
}

# --- Main ---
cd "$SCRIPT_DIR"

case "${1:-}" in
    --venv-only)
        check_python
        setup_venv
        exit 0
        ;;
    --quick)
        check_python
        setup_venv
        test_connection
        info "Instalación rápida completa. Ejecutar: source venv/bin/activate && python app.py"
        exit 0
        ;;
esac

echo ""
echo "==================================="
echo " Trazabilidad ANMAT - Deploy"
echo "==================================="
echo ""

check_python
check_firebird
setup_venv
setup_config
create_user
setup_systemd

echo ""
echo "==================================="
echo " Deploy completado"
echo "==================================="
echo ""
echo "Comandos útiles:"
echo "  Iniciar:   systemctl start trazabilidad_anmat"
echo "  Detener:   systemctl stop trazabilidad_anmat"
echo "  Estado:    systemctl status trazabilidad_anmat"
echo "  Logs:      journalctl -u trazabilidad_anmat -f"
echo "  Test:      cd $SCRIPT_DIR && source venv/bin/activate"
echo "             python test_firebird.py"
echo "             python test_anmat.py"
echo ""
echo "  Manual:    source venv/bin/activate && python app.py"
echo "  URL:       http://localhost:$PORT"
echo ""

if [ ! -f ".env" ] || grep -q "ANMAT_USUARIO=$" .env 2>/dev/null; then
    warn "⚠  Recordá completar .env con credenciales reales antes de iniciar"
    echo "   Editar: nano $SCRIPT_DIR/.env"
fi
