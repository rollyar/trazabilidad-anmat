import logging
import re
import traceback
import atexit
from logging.handlers import RotatingFileHandler
from datetime import datetime
from flask import Flask, jsonify, render_template, request
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from database import db
from anmat_reporter import reporter, AnmatReporter
from config import SCHEDULER_INTERVAL

app = Flask(__name__)

# --- Logging persistente con rotación ---
_fmt = '%(asctime)s %(levelname)s: %(message)s'
logging.basicConfig(level=logging.INFO, format=_fmt)
logger = logging.getLogger(__name__)
_file_handler = RotatingFileHandler('trazabilidad.log', maxBytes=5_000_000, backupCount=5)
_file_handler.setFormatter(logging.Formatter(_fmt))
logging.getLogger().addHandler(_file_handler)

# --- Rate limiting ---
limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=["300 per minute"],
    storage_uri="memory://",
)


# --- Helpers de validación ---
_FECHA_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')
_GLN_RE   = re.compile(r'^\d{13}$')


def _valid_fecha(s):
    if not s or not _FECHA_RE.match(s):
        return False
    try:
        datetime.strptime(s, '%Y-%m-%d')
        return True
    except ValueError:
        return False


def _valid_gln(s):
    return bool(s and _GLN_RE.match(str(s)))


def _err(msg, code=400):
    return jsonify({'error': msg}), code


def _internal_err(e, msg='Error interno del servidor'):
    logger.error(f"{msg}: {e}")
    return jsonify({'error': msg}), 500


@app.route('/api/health')
def health_check():
    try:
        fb_ok, fb_msg = db.test_connection()
        return jsonify({
            'status': 'ok' if fb_ok else 'degraded',
            'firebird': fb_msg,
        })
    except Exception as e:
        return _internal_err(e)


@app.after_request
def log_response(response):
    logger.debug(f"RESPONSE: {request.path} - Status: {response.status_code}")
    return response


def reporte_automatico():
    """Reporte automático - recorre todas las sucursales por ciclo."""
    logger.info("Iniciando reporte automático a ANMAT...")
    try:
        sucursales = db.get_sucursales()
        if not sucursales:
            logger.warning("No hay sucursales configuradas")
            return

        if not reporter.conectar():
            logger.error("No se pudo conectar a ANMAT para reporte automático")
            return

        for suc in sucursales:
            gln = suc.get('gln')
            if not gln:
                logger.warning(f"Sucursal sin GLN: {suc.get('sucursal')}")
                continue
            logger.info(f"Procesando GLN: {gln}")
            resultados = reporter.procesar_pendientes(gln)
            logger.info(
                f"GLN {gln}: {resultados.get('exitosos', 0)} exitosos, "
                f"{resultados.get('fallidos', 0)} fallidos"
            )

        logger.info("Reporte automático completado")

    except Exception as e:
        logger.error(f"Error en reporte automático: {e}")


executors = {'default': ThreadPoolExecutor(1)}
scheduler = BackgroundScheduler(executors=executors)
scheduler.add_job(func=reporte_automatico, trigger="interval", minutes=SCHEDULER_INTERVAL)
scheduler.start()
atexit.register(lambda: scheduler.shutdown(wait=False))


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/estadisticas')
def estadisticas():
    try:
        gln = request.args.get('gln') or None
        if gln and not _valid_gln(gln):
            return _err('GLN inválido: debe ser 13 dígitos')
        stats = db.get_dashboard_stats(gln)
        return jsonify(stats)
    except Exception as e:
        return _internal_err(e, 'Error al obtener estadísticas')


@app.route('/api/movimientos')
def movimientos():
    try:
        gln = request.args.get('gln') or None
        if gln and not _valid_gln(gln):
            return _err('GLN inválido: debe ser 13 dígitos')

        fecha_desde = request.args.get('fecha_desde') or None
        fecha_hasta = request.args.get('fecha_hasta') or None
        if fecha_desde and not _valid_fecha(fecha_desde):
            return _err('fecha_desde inválida: usar formato YYYY-MM-DD')
        if fecha_hasta and not _valid_fecha(fecha_hasta):
            return _err('fecha_hasta inválida: usar formato YYYY-MM-DD')

        try:
            limit = int(request.args.get('limit', 500))
            offset = int(request.args.get('offset', 0))
        except ValueError:
            return _err('limit y offset deben ser enteros')
        if limit < 1 or limit > 2000:
            return _err('limit debe estar entre 1 y 2000')
        if offset < 0:
            return _err('offset no puede ser negativo')

        filtro = request.args.get('filtro') or None

        data = db.get_todos_movimientos_mes(gln, limit, offset, fecha_desde, fecha_hasta, filtro)
        return jsonify(data)
    except Exception as e:
        return _internal_err(e, 'Error al obtener movimientos')


@app.route('/api/sucursales/lista')
def sucursales_lista():
    try:
        sucursales = db.get_sucursales()
        return jsonify(sucursales)
    except Exception as e:
        return _internal_err(e, 'Error al obtener sucursales')


@app.route('/api/anmat_no_confirmadas')
@limiter.limit("30 per minute")
def anmat_no_confirmadas():
    try:
        gln = request.args.get('gln') or None
        if gln and not _valid_gln(gln):
            return _err('GLN inválido: debe ser 13 dígitos')

        if not reporter.conectar():
            return _err('No se pudo conectar a ANMAT', 503)

        resultado = reporter.get_transacciones_no_confirmadas(gln)

        # 'sin_db=1' devuelve solo lo de ANMAT (rápido, sin tocar Firebird por tx).
        if request.args.get('sin_db') == '1':
            resultado['auto_registradas'] = 0
            return jsonify(resultado)

        auto_registradas = 0
        for tx in resultado.get('transacciones', []):
            gtin = tx.get('gtin', '').lstrip('0') or tx.get('gtin', '')
            lote = tx.get('lote', '')
            serial = tx.get('numero_serial', '')
            gln_destino = tx.get('gln_destino', '')

            if not gtin or (not lote and not serial):
                tx['auto_registrada'] = False
                continue

            matches = db.buscar_cmp_recibido_por_gtin(gtin, gln=gln_destino or gln)
            if len(matches) == 1:
                m = matches[0]
                ctm_id = m.get('ctm_id')
                vencimiento = tx.get('vencimiento') or None
                registrado = False
                if ctm_id:
                    db.actualizar_datos_trazabilidad(ctm_id, lote or None, vencimiento)
                    registrado = True
                    logger.info(f"Auto-actualizado cat_traza_mov {ctm_id} para GTIN {gtin} lote {lote}")
                else:
                    nuevo_id = db.crear_registro_trazabilidad(
                        m['cmp_recibido_item_id'],
                        gtin=gtin,
                        lote=lote or None,
                        numero_serial=serial or None,
                        caducidad=vencimiento,
                        ml_id=101,
                    )
                    registrado = nuevo_id is not None
                    if registrado:
                        logger.info(f"Auto-creado cat_traza_mov {nuevo_id} para GTIN {gtin} lote {lote}")
                tx['auto_registrada'] = registrado
                if registrado:
                    auto_registradas += 1
            else:
                tx['auto_registrada'] = False

        resultado['auto_registradas'] = auto_registradas
        return jsonify(resultado)
    except Exception as e:
        return _internal_err(e, 'Error al obtener transacciones no confirmadas')


@app.route('/api/reporte_manual', methods=['POST'])
@limiter.limit("30 per minute")
def reporte_manual():
    try:
        gln = request.args.get('gln')
        if not gln:
            return _err('GLN es requerido')
        if not _valid_gln(gln):
            return _err('GLN inválido: debe ser 13 dígitos')
        if reporter.conectar():
            resultados = reporter.procesar_pendientes(gln)
            return jsonify(resultados)
        else:
            return _err('No se pudo conectar a ANMAT', 503)
    except Exception as e:
        return _internal_err(e, 'Error en reporte manual')


@app.route('/api/reporte_errores', methods=['POST'])
@limiter.limit("30 per minute")
def reporte_errores():
    try:
        gln = request.args.get('gln')
        if not gln:
            return _err('GLN es requerido')
        if not _valid_gln(gln):
            return _err('GLN inválido: debe ser 13 dígitos')
        if reporter.conectar():
            resultados = reporter.procesar_corregir(gln)
            return jsonify(resultados)
        else:
            return _err('No se pudo conectar a ANMAT', 503)
    except Exception as e:
        return _internal_err(e, 'Error en corrección desde ANMAT')

@app.route('/api/sync_state', methods=['GET'])
@limiter.limit("60 per minute")
def sync_state():
    gln = request.args.get('gln')
    if not gln or not _valid_gln(gln):
        return jsonify({'completa': False, 'desde': None, 'error': None, 'hay_pendientes': False})
    try:
        state = AnmatReporter._cargar_sync_state()
        gln_state = state.get(gln, {})
        completa = gln_state.get('completa', False)
        desde = gln_state.get('desde') or gln_state.get('proximo_desde')
        hay_pendientes = len(db.get_movimientos_reportables(gln)) > 0
        return jsonify({
            'completa': completa,
            'desde': desde,
            'hay_pendientes': hay_pendientes,
            'error': None,
        })
    except Exception as e:
        return jsonify({'completa': False, 'desde': None, 'error': str(e), 'hay_pendientes': False})


@app.route('/api/probar_conexion')
def probar_conexion():
    try:
        ok = reporter.conectar()
        return jsonify({'conectado': ok})
    except Exception:
        return jsonify({'conectado': False, 'error': 'Error al conectar'})


@app.route('/api/reportar/<int:mov_id>', methods=['POST'])
@limiter.limit("120 per minute")
def reportar_movimiento(mov_id):
    try:
        gln = request.args.get('gln')
        if not gln:
            return _err('GLN es requerido')
        if not _valid_gln(gln):
            return _err('GLN inválido: debe ser 13 dígitos')

        if not reporter.conectar():
            return _err('No se pudo conectar a ANMAT', 503)

        movimiento = db.get_movimiento_by_id(mov_id)
        if not movimiento:
            return _err('Movimiento no encontrado', 404)

        resultado = reporter.reportar_movimiento(movimiento, gln=gln)

        if resultado.get('success'):
            logger.info(f"Movimiento {mov_id} reportado exitosamente")

            datos_actualizados = resultado.get('datos_actualizados')
            fecha_reporte_db = None
            if datos_actualizados:
                lote_nuevo = datos_actualizados.get('lote')
                vencimiento_nuevo = datos_actualizados.get('vencimiento')
                movimiento_actual = db.get_movimiento_by_id(mov_id)
                lote_actual = movimiento_actual.get('LOTE', '') if movimiento_actual else ''
                vencimiento_actual = movimiento_actual.get('VENCIMIENTO', '') if movimiento_actual else ''

                lote_a_actualizar = lote_nuevo if lote_nuevo and str(lote_nuevo).upper() != str(lote_actual or '').upper() else None
                venc_a_actualizar = vencimiento_nuevo if vencimiento_nuevo and vencimiento_nuevo != vencimiento_actual else None

                if lote_a_actualizar or venc_a_actualizar:
                    db.actualizar_datos_trazabilidad(mov_id, lote_a_actualizar, venc_a_actualizar)

                f_rep = datos_actualizados.get('fecha_reporte')
                if f_rep and '/' in str(f_rep):
                    partes = str(f_rep).split('/')
                    fecha_reporte_db = f"{partes[2]}-{partes[1]}-{partes[0]}"
                elif f_rep:
                    fecha_reporte_db = f_rep

            db.actualizar_reporte(
                mov_id=mov_id,
                transaccion=resultado.get('transaccion', ''),
                resultado='A',
                error_codigo=None,
                error_descripcion=None,
                fecha_reporte=fecha_reporte_db,
            )
        else:
            errores = resultado.get('errores', [])
            error_desc = errores[0] if errores else resultado.get('error', 'Error desconocido')
            error_codigo = None
            if errores:
                try:
                    codigo_extraido = errores[0].split(':')[0].strip()
                    int(codigo_extraido)
                    error_codigo = codigo_extraido
                except (ValueError, IndexError):
                    error_codigo = '999'

            db.actualizar_reporte(
                mov_id=mov_id,
                transaccion=resultado.get('transaccion', ''),
                resultado='R',
                error_codigo=error_codigo,
                error_descripcion=error_desc
            )

        resultado.pop('datos_enviados', None)
        return jsonify(resultado)
    except Exception as e:
        return _internal_err(e, f'Error al reportar movimiento {mov_id}')


@app.route('/api/subsanar/<int:mov_id>', methods=['POST'])
@limiter.limit("60 per minute")
def subsanar_movimiento(mov_id):
    try:
        data = request.get_json()
        gln = request.args.get('gln') or (data.get('gln') if data else None)

        if not gln:
            return _err('GLN es requerido')
        if not _valid_gln(gln):
            return _err('GLN inválido: debe ser 13 dígitos')

        lote = data.get('lote') if data else None
        vencimiento = data.get('vencimiento') if data else None

        db.actualizar_datos_trazabilidad(mov_id, lote, vencimiento)

        sucursal = db.get_sucursal_credenciales(gln)
        if sucursal:
            reporter.usuario_app = sucursal.get('anmat_user', reporter.usuario_app)
            reporter.password_app = sucursal.get('anmat_password', reporter.password_app)

        if not reporter.conectar():
            return _err('No se pudo conectar a ANMAT', 503)

        movimiento = db.get_movimiento_by_id(mov_id)
        if not movimiento:
            return _err('Movimiento no encontrado', 404)

        resultado = reporter.reportar_movimiento(movimiento, gln=gln)

        if resultado.get('success'):
            datos_actualizados = resultado.get('datos_actualizados')
            fecha_reporte_db = None
            if datos_actualizados:
                f_rep = datos_actualizados.get('fecha_reporte')
                if f_rep and '/' in str(f_rep):
                    partes = str(f_rep).split('/')
                    fecha_reporte_db = f"{partes[2]}-{partes[1]}-{partes[0]}"
                elif f_rep:
                    fecha_reporte_db = f_rep

            db.actualizar_reporte(
                mov_id=mov_id,
                transaccion=resultado.get('transaccion', ''),
                resultado=resultado.get('resultado', 'A'),
                error_codigo=None,
                error_descripcion=None,
                fecha_reporte=fecha_reporte_db,
            )

        resultado.pop('datos_enviados', None)
        return jsonify(resultado)
    except Exception as e:
        return _internal_err(e, f'Error al subsanar movimiento {mov_id}')


@app.route('/api/buscar_comprobantes')
def buscar_comprobantes():
    try:
        gtin = request.args.get('gtin')
        lote = request.args.get('lote')
        serie = request.args.get('serie')
        fecha_desde = request.args.get('fecha_desde')
        if fecha_desde and not _valid_fecha(fecha_desde):
            return _err('fecha_desde inválida: usar formato YYYY-MM-DD')
        resultados = db.buscar_comprobantes_recibidos(gtin, lote, serie, fecha_desde)
        return jsonify(resultados)
    except Exception as e:
        return _internal_err(e, 'Error al buscar comprobantes')


@app.route('/api/anmat_transacciones', methods=['GET', 'POST'])
@limiter.limit("30 per minute")
def anmat_transacciones():
    try:
        if request.method == 'POST':
            data = request.get_json() or {}
        else:
            data = request.args.to_dict()

        gln = data.get('gln')
        if gln and not _valid_gln(gln):
            return _err('GLN inválido: debe ser 13 dígitos')

        if gln:
            creds = db.get_sucursal_credenciales(gln)
            if creds:
                reporter.usuario_app = creds['anmat_user']
                reporter.password_app = creds['anmat_password']

        if reporter.conectar():
            resultado = reporter.buscar_transacciones_ws(data)
            return jsonify(resultado)
        else:
            return _err('No se pudo conectar a ANMAT', 503)
    except Exception as e:
        return _internal_err(e, 'Error al buscar transacciones en ANMAT')


@app.route('/api/seleccionar_comprobante', methods=['POST'])
@limiter.limit("60 per minute")
def seleccionar_comprobante():
    try:
        data = request.get_json()
        gln = data.get('gln')
        cmp_recibido_item_id = data.get('cmp_recibido_item_id')
        gtin = data.get('gtin')
        lote = data.get('lote')
        numero_serial = data.get('numero_serial')
        vencimiento = data.get('vencimiento')

        if not gln:
            return _err('GLN es requerido')
        if not _valid_gln(gln):
            return _err('GLN inválido: debe ser 13 dígitos')
        if not cmp_recibido_item_id:
            return _err('Falta cmp_recibido_item_id')

        mov = db.get_movimiento_por_cmp_recibido_item(cmp_recibido_item_id)

        if not mov:
            nuevo_mov_id = db.crear_registro_trazabilidad(
                cmp_recibido_item_id, gtin=gtin, lote=lote, numero_serial=numero_serial, caducidad=vencimiento
            )
            if not nuevo_mov_id:
                return _err('No se pudo crear el registro de trazabilidad', 500)
            mov = db.get_movimiento_by_id(nuevo_mov_id)
        else:
            db.actualizar_datos_trazabilidad(mov['ID'], lote, vencimiento)
            mov = db.get_movimiento_by_id(mov['ID'])

        sucursal = db.get_sucursal_credenciales(gln)
        if sucursal:
            reporter.usuario_app = sucursal.get('anmat_user', reporter.usuario_app)
            reporter.password_app = sucursal.get('anmat_password', reporter.password_app)

        if not reporter.conectar():
            return _err('No se pudo conectar a ANMAT', 503)

        resultado = reporter.reportar_movimiento(mov, gln=gln)

        if resultado.get('success'):
            datos_actualizados = resultado.get('datos_actualizados')
            fecha_reporte_db = None
            if datos_actualizados:
                f_rep = datos_actualizados.get('fecha_reporte')
                if f_rep and '/' in str(f_rep):
                    partes = str(f_rep).split('/')
                    fecha_reporte_db = f"{partes[2]}-{partes[1]}-{partes[0]}"
                elif f_rep:
                    fecha_reporte_db = f_rep

            db.actualizar_reporte(
                mov_id=mov['ID'],
                transaccion=resultado.get('transaccion', ''),
                resultado='A',
                error_codigo=None,
                error_descripcion=None,
                fecha_reporte=fecha_reporte_db,
            )
        else:
            errores = resultado.get('errores', [])
            error_desc = errores[0] if errores else resultado.get('error', 'Error desconocido')
            error_codigo = None
            if errores:
                try:
                    codigo_extraido = errores[0].split(':')[0].strip()
                    int(codigo_extraido)
                    error_codigo = codigo_extraido
                except (ValueError, IndexError):
                    error_codigo = '999'
            db.actualizar_reporte(
                mov_id=mov['ID'],
                transaccion=resultado.get('transaccion', ''),
                resultado='R',
                error_codigo=error_codigo,
                error_descripcion=error_desc
            )

        resultado.pop('datos_enviados', None)
        return jsonify(resultado)
    except Exception as e:
        return _internal_err(e, 'Error al seleccionar comprobante')


@app.route('/api/confirmar_transaccion', methods=['POST'])
@limiter.limit("30 per minute")
def confirmar_transaccion():
    try:
        data = request.get_json() or {}
        id_transaccion = data.get('id_transaccion')
        gln = data.get('gln')
        tx_data = data.get('tx_data', {})
        f_operacion = data.get('f_operacion')
        cmp_recibido_item_id = data.get('cmp_recibido_item_id')
        if not id_transaccion:
            return _err('id_transaccion es requerido')
        if not gln or not _valid_gln(gln):
            return _err('GLN inválido')
        if not reporter.conectar():
            return _err('No se pudo conectar a ANMAT', 503)
        if cmp_recibido_item_id:
            tx_data['cmp_recibido_item_id'] = cmp_recibido_item_id
        resultado = reporter.confirmar_transaccion(id_transaccion, gln, tx_data, f_operacion)
        return jsonify(resultado)
    except Exception as e:
        return _internal_err(e, 'Error al confirmar transacción')


@app.route('/api/conciliar/pendientes')
@limiter.limit("30 per minute")
def conciliar_pendientes():
    """Lista transacciones ANMAT pendientes con matching automático contra comprobantes locales."""
    try:
        gln = request.args.get('gln') or None
        if gln and not _valid_gln(gln):
            return _err('GLN inválido: debe ser 13 dígitos')
        if not reporter.conectar():
            return _err('No se pudo conectar a ANMAT', 503)
        resultado = reporter.get_pendientes_con_match(gln)
        return jsonify(resultado)
    except Exception as e:
        logger.error(traceback.format_exc())
        return _internal_err(e, 'Error al obtener pendientes con matching')


@app.route('/api/conciliar/comprobantes')
@limiter.limit("60 per minute")
def conciliar_comprobantes():
    """Busca comprobantes locales candidatos para conciliar con una transacción ANMAT."""
    try:
        gtin = request.args.get('gtin', '').lstrip('0') or request.args.get('gtin', '')
        n_factura = request.args.get('n_factura', '')
        n_remito = request.args.get('n_remito', '')
        gln_origen = request.args.get('gln_origen', '')
        gln_sucursal = request.args.get('gln_sucursal', '')
        if gln_sucursal and not _valid_gln(gln_sucursal):
            return _err('GLN sucursal inválido')
        fecha_desde = request.args.get('fecha_desde') or None
        fecha_hasta = request.args.get('fecha_hasta') or None
        if not gtin and not n_factura and not n_remito:
            return _err('Al menos uno de: gtin, n_factura, n_remito es requerido')
        resultados = db.buscar_cmp_recibido_por_filtros(
            gtin=gtin or None,
            n_factura=n_factura or None,
            n_remito=n_remito or None,
            gln_origen=gln_origen or None,
            gln_sucursal=gln_sucursal or None,
            fecha_desde=fecha_desde,
            fecha_hasta=fecha_hasta,
        )
        return jsonify(resultados)
    except Exception as e:
        return _internal_err(e, 'Error al buscar comprobantes para conciliar')


@app.route('/api/alertar_transaccion', methods=['POST'])
@limiter.limit("30 per minute")
def alertar_transaccion():
    try:
        data = request.get_json() or {}
        id_transaccion = data.get('id_transaccion')
        gln = data.get('gln')
        if not id_transaccion:
            return _err('id_transaccion es requerido')
        if not gln or not _valid_gln(gln):
            return _err('GLN inválido')
        if not reporter.conectar():
            return _err('No se pudo conectar a ANMAT', 503)
        resultado = reporter.alertar_transaccion(id_transaccion, gln)
        return jsonify(resultado)
    except Exception as e:
        return _internal_err(e, 'Error al alertar transacción')


if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=5001)
