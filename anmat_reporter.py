"""
Módulo para reportar eventos de medicamentos a ANMAT
Usa pyafipws.trazamed.TrazaMed (probado y funcional con ANMAT)
"""

import sys
import os
import json
import logging
import traceback
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from unittest.mock import MagicMock
import socket

logger = logging.getLogger(__name__)

# Workaround for Python 3.12+ compatibility with future package
# The 'imp' module was removed in Python 3.12 but future still tries to import it
sys.modules['imp'] = MagicMock()

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import ANMAT_WSDL, ANMAT_USUARIO, ANMAT_PASSWORD, ANMAT_TIMEOUT
from database import db

try:
    from pyafipws.trazamed import TrazaMed
except ImportError:
    print("\n TrazaMed de pyafipws no está disponible.")
    sys.exit(1)

# Cache directory para WSDL
CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'pyafipws', 'cache')

# Credenciales de TRANSPORTE WSSE (fijos para todos - especificación ANMAT)
WSS_USERNAME = "testwservice"
WSS_PASSWORD = "testwservicepsw"


class AnmatReporter:
    """Reportador de movimientos a ANMAT usando pyafipws.trazamed.TrazaMed"""

    # Archivo de estado para sincronización única desde ANMAT
    SYNC_STATE_FILE = os.path.join(os.path.dirname(__file__), 'anmat_sync_state.json')

    # Un TrazaMed y credenciales por thread para evitar race conditions
    _local = threading.local()

    def __init__(self):
        # No crear ws aquí — se crea por thread en la primera llamada
        pass

    def _anmat_call(self, method_name, timeout=ANMAT_TIMEOUT, *args, **kwargs):
        """Ejecuta un método del WS de ANMAT en un thread separado con timeout.
        
        Usa la conexión ya establecida de self.ws (main thread). Si el método
        se cuelga, el thread se abandona pero no bloquea el servidor.
        """
        from threading import Thread

        result = []
        error = []
        ws = self.ws

        def worker():
            try:
                socket.setdefaulttimeout(timeout)
                method = getattr(ws, method_name)
                r = method(*args, **kwargs)
                result.append(r)
            except Exception as e:
                error.append(e)

        t = Thread(target=worker, daemon=True)
        t.start()
        t.join(timeout=timeout)

        if t.is_alive():
            raise TimeoutError(f"ANMAT no respondió en {timeout}s (método {method_name})")
        if error:
            raise error[0]

        return result[0]

    @property
    def ws(self):
        if not getattr(self._local, 'ws', None):
            ws = TrazaMed()
            ws.Username = WSS_USERNAME
            ws.Password = WSS_PASSWORD
            ws.LanzarExcepciones = False
            self._local.ws = ws
        return self._local.ws

    @property
    def usuario_app(self):
        return getattr(self._local, 'usuario_app', ANMAT_USUARIO)

    @usuario_app.setter
    def usuario_app(self, value):
        self._local.usuario_app = value

    @property
    def password_app(self):
        return getattr(self._local, 'password_app', ANMAT_PASSWORD)

    @password_app.setter
    def password_app(self, value):
        self._local.password_app = value

    def conectar(self, usuario=None, password=None) -> bool:
        """
        Conecta al WebService de ANMAT usando TrazaMed (instancia por thread).
        """
        if usuario is None:
            usuario = ANMAT_USUARIO
        if password is None:
            password = ANMAT_PASSWORD

        self.usuario_app = usuario
        self.password_app = password

        try:
            socket.setdefaulttimeout(ANMAT_TIMEOUT)
            ok = self.ws.Conectar(cache=CACHE_DIR, wsdl=ANMAT_WSDL, timeout=ANMAT_TIMEOUT)
            if ok:
                logger.info("Conectado a ANMAT correctamente")
            else:
                logger.warning(f"No se pudo conectar a ANMAT: {self.ws.Excepcion or self.ws.Errores}")
            return ok
        except Exception as e:
            logger.error(f"Error al conectar a ANMAT: {e}")
            return False

    def reportar_movimiento(self, movimiento: Dict[str, Any], gln: str) -> Dict[str, Any]:
        """
        Reporta un movimiento a ANMAT

        :param movimiento: Diccionario con datos del movimiento desde BD
        :param gln: GLN de la sucursal (para producción, obtiene credenciales de la sucursal)
        :return: Diccionario con resultado del reporte
        """
        datos_enviar = None
        try:
            logger.info(f"Reportando movimiento ID: {movimiento.get('ID')}")

            # Configurar credenciales según el gln
            sucursal = db.get_sucursal_credenciales(gln)
            if sucursal:
                self.usuario_app = sucursal.get('anmat_user', ANMAT_USUARIO)
                self.password_app = sucursal.get('anmat_password', ANMAT_PASSWORD)
                logger.info(f"Usando credenciales de sucursal {gln}: {self.usuario_app}")
            else:
                logger.warning(f"No se encontraron credenciales para GLN {gln}, usando defaults")

            logger.debug(f"Credenciales a usar: usuario={self.usuario_app}")

            # Verificar si ya fue reportado (por otra aplicación)
            transaccion_existente = movimiento.get('ANMAT_TRANSACCION')
            resultado_existente = movimiento.get('ANMAT_RESULTADO')

            if transaccion_existente and resultado_existente == 1:
                logger.info(f"Movimiento {movimiento.get('ID')} ya reportado con transacción {transaccion_existente}")
                return {
                    'success': True,
                    'transaccion': transaccion_existente,
                    'resultado': 'A',
                    'ya_reportado': True,
                    'ID': movimiento.get('ID')
                }

            # Validar datos mínimos
            if not self._validar_movimiento(movimiento):
                logger.error(f"Datos incompletos para movimiento: {movimiento.get('ID')}")
                return {
                    'success': False,
                    'error': 'Datos incompletos del movimiento',
                    'ID': movimiento.get('ID')
                }

            # Determinar tipo de evento según ml_id
            id_evento = movimiento.get('ML_ID')
            logger.info(f"ML_ID: {id_evento}")

            if not id_evento:
                return {
                    'success': False,
                    'error': f'ML_ID no especificado (tipo de movimiento)',
                    'ID': movimiento.get('ID')
                }

            # ML_ID 101 = Ingreso, 108 = Recepción devolución, 111 = Dispensación
            es_recepcion = id_evento in (101, 108)

            # Fecha y hora del evento
            f_evento = self._formato_fecha(movimiento.get('FECHA_COMPROBANTE') or movimiento.get('ALTA') or datetime.now().strftime('%Y-%m-%d'))
            h_evento = "00:00"

            # Tipo de comprobante
            letra = (movimiento.get('LETRA_COMPROBANTE') or '').upper()
            nro_comprobante = movimiento.get('COMPROBANTE', '')

            # Para ingresos: n_remito si empieza con 'R', n_factura si es 'A' o 'B'
            n_remito = nro_comprobante if letra == 'R' else ''
            n_factura = nro_comprobante if letra in ['A', 'B', 'C'] else ''

            # GLN origen y destino según el tipo de movimiento
            if es_recepcion:
                # Ingreso: desde proveedor hacia la sucursal
                gln_proveedor_raw = movimiento.get('GLN_PROVEEDOR')
                gln_proveedor_str = str(gln_proveedor_raw).strip() if gln_proveedor_raw else ''

                # Verificar si el GLN es válido (13 dígitos y dígito verificador correcto)
                gln_valido = (
                    gln_proveedor_str and
                    gln_proveedor_str.isdigit() and
                    len(gln_proveedor_str) == 13 and
                    self._validar_gln(gln_proveedor_str)
                )

                # Si el GLN no es válido, intentar buscarlo de otra forma
                if not gln_valido:
                    logger.warning(f"GLN de proveedor inválido o no disponible: {gln_proveedor_str}, intentando buscar en BD...")
                    cmp_recibido_id = movimiento.get('CMP_RECIBIDO_ID')
                    if cmp_recibido_id:
                        gln_proveedor_str = db.get_proveedor_gln_por_comprobante(cmp_recibido_id) or ''

                    # Verificar de nuevo
                    if gln_proveedor_str and gln_proveedor_str.isdigit() and len(gln_proveedor_str) == 13 and self._validar_gln(gln_proveedor_str):
                        logger.info(f"GLN encontrado en BD: {gln_proveedor_str}")
                    elif gln_proveedor_str:
                        # GLN tiene formato pero dígito verificador incorrecto - usar de todas formas
                        logger.warning(f"GLN con dígito verificador incorrecto pero usando de todas formas: {gln_proveedor_str}")
                    else:
                        logger.warning(f"GLN de proveedor no disponible para movimiento {movimiento.get('ID')}, usando fallback 9999999999999")

                gln_origen = self._obtener_gln(gln_proveedor_str)
                gln_destino = self._obtener_gln(movimiento.get('GLN_SUCURSAL'))
                logger.info(f"DEBUG - gln_origen: {gln_origen}, gln_destino: {gln_destino}")
            else:
                # Dispensación: desde la sucursal hacia el paciente
                gln_origen = self._obtener_gln(movimiento.get('GLN_SUCURSAL'))
                gln_destino = ''  # Paciente no tiene GLN

            # Datos del afiliado (para dispensas)
            nombre_afiliado = ''
            apellido_afiliado = ''
            documento = ''
            tipo_doc_anmat = ''
            direccion = ''
            localidad = ''
            cp = ''

            if not es_recepcion:
                nombre_afiliado = movimiento.get('NOMBRE_AFILIADO', '') or ''
                apellido_afiliado = movimiento.get('APELLIDO_AFILIADO', '') or ''
                dni_afiliado = str(movimiento.get('DNI_AFILIADO', '') or '').strip()
                cuil_afiliado = str(movimiento.get('CUIL_AFILIADO', '') or '').strip()

                # Tipo documento: si tiene DNI, usar 96, sino 80
                if dni_afiliado:
                    tipo_doc_anmat = '96'
                    documento = dni_afiliado
                else:
                    tipo_doc_anmat = '80'
                    documento = cuil_afiliado

                # Datos de domicilio
                direccion = (movimiento.get('DIRECCION_AFILIADO') or '').strip()
                localidad = (movimiento.get('LOCALIDAD_AFILIADO') or '').strip()
                cp = (movimiento.get('CP_AFILIADO') or '').strip()

            # Enviar a ANMAT
            gtin_enviar = str(movimiento.get('GTIN', '')).strip()

            # Sanitizar campos para evitar errores SOAP
            def sanitize(val):
                if val is None:
                    return ''
                return str(val).encode('ascii', 'ignore').decode('ascii')

            datos_enviar = {
                'usuario': self.usuario_app,
                'password': self.password_app,
                'f_evento': f_evento,
                'h_evento': h_evento,
                'gln_origen': gln_origen,
                'gln_destino': gln_destino,
                'n_remito': n_remito,
                'n_factura': n_factura,
                'vencimiento': self._formato_fecha(movimiento.get('VENCIMIENTO')) or '',
                'gtin': sanitize(gtin_enviar),
                'lote': sanitize(movimiento.get('LOTE', '')),
                'numero_serial': sanitize(movimiento.get('NUMERO_SERIAL', '')),
                'id_obra_social': '',
                'id_evento': id_evento,
                'apellido': sanitize(apellido_afiliado),
                'nombres': sanitize(nombre_afiliado),
                'tipo_documento': tipo_doc_anmat,
                'n_documento': sanitize(documento),
                'direccion': sanitize(direccion),
                'localidad': sanitize(localidad),
                'n_postal': sanitize(cp),
            }

            try:
                ok = self.ws.SendMedicamentos(**datos_enviar)
                errores = self.ws.Errores or []
            except Exception as e:
                error_msg = str(e)
                logger.error(f"Error en SendMedicamentos para movimiento {movimiento.get('ID')}: {error_msg}")
                return {
                    'success': False,
                    'ID': movimiento.get('ID'),
                    'error': error_msg,
                    'errores': [error_msg],
                    'datos_enviados': datos_enviar
                }

            # Verificar si hay errores en la respuesta
            hay_errores = bool(errores)
            success = ok and not hay_errores

            # Error 2039: reintentar una vez
            if not success and any('2039' in str(e) for e in errores):
                logger.info(f"Error 2039 para movimiento {movimiento.get('ID')}, reintentando...")
                try:
                    ok = self.ws.SendMedicamentos(**datos_enviar)
                    errores = self.ws.Errores or []
                    hay_errores = bool(errores)
                    success = ok and not hay_errores
                    logger.info(f"Reintento 2039 resultado: success={success}, errores={errores}")
                except Exception as e:
                    errores = [str(e)]
                    success = False

            # Siempre obtener el código de transacción si existe
            codigo_transaccion = getattr(self.ws, 'CodigoTransaccion', None)
            logger.info(f"CodigoTransaccion: {codigo_transaccion}")
            logger.info(f"Resultado: {getattr(self.ws, 'Resultado', None)}")
            logger.info(f"Errores ANMAT: {errores}")

            resultado = {
                'success': success,
                'ID': movimiento.get('ID'),
                'transaccion': codigo_transaccion,
                'resultado': 'A' if success else 'R',
                'errores': errores,
                'id_evento': id_evento,
                'tipo': 'recepcion' if es_recepcion else 'dispensa',
                'movimiento': movimiento.get('ID'),
            }

            # Error 3034: recepción debe confirmarse via SendConfirmaTransacc
            error_3034 = next(
                (e for e in errores if e and '3034' in str(e)),
                None
            )
            if error_3034 and not resultado['success']:
                logger.info(f"Error 3034 para movimiento {movimiento.get('ID')}. Confirmando transacción del proveedor...")
                transaccion_confirmada = self._resolver_error_3034(movimiento)
                if transaccion_confirmada:
                    resultado['success'] = True
                    resultado['resultado'] = 'A'
                    resultado['transaccion'] = transaccion_confirmada.get('id_transaccion')
                    resultado['errores'] = []
                    resultado['datos_actualizados'] = {
                        'lote': transaccion_confirmada.get('lote'),
                        'vencimiento': transaccion_confirmada.get('vencimiento'),
                        'fecha_reporte': transaccion_confirmada.get('fecha_reporte'),
                    }
                    logger.info(f"Error 3034 resuelto: transacción {transaccion_confirmada.get('id_transaccion')} confirmada")
                else:
                    logger.warning(f"No se pudo resolver error 3034 para movimiento {movimiento.get('ID')}")

            # Error 3024: no se informó la recepción — confirmar recepción pendiente y reintentar
            error_3024 = next(
                (e for e in errores if e and '3024' in str(e)),
                None
            )
            if error_3024 and not resultado['success']:
                logger.info(f"Error 3024 para movimiento {movimiento.get('ID')}. Buscando recepción pendiente...")
                if self._resolver_error_3024(movimiento):
                    logger.info("Recepción confirmada, reintentando dispensa...")
                    try:
                        ok2 = self.ws.SendMedicamentos(**datos_enviar)
                        errores2 = self.ws.Errores or []
                        if ok2 and not errores2:
                            resultado['success'] = True
                            resultado['resultado'] = 'A'
                            resultado['transaccion'] = getattr(self.ws, 'CodigoTransaccion', None)
                            resultado['errores'] = []
                            logger.info(f"Reintento 3024 exitoso: transacción {resultado['transaccion']}")
                        else:
                            errores = errores2
                            resultado['errores'] = errores
                            logger.warning(f"Reintento 3024 falló: {errores2}")
                    except Exception as e:
                        logger.error(f"Error en reintento 3024: {e}")
                else:
                    logger.info("3024: no hay recepción pendiente, verificando si ya fue informado...")

            # Errores 3108 / 3018 / 3024 (ya informado): buscar transacción original
            if not resultado['success']:
                error_ya_reportado = next(
                    (e for e in errores if e and ('3108' in str(e) or '3018' in str(e) or '3024' in str(e))),
                    None
                )
            else:
                error_ya_reportado = None
            if error_ya_reportado:
                codigo_err = '3108' if '3108' in str(error_ya_reportado) else ('3018' if '3018' in str(error_ya_reportado) else '3024')
                logger.info(f"Error {codigo_err} para movimiento {movimiento.get('ID')}")

                gtin_buscar = str(movimiento.get('GTIN', '')).strip()
                serie_buscar = str(movimiento.get('NUMERO_SERIAL', '') or '').strip()
                lote_buscar = str(movimiento.get('LOTE', '') or '').strip().upper()
                gln_sucursal = movimiento.get('GLN_SUCURSAL', '')
                gln_proveedor = movimiento.get('GLN_PROVEEDOR', '')

                transaccion_encontrada = self._buscar_transaccion_para_3108(
                    gtin_buscar, serie_buscar, gln_sucursal, es_recepcion, gln_proveedor, lote_buscar
                )

                if transaccion_encontrada:
                    tx_codigo = transaccion_encontrada.get('codigo_transaccion') or transaccion_encontrada.get('id_transaccion') or ''
                    resultado['success'] = True
                    resultado['resultado'] = 'A'
                    resultado['transaccion'] = tx_codigo
                    resultado['errores'] = []
                    resultado['datos_actualizados'] = {
                        'lote': transaccion_encontrada.get('lote'),
                        'vencimiento': transaccion_encontrada.get('vencimiento'),
                        'fecha_reporte': transaccion_encontrada.get('fecha'),
                    }
                    logger.info(f"Error {codigo_err} resuelto: transacción {tx_codigo} encontrada")
                else:
                    logger.warning(f"No se encontró transacción para GTIN {gtin_buscar}, serie {serie_buscar}, lote={lote_buscar}")

            if not resultado['success']:
                logger.error(f"Error al reportar movimiento {movimiento.get('ID')}. Datos enviados: {datos_enviar}")
                resultado['datos_enviados'] = datos_enviar

            return resultado

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Excepción al reportar movimiento {movimiento.get('ID')}: {error_msg}")
            return {
                'success': False,
                'ID': movimiento.get('ID'),
                'error': error_msg,
                'errores': [error_msg],
                'datos_enviados': datos_enviar if datos_enviar is not None else None
            }

    def _resolver_error_3034(self, movimiento: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Resuelve error 3034.

        Caso A — Ya confirmado:
          1. Busca en GetTransaccionesWS por GTIN+serie+gln_destino
          2. Si encuentra, devuelve los datos para actualizar el registro local

        Caso B — Pendiente de confirmación (enfoque todo-o-nada):
          1. Obtiene las no confirmadas de ANMAT, encuentra item por GTIN+serie+destino
          2. Agrupa por mismo remito/factura + origen/destino
          3. Verifica que existe UN solo cmp_recibido con TODOS los GTINs del grupo
          4. Si existe: confirma cada transacción vía SendConfirmaTransacc
          5. Si no existe: retorna None
        """
        try:
            gtin = str(movimiento.get('GTIN', '')).strip()
            lote = str(movimiento.get('LOTE', '') or '').strip().upper()
            serie = str(movimiento.get('NUMERO_SERIAL', '') or '').strip()
            gln_sucursal = str(movimiento.get('GLN_SUCURSAL', '') or '').strip()
            fecha_comprobante = movimiento.get('FECHA_COMPROBANTE')

            logger.info(f"Resolviendo 3034: GTIN={gtin}, lote={lote}, serie={serie}, destino={gln_sucursal}")

            def _buscar_confirmada(gtin_b, serie_b, gln_b):
                for id_evento in [101, 108, None]:
                    params = {'gtin': gtin_b}
                    if gln_b:
                        params['gln_destino'] = gln_b
                    if id_evento:
                        params['id_evento'] = id_evento
                    res = self.buscar_transacciones_ws(params)
                    txs = res.get('transacciones', [])
                    if txs:
                        if serie_b:
                            for tx in txs:
                                tx_serie = (tx.get('numero_serial') or '').strip()
                                if tx_serie == serie_b:
                                    return tx
                        return txs[0]
                return None

            gtin_clean = gtin.lstrip('0') or gtin

            ok = self.ws.GetTransaccionesNoConfirmadas(
                usuario=self.usuario_app,
                password=self.password_app,
            )
            if not ok:
                logger.warning(f"GetTransaccionesNoConfirmadas falló: {self.ws.Errores}")
                tx = _buscar_confirmada(gtin, serie, gln_sucursal)
                if tx:
                    logger.info(f"3034-CasoA: transacción encontrada en GetTransaccionesWS")
                    cod = tx.get('codigo_transaccion') or tx.get('id_transaccion') or ''
                    return {
                        'id_transaccion': cod,
                        'lote': tx.get('lote') or lote,
                        'vencimiento': tx.get('vencimiento'),
                        'fecha_reporte': tx.get('fecha'),
                    }
                return None

            todas = []
            while self.ws.LeerTransaccion():
                todas.append({
                    'id_transaccion': self.ws.GetParametro("_id_transaccion"),
                    'gtin': (self.ws.GetParametro("_gtin") or '').strip(),
                    'lote': (self.ws.GetParametro("_lote") or '').strip().upper(),
                    'serie': (self.ws.GetParametro("_numero_serial") or '').strip(),
                    'destino': (self.ws.GetParametro("_gln_destino") or '').strip(),
                    'origen': (self.ws.GetParametro("_gln_origen") or '').strip(),
                    'n_remito': (self.ws.GetParametro("_n_remito") or '').strip(),
                    'n_factura': (self.ws.GetParametro("_n_factura") or '').strip(),
                    'vencimiento': self.ws.GetParametro("_vencimiento"),
                    'fecha': self.ws.GetParametro("_f_evento"),
                })

            def _match_tx(tx):
                tx_gtin = (tx['gtin'] or '').lstrip('0') or tx['gtin']
                tx_serie = (tx['serie'] or '').strip()
                tx_dest = tx['destino'].zfill(13)[:13]
                suc_dest = gln_sucursal.zfill(13)[:13]
                if tx_gtin != gtin_clean or tx_dest != suc_dest:
                    return False
                if serie and tx_serie:
                    return tx_serie == serie
                return True

            matching = None
            for tx in todas:
                if _match_tx(tx):
                    matching = tx
                    break

            if not matching:
                logger.info(f"3034: GTIN={gtin} no está en pendientes (no match por GTIN+destino), buscando en confirmadas...")
                tx = _buscar_confirmada(gtin, serie, gln_sucursal)
                if tx:
                    logger.info(f"3034-CasoA: encontrado en GetTransaccionesWS")
                    cod = tx.get('codigo_transaccion') or tx.get('id_transaccion') or ''
                    return {
                        'id_transaccion': cod,
                        'lote': tx.get('lote') or lote,
                        'vencimiento': tx.get('vencimiento'),
                        'fecha_reporte': tx.get('fecha'),
                    }
                logger.warning(f"No hay transacción pendiente/confirmada para GTIN={gtin}, serie={serie}")
                return None

            # Intentar confirmar individualmente (Caso A si ya fue confirmado)
            f_operacion = self._formato_fecha(fecha_comprobante) or datetime.now().strftime('%d/%m/%Y')
            self.ws.SendConfirmaTransacc(
                usuario=self.usuario_app,
                password=self.password_app,
                p_ids_transac=matching['id_transaccion'],
                f_operacion=f_operacion,
            )
            errores = self.ws.Errores or []
            if not errores:
                cod = getattr(self.ws, 'CodigoTransaccion', None)
                logger.info(f"3034: transacción individual {matching['id_transaccion']} confirmada → {cod}")
                return {
                    'id_transaccion': cod or matching['id_transaccion'],
                    'lote': matching.get('lote') or lote,
                    'vencimiento': matching.get('vencimiento'),
                    'fecha_reporte': matching.get('fecha'),
                }

            error_str = '; '.join(str(e) for e in errores).lower()
            if 'ya fue' in error_str or 'ya ha sido' in error_str or 'already' in error_str:
                logger.info(f"3034-CasoA: transacción ya confirmada, buscando datos existentes...")
                tx = _buscar_confirmada(gtin, serie, gln_sucursal)
                if tx:
                    cod = tx.get('codigo_transaccion') or tx.get('id_transaccion') or ''
                    return {
                        'id_transaccion': cod,
                        'lote': tx.get('lote') or lote,
                        'vencimiento': tx.get('vencimiento'),
                        'fecha_reporte': tx.get('fecha'),
                    }
                return None

            # Caso B — Enfoque todo-o-nada con el grupo
            logger.info(f"3034-CasoB: requiere confirmación grupal")
            group_key = matching.get('n_remito') or matching.get('n_factura') or ''
            gln_origen = matching.get('origen')
            gln_destino = matching.get('destino')

            if group_key:
                items_grupo = [
                    t for t in todas
                    if (t.get('n_remito') == group_key or t.get('n_factura') == group_key)
                    and t.get('origen') == gln_origen
                    and t.get('destino') == gln_destino
                ]
            else:
                items_grupo = [matching]

            gtins_grupo = list(dict.fromkeys(
                (t['gtin'] or '').lstrip('0') or t['gtin']
                for t in items_grupo if t.get('gtin')
            ))
            gtins_grupo = [g for g in gtins_grupo if g]

            logger.info(f"Grupo: {len(items_grupo)} items, {len(gtins_grupo)} GTINs únicos: {gtins_grupo}")

            cmp_match = db.buscar_cmp_recibido_por_lista_gtins(gtins_grupo, gln_sucursal)
            if not cmp_match:
                logger.warning(
                    f"No existe un comprobante con TODOS los GTINs del grupo: {gtins_grupo}. "
                    f"No se puede resolver 3034 automáticamente."
                )
                return None

            logger.info(f"Comprobante válido: cmp_recibido_id={cmp_match['cmp_recibido_id']}")

            ultimo_resultado = None
            confirmadas = 0
            fallidas = 0

            for tx in items_grupo:
                self.ws.SendConfirmaTransacc(
                    usuario=self.usuario_app,
                    password=self.password_app,
                    p_ids_transac=tx['id_transaccion'],
                    f_operacion=f_operacion,
                )
                errores = self.ws.Errores or []
                if errores:
                    fallidas += 1
                    logger.warning(f"Fallo al confirmar transacción {tx['id_transaccion']}: {errores}")
                else:
                    confirmadas += 1
                    cod = getattr(self.ws, 'CodigoTransaccion', None)
                    logger.info(f"Transacción {tx['id_transaccion']} confirmada → {cod}")
                    ultimo_resultado = {
                        'id_transaccion': cod or tx['id_transaccion'],
                        'lote': tx['lote'] or lote,
                        'vencimiento': tx['vencimiento'],
                        'fecha_reporte': tx.get('fecha'),
                    }

            logger.info(f"Resultado grupo: {confirmadas} confirmadas, {fallidas} fallidas")

            if ultimo_resultado:
                return ultimo_resultado
            logger.warning("No se pudo confirmar ninguna transacción del grupo")
            return None

        except Exception as e:
            logger.error(f"Error en _resolver_error_3034: {e}\n{traceback.format_exc()}")
            return None

    def _resolver_error_3024(self, movimiento: Dict[str, Any]) -> bool:
        """Busca y confirma la recepción pendiente para resolver error 3024 en dispensas.

        El error 3024 significa que no se informó la recepción antes de dispensar.
        Este método:
        1. Busca en GetTransaccionesNoConfirmadas una recepción que matchee GTIN+serie+dst
        2. Si existe, la confirma via SendConfirmaTransacc
        3. Retorna True si se confirmó, False si no hay recepción pendiente

        Returns:
            bool: True si se confirmó una recepción, False si no se encontró o falló
        """
        try:
            gtin = str(movimiento.get('GTIN', '')).strip()
            serie = str(movimiento.get('NUMERO_SERIAL', '') or '').strip()
            gln_sucursal = str(movimiento.get('GLN_SUCURSAL', '') or '').strip()

            if not gtin or not serie:
                logger.warning("3024: GTIN o serie vacíos, no se puede buscar recepción pendiente")
                return False

            ok = self.ws.GetTransaccionesNoConfirmadas(
                usuario=self.usuario_app,
                password=self.password_app,
            )
            if not ok:
                logger.warning(f"GetTransaccionesNoConfirmadas falló: {self.ws.Errores}")
                return False

            gtin_clean = gtin.lstrip('0') or gtin
            suc_dest = gln_sucursal.zfill(13)[:13]

            while self.ws.LeerTransaccion():
                tx_gtin = (self.ws.GetParametro("_gtin") or '').lstrip('0') or (self.ws.GetParametro("_gtin") or '')
                tx_serie = (self.ws.GetParametro("_numero_serial") or '').strip()
                tx_dest = (self.ws.GetParametro("_gln_destino") or '').strip()

                if tx_gtin == gtin_clean and tx_serie == serie and tx_dest.zfill(13)[:13] == suc_dest:
                    id_tx = self.ws.GetParametro("_id_transaccion")
                    f_operacion = self._formato_fecha(movimiento.get('FECHA_COMPROBANTE')) or datetime.now().strftime('%d/%m/%Y')

                    self.ws.SendConfirmaTransacc(
                        usuario=self.usuario_app,
                        password=self.password_app,
                        p_ids_transac=id_tx,
                        f_operacion=f_operacion,
                    )

                    if not self.ws.Errores:
                        logger.info(f"3024: recepción {id_tx} confirmada para GTIN={gtin}, serie={serie}")
                        return True

                    logger.warning(f"3024: SendConfirmaTransacc falló para {id_tx}: {self.ws.Errores}")
                    return False

            logger.info(f"3024: no hay recepción pendiente para GTIN={gtin}, serie={serie}")
            return False

        except Exception as e:
            logger.error(f"Error en _resolver_error_3024: {e}\n{traceback.format_exc()}")
            return False

    def _buscar_transaccion_para_3108(self, gtin: str, serie: Optional[str] = None,
                                      gln_sucursal: Optional[str] = None,
                                      es_recepcion: bool = False,
                                      gln_proveedor: Optional[str] = None,
                                      lote: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Busca una transacción en ANMAT para resolver errores 3108/3018.

        Para 3108 (dispensa ya reportada): busca en ANMAT transacciones existentes
        con nuestro GLN + GTIN + serie + lote (id_evento 111 primero, luego cualquiera).
        Para 3018 (recepción ya reportada): busca la recepción original por proveedor.

        Args:
            gtin: Código GTIN del producto
            serie: Número de serie opcional
            gln_sucursal: GLN de la sucursal para filtrar
            es_recepcion: True si es recepción (3018), False si es dispensa (3108)
            gln_proveedor: GLN del proveedor (para recepciones)
            lote: Número de lote para filtrar

        Returns:
            Dict con datos de la transacción si se encuentra, None si no
        """
        try:
            if es_recepcion:
                gln_filtro = str(gln_proveedor).strip() if gln_proveedor else None
                params = {
                    'gtin': gtin,
                    'serie': serie,
                }
                if gln_filtro:
                    params['gln_origen'] = gln_filtro
            else:
                gln_nuestro = str(gln_sucursal).strip() if gln_sucursal else None
                params = {
                    'gtin': gtin,
                    'serie': serie,
                    'gln_origen': gln_nuestro,
                    'id_evento': 111,
                }

            resultado = self.buscar_transacciones_ws(params)

            if resultado.get('error'):
                logger.error(f"Error en búsqueda de transacción para 3108: {resultado.get('error')}")
                return None

            transacciones = resultado.get('transacciones', [])

            if not transacciones and not es_recepcion:
                params.pop('id_evento', None)
                resultado = self.buscar_transacciones_ws(params)
                transacciones = resultado.get('transacciones', [])

            if not transacciones and not es_recepcion and params.get('gln_origen'):
                params['gln_origen'] = None
                resultado = self.buscar_transacciones_ws(params)
                transacciones = resultado.get('transacciones', [])

            if transacciones:
                for tx in transacciones:
                    return tx

            logger.info(f"No se encontró transacción para GTIN={gtin}, serie={serie}")
            return None

        except Exception as e:
            try:
                error_msg = str(e.args[0]) if e.args and len(e.args) > 0 else str(e)
            except:
                error_msg = str(e) if isinstance(e, str) else type(e).__name__
            logger.error(f"Error buscando transacción para 3108: {error_msg}")
            return None

    def _validar_movimiento(self, mov: Dict[str, Any]) -> bool:
        """Valida que un movimiento tenga los datos mínimos requeridos"""
        return bool(mov.get('ID') and mov.get('GTIN') and mov.get('ML_ID'))

    def _validar_gln(self, gln: str) -> bool:
        """Valida un GLN con el algoritmo de Mod 10 (dígito verificador)"""
        if not gln or len(gln) != 13:
            return False
        if not gln.isdigit():
            return False
        pesos = [3, 1] * 6
        suma = sum(int(gln[i]) * pesos[i] for i in range(12))
        digito_verificador = (10 - (suma % 10)) % 10
        return int(gln[12]) == digito_verificador

    def _obtener_gln(self, gln: Optional[str]) -> str:
        """Obtiene GLN formateado a 13 dígitos"""
        if not gln:
            return "9999999999999"
        gln_str = str(gln).strip()
        if len(gln_str) == 0:
            return "9999999999999"
        return gln_str.zfill(13)[:13]

    def _formato_fecha(self, fecha: Optional[str]) -> Optional[str]:
        """Convierte fecha al formato DD/MM/YYYY. Retorna None si no se puede convertir."""
        if not fecha:
            return None

        fecha_str = str(fecha).strip()

        # Ya en formato DD/MM/YYYY
        if len(fecha_str) == 10 and fecha_str[2] == '/' and fecha_str[5] == '/':
            return fecha_str

        # En formato YYYY-MM-DD
        if len(fecha_str) == 10 and fecha_str[4] == '-':
            try:
                dt = datetime.strptime(fecha_str, "%Y-%m-%d")
                return dt.strftime("%d/%m/%Y")
            except:
                return None

        return None

    def procesar_pendientes(self, gln: Optional[str] = None) -> Dict[str, Any]:
        """Procesa movimientos pendientes de reportar a ANMAT.

        Args:
            gln: Si se especifica, filtra por GLN de sucursal y usa sus credenciales.
                 Si es None, procesa todos los movimientos con credenciales globales.
        """
        if gln:
            return self._procesar_por_gln(gln)
        else:
            return self._procesar_todos()

    def procesar_reportables(self, gln: str) -> Dict[str, Any]:
        """Procesa movimientos pendientes + sin confirmar + con error,
        ordenados por ML_ID (recepciones primero).

        Args:
            gln: GLN de sucursal para filtrar y usar sus credenciales.
        """
        logger.info(f"Procesando reportables para GLN: {gln}")
        try:
            sucursal = db.get_sucursal_credenciales(gln)
        except Exception as e:
            logger.error(f"Error al obtener credenciales: {e}")
            return {'total': 0, 'exitosos': 0, 'fallidos': 0,
                    'error': f'Error al conectar a la base de datos: {str(e)}', 'detalles': []}

        if not sucursal or not sucursal.get('anmat_user') or not sucursal.get('anmat_password'):
            return {'total': 0, 'exitosos': 0, 'fallidos': 0,
                    'error': f'No se encontraron credenciales para GLN {gln}', 'detalles': []}

        self.usuario_app = sucursal['anmat_user']
        self.password_app = sucursal['anmat_password']

        movimientos = db.get_movimientos_reportables(gln)
        logger.info(f"Movimientos reportables encontrados: {len(movimientos)}")

        resultados = {
            'total': len(movimientos),
            'exitosos': 0, 'fallidos': 0,
            'gln': gln,
            'sucursal': sucursal.get('sucursal', gln),
            'detalles': []
        }

        for mov in movimientos:
            mov_id = mov.get('ID') or mov.get('id')
            resultado = self.reportar_movimiento(mov, gln=gln)
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
                    resultado='A',
                    error_codigo=None,
                    error_descripcion=None,
                    fecha_reporte=fecha_reporte_db,
                )
                resultados['exitosos'] += 1
            else:
                errores = resultado.get('errores', [])
                error_desc = errores[0] if errores else resultado.get('error', 'Error desconocido')
                db.actualizar_reporte(
                    mov_id=mov_id,
                    transaccion=None,
                    resultado='R',
                    error_codigo=resultado.get('error_codigo'),
                    error_descripcion=error_desc,
                )
                resultados['fallidos'] += 1
            resultados['detalles'].append({
                'mov_id': mov_id,
                'success': resultado.get('success', False),
                'transaccion': resultado.get('transaccion'),
                'error': resultado.get('error') or (resultado.get('errores') or [None])[0],
            })

    @staticmethod
    def _cargar_sync_state() -> Dict[str, Any]:
        try:
            if os.path.exists(AnmatReporter.SYNC_STATE_FILE):
                with open(AnmatReporter.SYNC_STATE_FILE) as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"No se pudo leer estado de sincronización: {e}")
        return {}

    @staticmethod
    def _guardar_sync_state(state: Dict[str, Any]) -> None:
        try:
            with open(AnmatReporter.SYNC_STATE_FILE, 'w') as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            logger.warning(f"No se pudo guardar estado de sincronización: {e}")

    def _sincronizar_anmat_lote(self, gln: str, fecha_desde: str, fecha_hasta: str) -> Dict[str, Any]:
        """
        Batch sync: fetch ALL ANMAT transactions for a date range where we are
        destino (recepciones) or origen (dispensas), index by GTIN+serie,
        and match against local errored/pending records.

        O(2) ANMAT calls vs O(N) individual queries — ~100x faster.
        """
        results = {
            'total': 0, 'corregidos': 0, 'no_encontrados': 0, 'errores': 0,
            'txs_recibidas': 0, 'txs_dispensadas': 0,
            'detalles': []
        }
        try:
            socket.setdefaulttimeout(ANMAT_TIMEOUT)
            params_rec = {'gln_destino': gln, 'fecha_desde': fecha_desde, 'fecha_hasta': fecha_hasta}
            res_rec = self.buscar_transacciones_ws(params_rec)
            txs_recepcion = res_rec.get('transacciones', [])
            results['txs_recibidas'] = len(txs_recepcion)
            logger.info(f"ANMAT: {len(txs_recepcion)} recepciones GLN {gln} [{fecha_desde} - {fecha_hasta}]")

            params_dis = {'gln_origen': gln, 'fecha_desde': fecha_desde, 'fecha_hasta': fecha_hasta}
            res_dis = self.buscar_transacciones_ws(params_dis)
            txs_dispensa = res_dis.get('transacciones', [])
            results['txs_dispensadas'] = len(txs_dispensa)
            logger.info(f"ANMAT: {len(txs_dispensa)} dispensas GLN {gln} [{fecha_desde} - {fecha_hasta}]")

            index_serie = {}
            index_lote = {}
            for tx in txs_recepcion + txs_dispensa:
                tx_gtin = (tx.get('gtin') or '').lstrip('0') or tx.get('gtin', '')
                tx_serie = (tx.get('numero_serial') or '').strip()
                tx_lote = (tx.get('lote') or '').strip().upper()
                if tx_gtin and tx_serie:
                    index_serie[f"{tx_gtin}|{tx_serie}"] = tx
                if tx_gtin and tx_lote:
                    key = f"{tx_gtin}|{tx_lote}"
                    if key not in index_lote:
                        index_lote[key] = tx

            logger.info(f"Index: {len(index_serie)} GTIN+serie, {len(index_lote)} GTIN+lote")

            movimientos = db.get_movimientos_reportables(gln)
            results['total'] = len(movimientos)
            logger.info(f"Locales a corregir: {len(movimientos)}")

            for mov in movimientos:
                mov_id = mov.get('ID') or mov.get('id')
                try:
                    mov_gtin = str(mov.get('GTIN', '') or mov.get('gtin', '')).strip().lstrip('0')
                    mov_serie = str(mov.get('NUMERO_SERIAL', '') or '').strip()
                    mov_lote = str(mov.get('LOTE', '') or mov.get('lote', '') or '').strip().upper()

                    if not mov_gtin:
                        results['no_encontrados'] += 1
                        continue

                    tx = index_serie.get(f"{mov_gtin}|{mov_serie}") if mov_serie else None
                    if not tx and mov_lote:
                        tx = index_lote.get(f"{mov_gtin}|{mov_lote}")

                    if tx:
                        fecha_reporte_db = None
                        f_rep = tx.get('fecha', '')
                        if f_rep and '/' in str(f_rep):
                            partes = str(f_rep).split('/')
                            fecha_reporte_db = f"{partes[2]}-{partes[1]}-{partes[0]}"
                        elif f_rep:
                            fecha_reporte_db = f_rep

                        db.actualizar_reporte(
                            mov_id=mov_id,
                            transaccion=tx.get('codigo_transaccion') or tx.get('id_transaccion') or '',
                            resultado='A',
                            error_codigo=None,
                            error_descripcion=None,
                            fecha_reporte=fecha_reporte_db,
                        )
                        results['corregidos'] += 1
                        results['detalles'].append({
                            'mov_id': mov_id, 'corregido': True,
                            'transaccion': tx.get('codigo_transaccion') or tx.get('id_transaccion'),
                        })
                    else:
                        results['no_encontrados'] += 1
                        results['detalles'].append({
                            'mov_id': mov_id, 'corregido': False, 'error': 'No encontrado en ANMAT',
                        })
                except Exception as e:
                    results['errores'] += 1
                    results['detalles'].append({
                        'mov_id': mov_id, 'corregido': False, 'error': str(e),
                    })

            logger.info(f"Sincronización GLN {gln}: {results['corregidos']} corregidos, {results['no_encontrados']} no encontrados, {results['errores']} errores")
            return results

        except Exception as e:
            logger.error(f"Error en sincronización lote: {e}")
            results['error'] = str(e)
            return results

    def procesar_corregir(self, gln: str, fecha_desde: Optional[str] = None, fecha_hasta: Optional[str] = None) -> Dict[str, Any]:
        """Corrige movimientos pendientes/erróneos vía sincronización batch desde ANMAT.

        Sincronización ÚNICA: busca la fecha del primer error pendiente, trae TODAS
        las transacciones ANMAT desde esa fecha hasta hoy, y las matchea en memoria.

        Si se corta, la próxima vez retoma desde donde quedó (estado persistido en
        anmat_sync_state.json). Una vez completa, no se vuelve a ejecutar.
        """
        logger.info(f"Corrigiendo desde ANMAT para GLN: {gln}")
        try:
            sucursal = db.get_sucursal_credenciales(gln)
        except Exception as e:
            return {'total': 0, 'corregidos': 0, 'error': f'Error al conectar a la base de datos: {str(e)}', 'detalles': []}

        if not sucursal or not sucursal.get('anmat_user') or not sucursal.get('anmat_password'):
            return {'total': 0, 'corregidos': 0, 'error': f'No se encontraron credenciales para GLN {gln}', 'detalles': []}

        self.usuario_app = sucursal['anmat_user']
        self.password_app = sucursal['anmat_password']

        hoy = datetime.now()
        hoy_str = f"{hoy.day:02d}/{hoy.month:02d}/{hoy.year}"

        state = self._cargar_sync_state()
        gln_state = state.get(gln, {})

        if gln_state.get('completa'):
            logger.info(f"GLN {gln}: sincronización ya completada")
            return {
                'total': 0, 'corregidos': 0, 'completa': True,
                'gln': gln, 'sucursal': sucursal.get('sucursal', gln),
                'mensaje': 'Sincronización ANMAT ya completada anteriormente',
            }

        fecha_desde = gln_state.get('proximo_desde')
        if not fecha_desde:
            primer_error = db.get_fecha_primer_error(gln)
            fecha_desde = primer_error or f"01/01/{hoy.year}"
            gln_state['desde'] = fecha_desde
            logger.info(f"GLN {gln}: primer error encontrado {fecha_desde}")

        fecha_hasta = hoy_str
        gln_state['proximo_desde'] = fecha_desde
        gln_state['completa'] = False
        gln_state['actualizado'] = datetime.now().isoformat()
        state[gln] = gln_state
        self._guardar_sync_state(state)

        logger.info(f"GLN {gln}: sincronizando [{fecha_desde} - {fecha_hasta}]")
        resultados = self._sincronizar_anmat_lote(gln, fecha_desde, fecha_hasta)
        resultados['gln'] = gln
        resultados['sucursal'] = sucursal.get('sucursal', gln)
        resultados['periodo'] = f"{fecha_desde} - {fecha_hasta}"

        restantes = resultados.get('total', 1) - resultados.get('corregidos', 0)
        if restantes <= 0 and fecha_desde:
            gln_state['completa'] = True
            gln_state['actualizado'] = datetime.now().isoformat()
            state[gln] = gln_state
            self._guardar_sync_state(state)
            resultados['completa'] = True
            logger.info(f"GLN {gln}: sincronización completada. No quedan movimientos pendientes.")
        else:
            gln_state['proximo_desde'] = fecha_hasta
            gln_state['actualizado'] = datetime.now().isoformat()
            state[gln] = gln_state
            self._guardar_sync_state(state)
            resultados['completa'] = False
            logger.info(f"GLN {gln}: sincronización parcial. Quedan {restantes} movimientos.")

        return resultados

    def _procesar_por_gln(self, gln: str) -> Dict[str, Any]:
        """Procesa pendientes para una sucursal específica con sus credenciales"""
        logger.info(f"Procesando pendientes para GLN: {gln}")

        try:
            sucursal = db.get_sucursal_credenciales(gln)
            logger.info(f"Credenciales obtenidas: {sucursal}")
        except Exception as e:
            logger.error(f"Error al obtener credenciales: {e}")
            return {
                'total': 0,
                'exitosos': 0,
                'fallidos': 0,
                'error': f'Error al conectar a la base de datos: {str(e)}',
                'detalles': []
            }

        if not sucursal or not sucursal.get('anmat_user') or not sucursal.get('anmat_password'):
            logger.warning(f"No se encontraron credenciales para GLN: {gln}")
            return {
                'total': 0,
                'exitosos': 0,
                'fallidos': 0,
                'error': f'No se encontraron credenciales para GLN {gln}',
                'detalles': []
            }

        self.usuario_app = sucursal['anmat_user']
        self.password_app = sucursal['anmat_password']

        # Paso 1: Auto-confirmar transacciones ANMAT con match exacto antes de procesar movimientos
        # Esto evita errores 3034 al reportar recepciones
        auto_confirmadas = 0
        try:
            pendientes_match = self.get_pendientes_con_match(gln)
            for tx in pendientes_match.get('transacciones', []):
                matching = tx.get('matching', {})
                if matching.get('estado') == 'match_exacto':
                    candidato = matching.get('seleccionado')
                    if candidato and candidato.get('cmp_recibido_item_id'):
                        res = self.confirmar_transaccion(
                            tx['id_transaccion'], gln,
                            {**tx, 'cmp_recibido_item_id': candidato['cmp_recibido_item_id']}
                        )
                        if res.get('success'):
                            auto_confirmadas += 1
                            logger.info(f"Auto-confirmada transacción {tx['id_transaccion']}")
                            cri_id = candidato.get('cmp_recibido_item_id')
                            if cri_id:
                                mov_local = db.get_movimiento_por_cmp_recibido_item(cri_id)
                                if mov_local and mov_local.get('ID'):
                                    db.actualizar_reporte(
                                        mov_id=mov_local['ID'],
                                        transaccion=res.get('transaccion', ''),
                                        resultado='A',
                                        error_codigo=None,
                                        error_descripcion=None,
                                        fecha_reporte=tx.get('fecha'),
                                    )
                                    logger.info(f"Traza {mov_local['ID']} actualizada con tx {res.get('transaccion', '')}")
                        else:
                            logger.warning(f"Auto-confirmación falló para {tx['id_transaccion']}: {res.get('error')}")
        except Exception as e:
            logger.error(f"Error en auto-confirmación de transacciones: {e}")

        paso_2 = ", " + str(auto_confirmadas) + " auto-confirmadas" if auto_confirmadas else ""

        try:
            movimientos = db.get_movimientos_pendientes_por_gln(gln)
            logger.info(f"Movimientos pendientes encontrados: {len(movimientos)}{paso_2}")
        except Exception as e:
            logger.error(f"Error al obtener movimientos pendientes: {e}")
            return {
                'total': 0,
                'exitosos': 0,
                'fallidos': 0,
                'error': f'Error al obtener movimientos: {str(e)}',
                'detalles': []
            }

        resultados = {
            'total': len(movimientos),
            'exitosos': 0,
            'fallidos': 0,
            'gln': gln,
            'sucursal': sucursal.get('sucursal', gln),
            'detalles': []
        }

        for mov in movimientos:
            mov_id = mov.get('ID') or mov.get('id')
            resultado = self.reportar_movimiento(mov, gln=gln)
            if resultado.get('success'):
                # Si hay datos actualizados (viene de resolución de 3108), actualizar trazabilidad
                datos_actualizados = resultado.get('datos_actualizados')
                if datos_actualizados:
                    lote_nuevo = datos_actualizados.get('lote')
                    vencimiento_nuevo = datos_actualizados.get('vencimiento')

                    # Obtener valores actuales de la view
                    movimiento_actual = db.get_movimiento_by_id(mov_id)
                    lote_actual = movimiento_actual.get('LOTE', '') if movimiento_actual else ''
                    vencimiento_actual = movimiento_actual.get('VENCIMIENTO', '') if movimiento_actual else ''

                    # Solo actualizar lote/vencimiento si son distintos
                    lote_a_actualizar = None
                    vencimiento_a_actualizar = None

                    if lote_nuevo and str(lote_nuevo).upper() != str(lote_actual or '').upper():
                        lote_a_actualizar = lote_nuevo
                    if vencimiento_nuevo and vencimiento_nuevo != vencimiento_actual:
                        vencimiento_a_actualizar = vencimiento_nuevo

                    if lote_a_actualizar or vencimiento_a_actualizar:
                        db.actualizar_datos_trazabilidad(mov_id, lote_a_actualizar, vencimiento_a_actualizar)
                        logger.info(f"Datos de trazabilidad actualizados para movimiento {mov_id}: lote={lote_a_actualizar}, vencimiento={vencimiento_a_actualizar}")

                # Extraer fecha de reporte original si la resolución la proveyó
                fecha_reporte_db = None
                if datos_actualizados:
                    f_rep = datos_actualizados.get('fecha_reporte')
                    if f_rep and '/' in str(f_rep):
                        partes = str(f_rep).split('/')
                        fecha_reporte_db = f"{partes[2]}-{partes[1]}-{partes[0]}"
                    elif f_rep:
                        fecha_reporte_db = f_rep

                # Siempre actualizar reporte con transacción (importante para 3108/3034)
                db.actualizar_reporte(
                    mov_id=mov_id,
                    transaccion=resultado.get('transaccion', ''),
                    resultado='A',
                    error_codigo=None,
                    error_descripcion=None,
                    fecha_reporte=fecha_reporte_db,
                )
                resultados['exitosos'] += 1
            else:
                errores = resultado.get('errores', [])
                error_desc = errores[0] if errores else resultado.get('error', 'Error desconocido')
                # Extraer código de error del mensaje
                error_codigo = None
                if errores:
                    try:
                        codigo_extraido = errores[0].split(':')[0].strip()
                        # Solo usar si es numérico
                        int(codigo_extraido)
                        error_codigo = codigo_extraido
                    except (ValueError, IndexError):
                        error_codigo = '999'

                transaccion_val = resultado.get('transaccion', '')
                logger.info(f"DEBUG resultado keys: {resultado.keys()}")
                logger.info(f"DEBUG transaccion_val: {transaccion_val}")
                logger.info(f"Guardando - transaccion: {transaccion_val}, error_codigo: {error_codigo}, error_desc: {error_desc[:50] if error_desc else None}")

                db.actualizar_reporte(
                    mov_id=mov_id,
                    transaccion=transaccion_val,
                    resultado='R',
                    error_codigo=error_codigo,
                    error_descripcion=error_desc
                )
                resultados['fallidos'] += 1

            resultados['detalles'].append({
                'id': mov_id,
                'resultado': resultado
            })

        return resultados

    def _procesar_todos(self) -> Dict[str, Any]:
        """Procesa todos los pendientes (todas las sucursales)"""
        sucursales = db.get_sucursales()
        resultados = {
            'total': 0,
            'exitosos': 0,
            'fallidos': 0,
            'por_sucursal': [],
            'detalles': []
        }

        for suc in sucursales:
            gln = suc.get('gln')
            if not gln:
                continue

            if suc.get('anmat_user') and suc.get('anmat_password'):
                res = self._procesar_por_gln(gln)
            else:
                res = {'exitosos': 0, 'fallidos': 0, 'total': 0, 'error': 'Sin credenciales'}

            resultados['por_sucursal'].append({
                'gln': gln,
                'sucursal': suc.get('sucursal', gln),
                'exitosos': res.get('exitosos', 0),
                'fallidos': res.get('fallidos', 0)
            })
            resultados['total'] += res.get('total', 0)
            resultados['exitosos'] += res.get('exitosos', 0)
            resultados['fallidos'] += res.get('fallidos', 0)
            resultados['detalles'].extend(res.get('detalles', []))

        return resultados

    def get_transacciones_no_confirmadas(self, gln: Optional[str] = None) -> Dict[str, Any]:
        """Obtiene transacciones pendientes de confirmación de ANMAT.

        Args:
            gln: Si se especifica, filtra por GLN. Si es None, consulta todas las sucursales.
        """
        if gln:
            return self._get_no_confirmadas_por_gln(gln)
        else:
            return self._get_no_confirmadas_todas()

    def _get_no_confirmadas_por_gln(self, gln: str, dias=90) -> Dict[str, Any]:
        """Obtiene transacciones no confirmadas para un GLN específico"""
        sucursal = db.get_sucursal_credenciales(gln)
        if not sucursal or not sucursal.get('anmat_user') or not sucursal.get('anmat_password'):
            return {
                'transacciones': [],
                'total': 0,
                'error': f'No se encontraron credenciales para GLN {gln}'
            }

        try:
            socket.setdefaulttimeout(ANMAT_TIMEOUT)
            ok = self.ws.GetTransaccionesNoConfirmadas(
                usuario=sucursal['anmat_user'],
                password=sucursal['anmat_password']
            )

            if not ok:
                return {
                    'transacciones': [],
                    'total': 0,
                    'error': f'Error: {self.ws.Errores}'
                }

            transacciones = []
            while self.ws.LeerTransaccion():
                dest = self.ws.GetParametro("_gln_destino")
                orig = self.ws.GetParametro("_gln_origen")
                transacciones.append({
                        'id_transaccion': self.ws.GetParametro("_id_transaccion"),
                        'gtin': self.ws.GetParametro("_gtin"),
                        'lote': self.ws.GetParametro("_lote"),
                        'vencimiento': self.ws.GetParametro("_vencimiento"),
                        'numero_serial': self.ws.GetParametro("_numero_serial"),
                        'fecha': self.ws.GetParametro("_f_evento"),
                        'gln_origen': orig,
                        'gln_destino': dest,
                        'codigo_transaccion': self.ws.GetParametro("_codigoTransaccion"),
                        'n_remito': self.ws.GetParametro("_n_remito"),
                        'n_factura': self.ws.GetParametro("_n_factura"),
                        'razon_social_origen': self.ws.GetParametro("_razon_social_origen"),
                        'razon_social_destino': self.ws.GetParametro("_razon_social_destino"),
                        'd_evento': self.ws.GetParametro("_d_evento"),
                        'id_evento': self.ws.GetParametro("_id_evento"),
                        'nombre': self.ws.GetParametro("_nombre"),
                    })

            return {
                'transacciones': transacciones,
                'total': len(transacciones),
                'gln': gln,
                'sucursal': sucursal.get('sucursal', gln)
            }

        except Exception as e:
            return {
                'transacciones': [],
                'total': 0,
                'error': str(e)
            }

    def _get_no_confirmadas_todas(self) -> Dict[str, Any]:
        """Obtiene transacciones no confirmadas de todas las sucursales"""
        sucursales = db.get_sucursales()
        todas_transacciones = []
        por_sucursal = []

        for suc in sucursales:
            gln = suc.get('gln')
            if not gln:
                continue

            if suc.get('anmat_user') and suc.get('anmat_password'):
                res = self._get_no_confirmadas_por_gln(gln)
            else:
                res = {'transacciones': [], 'total': 0, 'error': 'Sin credenciales'}

            todas_transacciones.extend(res.get('transacciones', []))
            por_sucursal.append({
                'gln': gln,
                'sucursal': suc.get('sucursal', gln),
                'total': res.get('total', 0),
                'error': res.get('error')
            })

        return {
            'transacciones': todas_transacciones,
            'total': len(todas_transacciones),
            'por_sucursal': por_sucursal
        }

    def get_pendientes_con_match(self, gln: Optional[str] = None) -> Dict[str, Any]:
        """Obtiene transacciones no confirmadas de ANMAT con matching automático contra comprobantes locales.

        Para cada transacción pendiente busca en cmp_recibido candidatos que matcheen por:
        - GTIN + n_factura (fuerte)
        - GTIN + gln_origen + fecha ±7 días (medio)
        - GTIN + fecha ±7 días (débil)

        Returns:
            Dict con transacciones enriquecidas con 'matching' info
        """
        try:
            no_confirmadas = self.get_transacciones_no_confirmadas(gln)
            transacciones = no_confirmadas.get('transacciones', [])

            for tx in transacciones:
                gtin = (tx.get('gtin') or '').lstrip('0') or tx.get('gtin', '')
                n_factura = (tx.get('n_factura') or '').strip()
                n_remito = (tx.get('n_remito') or '').strip()
                gln_origen = (tx.get('gln_origen') or '').strip()
                gln_destino = (tx.get('gln_destino') or '').strip()
                tx_fecha = tx.get('fecha', '')

                candidatos = []

                # 1) Match por GTIN + n_factura (fuerte)
                if gtin and n_factura:
                    res = db.buscar_cmp_recibido_por_filtros(
                        gtin=gtin, n_factura=n_factura, gln_sucursal=gln_destino or gln
                    )
                    for r in res:
                        r['_match_score'] = 100
                        r['_match_tipo'] = 'factura'
                    candidatos.extend(res)

                # 2) Match por GTIN + gln_origen + fecha (si no hubo match fuerte)
                if gtin and gln_origen and not any(c.get('_match_score', 0) >= 100 for c in candidatos):
                    fecha_desde, fecha_hasta = self._calcular_rango_fechas(tx_fecha, dias=7)
                    res = db.buscar_cmp_recibido_por_filtros(
                        gtin=gtin, gln_origen=gln_origen, gln_sucursal=gln_destino or gln,
                        fecha_desde=fecha_desde, fecha_hasta=fecha_hasta
                    )
                    for r in res:
                        if not any(c.get('cmp_recibido_id') == r.get('cmp_recibido_id') for c in candidatos):
                            r['_match_score'] = 70
                            r['_match_tipo'] = 'proveedor_fecha'
                    candidatos.extend(r for r in res if not any(
                        c.get('cmp_recibido_id') == r.get('cmp_recibido_id') for c in candidatos))

                # 3) Match por n_remito
                if n_remito and not any(c.get('_match_score', 0) >= 70 for c in candidatos):
                    res = db.buscar_cmp_recibido_por_filtros(
                        n_remito=n_remito, gln_sucursal=gln_destino or gln
                    )
                    for r in res:
                        if not any(c.get('cmp_recibido_id') == r.get('cmp_recibido_id') for c in candidatos):
                            r['_match_score'] = 60
                            r['_match_tipo'] = 'remito'
                    candidatos.extend(r for r in res if not any(
                        c.get('cmp_recibido_id') == r.get('cmp_recibido_id') for c in candidatos))

                # 4) Match por GTIN + fecha (débil) si no hay otro match
                if gtin and not any(c.get('_match_score', 0) >= 50 for c in candidatos):
                    fecha_desde, fecha_hasta = self._calcular_rango_fechas(tx_fecha, dias=15)
                    res = db.buscar_cmp_recibido_por_filtros(
                        gtin=gtin, gln_sucursal=gln_destino or gln,
                        fecha_desde=fecha_desde, fecha_hasta=fecha_hasta
                    )
                    for r in res:
                        if not any(c.get('cmp_recibido_id') == r.get('cmp_recibido_id') for c in candidatos):
                            r['_match_score'] = 40
                            r['_match_tipo'] = 'gtin_fecha'
                    candidatos.extend(r for r in res if not any(
                        c.get('cmp_recibido_id') == r.get('cmp_recibido_id') for c in candidatos))

                # Ordenar por score descendente
                candidatos.sort(key=lambda c: c.get('_match_score', 0), reverse=True)

                # Determinar estado de matching
                if not candidatos:
                    tx['matching'] = {'estado': 'sin_match', 'candidatos': [], 'mejor_score': 0}
                elif candidatos[0].get('_match_score', 0) >= 100:
                    tx['matching'] = {'estado': 'match_exacto', 'candidatos': candidatos[:3],
                                      'mejor_score': 100, 'seleccionado': candidatos[0]}
                elif candidatos[0].get('_match_score', 0) >= 60:
                    tx['matching'] = {'estado': 'match_parcial', 'candidatos': candidatos[:3],
                                      'mejor_score': candidatos[0].get('_match_score', 0)}
                else:
                    tx['matching'] = {'estado': 'match_debil', 'candidatos': candidatos[:3],
                                      'mejor_score': candidatos[0].get('_match_score', 0)}

            return {
                'transacciones': transacciones,
                'total': len(transacciones),
                'resumen': {
                    'match_exacto': sum(1 for t in transacciones if t.get('matching', {}).get('estado') == 'match_exacto'),
                    'match_parcial': sum(1 for t in transacciones if t.get('matching', {}).get('estado') == 'match_parcial'),
                    'match_debil': sum(1 for t in transacciones if t.get('matching', {}).get('estado') == 'match_debil'),
                    'sin_match': sum(1 for t in transacciones if t.get('matching', {}).get('estado') == 'sin_match'),
                },
                'por_sucursal': no_confirmadas.get('por_sucursal', [])
            }
        except Exception as e:
            logger.error(f"Error en get_pendientes_con_match: {e}\n{traceback.format_exc()}")
            return {
                'transacciones': [],
                'total': 0,
                'error': f'Error procesando matching: {str(e)}',
            }

    def _calcular_rango_fechas(self, fecha_str: str, dias: int = 7):
        """Calcula rango de fechas ±dias alrededor de una fecha."""
        if not fecha_str:
            return None, None
        try:
            if '/' in fecha_str:
                dt = datetime.strptime(fecha_str.strip(), '%d/%m/%Y')
            else:
                dt = datetime.strptime(fecha_str.strip()[:10], '%Y-%m-%d')
            desde = (dt - timedelta(days=dias)).strftime('%Y-%m-%d')
            hasta = (dt + timedelta(days=dias)).strftime('%Y-%m-%d')
            return desde, hasta
        except (ValueError, IndexError):
            return None, None

    def get_diferencias(self) -> Dict[str, Any]:
        """Compara transacciones en ANMAT vs base de datos local"""
        try:
            ok = self.ws.GetTransaccionesNoConfirmadas(
                usuario=self.usuario_app,
                password=self.password_app
            )

            if not ok:
                return {
                    'total_anmat': 0,
                    'total_local': 0,
                    'en_anmat_no_local': [],
                    'en_local_no_anmat': [],
                    'error': f'Error obteniendo transacciones: {self.ws.Errores}'
                }

            transacciones_anmat = []
            while self.ws.LeerTransaccion():
                trans = {}
                for clave in ['_id_transaccion', '_gtin', '_lote', '_numero_serial',
                              '_codigoTransaccion']:
                    valor = self.ws.GetParametro(clave)
                    if valor:
                        trans[clave] = valor
                transacciones_anmat.append(trans)

            movimientos_local = db.get_movimientos_reportados_mes()

            codigos_anmat = {t.get('_codigoTransaccion') for t in transacciones_anmat if t.get('_codigoTransaccion')}
            codigos_local = {m.get('anmat_transaccion') for m in movimientos_local if m.get('anmat_transaccion')}

            en_anmat_no_local = [t for t in transacciones_anmat if t.get('_codigoTransaccion') not in codigos_local]
            en_local_no_anmat = [m for m in movimientos_local if m.get('anmat_transaccion') not in codigos_anmat]

            return {
                'total_anmat': len(transacciones_anmat),
                'total_local': len(movimientos_local),
                'en_anmat_no_local': en_anmat_no_local,
                'en_local_no_anmat': en_local_no_anmat,
            }

        except Exception as e:
            return {
                'error': str(e),
                'en_anmat_no_local': [],
                'en_local_no_anmat': [],
                'total_anmat': 0,
                'total_local': 0,
            }

    def buscar_transacciones_por_gtin_serie(self, gtin: str, serie: Optional[str] = None) -> Dict[str, Any]:
        """Busca transacciones en ANMAT por GTIN y serie.

        Se usa para investigar errores 3108 que indican que un medicamento
        ya fue reportado con los mismos datos.

        Args:
            gtin: Código GTIN del producto (sin ceros adelante)
            serie: Número de serie opcional
        """
        try:
            gtin_clean = str(gtin).lstrip('0') or gtin

            transacciones = []

            # Primero obtener transacciones no confirmadas y filtrar por gtin
            ok = self.ws.GetTransaccionesNoConfirmadas(
                usuario=self.usuario_app,
                password=self.password_app
            )

            if ok:
                while self.ws.LeerTransaccion():
                    trans_gtin = self.ws.GetParametro("_gtin")
                    trans_serie = self.ws.GetParametro("_numero_serial")

                    # Comparar GTIN (puede venir con o sin ceros)
                    gtin_match = (trans_gtin or '').lstrip('0') == gtin_clean.lstrip('0')
                    serie_match = True
                    if serie and trans_serie:
                        serie_match = trans_serie == serie

                    if gtin_match and serie_match:
                        transacciones.append({
                            'id_transaccion': self.ws.GetParametro("_id_transaccion"),
                            'gtin': trans_gtin,
                            'lote': self.ws.GetParametro("_lote"),
                            'vencimiento': self.ws.GetParametro("_vencimiento"),
                            'numero_serial': trans_serie,
                            'fecha': self.ws.GetParametro("_f_evento"),
                            'gln_origen': self.ws.GetParametro("_gln_origen"),
                            'gln_destino': self.ws.GetParametro("_gln_destino"),
                            'codigo_transaccion': self.ws.GetParametro("_codigoTransaccion"),
                        })

            return {
                'transacciones': transacciones,
                'total': len(transacciones),
                'gtin_buscado': gtin_clean,
                'serie_buscada': serie
            }

        except Exception as e:
            logger.error(f"Error buscando transacciones por GTIN/serie: {e}")
            return {
                'transacciones': [],
                'total': 0,
                'error': str(e)
            }

    def estadisticas_por_sucursal(self) -> Dict[str, Any]:
        """Recopila información por GLN de sucursal.

        Para cada GLN existente en la base de datos obtiene:
        - cantidad de movimientos reportados
        - cantidad pendientes
        - cantidad de errores
        - transacciones ANMAT pendientes de confirmar filtradas por GLN
        """
        glns = db.get_sucursales_con_gln()
        resumen = {}

        for gln in glns:
            rep = db.get_movimientos_reportados_por_gln(gln)
            pen = db.get_movimientos_pendientes_por_gln(gln)
            err = db.get_errores_por_gln(gln)

            # consultar ANMAT pendientes y filtrarlas por el gln
            anmat_list = []
            ok = self.ws.GetTransaccionesNoConfirmadas(
                usuario=self.usuario_app,
                password=self.password_app
            )
            if ok:
                while self.ws.LeerTransaccion():
                    dest = self.ws.GetParametro("_gln_destino")
                    orig = self.ws.GetParametro("_gln_origen")
                    if dest == gln or orig == gln:
                        entry = {
                            'id_transaccion': self.ws.GetParametro("_id_transaccion"),
                            'gtin': self.ws.GetParametro("_gtin"),
                            'lote': self.ws.GetParametro("_lote"),
                            'fecha': self.ws.GetParametro("_f_evento"),
                        }
                        anmat_list.append(entry)
            else:
                # error en consulta ANMAT
                anmat_error = self.ws.Errores

            resumen[gln] = {
                'reportados': len(rep),
                'pendientes': len(pen),
                'errores': len(err),
                'anmat_pendientes': anmat_list,
            }
        return resumen

    def buscar_transacciones_ws(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Busca transacciones en ANMAT con todos los filtros de GetTransaccionesWS.

        Args:
            params: Diccionario con los filtros:
                - gtin: Código GTIN del producto
                - serie: Número de serie
                - gln_origen: GLN del agente origen
                - gln_destino: GLN del agente destino
                - id_evento: ID del tipo de evento (101, 111, etc.)
                - fecha_desde: Fecha desde (DD/MM/YYYY)
                - fecha_hasta: Fecha hasta (DD/MM/YYYY)
                - n_remito: Número de remito
                - n_factura: Número de factura
        """
        try:
            socket.setdefaulttimeout(ANMAT_TIMEOUT)
            gtin = params.get('gtin', '')
            serie = params.get('serie', '')
            gln_origen = params.get('gln_origen', '')
            gln_destino = params.get('gln_destino', '')
            id_evento = params.get('id_evento', '')
            fecha_desde = params.get('fecha_desde', '')
            fecha_hasta = params.get('fecha_hasta', '')
            n_remito = params.get('n_remito', '')
            n_factura = params.get('n_factura', '')

            gtin_clean = str(gtin).lstrip('0') if gtin else None
            if gtin_clean == '0':
                gtin_clean = None

            kwargs = {}
            if gtin_clean:
                kwargs['id_medicamento'] = gtin_clean
            if gln_origen:
                kwargs['id_agente_origen'] = gln_origen
            if gln_destino:
                kwargs['id_agente_destino'] = gln_destino
            if id_evento:
                try:
                    kwargs['id_evento'] = int(id_evento)
                except ValueError:
                    pass
            if fecha_desde:
                kwargs['fecha_desde_t'] = fecha_desde
            if fecha_hasta:
                kwargs['fecha_hasta_t'] = fecha_hasta
            if n_remito:
                kwargs['n_remito'] = n_remito
            if n_factura:
                kwargs['n_factura'] = n_factura

            ok = self.ws.GetTransaccionesWS(
                usuario=self.usuario_app,
                password=self.password_app,
                **kwargs
            )

            if not ok:
                raw = self.ws.Errores
                if isinstance(raw, (list, tuple)):
                    errores = [str(e) for e in raw]
                elif raw:
                    errores = [str(raw)]
                else:
                    errores = [self.ws.Excepcion or 'Error en consulta']
                return {
                    'transacciones': [],
                    'total': 0,
                    'error': '; '.join(errores)
                }

            transacciones = []
            while self.ws.LeerTransaccion():
                trans_serie = self.ws.GetParametro("_numero_serial") or ''

                if serie and serie != trans_serie:
                    continue

                transacciones.append({
                    'id_transaccion': self.ws.GetParametro("_id_transaccion"),
                    'gtin': self.ws.GetParametro("_gtin"),
                    'numero_serial': trans_serie,
                    'lote': self.ws.GetParametro("_lote"),
                    'vencimiento': self.ws.GetParametro("_vencimiento"),
                    'fecha': self.ws.GetParametro("_f_evento"),
                    'gln_origen': self.ws.GetParametro("_gln_origen"),
                    'gln_destino': self.ws.GetParametro("_gln_destino"),
                    'codigo_transaccion': self.ws.GetParametro("_codigoTransaccion"),
                    'id_estado': self.ws.GetParametro("_id_estado"),
                })

            return {
                'transacciones': transacciones,
                'total': len(transacciones),
                'filtros': kwargs
            }

        except Exception as e:
            logger.error(f"Error en buscar_transacciones_ws: {e}")
            return {
                'transacciones': [],
                'total': 0,
                'error': str(e)
            }


    def _crear_comprobante_desde_tx(self, tx: Dict[str, Any], gln_destino: str) -> Optional[int]:
        """Crea cmp_recibido + item a partir de datos de una transacción ANMAT.
        Retorna cmp_recibido_item_id o None si no se puede crear."""
        gtin = (tx.get('gtin') or '').lstrip('0') or tx.get('gtin', '')
        gln_origen = tx.get('gln_origen', '')

        catalogo = db.get_catalogo_por_gtin(gtin)
        if not catalogo:
            logger.warning(f"GTIN {gtin} no encontrado en catálogo, no se puede crear comprobante")
            return None

        sucursal = db.get_sucursal_por_gln(gln_destino)
        if not sucursal:
            logger.warning(f"Sucursal con GLN {gln_destino} no encontrada")
            return None

        prestador = db.get_prestador_por_gln(gln_origen) if gln_origen else None
        id_persona = prestador['persona_id'] if prestador else None

        n_factura = (tx.get('n_factura') or '').strip()
        n_remito = (tx.get('n_remito') or '').strip()
        if n_factura:
            letra = n_factura[0].upper() if n_factura[0].isalpha() else 'A'
        elif n_remito:
            letra = 'R'
        else:
            letra = None

        id_tipo_cmp = db.get_id_tipo_cmp_recepcion(letra)
        if not id_tipo_cmp:
            logger.error("No se encontró id_tipo_cmp para recepción en cmp_tipo")
            return None

        # Parsear pto_vta / id_cmp del número de comprobante (formato: {letra}{pto_vta:03}{id_cmp:09})
        cmp_str = n_factura or n_remito or ''
        pto_vta, id_cmp_num = 0, 0
        if cmp_str:
            limpio = cmp_str.lstrip('ABCDEFGHIJKLMNOPQRSTUVWXYZ')
            partes = limpio.replace('-', '').replace('/', '').strip()
            if len(partes) >= 3:
                try:
                    pto_vta = int(partes[:3])
                    resto = partes[3:]
                    id_cmp_num = int(resto) if resto else 0
                except (ValueError, IndexError):
                    try:
                        pto_vta = int(partes[:4]) if len(partes) >= 4 else 0
                        id_cmp_num = int(partes[4:]) if len(partes) > 4 else int(partes) if partes else 0
                    except (ValueError, IndexError):
                        pass

        fecha = tx.get('fecha') or datetime.now().strftime('%d/%m/%Y')
        cmp_recibido_id = db.crear_cmp_recibido(
            id_tipo_cmp=id_tipo_cmp,
            id_persona=id_persona,
            id_sucursal=sucursal['id_sucursal'],
            fecha=fecha,
            pto_vta=pto_vta,
            id_cmp_num=id_cmp_num,
        )
        if not cmp_recibido_id:
            logger.error("No se pudo crear cmp_recibido")
            return None

        cmp_item_id = db.crear_cmp_recibido_item(cmp_recibido_id, catalogo['id_catalogo'])
        if not cmp_item_id:
            logger.error("No se pudo crear cmp_recibido_items")
            return None

        logger.info(f"Comprobante creado: cmp_recibido={cmp_recibido_id}, item={cmp_item_id}")
        return cmp_item_id

    def confirmar_transaccion(self, id_transaccion: str, gln: str,
                              tx_data: Dict[str, Any],
                              f_operacion: Optional[str] = None) -> Dict[str, Any]:
        """Confirma una transacción ANMAT: crea comprobante local si no existe y llama SendConfirmaTransacc."""
        sucursal = db.get_sucursal_credenciales(gln)
        if not sucursal:
            return {'success': False, 'error': f'Sin credenciales para GLN {gln}'}

        usuario = sucursal['anmat_user']
        password = sucursal['anmat_password']
        gtin = (tx_data.get('gtin') or '').lstrip('0') or tx_data.get('gtin', '')
        lote = tx_data.get('lote')
        serie = tx_data.get('numero_serial')
        vencimiento = tx_data.get('vencimiento')
        id_evento = tx_data.get('id_evento')
        try:
            ml_id = int(id_evento) if id_evento else 101
        except (ValueError, TypeError):
            ml_id = 101

        cmp_item_id = tx_data.get('cmp_recibido_item_id')
        if not cmp_item_id:
            matches = db.buscar_cmp_recibido_por_gtin(gtin, gln=gln)

            if len(matches) == 1:
                m = matches[0]
                cmp_item_id = m.get('cmp_recibido_item_id')
                if not m.get('ctm_id'):
                    db.crear_registro_trazabilidad(cmp_item_id, gtin=gtin, lote=lote,
                                                   numero_serial=serie, caducidad=vencimiento, ml_id=ml_id)
                else:
                    db.actualizar_datos_trazabilidad(m['ctm_id'], lote, vencimiento)
            elif len(matches) > 1:
                logger.warning(f"Múltiples comprobantes para GTIN {gtin}, no se puede auto-seleccionar")
                return {'success': False, 'error': 'Múltiples comprobantes posibles. Seleccioná manualmente.',
                        'candidatos': matches, 'multiple': True}
            else:
                logger.info(f"Ningún comprobante para GTIN {gtin}, creando desde transacción ANMAT...")
                cmp_item_id = self._crear_comprobante_desde_tx(tx_data, gln)
                if not cmp_item_id:
                    return {'success': False, 'error': 'No se pudo crear el comprobante'}
                db.crear_registro_trazabilidad(cmp_item_id, gtin=gtin, lote=lote,
                                               numero_serial=serie, caducidad=vencimiento, ml_id=ml_id)
        else:
            logger.info(f"Usando cmp_recibido_item_id={cmp_item_id} proporcionado explícitamente")

        if not f_operacion:
            f_operacion = datetime.now().strftime('%d/%m/%Y')

        try:
            self.ws.SendConfirmaTransacc(
                usuario=usuario,
                password=password,
                p_ids_transac=id_transaccion,
                f_operacion=f_operacion,
            )
            errores = self.ws.Errores or []
            codigo = getattr(self.ws, 'CodigoTransaccion', None)
            if errores:
                return {'success': False, 'errores': errores, 'cmp_item_id': cmp_item_id}
            return {'success': True, 'codigo_transaccion': codigo, 'cmp_item_id': cmp_item_id}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def alertar_transaccion(self, id_transaccion: str, gln: str) -> Dict[str, Any]:
        """Envía alerta sobre una transacción ANMAT (SendAlertaTransacc)."""
        sucursal = db.get_sucursal_credenciales(gln)
        if not sucursal:
            return {'success': False, 'error': f'Sin credenciales para GLN {gln}'}
        try:
            self.ws.SendAlertaTransacc(
                usuario=sucursal['anmat_user'],
                password=sucursal['anmat_password'],
                p_ids_transac_ws=id_transaccion,
            )
            errores = self.ws.Errores or []
            if errores:
                return {'success': False, 'errores': errores}
            return {'success': True}
        except Exception as e:
            return {'success': False, 'error': str(e)}


# Instancia global del reportador
reporter = AnmatReporter()
