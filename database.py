from firebird.driver import connect as fb_connect
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
import logging
import threading
from datetime import datetime, timedelta
from config import FIREBIRD_CONN_STRING, FIREBIRD_USER, FIREBIRD_PASSWORD, FIREBIRD_ROLE, FIREBIRD_HOST, FIREBIRD_PORT, FIREBIRD_DB, FIREBIRD_CONNECT_TIMEOUT

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class FirebirdConnection:
    _instance = None
    _class_lock = threading.Lock()

    def __new__(cls):
        with cls._class_lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._connection = None
                cls._instance._last_error = None
                # RLock: reentrant so _execute_query can be called from within
                # methods that already hold the lock without deadlocking.
                cls._instance._op_lock = threading.RLock()
        return cls._instance

    def _ensure_connection(self):
        """Get or reconnect. Must be called with _op_lock already held."""
        if self._connection is not None:
            try:
                cur = self._connection.cursor()
                cur.execute("SELECT 1 FROM RDB$DATABASE")
                cur.fetchone()
                cur.close()
                return self._connection
            except Exception:
                logger.info("Conexión Firebird perdida, reconectando...")
                try:
                    self._connection.close()
                except Exception:
                    pass
                self._connection = None

        def _do_connect():
            return fb_connect(
                f"{FIREBIRD_HOST}/{FIREBIRD_PORT}:{FIREBIRD_DB}",
                user=FIREBIRD_USER,
                password=FIREBIRD_PASSWORD,
                role=FIREBIRD_ROLE,
                charset='UTF8'
            )

        with ThreadPoolExecutor(max_workers=1) as pool:
            fut = pool.submit(_do_connect)
            try:
                self._connection = fut.result(timeout=FIREBIRD_CONNECT_TIMEOUT)
            except FutureTimeout:
                logger.error(f"Firebird no responde después de {FIREBIRD_CONNECT_TIMEOUT}s")
                raise ConnectionError(
                    f"No se pudo conectar a Firebird ({FIREBIRD_HOST}:{FIREBIRD_PORT}) "
                    f"en {FIREBIRD_CONNECT_TIMEOUT}s. Verificá la red."
                )

        logger.info("Conexión a Firebird establecida correctamente")
        return self._connection

    def connect(self):
        """Conecta a Firebird con reconexión automática (thread-safe)."""
        with self._op_lock:
            try:
                return self._ensure_connection()
            except Exception as e:
                logger.error(f"Error al conectar a Firebird: {e}")
                self._last_error = str(e)
                raise

    def test_connection(self):
        """Prueba la conexión a la base de datos."""
        try:
            with self._op_lock:
                conn = self._ensure_connection()
                cur = conn.cursor()
                cur.execute("SELECT 1 FROM RDB$DATABASE")
                cur.fetchone()
                cur.close()
            return True, "Conexión OK"
        except Exception as e:
            return False, str(e)

    def close(self):
        """Cierra la conexión."""
        with self._op_lock:
            if self._connection:
                try:
                    self._connection.close()
                except Exception:
                    pass
                self._connection = None

    def _base_query(self):
        """Query base completa con todos los campos necesarios para ANMAT"""
        return """
            select t.id,
                coalesce(se.gln, sr.gln) gln_sucursal,
                coalesce(se.cuit, sr.cuit) cuit_sucursal,
                p1.cui cuil_afiliado,
                p1.primer_nombre nombre_afiliado,
                p1.segundo_nombre apellido_afiliado,
                pf.numero_doc dni_afiliado,
                coalesce(pr.gln, '') gln_proveedor,
                pr.prestador,
                coalesce(t1.letra || lpad(e.pto_vta, 3, '0') || lpad(e.id_cmp, 9, '0'), t2.letra || lpad(r.pto_vta, 3, '0') || lpad(r.id_cmp, 9, '0')) comprobante,
                coalesce(t1.letra, t2.letra) letra_comprobante,
                coalesce(e.fecha_emitido, r.fecha_comprobante) fecha_comprobante,
                c.codigo gtin,
                c.descripcion catalogo,
                t.lote,
                t.caducidad vencimiento,
                t.alta,
                t.nro_serie NUMERO_SERIAL,
                t.ml_id,
                t.ejecucion,
                t.anmat_transaccion,
                t.anmat_fh_reporte,
                t.anmat_resultado,
                t.anmat_error_codigo,
                t.anmat_error_descripcion,
                t.cmp_emitido_item_id,
                t.cmp_recibido_item_id,
                t.cmp_emitido_id,
                t.cmp_recibido_id,
                pl.calle direccion_afiliado,
                pl.localidad localidad_afiliado,
                pl.cp cp_afiliado
            from view_cat_traza_mov t
            left join catalogo c on t.id_catalogo = c.id
            left join cmp_emitido e on e.id = t.cmp_emitido_id
            left join cmp_tipo t1 on t1.id_tipo_cmp = e.id_tipo_cmp
            left join sucursal se on se.id_sucursal = e.id_sucursal
            left join persona p1 on p1.id = e.id_persona
            left join persona_fisica pf on pf.id_persona = p1.id_persona
            left join cmp_recibido r on r.id = t.cmp_recibido_id
            left join cmp_tipo t2 on t2.id_tipo_cmp = r.id_tipo_cmp
            left join view_prestador pr on pr.id_persona= r.id_persona
            left join sucursal sr on sr.id_sucursal = r.id_sucursal
            left join persona p2 on p2.id = r.id_persona
            left join persona_localizacion(p1.id) pl on 1 = 1
            where (((e.estado>0)  and (r.cerrado is null)) or
            ((e.estado is null)  and (r.cerrado=1)))
        """

    def _execute_query(self, query, params=()):
        """Ejecuta una query parametrizada y retorna lista de dicts"""
        logger.debug(f"EJECUTANDO QUERY: {query[:200]}...")
        with self._op_lock:
            conn = self._ensure_connection()
            cursor = conn.cursor()
            try:
                cursor.execute(query, params)
                if cursor.description is None:
                    return []
                columns = [desc[0] for desc in cursor.description]
                results = [dict(zip(columns, row)) for row in cursor.fetchall()]
                logger.debug(f"QUERY RETORNO: {len(results)} resultados")
                return results
            finally:
                try:
                    cursor.close()
                except Exception:
                    pass
                try:
                    conn.commit()
                except Exception:
                    pass

    def get_dashboard_stats(self, gln=None):
        """Conteos para el dashboard usando COUNT (rápido, sin traer filas completas)."""
        gln_cond = f"AND COALESCE(se.gln, sr.gln) = ?" if gln else ""
        params = (gln,) if gln else ()

        base = """
            FROM cat_traza_mov t
            LEFT JOIN CMP_EMITIDO_ITEM cei ON cei.id = t.CMP_EMITIDO_ITEM_ID
            LEFT JOIN cmp_emitido e ON e.id = cei.CMP_EMITIDO_ID
            LEFT JOIN sucursal se ON se.id_sucursal = e.id_sucursal
            LEFT JOIN CMP_RECIBIDO_ITEMS cri  ON cri.id = t.CMP_RECIBIDO_ITEM_ID
            LEFT JOIN cmp_recibido r ON r.id = cri.cmp_recibido_id
            LEFT JOIN sucursal sr ON sr.id_sucursal = r.id_sucursal
            WHERE (1=1)
        """

        with self._op_lock:
            conn = self._ensure_connection()
            cursor = conn.cursor()

            def count(extra):
                cursor.execute(f"SELECT COUNT(t.id) {base} {gln_cond} {extra}", params)
                return cursor.fetchone()[0]

            stats = {
                'ok_hoy':       count("AND t.anmat_resultado = 1 AND CAST(t.anmat_fh_reporte AS DATE) = CURRENT_DATE"),
                'error_hoy':    count("AND t.anmat_resultado = 0 AND CAST(t.anmat_fh_reporte AS DATE) = CURRENT_DATE"),
                'pendientes':   count("AND t.anmat_resultado IS NULL AND t.anmat_error_codigo IS NULL AND (t.cmp_recibido_item_id IS NULL OR r.cerrado = 1)"),
                'con_error':    count("""AND t.anmat_resultado = 0
                                       AND EXTRACT(YEAR FROM t.anmat_fh_reporte) = EXTRACT(YEAR FROM CURRENT_DATE)
                                       AND EXTRACT(MONTH FROM t.anmat_fh_reporte) = EXTRACT(MONTH FROM CURRENT_DATE)"""),
                'ok_mes':       count("""AND t.anmat_resultado = 1
                                       AND EXTRACT(YEAR FROM t.anmat_fh_reporte) = EXTRACT(YEAR FROM CURRENT_DATE)
                                       AND EXTRACT(MONTH FROM t.anmat_fh_reporte) = EXTRACT(MONTH FROM CURRENT_DATE)"""),
                'ingresos_hoy': count("AND t.ml_id IN (101, 108) AND t.anmat_resultado = 1 AND CAST(t.anmat_fh_reporte AS DATE) = CURRENT_DATE"),
                'egresos_hoy':  count("AND t.ml_id IN (111, 98)  AND t.anmat_resultado = 1 AND CAST(t.anmat_fh_reporte AS DATE) = CURRENT_DATE"),
            }
            cursor.close()
            try:
                conn.commit()
            except Exception:
                pass
        return stats

    def get_movimientos_reportados_mes(self):
        """Movimientos reportados en el mes actual a ANMAT"""
        query = self._base_query() + """
            AND EXTRACT(YEAR FROM t.anmat_fh_reporte) = EXTRACT(YEAR FROM CURRENT_DATE)
            AND EXTRACT(MONTH FROM t.anmat_fh_reporte) = EXTRACT(MONTH FROM CURRENT_DATE)
            ORDER BY t.anmat_fh_reporte DESC
        """
        return self._execute_query(query)

    # ---------- consultas por GLN ---------------------------------
    def get_sucursales_con_gln(self):
        """Devuelve lista de GLN únicos de sucursales no nulos"""
        try:
            query = """
                SELECT DISTINCT s.gln AS gln_sucursal
                FROM sucursal s
                WHERE s.gln IS NOT NULL
            """
            with self._op_lock:
                conn = self._ensure_connection()
                cursor = conn.cursor()
                cursor.execute(query)
                result = [row[0] for row in cursor.fetchall()]
                cursor.close()
                conn.commit()
            return result
        except Exception:
            return []

    def get_sucursales(self):
        """Devuelve lista de sucursales con GLN, nombre y credenciales ANMAT"""
        try:
            query = """
                SELECT s.id, s.sucursal, s.gln, s.anmat_user, s.anmat_passwd as anmat_password
                FROM sucursal s
                WHERE s.gln IS NOT NULL
                ORDER BY s.sucursal
            """
            with self._op_lock:
                conn = self._ensure_connection()
                cursor = conn.cursor()
                cursor.execute(query)
                columns = [desc[0].lower() for desc in cursor.description]
                result = [dict(zip(columns, row)) for row in cursor.fetchall()]
                cursor.close()
                conn.commit()
            return result
        except Exception as e:
            logger.error(f"Error get_sucursales: {e}")
            return []

    def get_sucursal_credenciales(self, gln):
        """Devuelve credenciales ANMAT para un GLN específico"""
        try:
            query = """
                SELECT s.anmat_user, s.anmat_passwd
                FROM sucursal s
                WHERE s.gln = ?
            """
            with self._op_lock:
                conn = self._ensure_connection()
                cursor = conn.cursor()
                cursor.execute(query, (gln,))
                row = cursor.fetchone()
                cursor.close()
                conn.commit()
            if row:
                return {
                    'anmat_user': row[0],
                    'anmat_password': row[1]
                }
            return None
        except Exception as e:
            logger.error(f"Error get_sucursal_credenciales: {e}")
            return None

    def _query_por_gln(self, gln, extra_condition=""):
        """Retorna (query, params) filtrada por gln de forma segura."""
        if not gln:
            return self._base_query() + f"\n            {extra_condition}", ()
        return (
            self._base_query() + f"\n            AND COALESCE(se.gln, sr.gln) = ?\n            {extra_condition}",
            (gln,)
        )

    def get_movimientos_reportados_por_gln(self, gln):
        cond = "AND (t.anmat_fh_reporte IS NOT NULL)"
        q, p = self._query_por_gln(gln, cond)
        return self._execute_query(q, p)

    def get_movimientos_pendientes_por_gln(self, gln):
        cond = "AND (t.anmat_transaccion IS NULL)"
        q, p = self._query_por_gln(gln, cond)
        return self._execute_query(q, p)

    def get_errores_por_gln(self, gln):
        cond = "AND (t.anmat_error_codigo IS NOT NULL)"
        q, p = self._query_por_gln(gln, cond)
        return self._execute_query(q, p)

    def get_movimientos_reportables(self, gln):
        """Movimientos pendientes + sin confirmar + con error, ordenados por ML_ID (recepciones primero)."""
        cond = (
            "AND ((t.anmat_transaccion IS NULL) "
            "OR (t.anmat_resultado = 0) "
            "OR (t.anmat_resultado IS NULL AND t.anmat_transaccion IS NOT NULL)) "
            "ORDER BY CASE WHEN t.ml_id IN (101,108) THEN 0 ELSE 1 END, t.ml_id"
        )
        q, p = self._query_por_gln(gln, cond)
        return self._execute_query(q, p)

    def get_fecha_primer_error(self, gln):
        """Retorna la fecha (DD/MM/YYYY) del error más antiguo para un GLN."""
        query = """
            SELECT MIN(t.ejecucion) as primer_error
            FROM view_cat_traza_mov t
            LEFT JOIN cmp_emitido e ON e.id = t.cmp_emitido_id
            LEFT JOIN cmp_recibido r ON r.id = t.cmp_recibido_id
            LEFT JOIN sucursal se ON se.id_sucursal = e.id_sucursal
            LEFT JOIN sucursal sr ON sr.id_sucursal = r.id_sucursal
            WHERE COALESCE(se.gln, sr.gln) = ?
            AND ((t.anmat_transaccion IS NULL)
              OR (t.anmat_resultado = 0)
              OR (t.anmat_resultado IS NULL AND t.anmat_transaccion IS NOT NULL))
        """
        rows = self._execute_query(query, (gln,))
        if rows and rows[0].get('PRIMER_ERROR'):
            fecha = rows[0]['PRIMER_ERROR']
            if isinstance(fecha, datetime):
                return fecha.strftime('%d/%m/%Y')
            return str(fecha)
        return None

    def get_ingresos_hoy(self):
        """Ingresos reportados hoy (ml_id en 101, 108 - recepciones)"""
        query = self._base_query() + f"""
            AND (t.ml_id IN (101, 108))
            AND (CAST(t.anmat_fh_reporte AS DATE) = CURRENT_DATE)
            ORDER BY t.anmat_fh_reporte DESC
        """
        return self._execute_query(query)

    def get_egresos_hoy(self):
        """Egresos reportados hoy (ml_id en 111, 98 - dispensación y distribución)"""
        query = self._base_query() + f"""
            AND (t.ml_id IN (111, 98))
            AND (CAST(t.anmat_fh_reporte AS DATE) = CURRENT_DATE)
            ORDER BY t.anmat_fh_reporte DESC
        """
        return self._execute_query(query)

    def get_ingresos_por_gln(self, gln):
        """Ingresos reportados hoy para un GLN específico"""
        try:
            query = self._base_query() + """
                AND (t.ml_id IN (101, 108))
                AND (CAST(t.anmat_fh_reporte AS DATE) = CURRENT_DATE)
                AND (COALESCE(se.gln, sr.gln) = ?)
                ORDER BY t.anmat_fh_reporte DESC
            """
            return self._execute_query(query, (gln,))
        except Exception:
            return self.get_ingresos_hoy()

    def get_egresos_por_gln(self, gln):
        """Egresos reportados hoy para un GLN específico"""
        try:
            query = self._base_query() + """
                AND (t.ml_id IN (111, 98))
                AND (CAST(t.anmat_fh_reporte AS DATE) = CURRENT_DATE)
                AND (COALESCE(se.gln, sr.gln) = ?)
                ORDER BY t.anmat_fh_reporte DESC
            """
            return self._execute_query(query, (gln,))
        except Exception:
            return self.get_egresos_hoy()

    _FILTROS_CARD = {
        'recepciones_hoy': "AND (t.ml_id IN (101,108)) AND (t.anmat_resultado = 1) AND (CAST(t.anmat_fh_reporte AS DATE) = CURRENT_DATE)",
        'dispensas_hoy':   "AND (t.ml_id IN (111,98))  AND (t.anmat_resultado = 1) AND (CAST(t.anmat_fh_reporte AS DATE) = CURRENT_DATE)",
        'pendientes':      "AND (t.anmat_transaccion IS NULL)",
        'con_error':       "AND (t.anmat_resultado = 0)",
    }

    def get_todos_movimientos_mes(self, gln=None, limit=500, offset=0, fecha_desde=None, fecha_hasta=None, filtro=None):
        """Todos los movimientos del mes (pendientes, reportados, errores)"""
        start_row = offset + 1
        end_row = offset + limit
        params = []

        gln_where = ""
        if gln:
            gln_where = "AND COALESCE(se.gln, sr.gln) = ?"
            params.append(gln)

        fecha_where = ""
        if filtro not in self._FILTROS_CARD:
            fecha_col = "CAST(COALESCE(e.fecha_emitido, r.fecha_comprobante, t.alta) AS DATE)"
            if fecha_desde and fecha_hasta:
                fecha_where = f"AND {fecha_col} BETWEEN ? AND ?"
                params.extend([fecha_desde, fecha_hasta])
            elif fecha_desde:
                fecha_where = f"AND {fecha_col} >= ?"
                params.append(fecha_desde)
            elif fecha_hasta:
                fecha_where = f"AND {fecha_col} <= ?"
                params.append(fecha_hasta)

        card_where = self._FILTROS_CARD.get(filtro, "")

        query = self._base_query() + f"""
            {gln_where}
            {fecha_where}
            {card_where}
            ORDER BY COALESCE(e.fecha_emitido, r.fecha_comprobante, t.alta) DESC
            ROWS {start_row} TO {end_row}
        """
        return self._execute_query(query, tuple(params))

    def get_movimientos_pendientes_paginado(self, gln=None, limit=100, offset=0):
        """Movimientos pendientes con paginación (solo los que nunca se reportaron)"""
        start_row = offset + 1
        end_row = offset + limit
        gln_where = "AND COALESCE(se.gln, sr.gln) = ?" if gln else ""
        params = (gln,) if gln else ()
        query = self._base_query() + f"""
            AND t.anmat_transaccion IS NULL
            {gln_where}
            ORDER BY t.id
            ROWS {start_row} TO {end_row}
        """
        return self._execute_query(query, params)

    def get_errores_paginado(self, gln=None, limit=100, offset=0):
        """Errores con paginación"""
        start_row = offset + 1
        end_row = offset + limit
        gln_where = "AND COALESCE(se.gln, sr.gln) = ?" if gln else ""
        params = (gln,) if gln else ()
        query = self._base_query() + f"""
            AND t.anmat_error_codigo IS NOT NULL
            {gln_where}
            ORDER BY t.anmat_fh_reporte DESC
            ROWS {start_row} TO {end_row}
        """
        return self._execute_query(query, params)

    def get_movimiento_by_id(self, mov_id):
        """Obtiene un movimiento específico por ID"""
        query = self._base_query() + " AND t.id = ?"
        logger.info(f"Obteniendo movimiento ID: {mov_id}")
        try:
            results = self._execute_query(query, (mov_id,))
            if results:
                logger.info(f"Movimiento {mov_id} encontrado, campos: {list(results[0].keys())}")
                return results[0]
            else:
                logger.warning(f"Movimiento {mov_id} no encontrado")
                return None
        except Exception as e:
            logger.error(f"Error al obtener movimiento {mov_id}: {e}")
            raise

    def actualizar_datos_trazabilidad(self, mov_id, lote=None, vencimiento=None):
        """Actualiza datos de trazabilidad de un movimiento (solo lote y vencimiento)"""
        updates = []
        params = []
        if lote is not None and str(lote).strip():
            updates.append("lote = ?")
            params.append(str(lote).strip())
        if vencimiento is not None and str(vencimiento).strip():
            updates.append("caducidad = ?")
            params.append(self._parse_fecha_db(vencimiento))

        if not updates:
            return False

        query = f"UPDATE view_cat_traza_mov SET {', '.join(updates)} WHERE id = ?"
        params.append(mov_id)
        logger.info(f"Actualizando trazabilidad movimiento {mov_id} con: {params[:-1]}")

        with self._op_lock:
            conn = self._ensure_connection()
            cursor = conn.cursor()
            cursor.execute(query, tuple(params))
            conn.commit()
            cursor.close()
        return True

    def _parse_fecha_db(self, fecha):
        """Convierte fecha DD/MM/YYYY a YYYY-MM-DD para Firebird"""
        if not fecha:
            return None
        fecha_str = str(fecha).strip()
        if '/' in fecha_str:
            partes = fecha_str.split('/')
            if len(partes) == 3:
                return f"{partes[2]}-{partes[1]}-{partes[0]}"
        return fecha_str

    def buscar_comprobantes_recibidos(self, gtin=None, lote=None, serie=None, fecha_desde=None, limit=25):
        """Busca comprobantes recibidos para recuperar datos de trazabilidad

        Args:
            gtin: Código GTIN del producto
            lote: Número de lote (opcional)
            serie: Número de serie (opcional)
            fecha_desde: Fecha mínima del comprobante (formato YYYY-MM-DD)
            limit: Límite de resultados
        """
        query = """
            SELECT p.nombre,
                   r.pto_vta,
                   r.id_cmp,
                   r.fecha_comprobante,
                   ca.codigo as gtin,
                   c.cantidad,
                   c.id as cmp_recibido_item_id,
                   ct.lote,
                   ct.nro_serie,
                   ct.ml_id,
                   ct.caducidad,
                   ct.id as ct_id,
                   ct.anmat_transaccion,
                   ct.anmat_error_codigo,
                   ct.anmat_error_descripcion
            FROM cmp_recibido r
            LEFT JOIN cmp_recibido_items c ON c.cmp_recibido_id = r.id
            LEFT JOIN catalogo ca ON ca.id_catalogo = c.id_catalogo
            LEFT JOIN view_cat_traza_mov ct ON ct.cmp_recibido_item_id = c.id
            LEFT JOIN persona p ON r.id_persona = p.id
            LEFT JOIN cmp_tipo t ON t.id_tipo_cmp = r.id_tipo_cmp
            WHERE t.signo_recepcion <> 0
              AND (ct.id IS NULL OR ct.anmat_transaccion IS NULL)
        """

        conditions = []
        params = []
        if fecha_desde:
            fecha_db = self._parse_fecha_db(fecha_desde)
            if fecha_db:
                conditions.append("r.fecha_comprobante >= ?")
                params.append(fecha_db)
        if gtin:
            conditions.append("ca.codigo = ?")
            params.append(gtin)
        if lote:
            conditions.append("UPPER(ct.lote) LIKE ?")
            params.append(f"%{lote.upper().strip()}%")
        if serie:
            conditions.append("ct.nro_serie = ?")
            params.append(serie.strip())

        if conditions:
            query += " AND " + " AND ".join(conditions)

        query += " ORDER BY r.fecha_comprobante DESC"
        query += f" ROWS {limit}"

        with self._op_lock:
            conn = self._ensure_connection()
            cursor = conn.cursor()
            cursor.execute(query, tuple(params))
            if cursor.description is None:
                cursor.close()
                return []
            columns = [desc[0] for desc in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            cursor.close()
        return results

    def actualizar_reporte(self, mov_id, transaccion, resultado, error_codigo=None, error_descripcion=None, fecha_reporte=None):
        """Actualiza el reporte de un movimiento. Reintenta una vez ante caída de conexión.

        Args:
            mov_id: ID del movimiento
            transaccion: Código de transacción ANMAT
            resultado: 'A' (aprobado) o 'R' (rechazado)
            error_codigo: Código de error opcional
            error_descripcion: Descripción del error opcional
            fecha_reporte: Fecha original del reporte (YYYY-MM-DD).
                          Si es None, usa CURRENT_TIMESTAMP.
        """
        transaccion_val = None
        if transaccion:
            try:
                transaccion_val = int(transaccion)
            except (ValueError, TypeError) as e:
                logger.error(f"Transacción no numérica para movimiento {mov_id}: '{transaccion}' → NULL. {e}")

        resultado_val = 1 if resultado == 'A' else 0

        error_codigo_val = None
        if error_codigo:
            try:
                error_codigo_val = int(error_codigo[:10]) if str(error_codigo)[:10].isdigit() else None
            except (ValueError, TypeError):
                error_codigo_val = None

        descripcion_extra = f"[{error_codigo}] " if error_codigo and error_codigo_val is None else ""
        error_descripcion_val = (descripcion_extra + str(error_descripcion))[:5000] if error_descripcion else None

        if fecha_reporte:
            query = """
                UPDATE cat_traza_mov
                SET anmat_transaccion = ?,
                    anmat_resultado = ?,
                    anmat_error_codigo = ?,
                    anmat_error_descripcion = ?,
                    anmat_fh_reporte = ?
                WHERE id = ?
            """
            params = (transaccion_val, resultado_val, error_codigo_val, error_descripcion_val, fecha_reporte, mov_id)
        else:
            query = """
                UPDATE cat_traza_mov
                SET anmat_transaccion = ?,
                    anmat_resultado = ?,
                    anmat_error_codigo = ?,
                    anmat_error_descripcion = ?,
                    anmat_fh_reporte = CURRENT_TIMESTAMP
                WHERE id = ?
            """
            params = (transaccion_val, resultado_val, error_codigo_val, error_descripcion_val, mov_id)

        for attempt in range(2):
            try:
                with self._op_lock:
                    conn = self._ensure_connection()
                    cursor = conn.cursor()
                    cursor.execute(query, params)
                    conn.commit()
                    cursor.close()
                logger.info(f"Actualizado movimiento {mov_id}: resultado={resultado_val}, transaccion={transaccion_val}")
                return
            except Exception as e:
                logger.error(f"Error actualizando reporte movimiento {mov_id} (intento {attempt+1}): {e}")
                if attempt == 0:
                    with self._op_lock:
                        if self._connection:
                            try:
                                self._connection.close()
                            except Exception:
                                pass
                            self._connection = None
                else:
                    raise

    def buscar_cmp_recibido_por_gtin(self, gtin, gln=None):
        """Busca cmp_recibido_items sin trazabilidad reportada para un GTIN dado.
        Incluye items sin cat_traza_mov y los que tienen cat_traza_mov pero sin anmat_transaccion.
        Retorna FIRST 2 para detectar ambigüedad (>1 = no se puede auto-registrar).
        """
        query = """
            SELECT FIRST 2
                cri.id          AS cmp_recibido_item_id,
                cri.cmp_recibido_id,
                cri.id_catalogo,
                r.fecha_comprobante,
                ctm.id          AS ctm_id
            FROM cmp_recibido_items cri
            JOIN cmp_recibido r      ON r.id  = cri.cmp_recibido_id
            JOIN cmp_tipo ct2        ON ct2.id_tipo_cmp = r.id_tipo_cmp
            JOIN catalogo ca         ON ca.id_catalogo  = cri.id_catalogo
            JOIN sucursal sr         ON sr.id_sucursal  = r.id_sucursal
            LEFT JOIN cat_traza_mov ctm ON ctm.cmp_recibido_item_id = cri.id
            WHERE (ctm.id IS NULL OR ctm.anmat_transaccion IS NULL)
              AND ct2.signo_recepcion <> 0
              AND ca.codigo = ?
        """
        params = [gtin]
        if gln:
            query += " AND sr.gln = ?"
            params.append(gln)
        query += " ORDER BY r.fecha_comprobante DESC"

        with self._op_lock:
            conn = self._ensure_connection()
            cursor = conn.cursor()
            try:
                cursor.execute(query, tuple(params))
                if cursor.description is None:
                    return []
                columns = [desc[0].lower() for desc in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
            except Exception as e:
                logger.error(f"Error en buscar_cmp_recibido_por_gtin: {e}")
                return []
            finally:
                cursor.close()

    def crear_registro_trazabilidad(self, cmp_recibido_item_id, gtin=None, lote=None, numero_serial=None, caducidad=None, ml_id=101):
        """Crea un nuevo registro en cat_traza_mov (y cat_lote/cat_traza si es necesario)"""
        try:
            with self._op_lock:
                conn = self._ensure_connection()
                cursor = conn.cursor()

                # 1. Obtener id_catalogo desde cmp_recibido_items
                cursor.execute("""
                    SELECT id_catalogo FROM cmp_recibido_items WHERE id = ?
                """, (cmp_recibido_item_id,))
                row = cursor.fetchone()
                if not row:
                    cursor.close()
                    return None
                id_catalogo = row[0]

                # 2. Buscar o crear cat_lote
                lote_str = str(lote).strip() if lote else None
                caducidad_db = self._parse_fecha_db(caducidad) if caducidad else None

                cursor.execute("""
                    SELECT id FROM cat_lote WHERE id_catalogo = ? AND lote = ?
                """, (id_catalogo, lote_str))
                lote_row = cursor.fetchone()

                if lote_row:
                    cat_lote_id = lote_row[0]
                else:
                    cursor.execute("""
                        INSERT INTO cat_lote (id_catalogo, lote, caducidad)
                        VALUES (?, ?, ?) RETURNING id
                    """, (id_catalogo, lote_str, caducidad_db))
                    lote_insert = cursor.fetchone()
                    cat_lote_id = lote_insert[0] if lote_insert else None
                    if not cat_lote_id:
                        cursor.close()
                        return None

                # 3. Buscar o crear cat_traza
                nro_serie = str(numero_serial).strip() if numero_serial else None
                if nro_serie:
                    cursor.execute("""
                        SELECT id FROM cat_traza WHERE cat_lote_id = ? AND nro_serie = ?
                    """, (cat_lote_id, nro_serie))
                else:
                    cursor.execute("""
                        SELECT id FROM cat_traza WHERE cat_lote_id = ? AND nro_serie IS NULL
                    """, (cat_lote_id,))
                traza_row = cursor.fetchone()

                if traza_row:
                    cat_traza_id = traza_row[0]
                else:
                    cursor.execute("""
                        INSERT INTO cat_traza (cat_lote_id, nro_serie)
                        VALUES (?, ?) RETURNING id
                    """, (cat_lote_id, nro_serie))
                    traza_insert = cursor.fetchone()
                    cat_traza_id = traza_insert[0] if traza_insert else None
                    if not cat_traza_id:
                        cursor.close()
                        return None

                # 4. Insertar en cat_traza_mov con ml_id (101=Ingreso por defecto)
                cursor.execute("""
                    INSERT INTO cat_traza_mov (cat_traza_id, cmp_recibido_item_id, ml_id)
                    VALUES (?, ?, ?) RETURNING id
                """, (cat_traza_id, cmp_recibido_item_id, ml_id))
                mov_insert = cursor.fetchone()
                cat_traza_mov_id = mov_insert[0] if mov_insert else None

                conn.commit()
                cursor.close()
            return cat_traza_mov_id
        except Exception as e:
            logger.error(f"Error al crear registro de trazabilidad: {e}")
            return None

    def get_movimiento_por_cmp_recibido_item(self, cmp_recibido_item_id):
        """Busca un movimiento de trazabilidad por cmp_recibido_item_id"""
        query = """
            SELECT t.*, c.codigo as GTIN
            FROM view_cat_traza_mov t
            LEFT JOIN cmp_recibido_items cri ON cri.id = t.cmp_recibido_item_id
            LEFT JOIN catalogo c ON c.id_catalogo = cri.id_catalogo
            WHERE t.cmp_recibido_item_id = ?
        """
        with self._op_lock:
            conn = self._ensure_connection()
            cursor = conn.cursor()
            cursor.execute(query, (cmp_recibido_item_id,))
            if cursor.description is None:
                cursor.close()
                return None
            columns = [desc[0] for desc in cursor.description]
            row = cursor.fetchone()
            cursor.close()
        if row:
            return dict(zip(columns, row))
        return None

    def get_proveedor_gln_por_comprobante(self, cmp_recibido_id):
        """Obtiene el GLN del proveedor asociado a un comprobante recibido"""
        try:
            query = """
                SELECT pr.gln
                FROM cmp_recibido r
                LEFT JOIN prestador pr ON pr.persona_id = r.id_persona
                WHERE r.id = ?
            """
            with self._op_lock:
                conn = self._ensure_connection()
                cursor = conn.cursor()
                cursor.execute(query, (cmp_recibido_id,))
                row = cursor.fetchone()
                cursor.close()
            if row and row[0]:
                return str(row[0])
            return None
        except Exception as e:
            logger.error(f"Error al obtener GLN de proveedor: {e}")
            return None


    def get_sucursal_por_gln(self, gln):
        """Devuelve id_sucursal para un GLN dado"""
        try:
            with self._op_lock:
                conn = self._ensure_connection()
                cursor = conn.cursor()
                cursor.execute("SELECT id_sucursal, sucursal FROM sucursal WHERE gln = ?", (gln,))
                row = cursor.fetchone()
                cursor.close()
            if row:
                return {'id_sucursal': row[0], 'sucursal': row[1]}
            return None
        except Exception as e:
            logger.error(f"Error get_sucursal_por_gln: {e}")
            return None

    def get_prestador_por_gln(self, gln):
        """Devuelve persona_id del prestador para un GLN dado"""
        try:
            with self._op_lock:
                conn = self._ensure_connection()
                cursor = conn.cursor()
                cursor.execute("SELECT p.persona_id FROM prestador p WHERE p.gln = ?", (gln,))
                row = cursor.fetchone()
                cursor.close()
            if row:
                return {'persona_id': row[0]}
            return None
        except Exception as e:
            logger.error(f"Error get_prestador_por_gln: {e}")
            return None

    def get_id_tipo_cmp_recepcion(self, letra=None):
        """Devuelve id_tipo_cmp de recepción buscando por letra (A/B/C=factura, R=remito)"""
        try:
            with self._op_lock:
                conn = self._ensure_connection()
                cursor = conn.cursor()
                if letra:
                    cursor.execute(
                        "SELECT FIRST 1 id_tipo_cmp FROM cmp_tipo WHERE signo_recepcion <> 0 AND letra = ?",
                        (letra.upper(),)
                    )
                else:
                    cursor.execute(
                        "SELECT FIRST 1 id_tipo_cmp FROM cmp_tipo WHERE signo_recepcion <> 0"
                    )
                row = cursor.fetchone()
                cursor.close()
            return row[0] if row else None
        except Exception as e:
            logger.error(f"Error get_id_tipo_cmp_recepcion: {e}")
            return None

    def get_catalogo_por_gtin(self, gtin):
        """Devuelve id_catalogo para un GTIN dado"""
        try:
            gtin_clean = str(gtin).lstrip('0') or str(gtin)
            with self._op_lock:
                conn = self._ensure_connection()
                cursor = conn.cursor()
                cursor.execute("SELECT FIRST 1 id_catalogo, codigo FROM catalogo WHERE codigo = ?", (gtin_clean,))
                row = cursor.fetchone()
                if not row:
                    cursor.execute("SELECT FIRST 1 id_catalogo, codigo FROM catalogo WHERE codigo = ?", (gtin,))
                    row = cursor.fetchone()
                cursor.close()
            return {'id_catalogo': row[0], 'codigo': row[1]} if row else None
        except Exception as e:
            logger.error(f"Error get_catalogo_por_gtin: {e}")
            return None

    def crear_cmp_recibido(self, id_tipo_cmp, id_persona, id_sucursal, fecha, pto_vta=0, id_cmp_num=0):
        """Crea una cabecera cmp_recibido y retorna su id"""
        try:
            fecha_db = self._parse_fecha_db(fecha) if fecha and '/' in str(fecha) else fecha
            with self._op_lock:
                conn = self._ensure_connection()
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO cmp_recibido (id_tipo_cmp, id_persona, id_sucursal, fecha_comprobante, pto_vta, id_cmp)
                    VALUES (?, ?, ?, ?, ?, ?) RETURNING id
                """, (id_tipo_cmp, id_persona, id_sucursal, fecha_db, pto_vta, id_cmp_num))
                row = cursor.fetchone()
                conn.commit()
                cursor.close()
            return row[0] if row else None
        except Exception as e:
            logger.error(f"Error crear_cmp_recibido: {e}")
            return None

    def crear_cmp_recibido_item(self, cmp_recibido_id, id_catalogo, cantidad=1):
        """Crea un item en cmp_recibido_items y retorna su id"""
        try:
            with self._op_lock:
                conn = self._ensure_connection()
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO cmp_recibido_items (cmp_recibido_id, id_catalogo, cantidad)
                    VALUES (?, ?, ?) RETURNING id
                """, (cmp_recibido_id, id_catalogo, cantidad))
                row = cursor.fetchone()
                conn.commit()
                cursor.close()
            return row[0] if row else None
        except Exception as e:
            logger.error(f"Error crear_cmp_recibido_item: {e}")
            return None


    def buscar_cmp_recibido_por_filtros(self, gtin=None, n_factura=None, n_remito=None,
                                         gln_origen=None, gln_sucursal=None,
                                         fecha_desde=None, fecha_hasta=None, limit=20):
        """Busca comprobantes recibidos por múltiples criterios para conciliación ANMAT.

        Args:
            gtin: GTIN del producto
            n_factura: Número de factura completo (ej: A001000000001)
            n_remito: Número de remito
            gln_origen: GLN del proveedor
            gln_sucursal: GLN de la sucursal destino
            fecha_desde: Fecha mínima (YYYY-MM-DD)
            fecha_hasta: Fecha máxima (YYYY-MM-DD)
            limit: Máximo de resultados
        """
        query = """
            SELECT r.id as cmp_recibido_id,
                   r.pto_vta,
                   r.id_cmp,
                   r.fecha_comprobante,
                   ct2.letra,
                   p.nombre as proveedor,
                   pr.gln as gln_proveedor,
                   cri.id as cmp_recibido_item_id,
                   cri.id_catalogo,
                   ca.codigo as gtin,
                   ca.descripcion as producto,
                   cri.cantidad,
                   ctm.id as ctm_id,
                   cl.lote,
                   ct.nro_serie,
                   cl.caducidad,
                   ctm.anmat_transaccion,
                   ctm.anmat_resultado,
                   ctm.ml_id
            FROM cmp_recibido r
            JOIN cmp_tipo ct2 ON ct2.id_tipo_cmp = r.id_tipo_cmp
            JOIN cmp_recibido_items cri ON cri.cmp_recibido_id = r.id
            JOIN catalogo ca ON ca.id_catalogo = cri.id_catalogo
            LEFT JOIN persona p ON p.id = r.id_persona
            LEFT JOIN prestador pr ON pr.persona_id = r.id_persona
            LEFT JOIN cat_traza_mov ctm ON ctm.cmp_recibido_item_id = cri.id
            LEFT JOIN cat_traza ct ON ct.id = ctm.cat_traza_id
            LEFT JOIN cat_lote cl ON cl.id = ct.cat_lote_id
            WHERE ct2.signo_recepcion <> 0
        """
        params = []
        conditions = []

        if gtin:
            conditions.append("ca.codigo = ?")
            params.append(gtin)

        if n_factura:
            letra = n_factura[0].upper() if n_factura[0].isalpha() else None
            resto = n_factura[1:] if letra else n_factura
            resto_limpio = resto.replace('-', '').replace('/', '').strip()
            if letra:
                conditions.append("ct2.letra = ?")
                params.append(letra)
            if resto_limpio.isdigit() and len(resto_limpio) >= 3:
                pto_vta = int(resto_limpio[:3])
                id_cmp = int(resto_limpio[3:]) if len(resto_limpio) > 3 else 0
                conditions.append("(r.pto_vta = ? AND r.id_cmp = ?)")
                params.extend([pto_vta, id_cmp])

        if n_remito:
            # ANMAT remito format: {letra}{pto_vta:03}{id_cmp:09} (mismo formato que factura)
            remito_str = n_remito.replace('-', '').replace('/', '').strip()
            if remito_str[0].isalpha():
                remito_resto = remito_str[1:]
            else:
                remito_resto = remito_str
            remito_resto = ''.join(c for c in remito_resto if c.isdigit())
            if remito_resto.isdigit() and len(remito_resto) >= 3:
                if len(remito_resto) > 9:
                    r_pto_vta = int(remito_resto[:3])
                    r_id_cmp = int(remito_resto[-9:])
                elif len(remito_resto) > 3:
                    r_pto_vta = int(remito_resto[:3])
                    r_id_cmp = int(remito_resto[3:])
                else:
                    r_pto_vta = int(remito_resto[:3])
                    r_id_cmp = 0
                conditions.append("(r.pto_vta = ? AND r.id_cmp = ?)")
                params.extend([r_pto_vta, r_id_cmp])

        if gln_origen:
            conditions.append("pr.gln = ?")
            params.append(gln_origen)

        if gln_sucursal:
            conditions.append("EXISTS (SELECT 1 FROM sucursal s WHERE s.id_sucursal = r.id_sucursal AND s.gln = ?)")
            params.append(gln_sucursal)

        if fecha_desde:
            conditions.append("r.fecha_comprobante >= ?")
            params.append(fecha_desde)

        if fecha_hasta:
            conditions.append("r.fecha_comprobante <= ?")
            params.append(fecha_hasta)

        if conditions:
            query += " AND " + " AND ".join(conditions)

        query += " ORDER BY r.fecha_comprobante DESC ROWS ?"
        params.append(limit)

        with self._op_lock:
            conn = self._ensure_connection()
            cursor = conn.cursor()
            try:
                cursor.execute(query, tuple(params))
                if cursor.description is None:
                    return []
                columns = [desc[0].lower() for desc in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
            except Exception as e:
                logger.error(f"Error en buscar_cmp_recibido_por_filtros: {e}")
                return []
            finally:
                cursor.close()

    def buscar_cmp_recibido_por_lista_gtins(self, gtins, gln_sucursal):
        """Busca un único cmp_recibido que contenga TODOS los GTINs dados para una sucursal.

        Args:
            gtins: Lista de códigos GTIN a buscar (se comparan con y sin ceros adelante)
            gln_sucursal: GLN de la sucursal destino

        Returns:
            Dict con cmp_recibido_id si existe exactamente 1 comprobante, None otherwise
        """
        if not gtins or not gln_sucursal:
            return None

        gtins_clean = [g.lstrip('0') or g for g in gtins if g]
        gtins_clean = list(dict.fromkeys(gtins_clean))
        if not gtins_clean:
            return None

        placeholders = ','.join(['?'] * len(gtins_clean))
        query = f"""
            SELECT r.id as cmp_recibido_id
            FROM cmp_recibido r
            JOIN cmp_recibido_items cri ON cri.cmp_recibido_id = r.id
            JOIN catalogo ca ON ca.id_catalogo = cri.id_catalogo
            JOIN cmp_tipo ct2 ON ct2.id_tipo_cmp = r.id_tipo_cmp
            JOIN sucursal s ON s.id_sucursal = r.id_sucursal
            WHERE s.gln = ?
              AND ca.codigo IN ({placeholders})
              AND ct2.signo_recepcion <> 0
            GROUP BY r.id
            HAVING COUNT(DISTINCT ca.codigo) = ?
        """
        params = [gln_sucursal] + gtins_clean + [len(gtins_clean)]

        with self._op_lock:
            conn = self._ensure_connection()
            cursor = conn.cursor()
            try:
                cursor.execute(query, tuple(params))
                if cursor.description is None:
                    return None
                columns = [desc[0] for desc in cursor.description]
                rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
                if len(rows) == 1:
                    return rows[0]
                if len(rows) > 1:
                    logger.warning(f"Múltiples comprobantes con todos los GTINs ({len(rows)})")
                    return None
                return None
            except Exception as e:
                logger.error(f"Error en buscar_cmp_recibido_por_lista_gtins: {e}")
                return None
            finally:
                cursor.close()

    def get_sucursal_por_gln_detalle(self, gln):
        """Devuelve datos completos de sucursal para un GLN."""
        try:
            with self._op_lock:
                conn = self._ensure_connection()
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT s.id_sucursal, s.sucursal, s.gln, s.cuit,
                           s.anmat_user, s.anmat_passwd as anmat_password
                    FROM sucursal s WHERE s.gln = ?
                """, (gln,))
                row = cursor.fetchone()
                cursor.close()
            if row:
                cols = ['id_sucursal', 'sucursal', 'gln', 'cuit', 'anmat_user', 'anmat_password']
                return dict(zip(cols, row))
            return None
        except Exception as e:
            logger.error(f"Error get_sucursal_por_gln_detalle: {e}")
            return None


db = FirebirdConnection()
