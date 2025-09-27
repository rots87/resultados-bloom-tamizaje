import psycopg2
from datetime import datetime
from decimal import Decimal
from typing import Dict, Any, Optional

def connect_to_db() -> Optional[psycopg2.extensions.connection]:
    """Establece conexión con la base de datos PostgreSQL."""
    try:
        return psycopg2.connect(
            dbname="labsis",
            user="labsis",
            password="labsis",
            host="172.17.90.26", # Ambiente de producción
            #host="localhost", # Ambiente de desarrollo
            port="5432"
        )
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

def parse_datetime(date_str: str, format_str: str) -> Optional[str]:
    """Convierte fecha/hora a formato string requerido."""
    if date_str is None:
        return None
    try:
        dt_obj = datetime.strptime(str(date_str), format_str)
        return dt_obj.strftime('%Y-%m-%d')
    except (ValueError, TypeError):
        return None

def parse_datetime_with_time(date_str: str) -> Optional[str]:
    """Convierte fecha/hora a formato con tiempo para timestamps."""
    if date_str is None:
        return None
    try:
        dt_obj = datetime.strptime(str(date_str), '%Y-%m-%d %H:%M:%S.%f')
        return f"#{dt_obj.strftime('%Y-%m-%d %H:%M:%S')}#"
    except (ValueError, TypeError):
        return None

def utf_to_ansi(text: str) -> str:
    """Convierte caracteres UTF a ANSI (ñ por n, vocales con tilde por sin tilde)."""
    replacements = {
        'ñ': 'n', 'Ñ': 'N',
        'á': 'a', 'Á': 'A', 'é': 'e', 'É': 'E',
        'í': 'i', 'Í': 'I', 'ó': 'o', 'Ó': 'O',
        'ú': 'u', 'Ú': 'U'
    }
    for utf_char, ansi_char in replacements.items():
        text = text.replace(utf_char, ansi_char)
    return text

def create_boleta_base(num_ingreso: str, fecha_toma: str, nombre_paciente: str, 
                      sexo_paciente: str, ci_paciente: str, edad_dias: int, 
                      fecha_recepcion: str, codigo_bloom: str, codigo_dtic: str) -> Dict[str, Any]:
    """Crea la estructura base de una boleta."""
    return {
        "codigoE": codigo_bloom,
        "Boleta": num_ingreso,
        "FechaTomaMx": f'#{fecha_toma}#',
        "Paciente": nombre_paciente,
        "Edad": edad_dias,
        "Sexo": sexo_paciente,
        "Expediente": ci_paciente,
        "Recepcion": fecha_recepcion,
        "Procesamiento": "#NULL#",
        "FResultado": "#NULL#",
        "FechaRechazo": fecha_recepcion,
        "EstadoPaciente": "#NULL#",
        "StdoBoleta": "R",
        "Update": "#NULL#",
        "ReferidoPor": "#NULL#",
        "Id": codigo_dtic,
        "Resultados": {
            852: '#NULL#', 859: '#NULL#', 854: '#NULL#', 883: '#NULL#',
            886: '#NULL#', 885: '#NULL#', 888: '#NULL#', 889: '#NULL#',
            890: '#NULL#', 891: '#NULL#', 892: '#NULL#'
        }
    }

def determine_result_value(id_prueba: int, resultado_numerico: Any, 
                          resultado_alpha: Any, validado_por: Any) -> str:
    """Determina el valor del resultado según el tipo de prueba."""
    if 889 <= id_prueba <= 892 and validado_por != 0:
        return resultado_alpha if resultado_alpha is not None else '#NULL#'
    return resultado_numerico if resultado_numerico is not None else '#NULL#'

def get_mas_antiguo_timestamp(ts1, ts2):
    """Devuelve el timestamp más antiguo (menor) entre dos valores parseados, o #NULL# si ambos son None."""
    if ts1 and ts2:
        try:
            dt1 = datetime.strptime(ts1.strip("#"), "%Y-%m-%d %H:%M:%S")
            dt2 = datetime.strptime(ts2.strip("#"), "%Y-%m-%d %H:%M:%S")
            return f"#{min(dt1, dt2).strftime('%Y-%m-%d %H:%M:%S')}#"
        except Exception:
            return ts1 or ts2 or "#NULL#"
    return ts1 or ts2 or "#NULL#"

def es_mas_antigua(fecha_nueva, fecha_existente):
    """Devuelve True si fecha_nueva es más antigua que fecha_existente."""
    try:
        dt_nueva = datetime.strptime(fecha_nueva.strip("#"), "%Y-%m-%d %H:%M:%S")
        dt_existente = datetime.strptime(fecha_existente.strip("#"), "%Y-%m-%d %H:%M:%S")
        return dt_nueva < dt_existente
    except Exception:
        return False

def generate_report(connection: psycopg2.extensions.connection, 
                   fecha_inicio: str, fecha_fin: str) -> Dict[str, Dict[str, Any]]:
    """Genera reporte de boletas agrupadas por número de ingreso."""
    if connection is None:
        print("No database connection available.")
        return {}

    boletas_agrupadas = {}
    
    query = """
        SELECT OT.num_ingreso, OT.fecha_toma_muestra, P.nombre, P.apellido, P.sexo, P.ci_paciente, 
               RN.actualizado_timestamp, RN.valor, PR.id, OT.numero, OTDE.edad_dias, OTDE.edad_horas, 
               SM.codigo_bloom, SM.codigo_dtic, OTDE.fecha_recepcion, RA.valor, RA.validado_por, 
               OTDE.update, RA.actualizado_timestamp
        FROM orden_trabajo OT
        LEFT JOIN paciente P ON OT.paciente_ID = P.id
        LEFT JOIN prueba_orden PO ON OT.id = PO.orden_id
        LEFT JOIN resultado_numer RN ON PO.id = RN.pruebao_id
        LEFT JOIN prueba PR ON PR.id = PO.prueba_id
        LEFT JOIN orden_trabajo_datos_extra OTDE ON OT.id = OTDE.orden_id
        LEFT JOIN servicio_medico SM ON OT.servicio_medico_id = SM.id
        LEFT JOIN resultado_alpha RA ON PO.id = RA.pruebao_id
        WHERE OTDE.fecha_recepcion BETWEEN %s AND %s
        ORDER BY OTDE.fecha_recepcion ASC
    """
    
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, (fecha_inicio, fecha_fin))
            rows = cursor.fetchall()

            for row in rows:
                num_ingreso = row[0]
                
                # Saltar entradas con num_ingreso igual a 1
                if num_ingreso == '1':
                    continue

                # Procesar datos básicos
                fecha_toma = parse_datetime(row[1], '%Y-%m-%d %H:%M:%S%z') or "NULL"
                nombre_paciente = utf_to_ansi(f"{str(row[2] or '').strip()} {str(row[3] or '').strip()}")
                sexo_paciente = row[4]
                ci_paciente = row[5]
                #actualizado_timestamp = parse_datetime_with_time(row[6])
                id_prueba = row[8]
                
                # Calcular edad
                edad_dias = row[10] or 0
                edad_horas = row[11] or 0
                if edad_dias == 0 and edad_horas > 0:
                    edad_dias = int(edad_horas / 24)

                # Procesar códigos y fecha de recepción
                codigo_bloom = row[12] if row[12] is not None else '#NULL#'
                codigo_dtic = row[13] if row[13] is not None else '#NULL#'
                fecha_recepcion = parse_datetime(row[14], '%Y-%m-%d %H:%M:%S')

                # Crear boleta si no existe
                if num_ingreso not in boletas_agrupadas:
                    boletas_agrupadas[num_ingreso] = create_boleta_base(
                        num_ingreso, fecha_toma, nombre_paciente, sexo_paciente,
                        ci_paciente, edad_dias, fecha_recepcion, codigo_bloom, codigo_dtic
                    )
                # Asignar el valor de Update desde OTDE.update
                boletas_agrupadas[num_ingreso]["Update"] = row[17] if row[17] is not None else "#NULL#"

                # Procesar resultado para la prueba si id_prueba es válido (o rechazarla)
                if id_prueba is not None:
                    clave_resultado = int(id_prueba)
                    valid_ids = {852, 859, 854, 883, 886, 885, 888, 889, 890, 891, 892}
                    if clave_resultado in valid_ids:
                        # Procesar resultado según el origen
                        if clave_resultado in (852, 854, 859, 883, 886, 885, 888):
                            valor_resultado = row[7] if row[7] not in (None, "") else "#NULL#"
                        elif clave_resultado in (889, 890, 891, 892):
                            if row[16] and row[16] != 0:
                                valor_resultado = row[15] if row[15] not in (None, "") else "#NULL#"
                            else:
                                valor_resultado = row[7] if row[7] not in (None, "") else "#NULL#"
                        else:
                            valor_resultado = "#NULL#"
                        
                        # Actualizar solo si se obtuvo un valor válido
                        if valor_resultado not in (None, "", "#NULL#"):
                            boletas_agrupadas[num_ingreso]["Resultados"][clave_resultado] = valor_resultado
                        
                        # Procesar y asignar las fechas de Procesamiento y FResultado
                        rn_timestamp = parse_datetime_with_time(row[6])   # RN.actualizado_timestamp
                        ra_timestamp = parse_datetime_with_time(row[18])      # RA.actualizado_timestamp
                        ts_final = rn_timestamp
                        if (not ts_final or ts_final == "#NULL#") and ra_timestamp and ra_timestamp != "#NULL#":
                            ts_final = ra_timestamp
                        if ts_final and ts_final != "#NULL#":
                            for campo_fecha in ("Procesamiento", "FResultado"):
                                fecha_actual = boletas_agrupadas[num_ingreso][campo_fecha]
                                if fecha_actual == "#NULL#":
                                    boletas_agrupadas[num_ingreso][campo_fecha] = ts_final
                                elif es_mas_antigua(ts_final, fecha_actual):
                                    boletas_agrupadas[num_ingreso][campo_fecha] = ts_final
                        
                        # Como es un resultado aceptado:
                        boletas_agrupadas[num_ingreso]["StdoBoleta"] = "A"
                        boletas_agrupadas[num_ingreso]["FechaRechazo"] = "#NULL#"
                    else:
                        # Si la prueba tiene un id que no está en el conjunto válido, se marca como rechazado
                        boletas_agrupadas[num_ingreso]["StdoBoleta"] = "R"
                        # Asumir que la fecha de recepción (ya procesada) se toma para FechaRechazo:
                        boletas_agrupadas[num_ingreso]["FechaRechazo"] = f"#{fecha_recepcion}#" if fecha_recepcion not in (None, "", "#NULL#") else "#NULL#"

                # Reordenar resultados para los ids 889-892
                # Se crea un diccionario temporal "reordered" con valores por defecto
                reordered = {889: "#NULL#", 890: "#NULL#", 891: "#NULL#", 892: "#NULL#"}
                # Se recorre cada resultado en ese rango (del vector en bruto)
                for key in [889, 890, 891, 892]:
                    val = boletas_agrupadas[num_ingreso]["Resultados"].get(key, "#NULL#")
                    if val != "#NULL#":
                        if val == "F":
                            reordered[889] = "F"
                        elif val == "A":
                            reordered[890] = "A"
                        elif val == "S":
                            reordered[891] = "S"
                        elif val == "C":
                            reordered[892] = "C"
                # Se asigna el vector ordenado de vuelta
                for key in [889, 890, 891, 892]:
                    boletas_agrupadas[num_ingreso]["Resultados"][key] = reordered[key]

    except Exception as e:
        print(f"Error generando el reporte: {e}")
    finally:
        if connection is not None:
            connection.close()
            print("Conexión a la base de datos cerrada.")
    
    # Al final del procesamiento, buscar boletas anormales
    anormales = []
    for boleta in boletas_agrupadas.values():
        if (
            boleta.get("StdoBoleta") == "R"
            and boleta.get("Procesamiento", "#NULL#") != "#NULL#"
            and boleta.get("FResultado", "#NULL#") != "#NULL#"
        ):
            anormales.append(boleta.get("Boleta") or boleta.get("num_ingreso"))
            # Corregir el estado y la fecha de rechazo antes que se escriba el CSV y en la tblresults
            boleta["StdoBoleta"] = "A"
            boleta["FechaRechazo"] = "#NULL#"

    if anormales:
        from PyQt6.QtWidgets import QMessageBox
        lista = ", ".join(str(x) for x in anormales)
        QMessageBox.critical(
            None,
            "Datos anormales detectados",
            f"Se detectaron boletas rechazadas (StdoBoleta=R) con fechas de procesamiento/resultados no nulas. se corrigio el estado y la fecha de rechazo pero se recomienda validacion manual de boletas por posible boleta duplicada\n"
            f"Boletas afectadas: {lista}"
        )
    
    return boletas_agrupadas

def format_result_value(val):
    try:
        if val in ("#NULL#", None):
            return "#NULL#"
        return f"{float(val):.1f}"
    except (ValueError, TypeError):
        return str(val)

def write_to_csv(boletas_agrupadas: Dict[str, Dict[str, Any]], filename: str = "reporte_labsis.csv") -> None:
    """Escribe los datos agrupados a un archivo CSV."""
    encabezados_internos = [
        "codigoE", "Boleta", "FechaTomaMx", "Paciente", "Edad", "Sexo", "Expediente",
        "Recepcion", "Procesamiento", "FResultado", "Resultado", "FechaRechazo", "EstadoPaciente",
        "StdoBoleta", "Update", "ReferidoPor", "Id", "ResultadoIRT", "ResultadoPKU", "Resultado17OH",
        "ResultadoJarabeA1", "ResultadoJarabeA2", "ResultadoTyr", "ResultHbF", "ResultHbA", "ResultHbS",
        "ResultHbC"
    ]

    resultado_map = {
        "Resultado": 852,
        "ResultadoIRT": 859,
        "ResultadoPKU": 854,
        "Resultado17OH": 883,
        "ResultadoJarabeA1": 886,
        "ResultadoJarabeA2": 885,
        "ResultadoTyr": 888,
        "ResultHbF": 889,
        "ResultHbA": 890,
        "ResultHbS": 891,
        "ResultHbC": 892,
        "Resultado": 852
    }

    def format_csv_value(value):
        # No comillas si empieza y termina con #
        if isinstance(value, str) and value.startswith("#") and value.endswith("#"):
            return value
        # No comillas si es numérico
        try:
            float_val = float(value)
            return f"{float_val:.1f}"
        except (ValueError, TypeError):
            pass
        # Comillas solo si es alfanumérico y no cumple lo anterior
        return f'"{value}"'

    try:
        with open(filename, mode="w", newline="", encoding="ANSI") as f:
            f.write(','.join([format_csv_value(h) for h in encabezados_internos]) + '\n')
            for boleta_data in boletas_agrupadas.values():
                row = []
                for campo in encabezados_internos:
                    if campo in resultado_map:
                        value = boleta_data.get("Resultados", {}).get(resultado_map[campo], "#NULL#")
                        value = format_result_value(value)
                        # Solo poner comillas si es numérico y no es #NULL#
                        if value not in ("#NULL#", None) and not (isinstance(value, str) and value.startswith("#") and value.endswith("#")):
                            try:
                                float_val = float(value)
                                value = f'"{float_val:.1f}"'
                            except (ValueError, TypeError):
                                value = f'"{value}"'
                        # Si es #NULL# o está entre #, no poner comillas
                    else:
                        value = boleta_data.get(campo, "#NULL#")
                        if campo == "Update":
                            value = value.upper() if value not in ("#NULL#", None) else "#NULL#"
                            if value not in ("#NULL#", None) and not (isinstance(value, str) and value.startswith("#") and value.endswith("#")):
                                value = f'"{value}"'
                        # Si el campo es Boleta, Expediente o Edad, siempre entre comillas
                        if campo in ("codigoE","Paciente","Boleta", "Expediente", "Edad", "Sexo", "Id","StdoBoleta"):
                            value = f'"{value}"'
                    # Si el campo es Recepcion, siempre entre ##
                    if campo == "Recepcion":
                        if not (isinstance(value, str) and value.startswith("#") and value.endswith("#")):
                            value = f"#{value}#"
                        row.append(value)
                    else:
                        row.append(value)
                    
                f.write(','.join(row) + '\n')

        print(f"Datos escritos en {filename} exitosamente.")

    except Exception as e:
        print(f"Error escribiendo el archivo CSV: {e}")

def update_boleta_update(num_ingreso: str, valor_update: str) -> None:
    """Actualiza el campo Update en la base de datos usando num_ingreso."""
    conn = connect_to_db()
    if conn is None:
        raise Exception("No se pudo conectar a la base de datos")
    try:
        with conn.cursor() as cursor:
            # Buscar el orden_id usando num_ingreso
            cursor.execute(
                "SELECT id FROM orden_trabajo WHERE num_ingreso = %s",
                (num_ingreso,)
            )
            result = cursor.fetchone()
            if not result:
                raise Exception(f"No se encontró orden_id para num_ingreso {num_ingreso}")
            orden_id = result[0]
            # Actualizar el campo update en orden_trabajo_datos_extra usando orden_id
            cursor.execute(
                "UPDATE orden_trabajo_datos_extra SET update = %s WHERE orden_id = %s",
                (valor_update if valor_update else None, orden_id)
            )
            conn.commit()
    finally:
        conn.close()