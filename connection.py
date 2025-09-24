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
            #host="172.17.90.26" # Ambiente de producción
            host="localhost", # Ambiente de desarrollo
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
               SM.codigo_bloom, SM.codigo_dtic, OTDE.fecha_recepcion, RA.valor, RA.validado_por
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
                actualizado_timestamp = parse_datetime_with_time(row[6])
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

                # Procesar resultado si hay id_prueba válido
                if id_prueba is not None:
                    clave_resultado = int(id_prueba)
                    
                    if clave_resultado in boletas_agrupadas[num_ingreso]["Resultados"]:
                        valor_resultado = determine_result_value(id_prueba, row[7], row[15], row[16])
                        
                        # Actualizar solo si hay valor válido
                        if valor_resultado and valor_resultado != "" and valor_resultado != '#NULL#':
                            boletas_agrupadas[num_ingreso]["Resultados"][clave_resultado] = valor_resultado
                            boletas_agrupadas[num_ingreso]["StdoBoleta"] = "A"
                            
                            if actualizado_timestamp:
                                boletas_agrupadas[num_ingreso]["Procesamiento"] = actualizado_timestamp
                                boletas_agrupadas[num_ingreso]["FResultado"] = actualizado_timestamp
                            
                            boletas_agrupadas[num_ingreso]["FechaRechazo"] = "#NULL#"

    except Exception as e:
        print(f"Error generando el reporte: {e}")
    finally:
        if connection is not None:
            connection.close()
            print("Conexión a la base de datos cerrada.")
    
    return boletas_agrupadas

def format_result_value(val: Any) -> str:
    """Formatea un valor de resultado para CSV."""
    if isinstance(val, str):
        val_strip = val.strip()
        if val_strip == "#NULL#" or val_strip.startswith("#"):
            return val_strip
        try:
            return f"{float(val_strip):.1f}"
        except ValueError:
            return f'"{val_strip}"'
    elif isinstance(val, (float, Decimal)):
        return f"{val:.1f}"
    else:
        return f'"{val}"'

def write_to_csv(boletas_agrupadas: Dict[str, Dict[str, Any]], filename: str = "reporte_labsis.csv") -> None:
    """Escribe los datos agrupados a un archivo CSV."""
    
    # Configuración de columnas y mapeo
    encabezados_internos = [
        "codigoE", "Boleta", "FechaTomaMx", "Paciente", "Edad", "Sexo", "Expediente",
        "Recepcion", "Procesamiento", "FResultado", "Resultado", "FechaRechazo", "EstadoPaciente",
        "StdoBoleta", "Update", "ReferidoPor", "Id", "ResultadoIRT", "ResultadoPKU", "Resultado17OH",
        "ResultadoJarabeA1", "ResultadoJarabeA2", "ResultadoTyr", "ResultHbF", "ResultHbA", "ResultHbS",
        "ResultHbC"
    ]

    id_to_header = {
        "852": "Resultado", "859": "ResultadoIRT", "854": "ResultadoPKU", "883": "Resultado17OH",
        "886": "ResultadoJarabeA1", "885": "ResultadoJarabeA2", "888": "ResultadoTyr",
        "889": "ResultHbF", "890": "ResultHbA", "891": "ResultHbS", "892": "ResultHbC"
    }

    try:
        with open(filename, mode="w", newline="", encoding="ANSI") as f:
            # Escribir encabezados
            f.write(','.join([f'"{h}"' for h in encabezados_internos]) + '\n')

            for boleta_data in boletas_agrupadas.values():
                row = ["#NULL#"] * len(encabezados_internos)

                # Llenar campos principales
                for idx, campo in enumerate(encabezados_internos):
                    if campo not in id_to_header.values():
                        val = boleta_data.get(campo, "#NULL#")
                        row[idx] = f'"{val}"' if not (isinstance(val, str) and val.startswith("#")) else val

                # Llenar resultados
                resultados = boleta_data.get('Resultados', {})
                for id_str, col_name in id_to_header.items():
                    idx = encabezados_internos.index(col_name)
                    val = resultados.get(int(id_str), "#NULL#")
                    row[idx] = format_result_value(val)

                f.write(','.join(row) + '\n')

        print(f"Datos escritos en {filename} exitosamente.")

    except Exception as e:
        print(f"Error escribiendo el archivo CSV: {e}")