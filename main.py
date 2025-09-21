#Interfaz que generara un reporte a partir de una conexion de PostgreSQL 
#paran posteriormente formatearlo y generar un CSV en el formato deseado
import string
import psycopg2
import csv
import json
from datetime import datetime
from decimal import Decimal

def connect_to_db():
    try:
        # Conectar a la base de datos PostgreSQL
        connection = psycopg2.connect(
            dbname="labsis",
            user="labsis",
            password="labsis",
            #host="172.17.90.26",  # Cambia esto si tu base de datos está en otro host
            host="localhost",  # Cambia esto si tu base de datos está en otro host
            port="5432"  # Cambia esto si tu base de datos usa otro puerto
        )
        return connection
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

def generate_report(connection):
    if connection is None:
        print("No database connection available.")
        return
    try:
        cursor = connection.cursor()
        query = """SELECT OT.num_ingreso, OT.fecha_toma_muestra, P.nombre, P.apellido, P.sexo, P.ci_paciente, RN.actualizado_timestamp,RN.valor, PR.id, OT.numero, OTDE.edad_dias, OTDE.edad_horas, SM.codigo_bloom, SM.codigo_dtic, OTDE.fecha_recepcion
        FROM orden_trabajo OT
        LEFT JOIN paciente P ON OT.paciente_ID = P.id
        LEFT JOIN prueba_orden PO ON OT.id = PO.orden_id
        LEFT JOIN resultado_numer RN ON PO.id = RN.pruebao_id
        LEFT JOIN prueba PR ON PR.id = PO.prueba_id
        LEFT JOIN orden_trabajo_datos_extra OTDE ON OT.id = OTDE.orden_id
        LEFT  JOIN servicio_medico SM ON OT.servicio_medico_id = SM.id
        WHERE OT.id BETWEEN %s AND %s
        """ 
        #fecha_inicio = '2025-09-01'  # TODO Reemplazar por un valor obtenido de un datepicker
        #fecha_fin = '2025-09-09'     # TODO Reemplazar por un valor obtenido de un datepicker
        orden_inicio = 169197
        orden_fin = 170955
        cursor.execute(query, (orden_inicio, orden_fin))  # Cambia esto por tu consulta
        rows = cursor.fetchall()
        #definicion de resultados
        resultados_esperados = {
            852: '#NULL#',
            859: '#NULL#',
            854: '#NULL#',
            883: '#NULL#',
            886: '#NULL#',
            885: '#NULL#',
            888: '#NULL#',
            889: '#NULL#',
            890: '#NULL#',
            891: '#NULL#',
            892: '#NULL#'
        }

        #Diccionario para agrupar las pruebas por numero de ingreso
        boletas_agrupadas = {}
        for row in rows:
            num_ingreso = row[0]
            if num_ingreso == '1':
                continue  # Saltar entradas con num_ingreso igual a 1
            #convertir fecha de toma de muestra recibida de ""2025-08-22 00:00:00-06"" a "2025-08-22"
            dt_obj = datetime.strptime(str(row[1]), '%Y-%m-%d %H:%M:%S%z') if row[1] is not None else None
            fecha_toma = dt_obj.strftime('%Y-%m-%d') if dt_obj is not None else "NULL"
            nombre_paciente = f"{str(row[2]).strip()} {str(row[3]).strip()}"
            sexo_paciente = row[4]
            ci_paciente = row[5]
            dt_obj = datetime.strptime(str(row[6]), '%Y-%m-%d %H:%M:%S.%f') if row[6] is not None else None
            actualizado_timestamp = f"#{dt_obj.strftime('%Y-%m-%d %H:%M:%S')}#" if dt_obj is not None else None # Fecha y hora del resultado
            valor_resultado = row[7] if row[7] is not None else '#NULL#'
            id_prueba = row[8]
            edad_dias = row[10]
            edad_horas = row[11]
            # Si la edad en dias es menor a 1, calculamos la edad en dias a partir de horas
            if edad_dias == 0:
                edad_dias = int(edad_horas / 24)
            codigo_bloom = row[12]
            codigo_dtic = row[13]
            #convertier fecha de recepcion recibida de "2025-08-28 00:00:00" a "2025-08-28"
            dt_obj = datetime.strptime(str(row[14]), '%Y-%m-%d %H:%M:%S') if row[14] is not None else None
            fecha_recepcion = dt_obj.strftime('%Y-%m-%d') if dt_obj is not None else None
        # Si la boleta no está en el diccionario, la agregamos
            if num_ingreso not in boletas_agrupadas:
                boletas_agrupadas[num_ingreso] = {
                    "codigoE": codigo_bloom,
                    "Boleta": num_ingreso,
                    "FechaTomaMx": '#'+str(fecha_toma)+'#',
                    "Paciente": utf_to_ansi(nombre_paciente),
                    "Edad": edad_dias, # Edad del paciente en dias
                    "Sexo": sexo_paciente,
                    "Expediente": ci_paciente,
                    "Recepcion": fecha_recepcion,
                    "Procesamiento": "#NULL#", # Fecha de Procesamiento
                    "FResultado": "#NULL#", # Fecha de Resultado
                    "FechaRechazo": fecha_recepcion, # Fecha de Rechazo
                    "EstadoPaciente": "#NULL#", # Segun muestra por defecto es #NULL#
                    "StdoBoleta": "R", # Valor por defecto, si encunetra un examen debe cambiarlo a A
                    "Update":"#NULL#", # TODO Pensar como utilizar este valor
                    "ReferidoPor": "#NULL#", # Segun muestra por defecto es #NULL#
                    "Id": codigo_dtic,
                    # Inicializamos los resultados de las pruebas
                    "Resultados": resultados_esperados.copy()
                }
            clave_resultado = int(id_prueba)
            if clave_resultado in boletas_agrupadas[num_ingreso]["Resultados"]:
                if valor_resultado is not None and valor_resultado != "":
                    boletas_agrupadas[num_ingreso]["Resultados"][clave_resultado] = valor_resultado
                boletas_agrupadas[num_ingreso]["StdoBoleta"] = "A" # Si encuentra un examen cambia el estado de la boleta a A
                if actualizado_timestamp is not None:
                    boletas_agrupadas[num_ingreso]["Procesamiento"] = actualizado_timestamp # Asigna la fecha de procesamiento
                    boletas_agrupadas[num_ingreso]["FResultado"] = actualizado_timestamp # Asigna la fecha de resultado
                boletas_agrupadas[num_ingreso]["FechaRechazo"] = "#NULL#" # Quita la fecha de rechazo si hay resultado
            # Si el nombre de la prueba no está en nuestro diccionario esperado, la ignoramos.
            #elif boletas_agrupadas[num_ingreso]["StdoBoleta"] == "R":
                #boletas_agrupadas[num_ingreso]["FechaRechazo"] = boletas_agrupadas[num_ingreso]["FechaRecepcion"] # Mantiene la fecha de rechazo si es un rechazo
            else:
                continue # Ignorar resultados no esperados FIXME: ESTO ES TEMPORAL, HAY QUE AVISAR AL USUARIO
            # Asignamos el valor del resultado al sub-diccionario 'resultados'.
            # Usamos un `try-except` para evitar errores.
            try:
                boletas_agrupadas[num_ingreso]["Resultados"][clave_resultado] = valor_resultado
            except KeyError:
                print(f"Advertencia: La prueba '{id_prueba}' no está en la lista de resultados esperados.")

        print("\n--- Diccionario de datos agrupados por boleta ---")
        print(json.dumps(boletas_agrupadas, indent=4, default=str))
        print("--- Fin del diccionario ---")
    except Exception as e:
        print(f"Error generando el reporte: {e}")
    finally:
        if connection is not None:
            connection.close()
            print("Conexión a la base de datos cerrada.")
            return boletas_agrupadas

def write_to_csv(boletas_agrupadas, filename="reporte_labsis.csv"):
    try:
        # Columnas internas
        encabezados_internos = [
            "codigoE", "Boleta", "FechaTomaMx", "Paciente", "Edad", "Sexo", "Expediente",
            "Recepcion", "Procesamiento", "FResultado", "Resultado", "FechaRechazo", "EstadoPaciente",
            "StdoBoleta", "Update", "ReferidoPor", "Id", "ResultadoIRT", "ResultadoPKU", "Resultado17OH",
            "ResultadoJarabeA1", "ResultadoJarabeA2", "ResultadoTyr", "ResultHbF", "ResultHbA", "ResultHbS",
            "ResultHbC"
        ]

        # Mapeo de id de prueba a columna
        id_to_header = {
            "852": "Resultado",
            "859": "ResultadoIRT",
            "854": "ResultadoPKU",
            "883": "Resultado17OH",
            "886": "ResultadoJarabeA1",
            "885": "ResultadoJarabeA2",
            "888": "ResultadoTyr",
            "889": "ResultHbF",
            "890": "ResultHbA",
            "891": "ResultHbS",
            "892": "ResultHbC"
        }

        with open(filename, mode="w", newline="", encoding="ANSI") as f:
            # Escribimos encabezados con comillas
            f.write(','.join([f'"{h}"' for h in encabezados_internos]) + '\n')

            for boleta_data in boletas_agrupadas.values():
                # Inicializamos la fila con #NULL#
                row = ["#NULL#"] * len(encabezados_internos)

                # Llenamos campos principales (excepto resultados)
                for idx, k in enumerate(encabezados_internos):
                    if k in id_to_header.values():
                        continue
                    val = boleta_data.get(k, "#NULL#")
                    row[idx] = f'"{val}"' if not (isinstance(val, str) and val.startswith("#")) else val

                # Llenamos resultados en la columna correcta
                resultados = boleta_data.get('Resultados', {})
                for id_res_str, col_name in id_to_header.items():
                    idx = encabezados_internos.index(col_name)
                    val = resultados.get(int(id_res_str), "#NULL#")  # usar la clave tal cual como string

                    if isinstance(val, str):
                        val_strip = val.strip()
                        if val_strip == "#NULL#" or val_strip.startswith("#"):
                            row[idx] = val_strip
                        else:
                            try:
                                row[idx] = f"{float(val_strip):.1f}"  # 1 decimal
                            except ValueError:
                                row[idx] = f'"{val_strip}"'
                    elif isinstance(val, (float, Decimal)):
                        row[idx] = f"{val:.1f}"
                    else:
                        row[idx] = f'"{val}"'

                # Escribimos la fila
                f.write(','.join(row) + '\n')

        print(f"Datos escritos en {filename} exitosamente.")

    except Exception as e:
        print(f"Error escribiendo el archivo CSV: {e}")

# Clase que convierte el nombre que estan en UTF a ANSI. cambia la ñ por n y las vocales con tilde por vocal sin tilde 
def utf_to_ansi(text):
    replacements = {
        'ñ': 'n', 'Ñ': 'N',
        'á': 'a', 'Á': 'A',
        'é': 'e', 'É': 'E',
        'í': 'i', 'Í': 'I',
        'ó': 'o', 'Ó': 'O',
        'ú': 'u', 'Ú': 'U'
    }
    for utf_char, ansi_char in replacements.items():
        text = text.replace(utf_char, ansi_char)
    return text






# cLASE PARA LEER UN CSV LLAMADO 

if __name__ == "__main__":
    db_connection = connect_to_db()
    datos_obtenidos = generate_report(db_connection)
    write_to_csv(datos_obtenidos)
    print("Proceso completado.")