#Interfaz que generara un reporte a partir de una conexion de PostgreSQL 
#paran posteriormente formatearlo y generar un CSV en el formato deseado
import psycopg2
import csv
import json
from datetime import datetime

def connect_to_db():
    try:
        # Conectar a la base de datos PostgreSQL
        connection = psycopg2.connect(
            dbname="labsis",
            user="labsis",
            password="labsis",
            host="172.17.90.26",  # Cambia esto si tu base de datos está en otro host
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
        query = """SELECT OT.num_ingreso, OT.fecha, P.nombre, P.apellido, P.sexo, P.ci_paciente, RN.actualizado_timestamp,RN.valor, PR.id, OT.numero, OTDE.edad_dias, OTDE.edad_horas, SM.codigo_bloom, SM.codigo_dtic
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
        orden_fin = 169462
        cursor.execute(query, (orden_inicio, orden_fin))  # Cambia esto por tu consulta
        # WHERE OT.numero = '2508210123'
        rows = cursor.fetchall()
        fecha_recepcion = "#2025-08-14#"  # TODO Obtener la fecha de recepción de los datos
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
            dt_obj = datetime.strptime(str(row[1]), '%Y-%m-%d %H:%M:%S.%f')
            fecha_toma = dt_obj.strftime('%Y-%m-%d')
            nombre_paciente = f"{row[2]} {row[3]}"
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
        # Si la boleta no está en el diccionario, la agregamos
            if num_ingreso not in boletas_agrupadas:
                boletas_agrupadas[num_ingreso] = {
                    "codigoE": f'"{codigo_bloom}"',
                    "Boleta": f'"{num_ingreso}"',
                    "FechaTomaMx": '#'+str(fecha_toma)+'#',
                    "Paciente": f'"{nombre_paciente}"',
                    "Edad": f'"{edad_dias}"', # Edad del paciente en dias
                    "Sexo": f'"{sexo_paciente}"',
                    "Expediente": f'"{ci_paciente}"',
                    "Recepcion": "#NULL#", # TODO Fecha de Recepcion (Manual temporalmente)
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
                    boletas_agrupadas[num_ingreso]["Resultados"][clave_resultado] = f'"{valor_resultado}"'
                boletas_agrupadas[num_ingreso]["StdoBoleta"] = "A" # Si encuentra un examen cambia el estado de la boleta a A
                if actualizado_timestamp is not None:
                    #actualizado_timestamp = "#NULL#"
                    boletas_agrupadas[num_ingreso]["Recepcion"] = fecha_recepcion # Asigna la fecha de recepcion
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

# Escribir los datos en un archivo CSV
def write_to_csv(boletas_agrupadas, filename="reporte_labsis.csv"):
    try:
        
        #Definicio de los encabezados del CSV
        encabezados = ['"codigoE"', '"Boleta"', '"FechaTomaMx"', '"Paciente"', '"Edad"', '"Sexo"', '"Expediente"', 
                       '"Recepcion"', '"Procesamiento"', '"FResultado"', '"Resultado"', '"FechaRechazo"', '"EstadoPaciente"', 
                       '"StdoBoleta"', '"Update"', '"ReferidoPor"', '"Id"', '"ResultadoIRT"', '"ResultadoPKU"', '"Resultado17OH"', 
                       '"ResultadoJarabeA1"', '"ResultadoJarabeA2"', '"ResultadoTyr"', '"ResultHbF"', '"ResultHbA"', '"ResultHbS"', 
                       '"ResultHbC"']

        #Mapeo de ids de resultados a los encabezados:
        
        id_to_header = {
            852: "Resultado",
            859: "ResultadoIRT",
            854: "ResultadoPKU",
            883: "Resultado17OH",
            886: "ResultadoJarabeA1",
            885: "ResultadoJarabeA2",
            888: "ResultadoTyr",
            889: "ResultHbF",
            890: "ResultHbA",
            891: "ResultHbS",
            892: "ResultHbC"
        }
        
        with open(filename, mode="w", newline="") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=encabezados)
            writer.writeheader()
            for boleta_data in boletas_agrupadas.values():
                row = boleta_data.copy()
                # Extraemos los resultados del sub-diccionario y los asignamos a las columnas correspondientes
                resultados = row.pop("Resultados")
                for id_resultado, valor in resultados.items():
                    header = id_to_header.get(id_resultado)
                    if header:
                        row[header] = valor
                
                #Agregando campos especiales
                #row["Boleta"] = row.get("Boleta")
                writer.writerow(row)
        print(f"Datos escritos en {filename} exitosamente.")
    except Exception as e:
        print(f"Error escribiendo el archivo CSV: {e}")

# cLASE PARA LEER UN CSV LLAMADO 

if __name__ == "__main__":
    db_connection = connect_to_db()
    datos_obtenidos = generate_report(db_connection)
    write_to_csv(datos_obtenidos)
    print("Proceso completado.")