import os

# Limitar threads
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["OMP_NUM_THREADS"]      = "1"
os.environ["MKL_NUM_THREADS"]      = "1"
os.environ["NUMEXPR_NUM_THREADS"]  = "1"

import pandas as pd
import mysql.connector
from dotenv import load_dotenv

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Obtener los datos de conexión desde las variables de entorno
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")

# Conexión a la base de datos MySQL
conexion = mysql.connector.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DATABASE,
    allow_local_infile=True
)
cursor = conexion.cursor()

# Ruta a la carpeta con las consultas
carpeta_consultas = 'consultas'

# Diccionario de carpetas y tablas
carpetas_tablas = {
    'jcom2': ('t_temp_vat_jcom2_es', 't_informe_vat_jcom2_es')
}

# Lista de columnas que contienen fechas
columnas_fecha = [
    'TAX_CALCULATION_DATE', 'TRANSACTION_DEPART_DATE', 
    'TRANSACTION_ARRIVAL_DATE', 'TRANSACTION_COMPLETE_DATE', 'VAT_INV_EXCHANGE_RATE_DATE'
]

def eliminar_archivo_seguro(path):
    """Elimina un archivo si existe y no es .gitkeep."""
    try:
        if os.path.isfile(path) and os.path.basename(path).lower() != '.gitkeep':
            os.remove(path)
            print(f"[INFO] Archivo eliminado: {path}")
    except Exception as e:
        print(f"[WARN] No se pudo eliminar '{path}': {e}")

# Recorrer cada carpeta en el diccionario
for subcarpeta, (tabla_temp, tabla_final) in carpetas_tablas.items():
    # Verificar si la carpeta existe y tiene archivos TXT
    if os.path.exists(subcarpeta) and os.listdir(subcarpeta):
        archivos_txt = [archivo for archivo in os.listdir(subcarpeta) if archivo.endswith('.txt') and not archivo.endswith('_corregido.txt')]

        for archivo_txt in archivos_txt:
            ruta_archivo = os.path.join(subcarpeta, archivo_txt)
            ruta_corregida = ruta_archivo.replace('.txt', '_corregido.txt')

            try:
                # Leer el archivo TXT delimitado por tabulaciones
                df = pd.read_csv(ruta_archivo, sep='\t', engine='python')

                # Convertir las columnas de fecha al formato correcto
                for col in columnas_fecha:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], format='%d-%m-%Y', errors='coerce').dt.strftime('%Y-%m-%d')

                # Guardar el DataFrame corregido en un nuevo archivo TXT
                df.to_csv(ruta_corregida, sep='\t', index=False, na_rep='NULL')
                print(f"[INFO] Archivo '{archivo_txt}' corregido y guardado como '{ruta_corregida}'.")

                # Asegurar que la ruta sea absoluta para MySQL
                ruta_completa_mysql = os.path.abspath(ruta_corregida).replace("\\", "/")

                # 1) Eliminar datos antiguos en la tabla temporal
                with open(os.path.join(carpeta_consultas, 'delete_query.sql'), 'r', encoding='utf-8') as file:
                    delete_query = file.read().format(tabla_temp=tabla_temp)
                cursor.execute(delete_query)
                conexion.commit()
                print(f"[INFO] Datos anteriores eliminados de la tabla temporal '{tabla_temp}'.")

                # 2) Insertar en tabla temporal desde el TXT corregido
                with open(os.path.join(carpeta_consultas, 'insercion_query.sql'), 'r', encoding='utf-8') as file:
                    insercion = file.read().format(ruta_completa_mysql=ruta_completa_mysql, tabla_temp=tabla_temp)
                cursor.execute(insercion)
                conexion.commit()
                print(f"[INFO] Datos del archivo '{archivo_txt}' insertados en la tabla temporal '{tabla_temp}'.")

                # 3) Actualizar columna 'indice'
                with open(os.path.join(carpeta_consultas, 'update_indice_query.sql'), 'r', encoding='utf-8') as file:
                    update_indice = file.read().format(tabla_temp=tabla_temp)
                cursor.execute(update_indice)
                conexion.commit()
                print(f"[INFO] Columna 'indice' actualizada en '{tabla_temp}'.")

                # 4) Actualizar columna 'indicemd5'
                with open(os.path.join(carpeta_consultas, 'update_md5_query.sql'), 'r', encoding='utf-8') as file:
                    update_md5 = file.read().format(tabla_temp=tabla_temp)
                cursor.execute(update_md5)
                conexion.commit()
                print(f"[INFO] Columna 'indicemd5' actualizada en '{tabla_temp}'.")

                # 5) Insertar nuevos en tabla final
                with open(os.path.join(carpeta_consultas, 'insercion_final_query.sql'), 'r', encoding='utf-8') as file:
                    insercion_final = file.read().format(tabla_temp=tabla_temp, tabla_final=tabla_final)
                cursor.execute(insercion_final)
                conexion.commit()
                print(f"[INFO] Datos nuevos insertados en '{tabla_final}' desde '{tabla_temp}'.")

                # 6) Si todo salió bien, eliminar archivos (original y corregido)
                eliminar_archivo_seguro(ruta_archivo)
                eliminar_archivo_seguro(ruta_corregida)

            except pd.errors.ParserError as e:
                print(f"[ERROR] Formato inválido en '{archivo_txt}' en '{subcarpeta}': {str(e)}")
            except Exception as e:
                print(f"[ERROR] Error al procesar '{archivo_txt}' en '{subcarpeta}': {str(e)}")
                # No eliminamos archivos si hubo error, continuamos con el siguiente
                continue

# Cerrar conexión
try:
    cursor.close()
except Exception:
    pass
try:
    conexion.close()
except Exception:
    pass

print("[OK] Proceso completado.")
