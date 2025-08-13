# Proyecto: Ingesta VAT JCOM2 (TXT → MySQL)

Este proyecto automatiza la **ingesta de reportes VAT** (archivos `.txt` delimitados por tabulaciones) hacia MySQL.  
Corrige formatos de fecha desde `dd-mm-YYYY` a `YYYY-mm-dd`, guarda una versión `_corregido.txt` y ejecuta una serie de **consultas SQL** para cargar datos en una tabla temporal y consolidarlos en una tabla final.

---

## 📂 Estructura recomendada del proyecto

```
.
├── .env                         # Variables de entorno (ver sección Configuración)
├── consultas/                   # Consultas SQL usadas por el script
│   ├── delete_query.sql
│   ├── insercion_query.sql
│   ├── update_indice_query.sql
│   ├── update_md5_query.sql
│   └── insercion_final_query.sql
├── jcom2/                       # Carpeta con los archivos TXT (origen)
│   ├── ejemplo1.txt
│   └── ejemplo2.txt
└── procesar_vat.py              # Script de Python (el que muestras en el mensaje)
```

> **Nota:** El script espera, por defecto, la subcarpeta `jcom2/` y la carpeta `consultas/` en el mismo nivel que el archivo `.py`.

---

## ⚙️ Requisitos

- Python **3.8+**
- MySQL con `local_infile=1` habilitado
- Librerías de Python:
  - `pandas`
  - `mysql-connector-python`
  - `python-dotenv`

Instala dependencias (opcionalmente con un virtualenv):
```bash
pip install pandas mysql-connector-python python-dotenv
```

---

## 🔐 Configuración (.env)

Crea un archivo `.env` junto al script con las credenciales de tu BD:

```env
MYSQL_HOST=localhost
MYSQL_USER=usuario
MYSQL_PASSWORD=contraseña
MYSQL_DATABASE=nombre_bd
```

---

## 🧠 ¿Qué hace el script?

1. **Lee variables de entorno** para conectarse a MySQL.
2. **Recorre la carpeta `jcom2/`**, buscando archivos `.txt` separados por tabulaciones.
3. **Convierte fechas** en las columnas (si existen):
   - `TAX_CALCULATION_DATE`
   - `TRANSACTION_DEPART_DATE`
   - `TRANSACTION_ARRIVAL_DATE`
   - `TRANSACTION_COMPLETE_DATE`
   - `VAT_INV_EXCHANGE_RATE_DATE`
4. **Guarda un TXT corregido** por cada archivo de entrada, con sufijo `_corregido.txt`.
5. **Ejecuta consultas SQL** desde `consultas/` en este orden:
   - `delete_query.sql` → limpia la tabla temporal.
   - `insercion_query.sql` → inserta datos del TXT corregido en la tabla temporal.
   - `update_indice_query.sql` → rellena/ajusta la columna `indice`.
   - `update_md5_query.sql` → rellena/ajusta la columna `indicemd5`.
   - `insercion_final_query.sql` → inserta **solo nuevos** registros en la tabla final.
6. **Confirma (commit) los cambios** tras cada paso y registra mensajes en consola.

La **tabla temporal** y la **final** se definen en el diccionario `carpetas_tablas` del script, por ejemplo:
```python
carpetas_tablas = {
    'jcom2': ('t_temp_vat_jcom2_es', 't_informe_vat_jcom2_es')
}
```
Puedes añadir más pares `subcarpeta -> (tabla_temp, tabla_final)` si agregas nuevos orígenes.

---

## ▶️ Ejecución

1. Copia tus **archivos .txt** (tab-delimited) dentro de `jcom2/`.
2. Verifica que existan las **consultas** en `consultas/` y se ajusten a tus tablas.
3. Ejecuta el script:
   ```bash
   python procesar_vat.py
   ```

> El script genera archivos `*_corregido.txt` en la misma carpeta `jcom2/` y los usa para la ingesta.

---

## 🧩 Ejemplos de consultas (referencia)

- **delete_query.sql**
  ```sql
  DELETE FROM {tabla_temp};
  ```

- **insercion_query.sql** (ejemplo típico con `LOAD DATA`)
  ```sql
  LOAD DATA LOCAL INFILE '{ruta_completa_mysql}'
  INTO TABLE {tabla_temp}
  CHARACTER SET utf8mb4
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
  IGNORE 1 LINES
  (... columnas en el mismo orden del TXT ...);
  ```

- **update_indice_query.sql**
  ```sql
  UPDATE {tabla_temp}
  SET indice = CONCAT_WS('|', col1, col2, col3); -- ajusta a tus claves
  ```

- **update_md5_query.sql**
  ```sql
  UPDATE {tabla_temp}
  SET indicemd5 = MD5(indice);
  ```

- **insercion_final_query.sql**
  ```sql
  INSERT INTO {tabla_final} (...cols...)
  SELECT ...cols...
  FROM {tabla_temp} t
  WHERE NOT EXISTS (
      SELECT 1
      FROM {tabla_final} f
      WHERE f.indicemd5 = t.indicemd5
  );
  ```

> Ajusta los nombres de columnas a tu layout real del TXT y de las tablas.

---

## 📝 Notas y consejos

- El script usa `allow_local_infile=True` y construye rutas absolutas con separador `/` para compatibilidad con MySQL en Windows.
- Si obtienes error con `LOAD DATA LOCAL INFILE`, verifica:
  - En el **servidor**: `local_infile=1`.
  - En el **cliente/conector**: `allow_local_infile=True` (ya incluido).
  - Permisos del usuario MySQL.
- Fechas inválidas quedan como `NULL` por `errors='coerce'`.
- Puedes cambiar el separador de salida del TXT corregido (actualmente `\t`) o el `na_rep` (`NULL`) según tus necesidades.
- Para procesar **múltiples orígenes**, añade más claves en `carpetas_tablas` y crea las carpetas respectivas.

---

## 📄 Licencia

Uso interno. Sin licencia pública.
