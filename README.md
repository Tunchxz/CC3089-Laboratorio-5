# Laboratorio 5: SQL + NoSQL

Este laboratorio pone en práctica lo aprendido en la conferencia SQL + NoSQL, es decir, integrar datos que se encuentren en una base de datos relacional con datos que se encuentren en una base de datos no relacional, para luego ser cargados en una base de datos que haga las veces de un data warehouse y finalmente, obtener insights relevantes de los datos ya integrados.

## Estructura

```text
CC3089-Laboratorio-5/
├── db_source/                  # Scripts SQL y CSV de inicialización para DB Transaccional
├── db_warehouse/               # Modelo Relacional DDL de la DB Analítica de Destino
├── output/                     # Gráficas PNG resultantes del análisis de datos
├── docker-compose.yml          # Orquestador de la infraestructura de Bases de Datos
├── .env                        # Credenciales seguras (Variables de Entorno)
├── etl.py                      # Script ETL en Python (Extracción, Transformación, Carga)
├── analisis.py                 # Script de análisis y creación de gráficas con Pandas/Seaborn
└── README.md                   # Documentación principal
```

## Cómo Ejecutar

### 1. Requisitos Previos

- Instalar **Docker** y **Docker Compose**.  

- Instalar **Python 3.8+**.  

- Un entorno virtual (`.venv`) configurado con las dependencias listadas en el proyecto.  

*(Si tu entorno virtual no está listo, instálalo con: `pip install pandas psycopg2-binary pymongo sqlalchemy python-dotenv matplotlib seaborn`)*

### 2. Levantar la Infraestructura

El entorno utiliza dos contenedores locales separados simulando arquitecturas reales:  

- `db_source`: Base de datos de origen poblada automáticamente con los *.csv* y *.sql*.  

- `db_dw`: El clúster Data Warehouse alojando el DDL de destino.  

Abre la terminal en la raíz del proyecto y ejecuta:  

```bash
docker-compose up -d
```  

*(Nota: Espera unos 10 a 15 segundos para que los archivos de inicialización `.csv` terminen de cargarse silenciosamente dentro de postgres).*

### 3. Ejecutar el Proceso ETL

Una vez las bases de datos están sanas (`pg_isready`), cruzamos la data de Postgres y la de MongoDB ejecutando:  

```bash
python etl.py
```  

*(El script tomará los datos, ejecutará traducciones de idiomas, resolverá conflictos de nulos promediando indicadores poblacionales y cargará la tabla dimensional final al Warehouse).*

### 4. Generar el Análisis y Gráficas

Con los datos unificados descansando en el Warehouse, ejecuta el panel analítico:  

```bash
python analisis.py
```  

*(Se procesarán y exportarán 3 archivos en formato PNG hacia la carpeta `/output`).*

## Explicación de Etapas del Laboratorio

1. **Contenerización (Docker)**:  
   
   Se aíslan dos instancias de PostgreSQL en una misma red *bridge* para asegurar comunicación controlada y se mapean puertos diferentes para evitar colisiones en tu equipo (`5432` y `5433`). La base Source consume sus scripts iniciales vía volúmenes `/docker-entrypoint-initdb.d/`.
   
2. **El Proceso ETL (`etl.py`)**:  
   
   - **Extract**: Usa SQLAlchemy y PyMongo para sacar los datos tabulares de SQL y los documentos anidados JSON de Atlas.  

   - **Transform**: Aplica diccionarios en memoria para traducir nombres (llaves de cruce), extrae claves de los diccionarios incrustados en Mongo e imputa nulos usando de respaldo columnas entre MongoDB o Postgres (estrategia *Combine First* y promedios). Finalmente se deshace de elementos basuras (completitud de dataset).  

   - **Load**: Purga (`TRUNCATE CASCADE`) los datos antiguos si los hubiese para no romper jerarquías `Foreign Key` en el DDL e inserta (*Append*) el dataframe al modelo Dimension/Hechos de destino.
   
3. **Análisis de Discrepancias (`analisis.py`)**:  
   
   Demuestra el poder de un Data Warehouse al interconectar las variables en gráficas descriptivas:  

   - Evaluando países atípicos: Lugares baratos localmente pero de explotación turística cara.  
   - Entendiendo el impacto poblacional e inflacionario en los mercados recreativos.  
   - Perfilando los promedios económicos para orientar a turistas entre distintos continentes.  
