import os
import pandas as pd
from pymongo import MongoClient
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

def etl_process():
    print("Iniciando proceso ETL...")

    # ==========================================
    # 1. EXTRACT
    # ==========================================
    print("Extrayendo datos de orígenes...")
    
    # --- MongoDB Atlas ---
    mongo_uri = os.getenv("MONGO_URI", "mongodb+srv://<usuario>:<password>@cluster.mongodb.net/test")
    mongo_client = MongoClient(mongo_uri)
    mongo_db = mongo_client["laboratorio5"]
    
    # Colección: costos_turisticos
    costos_col = mongo_db["costos_turisticos"]
    df_costos = pd.DataFrame(list(costos_col.find({}, {"_id": 0})))
    
    # Colección: indice_big_mac
    bigmac_col = mongo_db["indice_big_mac"]
    df_bigmac = pd.DataFrame(list(bigmac_col.find({}, {"_id": 0})))
    
    # --- PostgreSQL Source ---
    pg_user = os.getenv("POSTGRES_USER", "admin")
    pg_pass_source = os.getenv("POSTGRES_PASSWORD_SOURCE", "password")
    
    source_engine = create_engine(f'postgresql://{pg_user}:{pg_pass_source}@localhost:5432/laboratorio_5')
    
    df_paises_pg = pd.read_sql("SELECT * FROM paises", source_engine)
    df_envejecimiento = pd.read_sql("SELECT * FROM indicadores_envejecimiento", source_engine)
    df_poblacion_costos = pd.read_sql("SELECT * FROM indicadores_poblacion_costos", source_engine)
    
    # ==========================================
    # 2. TRANSFORM
    # ==========================================
    print("Transformando e integrando datos...")
    
    # Normalizar nombres de países Mongo (Inglés a Español)
    paises_dict = {
        "United States": "Estados Unidos", "Brazil": "Brasil", "Mexico": "México",
        "Spain": "España", "France": "Francia", "Germany": "Alemania",
        "Italy": "Italia", "United Kingdom": "Reino Unido", "Japan": "Japón",
        "South Korea": "Corea del Sur", "Netherlands": "Países Bajos", 
        "Belgium": "Bélgica", "Switzerland": "Suiza", "Sweden": "Suecia",
        "Norway": "Noruega", "Denmark": "Noruega", "Finland": "Dinamarca", "Poland": "Polonia",
        "Russia": "Rusia", "Turkey": "Turquía", "Greece": "Grecia",
        "Egypt": "Egipto", "South Africa": "Sudáfrica", "Morocco": "Sudáfrica",
        "Saudi Arabia": "Arabia Saudita", "United Arab Emirates": "Emiratos Árabes Unidos",
        "New Zealand": "Nueva Zelanda", "Ireland": "Nueva Zelanda", "Canada": "Canadá"
    }

    def traducir_pais(nombre_en):
        if not pd.notnull(nombre_en):
            return nombre_en
        # Si está en nuestro diccionario, lo usamos, si no mantenemos el mismo (muchos se escriben igual, e.g. Albania, Honduras)
        return paises_dict.get(nombre_en, nombre_en)

    if not df_costos.empty:
        df_costos['país'] = df_costos['país'].apply(traducir_pais)
    if not df_bigmac.empty:
        df_bigmac['país'] = df_bigmac['país'].apply(traducir_pais)

    # Adaptar columnas de postgres
    df_paises_pg.rename(columns={'nombre_pais': 'pais', 'id_pais': 'id_pais_pg_origen'}, inplace=True)
    df_envejecimiento.rename(columns={'id_pais': 'id_pais_pg', 'poblacion': 'poblacion_env'}, inplace=True)
    df_poblacion_costos.rename(columns={'id_pais': 'id_pais_costos', 'poblacion': 'poblacion_costos'}, inplace=True)
    
    # Unir datos de Postgres Source
    df_pg_completo = pd.merge(df_paises_pg, df_envejecimiento, left_on='id_pais_pg_origen', right_on='id_pais_pg', how='left')
    df_pg_completo = pd.merge(df_pg_completo, df_poblacion_costos, left_on='id_pais_pg_origen', right_on='id_pais_costos', how='left')
    
    # Determinar la población de Postgres (promedio redondeado si existe en ambas, o el valor único disponible)
    df_pg_completo['poblacion_pg'] = df_pg_completo[['poblacion_env', 'poblacion_costos']].mean(axis=1).round()
    
    # Transformar documentos anidados de MongoDB en columnas planas
    if not df_costos.empty:
        df_costos['costo_hospedaje_promedio'] = df_costos['costos_diarios_estimados_en_dólares'].apply(lambda x: x.get('hospedaje', {}).get('precio_promedio_usd') if isinstance(x, dict) else None)
        df_costos['costo_comida_mongo'] = df_costos['costos_diarios_estimados_en_dólares'].apply(lambda x: x.get('comida', {}).get('precio_promedio_usd') if isinstance(x, dict) else None)
        df_costos['costo_transporte_promedio'] = df_costos['costos_diarios_estimados_en_dólares'].apply(lambda x: x.get('transporte', {}).get('precio_promedio_usd') if isinstance(x, dict) else None)
        df_costos['costo_entretenimiento_mongo'] = df_costos['costos_diarios_estimados_en_dólares'].apply(lambda x: x.get('entretenimiento', {}).get('precio_promedio_usd') if isinstance(x, dict) else None)
        df_costos.rename(columns={'país': 'pais', 'población': 'poblacion_mongo', 'capital': 'capital_mongo', 'continente': 'continente_mongo', 'región': 'region_mongo'}, inplace=True)
        df_costos.drop(columns=['costos_diarios_estimados_en_dólares'], inplace=True, errors='ignore')

    if not df_bigmac.empty:
        df_bigmac.rename(columns={'país': 'pais'}, inplace=True)
        
    # Integrar todo por "pais"
    df_integrado = df_pg_completo.copy()
    
    if not df_costos.empty:
        df_integrado = pd.merge(df_integrado, df_costos, on='pais', how='outer')
        
    if not df_bigmac.empty:
        df_integrado = pd.merge(df_integrado, df_bigmac[['pais', 'precio_big_mac_usd']], on='pais', how='outer')
        
    # ============== REGLAS DE NEGOCIO Y CAMPOS FALTANTES ==============
    
    # Campos base del país: Si no estaba en Postgres, usar la de Mongo, y sino None.
    df_integrado['nombre_pais'] = df_integrado['pais']
    df_integrado['capital'] = df_integrado['capital'].combine_first(df_integrado.get('capital_mongo'))
    df_integrado['continente'] = df_integrado['continente'].combine_first(df_integrado.get('continente_mongo'))
    df_integrado['region'] = df_integrado['region'].combine_first(df_integrado.get('region_mongo'))
    
    # Población: Prioridad Postgres (ya promediada). Si no existe, usar población de Mongo.
    df_integrado['poblacion'] = df_integrado.get('poblacion_pg', pd.Series(dtype='float64')).combine_first(df_integrado.get('poblacion_mongo'))
    
    # Tasa de envejecimiento: Exclusivamente de Postgres (las filas sin ella quedarán None).
    # Costo X promedio: Mongo primero, si no, usar Postgres (costo_promedio_comida, costo_promedio_entretenimiento)
    df_integrado['costo_comida_promedio'] = df_integrado.get('costo_comida_mongo', pd.Series(dtype='float64')).combine_first(df_integrado.get('costo_promedio_comida'))
    df_integrado['costo_entretenimiento_promedio'] = df_integrado.get('costo_entretenimiento_mongo', pd.Series(dtype='float64')).combine_first(df_integrado.get('costo_promedio_entretenimiento'))
    
    # Eliminar registros incompletos 
    # (si algún país no tiene sus datos enteros antes de ingresarse al warehouse, entonces eliminar el registro)
    columnas_requeridas = [
        'nombre_pais', 'capital', 'continente', 'region', 
        'poblacion', 'tasa_de_envejecimiento', 
        'costo_hospedaje_promedio', 'costo_comida_promedio', 
        'costo_transporte_promedio', 'costo_entretenimiento_promedio', 
        'precio_big_mac_usd'
    ]
    df_integrado.dropna(subset=columnas_requeridas, inplace=True)
    
    # Preparar DIM_PAIS
    dim_pais = df_integrado[['nombre_pais', 'capital', 'continente', 'region']].drop_duplicates().reset_index(drop=True)
    dim_pais.insert(0, 'id_pais', range(1, 1 + len(dim_pais))) # Generar llaves subrogadas nuevas
    
    # Preparar tabla de HECHOS
    # Unir id_pais generado con el dataframe integrado
    df_integrado = pd.merge(df_integrado, dim_pais[['nombre_pais', 'id_pais']], on='nombre_pais', how='left')
    
    hechos_indicadores = df_integrado[[
        'id_pais', 
        'poblacion', 
        'tasa_de_envejecimiento',
        'costo_hospedaje_promedio',
        'costo_comida_promedio',
        'costo_transporte_promedio',
        'costo_entretenimiento_promedio',
        'precio_big_mac_usd'
    ]].copy()
    
    hechos_indicadores.rename(columns={
        'tasa_de_envejecimiento': 'tasa_envejecimiento',
        'precio_big_mac_usd': 'precio_big_mac'
    }, inplace=True)
    
    hechos_indicadores.insert(0, 'id_hecho', range(1, 1 + len(hechos_indicadores)))

    # ==========================================
    # 3. LOAD
    # ==========================================
    print("Cargando datos al Data Warehouse...")
    
    pg_pass_dw = os.getenv("POSTGRES_PASSWORD_DW", "password")
    dw_engine = create_engine(f'postgresql://{pg_user}:{pg_pass_dw}@localhost:5433/data_warehouse')
    
    with dw_engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE hechos_indicadores_pais, dim_pais CASCADE;"))

    # Insertar Dimensiones (append en vez de replace para conservar constraints)
    dim_pais.to_sql('dim_pais', dw_engine, if_exists='append', index=False)
    
    # Insertar Hechos
    hechos_indicadores.to_sql('hechos_indicadores_pais', dw_engine, if_exists='append', index=False)
    
    print("[OK] Proceso ETL completado con éxito.")

if __name__ == "__main__":
    etl_process()
