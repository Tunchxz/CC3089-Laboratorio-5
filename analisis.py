import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

def generar_analisis():
    print("Conectando al Data Warehouse...")
    pg_user = os.getenv("POSTGRES_USER", "admin")
    pg_pass_dw = os.getenv("POSTGRES_PASSWORD_DW", "password")
    
    dw_engine = create_engine(f'postgresql://{pg_user}:{pg_pass_dw}@localhost:5433/data_warehouse')

    # Query para extraer las métricas de interés cruzando las dimensiones
    query = """
    SELECT 
        d.nombre_pais, 
        d.continente, 
        h.poblacion, 
        h.precio_big_mac,
        (h.costo_hospedaje_promedio + h.costo_comida_promedio + 
        h.costo_transporte_promedio + h.costo_entretenimiento_promedio) AS costo_turistico_diario
    FROM dim_pais d
    JOIN hechos_indicadores_pais h ON d.id_pais = h.id_pais
    """
    
    df = pd.read_sql(query, dw_engine)

    # Crear carpeta output
    os.makedirs('output', exist_ok=True)
    sns.set_theme(style="whitegrid")

    print("1. Generando: Países con alto costo turístico pero bajo precio Big Mac...")
    plt.figure(figsize=(12, 7))
    sns.scatterplot(data=df, x='costo_turistico_diario', y='precio_big_mac', hue='continente', s=100, palette='Set1')
    
    # Calcular medianas para crear los cuadrantes conceptuales
    mediana_costo = df['costo_turistico_diario'].median()
    mediana_big_mac = df['precio_big_mac'].median()
    
    plt.axvline(mediana_costo, color='gray', linestyle='--', alpha=0.7)
    plt.axhline(mediana_big_mac, color='gray', linestyle='--', alpha=0.7)
    
    # Añadir texto al cuadrante deseado (Costo Alto, Big Mac Bajo = Abajo a la derecha)
    for _, row in df.iterrows():
        if pd.notnull(row['costo_turistico_diario']) and pd.notnull(row['precio_big_mac']):
            if row['costo_turistico_diario'] > mediana_costo and row['precio_big_mac'] < mediana_big_mac:
                plt.text(row['costo_turistico_diario'] + 1, row['precio_big_mac'] - 0.05, row['nombre_pais'], 
                        fontsize=9, color='darkred', weight='bold')

    plt.title('Discrepancia: Costo Turístico Diario vs Precio Big Mac', fontsize=14)
    plt.xlabel('Costo Turístico Diario Estimado (USD)', fontsize=12)
    plt.ylabel('Precio Big Mac (USD)', fontsize=12)
    plt.tight_layout()
    plt.savefig('output/1_costo_vs_big_mac.png', dpi=300)
    plt.close()

    print("2. Generando: Relación entre población y costos turísticos...")
    plt.figure(figsize=(11, 6))
    sns.scatterplot(data=df, x='poblacion', y='costo_turistico_diario', s=100, color='coral', edgecolor='black', alpha=0.8)
    plt.xscale('log') # Escala logarítmica porque las poblaciones varían enormemente
    
    for i, row in df.iterrows():
        if pd.notnull(row['poblacion']) and pd.notnull(row['costo_turistico_diario']):
            # Mostramos el nombre de algunos países de ejemplo (los más poblados o más caros) para no saturar
            if row['poblacion'] > 1e8 or row['costo_turistico_diario'] > df['costo_turistico_diario'].quantile(0.85):
                plt.text(row['poblacion'] * 1.05, row['costo_turistico_diario'], row['nombre_pais'], fontsize=8)

    plt.title('Relación entre Población (Log) y Carga de Costos Turísticos', fontsize=14)
    plt.xlabel('Población de País', fontsize=12)
    plt.ylabel('Costo Turístico Diario Estimado (USD)', fontsize=12)
    plt.tight_layout()
    plt.savefig('output/2_poblacion_vs_costo.png', dpi=300)
    plt.close()

    print("3. Generando: Comparación de continentes según costos de viaje...")
    plt.figure(figsize=(10, 6))
    
    # Ordenar los continentes por la mediana del costo para mejor visualización
    orden = df.groupby('continente')['costo_turistico_diario'].median().sort_values(ascending=False).index
    
    sns.boxplot(data=df, x='continente', y='costo_turistico_diario', order=orden, palette='viridis')
    sns.stripplot(data=df, x='continente', y='costo_turistico_diario', order=orden, color=".3", size=4, alpha=0.6)
    
    plt.title('Distribución de Costos de Viaje por Continente', fontsize=14)
    plt.xlabel('Continente', fontsize=12)
    plt.ylabel('Costo Turístico Diario (USD)', fontsize=12)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('output/3_costos_por_continente.png', dpi=300)
    plt.close()
    
    print("[OK] Análisis completado. Las imágenes se han guardado en la carpeta 'output'.")

if __name__ == "__main__":
    generar_analisis()
