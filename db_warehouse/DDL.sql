CREATE TABLE dim_pais (
    id_pais SERIAL PRIMARY KEY,
    nombre_pais VARCHAR(100),
    capital VARCHAR(100),
    continente VARCHAR(50),
    region VARCHAR(100)
);

CREATE TABLE hechos_indicadores_pais (
    id_hecho SERIAL PRIMARY KEY,
    id_pais INT,
    
    poblacion BIGINT,
    tasa_envejecimiento DECIMAL(5,2),

    costo_hospedaje_promedio DECIMAL(10,2),
    costo_comida_promedio DECIMAL(10,2),
    costo_transporte_promedio DECIMAL(10,2),
    costo_entretenimiento_promedio DECIMAL(10,2),

    precio_big_mac DECIMAL(10,2),

    FOREIGN KEY (id_pais) REFERENCES dim_pais(id_pais)
);