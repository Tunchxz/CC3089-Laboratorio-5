CREATE TABLE paises (
    id_pais SERIAL PRIMARY KEY,
    nombre_pais VARCHAR(100) NOT NULL UNIQUE,
    capital VARCHAR(100),
    continente VARCHAR(50),
    region VARCHAR(100)
);

CREATE TABLE indicadores_envejecimiento (
    id_registro SERIAL PRIMARY KEY,
    id_pais INT NOT NULL,
    poblacion NUMERIC,
    tasa_de_envejecimiento DECIMAL(5,2),
    FOREIGN KEY (id_pais) REFERENCES paises(id_pais)
);

CREATE TABLE indicadores_poblacion_costos (
    id_registro SERIAL PRIMARY KEY,
    id_pais INT NOT NULL,
    poblacion BIGINT,
    costo_bajo_hospedaje DECIMAL(10,2),
    costo_promedio_comida DECIMAL(10,2),
    costo_bajo_transporte DECIMAL(10,2),
    costo_promedio_entretenimiento DECIMAL(10,2),
    FOREIGN KEY (id_pais) REFERENCES paises(id_pais)
);