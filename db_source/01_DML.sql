CREATE TEMP TABLE staging_envejecimiento (
    id_pais INT,
    nombre_pais VARCHAR(100),
    capital VARCHAR(100),
    continente VARCHAR(50),
    region VARCHAR(100),
    poblacion NUMERIC,
    tasa_de_envejecimiento DECIMAL(5,2)
);

COPY staging_envejecimiento
FROM '/data/pais_envejecimiento.csv'
DELIMITER ','
CSV HEADER;

INSERT INTO paises (id_pais, nombre_pais, capital, continente, region)
SELECT id_pais, nombre_pais, capital, continente, region
FROM staging_envejecimiento;

INSERT INTO indicadores_envejecimiento (id_pais, poblacion, tasa_de_envejecimiento)
SELECT id_pais, poblacion, tasa_de_envejecimiento
FROM staging_envejecimiento;