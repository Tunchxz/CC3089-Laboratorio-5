CREATE TEMP TABLE staging_poblacion_costos (
    mongo_id TEXT,
    continente VARCHAR(50),
    pais VARCHAR(100),
    poblacion BIGINT,
    costo_bajo_hospedaje DECIMAL(10,2),
    costo_promedio_comida DECIMAL(10,2),
    costo_bajo_transporte DECIMAL(10,2),
    costo_promedio_entretenimiento DECIMAL(10,2)
);

COPY staging_poblacion_costos
FROM '/data/pais_poblacion.csv'
DELIMITER ','
CSV HEADER;

INSERT INTO indicadores_poblacion_costos (
    id_pais,
    poblacion,
    costo_bajo_hospedaje,
    costo_promedio_comida,
    costo_bajo_transporte,
    costo_promedio_entretenimiento
)
SELECT p.id_pais,
        s.poblacion,
        s.costo_bajo_hospedaje,
        s.costo_promedio_comida,
        s.costo_bajo_transporte,
        s.costo_promedio_entretenimiento
FROM staging_poblacion_costos s
JOIN paises p
ON p.nombre_pais = s.pais;