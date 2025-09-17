-- =====================================================
-- CREATE TABLE statements para datos del INDEC Argentina
-- =====================================================

-- Crear schema canonical si no existe
CREATE SCHEMA IF NOT EXISTS canonical;

-- =====================================================
-- 1. IPC por categorías (núcleo, regulados, estacionales)
-- =====================================================
CREATE TABLE IF NOT EXISTS canonical.ipc_categories (
    indice_tiempo DATE NOT NULL,
    ipc_nucleo DECIMAL(10,4),
    ipc_regulados DECIMAL(10,4), 
    ipc_estacionales DECIMAL(10,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (indice_tiempo)
);

-- Comentarios para documentar la tabla
COMMENT ON TABLE canonical.ipc_categories IS 'Índice de Precios al Consumidor (IPC) por categorías - Fuente: INDEC Argentina';
COMMENT ON COLUMN canonical.ipc_categories.indice_tiempo IS 'Fecha del índice (mensual)';
COMMENT ON COLUMN canonical.ipc_categories.ipc_nucleo IS 'IPC categoría núcleo';
COMMENT ON COLUMN canonical.ipc_categories.ipc_regulados IS 'IPC precios regulados';  
COMMENT ON COLUMN canonical.ipc_categories.ipc_estacionales IS 'IPC productos estacionales';

-- =====================================================
-- 2. Tasa de desempleo EPH (Gran San Juan)
-- =====================================================
CREATE TABLE IF NOT EXISTS canonical.unemployment_rates (
    indice_tiempo DATE NOT NULL,
    eph_continua_tasa_desempleo_total_gran_san_juan DECIMAL(10,8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (indice_tiempo)
);

-- Comentarios para documentar la tabla  
COMMENT ON TABLE canonical.unemployment_rates IS 'Tasa de desempleo EPH continua para Gran San Juan - Fuente: INDEC Argentina';
COMMENT ON COLUMN canonical.unemployment_rates.indice_tiempo IS 'Fecha del indicador (trimestral)';
COMMENT ON COLUMN canonical.unemployment_rates.eph_continua_tasa_desempleo_total_gran_san_juan IS 'Tasa de desempleo total en Gran San Juan (proporción)';

-- =====================================================
-- 3. EMAE (Estimador Mensual de Actividad Económica)
-- =====================================================
CREATE TABLE IF NOT EXISTS canonical.emae_indicators (
    indice_tiempo DATE NOT NULL,
    s_original_nivel_gral DECIMAL(15,8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (indice_tiempo)
);

-- Comentarios para documentar la tabla
COMMENT ON TABLE canonical.emae_indicators IS 'Estimador Mensual de Actividad Económica (EMAE) nivel general - Fuente: INDEC Argentina';
COMMENT ON COLUMN canonical.emae_indicators.indice_tiempo IS 'Fecha del estimador (mensual)';
COMMENT ON COLUMN canonical.emae_indicators.s_original_nivel_gral IS 'EMAE nivel general serie original';

-- =====================================================
-- Grants para el usuario siu
-- =====================================================
GRANT USAGE ON SCHEMA canonical TO siu;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA canonical TO siu;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA canonical TO siu;