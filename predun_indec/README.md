# INDEC Economic Indicators Data Pipeline

Este módulo implementa un pipeline de datos para extraer indicadores económicos de las APIs del INDEC (Instituto Nacional de Estadística y Censos) de Argentina y cargarlos directamente en PostgreSQL.

## Descripción

El pipeline extrae automáticamente datos económicos actualizados de Argentina desde las APIs oficiales del INDEC y los almacena en tablas estructuradas en PostgreSQL para su posterior análisis.

## Datasets Extraídos

### 1. IPC por Categorías (Índice de Precios al Consumidor)
- **Categorías**: Núcleo, Regulados y Estacionales
- **Frecuencia**: Mensual
- **Año base**: 2016
- **API**: `https://apis.datos.gob.ar/series/api/series?ids=103.1_I2N_2016_M_15,103.1_I2R_2016_M_18,103.1_I2E_2016_M_21`

### 2. Tasa de Desempleo EPH (Encuesta Permanente de Hogares)
- **Frecuencia**: Trimestral
- **Fuente**: Encuesta Continua de Hogares
- **API**: `https://apis.datos.gob.ar/series/api/series/?ids=45.2_ECTDTGSJ_0_T_47`

### 3. Indicadores EMAE (Estimador Mensual de Actividad Económica)
- **Indicador**: Nivel general de actividad económica
- **Frecuencia**: Mensual
- **Serie**: Original
- **API**: `https://apis.datos.gob.ar/series/api/series/?ids=302.3_S_ORIGINALRAL_0_S_21`

## Estructura de Archivos

```
predun_indec/
├── README.md                    # Este archivo
├── indec_data_pipeline.py      # Pipeline principal
├── create_tables.sql           # Scripts de creación de tablas
└── .env.example               # Variables de entorno de ejemplo
```

## Configuración

### Variables de Entorno

Crea un archivo `.env` basado en `.env.example`:

```bash
cp .env.example .env
```

Edita las variables según tu configuración:

```bash
# Configuración de PostgreSQL
DB_HOST=localhost
DB_NAME=postgres
DB_USER=siu
DB_PASSWORD=siu
DB_PORT=5432
DB_SCHEMA=canonical
```

## Instalación de Dependencias

```bash
pip install requests pandas psycopg2-binary
```

## Uso

### 1. Crear las Tablas en PostgreSQL

```bash
psql -U postgres -h localhost -p 5432 -f create_tables.sql
```

### 2. Ejecutar el Pipeline

```bash
python indec_data_pipeline.py
```

## Tablas Creadas

El pipeline crea las siguientes tablas en el schema `canonical`:

### `canonical.ipc_categories`
- `indice_tiempo`: Fecha del índice (mensual)
- `ipc_nucleo`: IPC categoría núcleo
- `ipc_regulados`: IPC precios regulados
- `ipc_estacionales`: IPC productos estacionales

### `canonical.unemployment_rates`
- `indice_tiempo`: Fecha del indicador (trimestral)
- `eph_continua_tasa_desempleo_total_gran_san_juan`: Tasa de desempleo total

### `canonical.emae_indicators`
- `indice_tiempo`: Fecha del estimador (mensual)
- `s_original_nivel_gral`: EMAE nivel general serie original

## Características del Pipeline

- **Extracción directa**: Conecta directamente a las APIs del INDEC
- **Limpieza automática**: Filtra registros con valores nulos
- **Carga incremental**: Trunca y recarga las tablas en cada ejecución
- **Logging completo**: Registra todas las operaciones y errores
- **Manejo de errores**: Gestión robusta de errores de red y base de datos

## Logging

El pipeline genera logs detallados que incluyen:
- Conexiones a la base de datos
- Número de registros procesados
- Errores de red o base de datos
- Tiempos de ejecución

## Integración con el Proyecto PREDUN

Este pipeline forma parte del sistema MLOps de PREDUN y alimenta las tablas canónicas que posteriormente son utilizadas por:
- Modelos de machine learning para predicción de deserción
- Análisis económico contextuales
- Reportes y dashboards

## Troubleshooting

### Error de Conexión a PostgreSQL
```bash
# Verificar que PostgreSQL esté corriendo
docker ps | grep postgres

# Verificar conectividad
psql -U postgres -h localhost -p 5432 -c "SELECT 1;"
```

### Error de API del INDEC
- Verificar conectividad a internet
- Las APIs del INDEC pueden tener mantenimiento ocasional
- Revisar logs para detalles específicos del error

### Problemas de Dependencias
```bash
# Reinstalar dependencias
pip install --upgrade requests pandas psycopg2-binary
```