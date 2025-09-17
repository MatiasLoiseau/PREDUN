#!/usr/bin/env python3
"""
Pipeline INDEC → PostgreSQL
Extrae datos de las APIs del INDEC Argentina y los carga directamente a PostgreSQL
"""

import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os
from datetime import datetime
import logging
from io import StringIO

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuración de la base de datos desde variables de entorno
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'siu'),
    'password': os.getenv('DB_PASSWORD', 'siu'),
    'port': os.getenv('DB_PORT', '5432')
}

def get_db_connection():
    """Crear conexión a PostgreSQL"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("Conexión a PostgreSQL establecida")
        return conn
    except Exception as e:
        logger.error(f"Error conectando a PostgreSQL: {str(e)}")
        raise

def download_and_load_ipc_categories():
    """
    Descarga datos de IPC y los carga directamente a PostgreSQL
    """
    url = "https://apis.datos.gob.ar/series/api/series?ids=103.1_I2N_2016_M_15,103.1_I2R_2016_M_18,103.1_I2E_2016_M_21&start_date=2016-04&format=csv"
    
    try:
        logger.info("Descargando y cargando datos de IPC por categorías...")
        
        # Descargar datos
        response = requests.get(url)
        response.raise_for_status()
        
        # Procesar con pandas directamente desde el texto
        df = pd.read_csv(StringIO(response.text))
        df['indice_tiempo'] = pd.to_datetime(df['indice_tiempo'])
        
        # Filtrar filas con valores nulos
        df_clean = df.dropna()
        logger.info(f"IPC: Datos originales: {len(df)} filas, datos limpios: {len(df_clean)} filas")
        
        # Conectar a PostgreSQL
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Limpiar tabla antes de insertar
        cursor.execute("TRUNCATE TABLE canonical.ipc_categories")
        
        # Preparar datos para inserción
        data = [
            (row['indice_tiempo'], row['ipc_2016_nucleo'], row['ipc_2016_regulados'], row['ipc_2016_estacionales'])
            for _, row in df_clean.iterrows()
        ]
        
        # Insertar datos
        insert_query = """
            INSERT INTO canonical.ipc_categories 
            (indice_tiempo, ipc_nucleo, ipc_regulados, ipc_estacionales)
            VALUES %s
        """
        
        execute_values(cursor, insert_query, data)
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"IPC: Insertadas {len(data)} filas en canonical.ipc_categories")
        
    except Exception as e:
        logger.error(f"Error descargando/cargando IPC: {str(e)}")
        raise

def download_and_load_unemployment_rates():
    """
    Descarga datos de desempleo y los carga directamente a PostgreSQL
    """
    url = "https://apis.datos.gob.ar/series/api/series/?ids=45.2_ECTDTGSJ_0_T_47&format=csv"
    
    try:
        logger.info("Descargando y cargando datos de tasas de desempleo...")
        
        # Descargar datos
        response = requests.get(url)
        response.raise_for_status()
        
        # Procesar con pandas directamente desde el texto
        df = pd.read_csv(StringIO(response.text))
        df['indice_tiempo'] = pd.to_datetime(df['indice_tiempo'])
        
        # Filtrar filas con valores nulos en la tasa de desempleo
        df_clean = df.dropna(subset=['eph_continua_tasa_desempleo_total_gran_san_juan'])
        logger.info(f"Desempleo: Datos originales: {len(df)} filas, datos limpios: {len(df_clean)} filas")
        
        # Conectar a PostgreSQL
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Limpiar tabla antes de insertar
        cursor.execute("TRUNCATE TABLE canonical.unemployment_rates")
        
        # Preparar datos para inserción (solo datos válidos)
        data = [
            (row['indice_tiempo'], row['eph_continua_tasa_desempleo_total_gran_san_juan'])
            for _, row in df_clean.iterrows()
        ]
        
        # Insertar datos
        insert_query = """
            INSERT INTO canonical.unemployment_rates 
            (indice_tiempo, eph_continua_tasa_desempleo_total_gran_san_juan)
            VALUES %s
        """
        
        execute_values(cursor, insert_query, data)
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Desempleo: Insertadas {len(data)} filas en canonical.unemployment_rates")
        
    except Exception as e:
        logger.error(f"Error descargando/cargando desempleo: {str(e)}")
        raise

def download_and_load_emae_indicators():
    """
    Descarga datos de EMAE y los carga directamente a PostgreSQL
    """
    url = "https://apis.datos.gob.ar/series/api/series/?ids=302.3_S_ORIGINALRAL_0_S_21&format=csv"
    
    try:
        logger.info("Descargando y cargando datos de EMAE...")
        
        # Descargar datos
        response = requests.get(url)
        response.raise_for_status()
        
        # Procesar con pandas directamente desde el texto
        df = pd.read_csv(StringIO(response.text))
        df['indice_tiempo'] = pd.to_datetime(df['indice_tiempo'])
        
        # Filtrar filas con valores nulos
        df_clean = df.dropna()
        logger.info(f"EMAE: Datos originales: {len(df)} filas, datos limpios: {len(df_clean)} filas")
        
        # Conectar a PostgreSQL
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Limpiar tabla antes de insertar
        cursor.execute("TRUNCATE TABLE canonical.emae_indicators")
        
        # Preparar datos para inserción
        data = [
            (row['indice_tiempo'], row['s_original_nivel_gral'])
            for _, row in df_clean.iterrows()
        ]
        
        # Insertar datos
        insert_query = """
            INSERT INTO canonical.emae_indicators 
            (indice_tiempo, s_original_nivel_gral)
            VALUES %s
        """
        
        execute_values(cursor, insert_query, data)
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"EMAE: Insertadas {len(data)} filas en canonical.emae_indicators")
        
    except Exception as e:
        logger.error(f"Error descargando/cargando EMAE: {str(e)}")
        raise

def main():
    """Función principal para ejecutar todo el pipeline"""
    
    logger.info("=== INICIANDO PIPELINE INDEC → POSTGRESQL ===")
    
    try:
        download_and_load_ipc_categories()
        download_and_load_unemployment_rates()
        download_and_load_emae_indicators()
        logger.info("=== PIPELINE COMPLETADO EXITOSAMENTE ===")
    except Exception as e:
        logger.error(f"Error durante el pipeline: {str(e)}")
        raise

if __name__ == "__main__":
    main()