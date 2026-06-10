from pyspark.sql import SparkSession
from pyspark.sql.functions import col,avg,row_number
from pyspark.sql import functions as F
from pyspark.sql.functions import explode, create_map, lit
#import kagglehub
import os 

# --- SOLUCIÓN PARA WINDOWS ---
# Indicamos a Spark dónde encontrar las herramientas de Hadoop para windows ya que spark nacio en linux
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["hadoop.home.dir"] = "C:\\hadoop"
os.environ["PATH"] += os.pathsep + "C:\\hadoop\\bin"
# ------------------------------

# Creamos ruta donde estan los datos, en este caso la carpeta raw que se encuentra dentro de data, 
# que es donde se descargan los datos de kagglehub
path = "./data/raw"
path_silver = "./data/silver"

#kagglehub.dataset_download("olistbr/brazilian-ecommerce")

# Inicializamos la sesión de Spark,
#file temp para evitar problemas de permisos en Windows por parte de java al crear bases de datos temporales,
#arrow para traducir correctamnte entre python y la maquina de java por los archivos parquet
spark = SparkSession.builder \
    .appName("PracticaOlist") \
    .config("spark.sql.warehouse.dir", "file:///C:/temp") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

print("--- ¡Spark se inició correctamente! ---")
print("Ruta del dataset leido:", path)

# 1. Definimos las reglas de desduplicación para TODAS las tablas
reglas_desduplicacion = {
    "olist_orders_dataset.csv": ["order_id"],
    "olist_customers_dataset.csv": ["customer_id"],
    "olist_order_items_dataset.csv": ["order_id", "order_item_id"],
    "olist_order_payments_dataset.csv": ["order_id", "payment_sequential"],
    "olist_sellers_dataset.csv": ["seller_id"],
    "product_category_name_translation.csv": ["product_category_name"],
    "olist_geolocation_dataset.csv": ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng"]
    }  

    # 2. Iteramos para limpiar y guardar
for nombre_archivo, llaves in reglas_desduplicacion.items():
    # Cargamos el archivo original desde RAW
    df = spark.read.csv(os.path.join(path, nombre_archivo), header=True, inferSchema=True)
    
    # Aplicamos la desduplicación específica para esta tabla
    df_limpio = df.dropDuplicates(llaves)
    
    # Guardamos en SILVER
    nombre_parquet = nombre_archivo.replace(".csv", ".parquet")
    df_limpio.write.mode("overwrite").parquet(os.path.join("data", "silver", nombre_parquet))
    
    print(f"Tabla {nombre_archivo} desduplicada y guardada en Silver.")