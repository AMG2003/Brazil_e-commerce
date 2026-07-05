from pyspark.sql import SparkSession
from pyspark.sql.functions import col,avg,row_number
from pyspark.sql import functions as F
from pyspark.sql.functions import explode, create_map, lit
#import kagglehub
import os 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# --- SOLUCIÓN PARA WINDOWS ---
# Indicamos a Spark dónde encontrar las herramientas de Hadoop para windows ya que spark nacio en linux
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["hadoop.home.dir"] = "C:\\hadoop"
os.environ["PATH"] += os.pathsep + "C:\\hadoop\\bin"
# ------------------------------

# Creamos ruta donde estan los datos, en este caso la carpeta raw que se encuentra dentro de data, 
# que es donde se descargan los datos de kagglehub
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
print("Ruta del dataset leido:", path_silver)

schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_unique_id", StringType(), True),
    StructField("customer_zip_code_prefix", IntegerType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_state", StringType(), True)
])

df_orders = spark.read.schema(schema).parquet("./data/silver/olist_customers_dataset.parquet")
df_orders.show(5, truncate=False)
df_orders.printSchema()
