from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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

# 1. Cargar las tablas Silver
df_orders = spark.read.parquet("./data/silver/olist_orders_dataset.parquet")
df_items = spark.read.parquet("./data/silver/olist_order_items_dataset.parquet")
df_reviews = spark.read.parquet("./data/silver/olist_order_reviews_satisfaccion.parquet")

# 2. Agregar métricas a nivel de pedido
# Como un pedido puede tener varios items, primero agrupamos los items para tener 
# un total de dinero por order_id
df_items_agg = df_items.groupBy("order_id").agg(
    F.sum("price").alias("total_price"),
    F.sum("freight_value").alias("total_freight"),
    F.count("product_id").alias("total_items")
)

# 3. Crear la tabla de hechos uniendo todo
# Usamos un left join con orders para mantener todos los pedidos
fact_orders = df_orders \
    .join(df_items_agg, "order_id", "left") \
    .join(df_reviews, "order_id", "left") \
    .select(
        "order_id",
        "customer_id",
        "order_status",
        "total_price",
        "total_freight",
        "total_items",
        "review_score"
    )

# 4. Limpieza básica para ML (Llenar nulos de métricas)
fact_orders = fact_orders.fillna({
    "total_price": 0.0,
    "total_freight": 0.0,
    "total_items": 0
})

# 5. Guardar en la capa Gold
fact_orders.write.mode("overwrite").parquet("./data/gold/fact_orders.parquet")

print("Tabla de hechos 'fact_orders' creada exitosamente en la capa Gold.")