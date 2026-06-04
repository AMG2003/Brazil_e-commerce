from pyspark.sql import SparkSession
from pyspark.sql.functions import col,avg,row_number
from pyspark.sql import functions as F
from pyspark.sql.functions import explode, create_map, lit
from pyspark.sql.types import IntegerType 
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

# Listar archivos encontrados
files = os.listdir(path)
print("Archivos en el dataset:")
for f in files:
    print(f"  - {f}")

for file in files:
    if file.endswith(".csv"):
        df = spark.read.csv(os.path.join(path, file), header=True, inferSchema=True)
        print(f"Primeras filas de {file}:")
        df.printSchema()
        df.show(5, truncate=False)

#mostrar nulos 
for file in files:
    if file.endswith(".csv"):
        df = spark.read.csv(os.path.join(path, file), header=True, inferSchema=True)
        print(f"Nulos en {file}:")
        # 1. Creamos una lista de expresiones de suma de nulos
        null_counts = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns])

        # Convertimos las columnas a un mapa y lo expandimos
        df_nulls = null_counts.select(explode(create_map([F.lit(x) for x in df.columns for x in (x, F.col(x))])).alias("Columna", "Nulos"))

        # 3. Mostrar el resultado
        df_nulls.show(truncate=False)

################REVIEWS DATASET################

# 1. CARGA INICIAL
df_reviews = spark.read.csv(os.path.join(path, "olist_order_reviews_dataset.csv"), header=True, inferSchema=True)

# 2. LIMPIEZA BASE
# Filtramos filas donde review_score no sea un número válido
# Usamos un regex para asegurar que el score solo contenga dígitos
df_base = df_reviews.dropDuplicates(["review_id"]) \
                    .dropDuplicates(["order_id"]) \
                    .filter(F.col("review_id").isNotNull()) \
                    .filter(F.col("order_id").isNotNull()) \
                    .filter(F.col("review_score").rlike("^[0-9]+$")) # <--- ESTA LÍNEA ES LA CLAVE

# 2. Ahora que sabemos que todos son números, hacemos el cast
df_base = df_base.withColumn("review_score", F.col("review_score").cast(IntegerType())) \
                 .withColumn("creation_date", F.to_timestamp(F.col("review_creation_date"), "yyyy-MM-dd HH:mm:ss")) \
                 .withColumn("answer_date", F.to_timestamp(F.col("review_answer_timestamp"), "yyyy-MM-dd HH:mm:ss"))

df_base = df_base.na.fill({
    "review_comment_title": "Sin título",
    "review_comment_message": "Sin comentarios"
})

# IMPORTANTE: Si el error sigue, imprime el esquema antes de limpiar para ver si los nombres coinciden
df_base.printSchema()
df_base.show(5, truncate=False)

# 3. DATASET DE SATISFACCIÓN (Solo filas válidas)
# Filtramos nulos en score porque no nos sirven para promedios
df_satisfaccion = df_base.filter(F.col("review_score").isNotNull())

print("###################Dataset de Satisfacción score sin nulos:")
df_satisfaccion.printSchema()
df_satisfaccion.show(5, truncate=False)

# 4. DATASET DE COMPORTAMIENTO (Todo el historial)
# Aquí mantenemos todo y agregamos los flags de análisis
df_comportamiento = df_base.withColumn("has_score", F.col("review_score").isNotNull())

print("###################Dataset de Comportamiento con flags en score:")
df_comportamiento.printSchema()
df_comportamiento.show(5, truncate=False)



################PRODUCTS DATASET################

df_products = spark.read.csv(os.path.join(path, "olist_products_dataset.csv"), header=True, inferSchema=True)
# 1. Desduplicar por ID de producto
df_products_clean = df_products.dropDuplicates(["product_id"])

# 2. Rellenar nulos informativos
# Categoría y textos
df_products_clean = df_products_clean.na.fill({
    "product_category_name": "Others"
})

# Métricas técnicas (rellenar con 0 para evitar nulos en sumas)
cols_to_zero = ["product_name_lenght", "product_description_lenght", "product_photos_qty", 
                "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"]

df_products_clean = df_products_clean.na.fill({col: 0 for col in cols_to_zero})


df_products_clean.show(5, truncate=False)

##########################Carga de tablas a Silver##########################

# 1. TUS TABLAS VALIDADAS (Las que ya limpiamos juntos)
df_satisfaccion.write.mode("overwrite").parquet("data/silver/olist_order_reviews_satisfaccion.parquet")
df_comportamiento.write.mode("overwrite").parquet("data/silver/olist_order_reviews_comportamiento.parquet")
df_products_clean.write.mode("overwrite").parquet("data/silver/olist_products_dataset.parquet")

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
    df = spark.read.csv(os.path.join("data", "raw", nombre_archivo), header=True, inferSchema=True)
    
    # Aplicamos la desduplicación específica para esta tabla
    df_limpio = df.dropDuplicates(llaves)
    
    # Guardamos en SILVER
    nombre_parquet = nombre_archivo.replace(".csv", ".parquet")
    df_limpio.write.mode("overwrite").parquet(os.path.join("data", "silver", nombre_parquet))
    
    print(f"Tabla {nombre_archivo} desduplicada y guardada en Silver.")

    #customers , geolocation , order_items , order_payments , sellers , orders , product_category_name_translation

# Apagamos la sesión al terminar
spark.stop()