import os
os.environ["JAVA_HOME"] = "C:\\Program Files\\Eclipse Adoptium\\jdk-17.0.18.8-hotspot"
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

print("⏳ กำลังสตาร์ทเครื่องยนต์ Apache Spark พร้อมปลั๊กอิน Kafka และ PostgreSQL...")

# 1. สร้างสมองกล Spark และโหลดปลั๊กอินสำหรับต่อ Kafka
spark = SparkSession.builder \
    .appName("RealTimeStockConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0") \
    .master("local[*]") \
    .getOrCreate()

# ลดข้อความแจ้งเตือนขยะบนหน้าจอ
spark.sparkContext.setLogLevel("ERROR")
print("✅ Spark พร้อมทำงาน! กำลังเชื่อมต่อเข้าท่อ Kafka...")

# 2. กำหนดรูปร่างของข้อมูล (Schema) ให้ตรงกับที่เรายิงมา
schema = StructType([
    StructField("ticker", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("event_time", StringType(), True)
])

# 3. ดูดข้อมูลจาก Kafka (แบบ Real-time Streaming)
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "realtime_stock_data") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")
    
# 4. ฟังก์ชันสำหรับเซฟข้อมูลลง PostgreSQL (ทีละ Batch)
def write_to_postgres(batch_df, batch_id):
    print(f"📦 กำลังบันทึก Batch ที่ {batch_id} ลงฐานข้อมูล PostgreSQL...")
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/financial_db") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "realtime_stock") \
        .option("user", "admin") \
        .option("password", "admin") \
        .mode("append") \
        .save()

print("📊 ระบบพร้อม! กำลังดักจับข้อมูลและส่งเข้าฐานข้อมูล (รอสักครู่...)")

# 5. สั่ง Spark ให้ใช้ฟังก์ชันเอาข้อมูลยัดลง DB แทนการโชว์หน้าจอ
query = parsed_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()