# file: emt_arrivals_to_iceberg.py
import argparse
from pyspark.sql import SparkSession, functions as F, types as T
def get_spark():
    return (SparkSession.builder
            .appName("emt_arrivals_to_iceberg")
            .enableHiveSupport()
            .getOrCreate())
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True)
    p.add_argument("--warehouse", required=True)
    p.add_argument("--db", default="emt")
    return p.parse_args()
def ensure_tables(spark, db, warehouse):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS glue_catalog.{db}")
    # Tabla principal de predicciones de llegada (una fila por elemento de Arrive[])
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{db}.arrivals_predictions (
      id STRING,
      event_ts TIMESTAMP,
      line STRING,
      stop_id STRING,
      destination STRING,
      is_head BOOLEAN,
      deviation INT,
      bus INT,
      estimate_arrive_sec INT,
      distance_bus_m INT,
      position_type_bus STRING,
      lon DOUBLE,
      lat DOUBLE,
      ingest_ts TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY ( days(event_ts), truncate(line, 2) )
    TBLPROPERTIES (
      'format-version'='2',
      'write.delete.mode'='merge-on-read',
      'write.update.mode'='merge-on-read'
    )
    LOCATION '{warehouse}/{db}/arrivals_predictions'
    """)
    # Dimensión de paradas (último valor por stop_id)
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{db}.stops (
      stop_id STRING,
      stop_name STRING,
      lon DOUBLE,
      lat DOUBLE,
      direction STRING,
      lines ARRAY<STRING>,
      updated_ts TIMESTAMP
    )
    USING iceberg
    LOCATION '{warehouse}/{db}/stops'
    """)
def main():
    args = parse_args()
    spark = get_spark()
    # ====== LECTURA DEL EXCEL ======
    df_xlsx = (spark.read.format("com.crealytics.spark.excel")
               .option("header", "true")
               .option("inferSchema", "false")   # declaramos nosotros
               .load(args.input))
    # Normaliza nombres de columnas (sin espacios ni puntos)
    for c in df_xlsx.columns:
        df_xlsx = df_xlsx.withColumnRenamed(
            c, c.strip().replace(" ", "_").replace(".", "_").lower()
        )
    # Esperados: timestamp, arrive, stopinfo, extrainfo, incident_listaincident_data, stopid, line, ...
    df = df_xlsx.withColumn("event_ts", F.to_timestamp("timestamp")).drop("timestamp")
    # ====== SCHEMAS DE PARSEO (JSON embebido en celdas) ======
    geometry_schema = T.StructType([
        T.StructField("type", T.StringType()),
        T.StructField("coordinates", T.ArrayType(T.DoubleType()))
    ])
    arrive_item_schema = T.StructType([
        T.StructField("line", T.StringType()),
        T.StructField("stop", T.StringType()),
        T.StructField("isHead", T.StringType()),             
        T.StructField("destination", T.StringType()),
        T.StructField("deviation", T.IntegerType()),
        T.StructField("bus", T.IntegerType()),
        T.StructField("geometry", geometry_schema),
        T.StructField("estimateArrive", T.IntegerType()),
        T.StructField("DistanceBus", T.IntegerType()),
        T.StructField("positionTypeBus", T.StringType())
    ])
    stopinfo_item_schema = T.StructType([
        T.StructField("lines", T.ArrayType(T.StringType())),
        T.StructField("stopId", T.StringType()),
        T.StructField("stopName", T.StringType()),
        T.StructField("geometry", geometry_schema),
        T.StructField("Direction", T.StringType())
    ])
    # Incidents: desconocido el detalle -> de momento, array<string> (lo podrás enriquecer luego)
    incidents_schema = T.ArrayType(T.StringType())
    # ====== PARSEO ======
    df = (df
        .withColumn("arrive_json", F.from_json(F.col("arrive"), T.ArrayType(arrive_item_schema)))
        .withColumn("stopinfo_json", F.from_json(F.col("stopinfo"), T.ArrayType(stopinfo_item_schema)))
    )
    if "incident_listaincident_data" in df.columns:
        df = df.withColumn("incident_data_json", F.from_json(F.col("incident_listaincident_data"), incidents_schema))
    # ====== EXPLOSIÓN DE ARRIVE[] A FILAS ======
    df_pred = (df
        .withColumn("arr", F.explode_outer("arrive_json"))
        .select(
            "event_ts",
            F.coalesce(F.col("stopid"), F.col("arr.stop")).alias("stop_id"),
            F.coalesce(F.col("line"), F.col("arr.line")).alias("line"),
            F.col("arr.destination").alias("destination"),
            F.when(F.lower(F.col("arr.isHead")) == F.lit("true"), F.lit(True)).otherwise(F.lit(False)).alias("is_head"),
            F.col("arr.deviation").alias("deviation"),
            F.col("arr.bus").alias("bus"),
            F.col("arr.estimateArrive").alias("estimate_arrive_sec"),
            F.col("arr.DistanceBus").alias("distance_bus_m"),
            F.col("arr.positionTypeBus").alias("position_type_bus"),
            F.col("arr.geometry.coordinates").getItem(0).alias("lon"),
            F.col("arr.geometry.coordinates").getItem(1).alias("lat")
        )
        .withColumn("ingest_ts", F.current_timestamp())
    )
    # ID determinista para upsert (evita duplicados si re-procesas el mismo Excel)
    df_pred = df_pred.withColumn(
        "id",
        F.sha2(F.concat_ws("||",
            F.col("event_ts").cast("string"),
            F.col("stop_id"),
            F.col("line"),
            F.col("bus").cast("string"),
            F.col("estimate_arrive_sec").cast("string")
        ), 256)
    )
    # ====== ASEGURA TABLAS ======
    ensure_tables(spark, args.db, args.warehouse)
    # ====== MERGE A ICEBERG (Silver) ======
    df_pred.createOrReplaceTempView("staging_pred")
    spark.sql(f"""
    MERGE INTO glue_catalog.{args.db}.arrivals_predictions t
    USING staging_pred s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)
    df_stops = (df
      .withColumn("si", F.element_at("stopinfo_json", 1))   # primer elemento
      .select(
          F.col("si.stopId").alias("stop_id"),
          F.col("si.stopName").alias("stop_name"),
          F.col("si.geometry.coordinates").getItem(0).alias("lon"),
          F.col("si.geometry.coordinates").getItem(1).alias("lat"),
          F.col("si.Direction").alias("direction"),
          F.col("si.lines").alias("lines")
      )
      .where(F.col("stop_id").isNotNull())
      .dropDuplicates(["stop_id"])
      .withColumn("updated_ts", F.current_timestamp())
    )
    df_stops.createOrReplaceTempView("staging_stops")
    spark.sql(f"""
    MERGE INTO glue_catalog.{args.db}.stops t
    USING staging_stops s
    ON t.stop_id = s.stop_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)
    spark.stop()
if __name__ == "__main__":
    main()