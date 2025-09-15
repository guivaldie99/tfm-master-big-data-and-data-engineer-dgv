# file: emt_meteo_to_iceberg.py
import argparse
from pyspark.sql import SparkSession, functions as F, types as T

def spark():
    return (SparkSession.builder
            .appName("emt_meteo_to_iceberg")
            .enableHiveSupport()
            .getOrCreate())

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True)
    p.add_argument("--warehouse", required=True)
    p.add_argument("--db", default="meteorology")
    return p.parse_args()

def ensure_table(spark, db, warehouse):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS glue_catalog.{db}")
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{db}.meteorology_current (
      id STRING,
      run_ts_utc TIMESTAMP,
      obs_ts TIMESTAMP,
      location_name STRING,
      location_region STRING,
      location_country STRING,
      lat DOUBLE,
      lon DOUBLE,
      tz_id STRING,
      localtime_epoch BIGINT,
      localtime TIMESTAMP,

      temp_c DOUBLE,
      temp_f DOUBLE,
      is_day INT,
      condition_text STRING,
      condition_icon STRING,
      condition_code INT,

      wind_mph DOUBLE,
      wind_kph DOUBLE,
      wind_degree INT,
      wind_dir STRING,
      pressure_mb DOUBLE,
      pressure_in DOUBLE,
      precip_mm DOUBLE,
      precip_in DOUBLE,
      humidity INT,
      cloud INT,
      feelslike_c DOUBLE,
      feelslike_f DOUBLE,
      windchill_c DOUBLE,
      windchill_f DOUBLE,
      heatindex_c DOUBLE,
      heatindex_f DOUBLE,
      dewpoint_c DOUBLE,
      dewpoint_f DOUBLE,
      vis_km DOUBLE,
      vis_miles DOUBLE,
      uv DOUBLE,
      gust_mph DOUBLE,
      gust_kph DOUBLE,

      air_co DOUBLE,
      air_no2 DOUBLE,
      air_o3 DOUBLE,
      air_so2 DOUBLE,
      air_pm2_5 DOUBLE,
      air_pm10 DOUBLE,
      air_us_epa_index INT,
      air_gb_defra_index INT,

      ingest_ts TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY ( hours(obs_ts) )
    TBLPROPERTIES ('format-version'='2')
    LOCATION '{warehouse}/{db}/meteorology_current'
    """)

def mk_double(col):
    # Reemplaza coma por punto y castea a DOUBLE (soporta "1,76E+09", "40,4", etc.)
    return F.regexp_replace(F.col(col), ",", ".").cast("double")

def mk_int(col):
    return F.regexp_replace(F.col(col), ",", ".").cast("int")

def mk_bigint_from_decimal(col):
    # Para epochs con coma (p.ej. "1,76E+09"): pasamos a double y luego a BIGINT truncado
    return F.regexp_replace(F.col(col), ",", ".").cast("double").cast("bigint")

def main():
    args = parse_args()
    s = spark()

    # 1) Leer Excel
    df_x = (s.read.format("com.crealytics.spark.excel")
            .option("header", "true")
            .option("inferSchema", "false")
            .load(args.input))

    # 2) Normalizar nombres: minúsculas, '_' y sin '.'
    for c in df_x.columns:
        df_x = df_x.withColumnRenamed(
            c, c.strip().lower().replace(" ", "_").replace(".", "_").replace("-", "_")
        )

    # 3) Casteos y derivadas
    # Columnas base
    df = (df_x
          .withColumn("run_ts_utc", F.to_timestamp("run_ts_utc"))
          .withColumn("location_name", F.col("location_name"))
          .withColumn("location_region", F.col("location_region"))
          .withColumn("location_country", F.col("location_country"))
          .withColumn("lat", mk_double("location_lat"))
          .withColumn("lon", mk_double("location_lon"))
          .withColumn("tz_id", F.col("location_tz_id"))
          .withColumn("localtime_epoch", mk_bigint_from_decimal("location_localtime_epoch"))
          .withColumn("localtime", F.to_timestamp("location_localtime"))
          .withColumn("obs_ts", F.to_timestamp("current_last_updated")) # timestamp de observación
          # Métricas actuales
          .withColumn("temp_c", mk_double("current_temp_c"))
          .withColumn("temp_f", mk_double("current_temp_f"))
          .withColumn("is_day", mk_int("current_is_day"))
          .withColumn("condition_text", F.col("current_condition_text"))
          .withColumn("condition_icon", F.col("current_condition_icon"))
          .withColumn("condition_code", mk_int("current_condition_code"))
          .withColumn("wind_mph", mk_double("current_wind_mph"))
          .withColumn("wind_kph", mk_double("current_wind_kph"))
          .withColumn("wind_degree", mk_int("current_wind_degree"))
          .withColumn("wind_dir", F.col("current_wind_dir"))
          .withColumn("pressure_mb", mk_double("current_pressure_mb"))
          .withColumn("pressure_in", mk_double("current_pressure_in"))
          .withColumn("precip_mm", mk_double("current_precip_mm"))
          .withColumn("precip_in", mk_double("current_precip_in"))
          .withColumn("humidity", mk_int("current_humidity"))
          .withColumn("cloud", mk_int("current_cloud"))
          .withColumn("feelslike_c", mk_double("current_feelslike_c"))
          .withColumn("feelslike_f", mk_double("current_feelslike_f"))
          .withColumn("windchill_c", mk_double("current_windchill_c"))
          .withColumn("windchill_f", mk_double("current_windchill_f"))
          .withColumn("heatindex_c", mk_double("current_heatindex_c"))
          .withColumn("heatindex_f", mk_double("current_heatindex_f"))
          .withColumn("dewpoint_c", mk_double("current_dewpoint_c"))
          .withColumn("dewpoint_f", mk_double("current_dewpoint_f"))
          .withColumn("vis_km", mk_double("current_vis_km"))
          .withColumn("vis_miles", mk_double("current_vis_miles"))
          .withColumn("uv", mk_double("current_uv"))
          .withColumn("gust_mph", mk_double("current_gust_mph"))
          .withColumn("gust_kph", mk_double("current_gust_kph"))
          # air quality (algunas con guiones en el nombre original)
          .withColumn("air_co", mk_double("current_air_quality_co"))
          .withColumn("air_no2", mk_double("current_air_quality_no2"))
          .withColumn("air_o3", mk_double("current_air_quality_o3"))
          .withColumn("air_so2", mk_double("current_air_quality_so2"))
          .withColumn("air_pm2_5", mk_double("current_air_quality_pm2_5"))
          .withColumn("air_pm10", mk_double("current_air_quality_pm10"))
          .withColumn("air_us_epa_index", mk_int("current_air_quality_us_epa_index"))
          .withColumn("air_gb_defra_index", mk_int("current_air_quality_gb_defra_index"))
          .withColumn("ingest_ts", F.current_timestamp())
    )

    # 4) id estable (por localización + epoch observado + run_ts)
    df = df.withColumn(
        "id",
        F.sha2(F.concat_ws("||",
            F.coalesce(F.col("location_name"), F.lit("")),
            F.coalesce(F.col("location_region"), F.lit("")),
            F.coalesce(F.col("location_country"), F.lit("")),
            F.coalesce(F.col("localtime_epoch").cast("string"), F.lit("")),
            F.coalesce(F.col("obs_ts").cast("string"), F.lit("")),
            F.coalesce(F.col("run_ts_utc").cast("string"), F.lit(""))
        ), 256)
    )

    # 5) Asegura tabla y MERGE (idempotente)
    ensure_table(s, args.db, args.warehouse)

    df.createOrReplaceTempView("staging_meteo")
    s.sql(f"""
    MERGE INTO glue_catalog.{args.db}.meteorology_current t
    USING staging_meteo s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

    s.stop()

if __name__ == "__main__":
    main()