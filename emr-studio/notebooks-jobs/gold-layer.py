# file: emt_build_gold.py
# Spark 3.5.x | Iceberg v2
import argparse
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window

# ---------------------------
# Helpers
# ---------------------------

def get_spark():
    return (SparkSession.builder
            .appName("emt_build_gold")
            .enableHiveSupport()
            .getOrCreate())

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--silver_db_emt", default="tfm_dgv_mbdde_emt_silver")
    p.add_argument("--silver_db_met", default="tfm_dgv_mbdde_meteorology_silver")
    p.add_argument("--gold_db_emt", default="tfm_dgv_mbdde_emt_gold")
    p.add_argument("--warehouse", required=True) # s3://<bucket>/gold/warehouse
    p.add_argument("--tz", default="Europe/Madrid")
    # Umbrales
    p.add_argument("--delay_ratio_thresh", type=float, default=1.30) # >130% del headway esperado => retraso
    p.add_argument("--wx_recent_minutes", type=int, default=60) # meteo válida +/-60 min del evento
    return p.parse_args()

def ensure_ns_and_tables(spark, gold_db, warehouse):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS glue_catalog.{gold_db}")

    # Tabla 1: hechos enriquecidos (una fila por predicción de llegada)
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{gold_db}.arrivals_weather (
      id STRING,                               -- de Silver (arrivals_predictions)
      event_ts TIMESTAMP,
      line STRING,
      stop_id STRING,
      destination STRING,
      estimate_arrive_sec INT,
      distance_bus_m INT,
      bus INT,

      -- esperado por oferta (headway) según timetable
      expected_headway_min INT,
      delay_ratio DOUBLE,
      delayed BOOLEAN,

      -- features de meteo
      obs_ts TIMESTAMP,
      temp_c DOUBLE,
      wind_kph DOUBLE,
      precip_mm DOUBLE,
      humidity INT,
      vis_km DOUBLE,
      uv DOUBLE,
      air_pm10 DOUBLE,
      air_pm2_5 DOUBLE,
      condition_text STRING,

      -- severidad y atribución
      weather_severity STRING,   -- 'good'|'moderate'|'bad'|'severe'
      weather_adverse BOOLEAN,
      delay_weather_attributed BOOLEAN,  -- delayed AND weather_adverse

      build_ts TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (date(event_ts))
    TBLPROPERTIES ('format-version'='2')
    LOCATION '{warehouse}/{gold_db}/arrivals_weather'
    """)

    # Tabla 2: agregados por hora y línea (para BI/monitoring)
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{gold_db}.delay_weather_hourly (
      event_hour TIMESTAMP,  -- date_trunc('hour', event_ts)
      line STRING,

      total_preds BIGINT,
      delayed_preds BIGINT,
      delay_rate DOUBLE,

      adverse_preds BIGINT,
      adverse_rate DOUBLE,

      weather_attrib_delays BIGINT, -- retrasos con clima adverso
      weather_attrib_rate DOUBLE,

      avg_temp_c DOUBLE,
      avg_wind_kph DOUBLE,
      avg_precip_mm DOUBLE,

      build_ts TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (date(event_hour))
    TBLPROPERTIES ('format-version'='2')
    LOCATION '{warehouse}/{gold_db}/delay_weather_hourly'
    """)

# ---------------------------
# Lógica: day_type, timetable y expected headway
# ---------------------------

def derive_day_type(col_ts, tz):
    # 1=Sun, 2=Mon, ... 7=Sat en Spark
    dow = F.dayofweek(F.from_utc_timestamp(col_ts, tz))
    return (
        F.when(dow.between(2, 6), F.lit("LA"))  # Lunes–Viernes
         .when(dow == 7, F.lit("SA"))           # Sábado
         .otherwise(F.lit("FE"))                # Domingo (y festivos si luego los modelas)
    )

def time_to_min_expr(col_str):
    # 'HH:MM' -> minutos desde medianoche
    return (F.split(col_str, ":").getItem(0).cast("int")*60 +
            F.split(col_str, ":").getItem(1).cast("int"))

def build_expected_headway_view(spark, silver_db_emt, tz):
    tt = spark.table(f"glue_catalog.{silver_db_emt}.lines_timetable")

    # Dos direcciones; generamos dos filas por cada una y luego nos quedamos con el mínimo headway disponible
    tt_d1 = (tt
        .select(
            "line","label","name_a","name_b","day_type",
            F.col("d1_start_time").alias("start_time"),
            F.col("d1_stop_time").alias("stop_time"),
            F.col("d1_min_freq").alias("min_freq"),
            F.col("d1_max_freq").alias("max_freq"),
            F.lit("D1").alias("dir")
        ))

    tt_d2 = (tt
        .select(
            "line","label","name_a","name_b","day_type",
            F.col("d2_start_time").alias("start_time"),
            F.col("d2_stop_time").alias("stop_time"),
            F.col("d2_min_freq").alias("min_freq"),
            F.col("d2_max_freq").alias("max_freq"),
            F.lit("D2").alias("dir")
        ))

    tt_union = tt_d1.unionByName(tt_d2)

    tt_ranges = (tt_union
        .where(F.col("start_time").isNotNull() & F.col("stop_time").isNotNull())
        .withColumn("start_min", time_to_min_expr(F.col("start_time")))
        .withColumn("stop_min", time_to_min_expr(F.col("stop_time")))
        .withColumn("min_freq", F.col("min_freq")) # ya int
        .withColumn("max_freq", F.col("max_freq"))
    )

    tt_ranges.createOrReplaceTempView("tt_ranges")

    # Vista con headway "más favorable" (menor) por línea, day_type y minuto del día (en rango)
    # Nota: si hay solapes D1/D2, tomamos el menor min_freq disponible.
    spark.sql("""
    CREATE OR REPLACE TEMP VIEW expected_headway AS
    SELECT line, day_type, start_min, stop_min,
           min(min_freq) AS expected_min_freq
    FROM tt_ranges
    GROUP BY line, day_type, start_min, stop_min
    """)

# ---------------------------
# Lógica: Meteo + severidad
# ---------------------------

def build_weather_nearby_df(spark, silver_db_met, tz):
    wx = spark.table(f"glue_catalog.{silver_db_met}.meteorology_current") \
              .select(
                  "obs_ts","temp_c","wind_kph","precip_mm","humidity","vis_km","uv",
                  "air_pm10","air_pm2_5","condition_text"
              )
    # Redondeamos a 30 min para facilitar joins aproximados
    wx_rounded = (wx
      .withColumn("obs_30m", F.date_trunc("minute", F.col("obs_ts")))
      .withColumn("m5", F.minute("obs_30m") % 30)
      .withColumn(
          "obs_slot",
          F.to_timestamp(F.from_unixtime(F.col("obs_30m").cast("long") - F.col("m5")*60))
      )
      .drop("m5")
    )
    return wx_rounded

def weather_severity_expr():
    # Reglas:
    # 'severe': precip >= 2mm OR wind_kph >= 45 OR vis_km < 3
    # 'bad'   : precip >= 0.5 OR wind_kph >= 30 OR vis_km < 6
    # 'moderate': precip >= 0.1 OR wind_kph >= 20
    # 'good'  : resto
    return (F.when((F.col("precip_mm") >= 2) | (F.col("wind_kph") >= 45) | (F.col("vis_km") < 3), "severe")
             .when((F.col("precip_mm") >= 0.5) | (F.col("wind_kph") >= 30) | (F.col("vis_km") < 6), "bad")
             .when((F.col("precip_mm") >= 0.1) | (F.col("wind_kph") >= 20), "moderate")
             .otherwise("good"))

# ---------------------------
# Build Gold
# ---------------------------

def main():
    args = parse_args()
    s = get_spark()

    ensure_ns_and_tables(s, args.gold_db_emt, args.warehouse)

    # 1) Cargar Silver (arrivals)
    arr = s.table(f"glue_catalog.{args.silver_db_emt}.arrivals_predictions") \
           .select("id","event_ts","line","stop_id","destination","estimate_arrive_sec",
                   "distance_bus_m","bus")

    # 2) Headway esperado a partir de timetable
    build_expected_headway_view(s, args.silver_db_emt, args.tz)

    # Preparamos arrivals con columnas para join temporal
    arr2 = (arr
        .withColumn("day_type", derive_day_type(F.col("event_ts"), args.tz))
        .withColumn("event_local", F.from_utc_timestamp("event_ts", args.tz))
        .withColumn("event_min", F.hour("event_local")*60 + F.minute("event_local"))
        .withColumn("event_slot", F.date_trunc("minute", F.col("event_ts")))
    )

    # Columns de 'a' para el groupBy (equivalente a "a.*" pero válido)
    cols_a = [F.col(f"a.{c}") for c in arr2.columns]

    arr_headway = (arr2.alias("a")
        .join(F.broadcast(s.table("expected_headway")).alias("h"),
              (F.col("a.line") == F.col("h.line")) &
              (F.col("a.day_type") == F.col("h.day_type")) &
              (F.col("a.event_min") >= F.col("h.start_min")) &
              (F.col("a.event_min") < F.col("h.stop_min")),
              "left")
        .groupBy(*cols_a)   # en vez de "a.*"
        .agg(F.min(F.col("h.expected_min_freq")).alias("expected_headway_min"))
    )

    # 3) Meteo cercana
    wx = build_weather_nearby_df(s, args.silver_db_met, args.tz)

    # Creamos slots de 30' para arrivals y unimos al nearest dentro de +/- wx_recent_minutes
    w = args.wx_recent_minutes
    arr_with_slot = arr_headway.withColumn(
        "event_slot",
        F.expr("date_trunc('minute', event_ts) - make_interval(0,0,0,0,0, minute(date_trunc('minute', event_ts)) % 30, 0)")
    )

    joined = (arr_with_slot.alias("a")
        .join(wx.alias("w"),
              F.col("w.obs_slot").between(
                  F.expr("a.event_slot - interval {} minutes".format(w)),
                  F.expr("a.event_slot + interval {} minutes".format(w))
              ),
              "left")
        # elegimos la observación más cercana en tiempo
        .withColumn("time_diff",
                    F.abs(F.unix_timestamp("a.event_ts") - F.unix_timestamp("w.obs_ts")))
        .withColumn("rn", F.row_number().over(
            Window.partitionBy("a.id").orderBy(F.col("time_diff").asc_nulls_last())
        ))
        .where(F.col("rn") == 1)
        .drop("rn","time_diff","obs_slot")
    )

    # 4) Etiquetas de retraso y clima

    delayed_flag = F.when(
        (F.col("expected_headway_min").isNotNull()) &
        (F.col("estimate_arrive_sec") > F.col("expected_headway_min")*60*args.delay_ratio_thresh),
        F.lit(True)
    ).otherwise(F.lit(False))

    wx_severity = weather_severity_expr()
    wx_adverse = F.col("weather_severity").isin("bad","severe")

    enriched = (joined
        .withColumn("delay_ratio",
                    F.when(F.col("expected_headway_min").isNotNull(),
                           F.col("estimate_arrive_sec") / (F.col("expected_headway_min")*60.0))
                     .otherwise(F.lit(None).cast("double")))
        .withColumn("delayed", delayed_flag)
        .withColumn("weather_severity", wx_severity)
        .withColumn("weather_adverse", wx_adverse)
        .withColumn("delay_weather_attributed",
                    F.when(F.col("delayed") & F.col("weather_adverse"), F.lit(True)).otherwise(F.lit(False)))
        .withColumn("build_ts", F.current_timestamp())
        .select(
            "id","event_ts","line","stop_id","destination","estimate_arrive_sec","distance_bus_m","bus",
            F.col("expected_headway_min").cast("int").alias("expected_headway_min"),
            "delay_ratio","delayed",
            F.col("obs_ts"),
            "temp_c","wind_kph","precip_mm","humidity","vis_km","uv","air_pm10","air_pm2_5","condition_text",
            "weather_severity","weather_adverse","delay_weather_attributed",
            "build_ts"
        )
    )

    # 5) MERGE a Gold.arrivals_weather (idempotente)
    enriched.createOrReplaceTempView("staging_aw")
    s.sql(f"""
    MERGE INTO glue_catalog.{args.gold_db_emt}.arrivals_weather t
    USING staging_aw s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

    # 6) Agregados por hora y línea
    hourly = (enriched
        .withColumn("event_hour", F.date_trunc("hour", F.col("event_ts")))
        .groupBy("event_hour","line")
        .agg(
            F.count("*").alias("total_preds"),
            F.sum(F.col("delayed").cast("int")).alias("delayed_preds"),
            (F.sum(F.col("delayed").cast("int"))/F.count("*")).alias("delay_rate"),
            F.sum(F.col("weather_adverse").cast("int")).alias("adverse_preds"),
            (F.sum(F.col("weather_adverse").cast("int"))/F.count("*")).alias("adverse_rate"),
            F.sum(F.col("delay_weather_attributed").cast("int")).alias("weather_attrib_delays"),
            (F.sum(F.col("delay_weather_attributed").cast("int"))/F.count("*")).alias("weather_attrib_rate"),
            F.avg("temp_c").alias("avg_temp_c"),
            F.avg("wind_kph").alias("avg_wind_kph"),
            F.avg("precip_mm").alias("avg_precip_mm")
        )
        .withColumn("build_ts", F.current_timestamp())
    )
    hourly.createOrReplaceTempView("staging_hourly")

    # Upsert por (event_hour,line)
    s.sql(f"""
    MERGE INTO glue_catalog.{args.gold_db_emt}.delay_weather_hourly t
    USING staging_hourly s
    ON t.event_hour = s.event_hour AND t.line = s.line
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

    # Mantenimiento Iceberg
    s.sql(f"CALL glue_catalog.system.rewrite_data_files('{args.gold_db_emt}.arrivals_weather')")
    s.sql(f"CALL glue_catalog.system.rewrite_data_files('{args.gold_db_emt}.delay_weather_hourly')")

    s.stop()

if __name__ == "__main__":
    main()