# file: emt_lines_to_iceberg.py
# EMR 7.0.0 | Spark 3.5.x | Iceberg v2 | sin egress (usa --jars con tu bundle spark-excel)

import argparse
import re
from pyspark.sql import SparkSession, functions as F, types as T

def get_spark():
    return (SparkSession.builder
            .appName("emt_lines_to_iceberg")
            .enableHiveSupport()
            .getOrCreate())

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True)
    p.add_argument("--warehouse", required=True)
    p.add_argument("--db", default="emt")
    return p.parse_args()

def normalize_col(cname: str) -> str:
    c = cname.strip().lower()
    c = c.replace(" ", "_").replace(".", "_").replace("-", "_")
    c = re.sub(r"[^a-z0-9_]", "_", c)
    c = re.sub(r"__+", "_", c)
    return c

def ensure_namespace(spark, db):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS glue_catalog.{db}")

# --------- Esquemas para parsear JSON embebido ---------

stops_schema = T.ArrayType(T.StructType([
    T.StructField("stop", T.StringType()),
    T.StructField("name", T.StringType()),
    T.StructField("postalAddress", T.StringType()),
    T.StructField("geometry", T.StructType([
        T.StructField("type", T.StringType()),
        T.StructField("coordinates", T.ArrayType(T.DoubleType()))
    ])),
    T.StructField("pmv", T.StringType()),
    T.StructField("dataLine", T.ArrayType(T.StringType()))
]))

timetable_schema = T.ArrayType(T.StructType([
    T.StructField("IdLine", T.StringType()),
    T.StructField("Label", T.StringType()),
    T.StructField("nameA", T.StringType()),
    T.StructField("nameB", T.StringType()),
    T.StructField("typeOfDays", T.ArrayType(T.StructType([
        T.StructField("Direction1", T.StructType([
            T.StructField("StartTime", T.StringType()),
            T.StructField("StopTime", T.StringType()),
            T.StructField("MinimunFrequency", T.StringType()),
            T.StructField("MaximumFrequency", T.StringType()),
            T.StructField("Frequency", T.StringType())
        ])),
        T.StructField("Direction2", T.StructType([
            T.StructField("StartTime", T.StringType()),
            T.StructField("StopTime", T.StringType()),
            T.StructField("MinimunFrequency", T.StringType()),
            T.StructField("MaximumFrequency", T.StringType()),
            T.StructField("Frequency", T.StringType())
        ])),
        T.StructField("DayType", T.StringType())
    ])))
]))

def main():
    args = parse_args()
    spark = get_spark()
    ensure_namespace(spark, args.db)

    # 1) Leer Excel (con tu bundle com.crealytics via --jars)
    df = (spark.read.format("com.crealytics.spark.excel")
          .option("header", "true")
          .option("inferSchema", "false")
          .load(args.input))

    # 2) Normalizar cabeceras
    for c in df.columns:
        df = df.withColumnRenamed(c, normalize_col(c))
    # Esperado: line, label, stops, timetable, stop_id, timetable_idline, timetable_label, ...

    # 3) Columnas base
    base = (df
            .select(
                F.col("line").cast("string").alias("line"),
                F.col("label").cast("string").alias("label"),
                F.col("stops").cast("string").alias("stops_json"),
                F.col("timetable").cast("string").alias("timetable_json")
            )
            .withColumn("ingest_ts", F.current_timestamp())
           )

    # 4) Tabla 1: emt.lines  (maestro de líneas)
    # Tomamos nameA/nameB del timetable si viene, y lo “alineamos” por la propia línea.
    tt = base.withColumn("tt_arr", F.from_json("timetable_json", timetable_schema)) \
             .withColumn("tt", F.explode_outer("tt_arr"))

    # Si el Excel trae varios entries en timetable (para varias líneas), nos quedamos con los que correspondan a la propia 'line' si existe,
    # si no, nos quedamos con todos y coalesce a 'tt.IdLine' como identificador.
    lines_master = (tt
        .withColumn("line_id_tt", F.col("tt.IdLine"))
        .withColumn("label_tt", F.col("tt.Label"))
        .withColumn("name_a", F.col("tt.nameA"))
        .withColumn("name_b", F.col("tt.nameB"))
        .withColumn("line_effective",
                    F.coalesce(F.col("line"), F.col("line_id_tt")))
        .withColumn("label_effective",
                    F.coalesce(F.col("label"), F.col("label_tt")))
        .select(
            F.col("line_effective").alias("line"),
            F.col("label_effective").alias("label"),
            "name_a", "name_b", "ingest_ts"
        )
        .dropDuplicates(["line"])  # una fila por línea
    )

    lines_tbl = f"glue_catalog.{args.db}.lines"
    lines_loc = f"{args.warehouse}/{args.db}/lines"

    lines_master.createOrReplaceTempView("staging_lines")
    spark.sql(f"""
    CREATE OR REPLACE TABLE {lines_tbl}
    USING iceberg
    LOCATION '{lines_loc}'
    AS SELECT * FROM staging_lines
    """)

    # 5) Tabla 2: emt.lines_stops  (explode de stops)
    stops_parsed = base.withColumn("stops_arr", F.from_json("stops_json", stops_schema)) \
                       .withColumn("st", F.explode_outer("stops_arr"))

    lines_stops = (stops_parsed
        .select(
            F.col("line").alias("line"),
            F.col("label").alias("label"),
            F.col("st.stop").alias("stop_id"),
            F.col("st.name").alias("stop_name"),
            F.col("st.postalAddress").alias("postal_address"),
            F.when(F.size(F.col("st.geometry.coordinates")) >= 2,
                   F.col("st.geometry.coordinates").getItem(0)).cast("double").alias("lon"),
            F.when(F.size(F.col("st.geometry.coordinates")) >= 2,
                   F.col("st.geometry.coordinates").getItem(1)).cast("double").alias("lat"),
            F.col("st.pmv").cast("string").alias("pmv"),
            F.col("st.dataLine").alias("data_lines"),
            F.col("ingest_ts")
        )
        .dropDuplicates(["line","stop_id"])
    )

    lines_stops_tbl = f"glue_catalog.{args.db}.lines_stops"
    lines_stops_loc = f"{args.warehouse}/{args.db}/lines_stops"

    lines_stops.createOrReplaceTempView("staging_lines_stops")
    spark.sql(f"""
    CREATE OR REPLACE TABLE {lines_stops_tbl}
    USING iceberg
    LOCATION '{lines_stops_loc}'
    AS SELECT * FROM staging_lines_stops
    """)

    # 6) Tabla 3: emt.lines_timetable  (explode timetable -> typeOfDays -> Direction1/2)
    ttd = tt.withColumn("tod", F.explode_outer(F.col("tt.typeOfDays")))

    # Campos direction1/direction2; pasamos frecuencias a INT cuando posible
    def to_int_str(col):
        # Algunas vienen como string; null-safe y vacíos -> null
        return F.when(F.col(col).rlike("^[0-9]+$"), F.col(col).cast("int")).otherwise(F.lit(None).cast("int"))

    lines_tt = (ttd
        .select(
            F.coalesce(F.col("line"), F.col("tt.IdLine")).alias("line"),
            F.coalesce(F.col("label"), F.col("tt.Label")).alias("label"),
            F.col("tt.nameA").alias("name_a"),
            F.col("tt.nameB").alias("name_b"),
            F.col("tod.DayType").alias("day_type"),

            F.col("tod.Direction1.StartTime").alias("d1_start_time"),
            F.col("tod.Direction1.StopTime").alias("d1_stop_time"),
            to_int_str("tod.Direction1.MinimunFrequency").alias("d1_min_freq"),
            to_int_str("tod.Direction1.MaximumFrequency").alias("d1_max_freq"),
            to_int_str("tod.Direction1.Frequency").alias("d1_freq"),

            F.col("tod.Direction2.StartTime").alias("d2_start_time"),
            F.col("tod.Direction2.StopTime").alias("d2_stop_time"),
            to_int_str("tod.Direction2.MinimunFrequency").alias("d2_min_freq"),
            to_int_str("tod.Direction2.MaximumFrequency").alias("d2_max_freq"),
            to_int_str("tod.Direction2.Frequency").alias("d2_freq"),

            F.col("ingest_ts")
        )
        .dropDuplicates(["line","day_type","d1_start_time","d2_start_time"])  # evita filas repetidas
    )

    lines_tt_tbl = f"glue_catalog.{args.db}.lines_timetable"
    lines_tt_loc = f"{args.warehouse}/{args.db}/lines_timetable"

    lines_tt.createOrReplaceTempView("staging_lines_timetable")
    spark.sql(f"""
    CREATE OR REPLACE TABLE {lines_tt_tbl}
    USING iceberg
    LOCATION '{lines_tt_loc}'
    AS SELECT * FROM staging_lines_timetable
    """)

    spark.stop()

if __name__ == "__main__":
    main()