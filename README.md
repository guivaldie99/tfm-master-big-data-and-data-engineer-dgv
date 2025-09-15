# TFM — Data Lake EMT + Meteorología (S3/Glue/Iceberg + EMR Serverless + Step Functions + ECS + Kinesis/Firehose)

Este repositorio contiene **infraestructura como código** (CloudFormation) y **código de procesamiento** (Spark y Python) para construir un **data lake por capas (raw → bronze → silver → gold)** con **catálogo Glue + Iceberg**, ingestas **batch** (EMT) y **near-real-time** (meteorología), y **orquestación** con **Step Functions** y **EventBridge**.

---

## 1) Arquitectura

┌──────────────────────────┐           ┌───────────────────────┐

│         Fuentes          │           │ Ingesta en tiempo real │

│  EMT APIs / Excel        │           │  ECS (Fargate) Docker  │

│  WeatherAPI (current)    │           │  → Kinesis DataStream  │

└───────────┬──────────────┘           └───────────┬───────────┘

            │                                      │
			
            │ (ficheros xlsx)                      │ (JSON)
			
            ▼                                      ▼
			
      s3://dl-.../raw/                       Kinesis Data Stream
	  
            │                                      │
			
            │                       ┌──────────────▼──────────────┐
			
            │                       │   Kinesis Firehose (Iceberg)│
			
            │                       │   + Lambda Transform         │
			
            │                       └──────────────┬──────────────┘
			
            │                                      │
			
            ▼                                      ▼
			
      s3://dl-.../bronze/                   s3://dl-.../silver/warehouse
	  
            │                           Glue Catalog + Iceberg (Silver)
			
            │  (S3 event)                          │
			
            │  Lambda execute-EMR                  │
			
            ▼                                      │
			
      EMR Serverless (Spark)  ─────────────────────┘
	  
            │
			
            ▼
			
  Glue Catalog + Iceberg (Silver)
  
            │
			
            │  Step Functions + EMR Serverless (Spark)
			
            ▼
			
  Glue Catalog + Iceberg (Gold)
  
        ├─ emt_gold.arrivals_weather (hechos enriquecidos)
		
        └─ emt_gold.delay_weather_hourly (agregados)
		

Puntos clave
    •    Iceberg v2 en S3, catálogo Glue.
    •    EMR Serverless para jobs Spark batch (Excel → Silver, y Silver → Gold).
    •    ECS Fargate + Kinesis/Firehose + Lambda para meteorología near-real-time a Silver.
    •    Step Functions orquesta el Gold; EventBridge programa la ejecución.

## 2) Estructura del repositorio

.
├── config-resources.yml                # S3 config bucket + KMS

├── datalake-resources.yml              # S3 Data Lake + Glue DBs + Athena WG

├── real-time.yml                       # Kinesis Data Stream + Firehose (Iceberg dest)

├── emr-studio

│   ├── emr-resources.yml               # EMR Studio + EMR Serverless + roles

│   ├── emr-gold-layer.yml              # Step Functions + EventBridge (job GOLD)

│   └── notebooks-jobs

│       ├── emt-arrivals.py             # Spark → Silver (arrivals)

│       ├── emt-lines.py                # Spark → Silver (lines & timetable)

│       ├── meteorology.py              # Spark → Silver (weather desde Excel batch)

│       └── gold-layer.py               # Spark → GOLD (enriquecido + agregados)

├── lambdas

│   ├── to-bronze-lambda/               # mueve s3://.../raw/ → s3://.../bronze/

│   ├── execute-emr-notebook-lambda/    # dispara EMR Serverless (procesa Excel)

│   ├── transformation-lambda/          # Firehose Transform → invoca writer

│   └── to-iceberg-lambda/              # asegura tabla Iceberg (Athena DDL)

├── microservices

│   └── meteorology-microservice

│       ├── Dockerfile

│       ├── meteorology-resources.yml   # ECS Service/Task + Secret + roles

│       └── src

│           ├── meteorology.py          # llama WeatherAPI y envía a Kinesis

│           └── requirements.txt

├── emt

│   ├── arrivals

│   │   ├── arrivals-YYYY-MM-DD.xlsx

│   │   └── arrivals-bus.py             # cliente EMT: genera Excels de llegadas

│   ├── lines

│   │   ├── bus_lines.xlsx

│   │   ├── bus_stops.xlsx

│   │   ├── emt_lines.xlsx

│   │   ├── lines-emt.py                # cliente EMT: líneas → Excel

│   │   └── stops-in-lines-emt.py       # cliente EMT: paradas por línea → Excel

│   └── stops

│       ├── bus_stops.xlsx

│       └── stops-emt.py                # cliente EMT: paradas → Excel

├── meteorology-alternative

│   ├── meteorology-YYYY-MM-DD.xlsx

│   └── weather_to_excel.py             # alternativa batch (Excel) para meteo

└── README.md

## 3) Prerrequisitos

    •    AWS CLI configurado con permisos administrativos (o por stack).
    •    Cuenta y región con VPC “SpokeVPC”, subnets privadas exportadas (usadas por EMR/ECS).
    •    SSO user (para EMR Studio) si lo vas a desplegar.
    •    WeatherAPI key para el microservicio de meteorología.
    •    Buckets únicos: los nombres están parametrizados; por defecto tfm/dgv/mbdde/dev.
    •    JAR spark-excel subido a config-...-bucket/jars/spark-excel/spark-excel-bundle-3.5.1_0.20.4-all.jar.

## 4) Despliegue de infrastructura (CloudFormation)

### 4.1) Orden recomendado

	1.	config-resources.yml
	2.	datalake-resources.yml
	3.	emr-studio/emr-resources.yml (si usas EMR Studio y EMR Serverless)
	4.	lambdas/*-resources.yml (to-bronze, execute-emr-notebook, transformation, to-iceberg)
	5.	real-time.yml
	6.	microservices/meteorology-microservice/meteorology-resources.yml
	7.	emr-studio/emr-gold-layer.yml (Step Functions + EventBridge)

Todos los templates exportan outputs con Fn::ImportValue que encajan entre sí.

### 4.2) Ejemplo de despliegue 

# 1) Config
aws cloudformation deploy \
  --template-file config-resources.yml \
  --stack-name dl-config-tfm-dgv-mbdde \
  --capabilities CAPABILITY_NAMED_IAM

# 2) Data Lake
aws cloudformation deploy \
  --template-file datalake-resources.yml \
  --stack-name dl-datalake-tfm-dgv-mbdde-dev \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides Environment=dev

# 3) EMR Studio + EMR Serverless
aws cloudformation deploy \
  --template-file emr-studio/emr-resources.yml \
  --stack-name dl-emr-resources-dgv-mbdde \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides EmrStudioUserName=<tu_usuario_sso> Environment=dev \
  --parameter-overrides SubnetOverride="subnet-aaa,subnet-bbb,subnet-ccc" # opcional

# 4) Lambdas
#   4.1) mover raw→bronze
aws cloudformation deploy \
  --template-file lambdas/to-bronze-lambda/to-bronze-lambda-resources.yml \
  --stack-name dl-to-bronze-lambda-dev \
  --capabilities CAPABILITY_NAMED_IAM

#   4.2) execute EMR notebook (arrivals|lines|meteorology)
aws cloudformation deploy \
  --template-file lambdas/execute-emr-notebook-lambda/execute-emr-notebook-lambda-resources.yml \
  --stack-name dl-exec-arrivals-emr-lambda-dev \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides Domain=arrivals \
    JobScriptS3=s3://dl-...-emr-bucket/mbdde/.../emt-arrivals.py \
    DatabaseName=tfm_dgv_mbdde_emt_silver

#   4.3) transformación Firehose → writer
aws cloudformation deploy \
  --template-file lambdas/transformation-lambda/transformation-lambda-resources.yml \
  --stack-name dl-transform-lambda-dev \
  --capabilities CAPABILITY_NAMED_IAM

#   4.4) writer/ensure Iceberg
aws cloudformation deploy \
  --template-file lambdas/to-iceberg-lambda/to-iceberg-lambda-resources.yml \
  --stack-name dl-to-iceberg-lambda-dev \
  --capabilities CAPABILITY_NAMED_IAM

# 5) Real-time (Kinesis + Firehose Iceberg destination)
aws cloudformation deploy \
  --template-file real-time.yml \
  --stack-name dl-realtime-dev \
  --capabilities CAPABILITY_NAMED_IAM

# 6) Microservicio meteorología (ECS)
aws cloudformation deploy \
  --template-file microservices/meteorology-microservice/meteorology-resources.yml \
  --stack-name meteorology-microservice-dev \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides ApyKeyMeterology=<TU_API_KEY> Environment=dev DeployStack=true

# 7) Orquestación GOLD
aws cloudformation deploy \
  --template-file emr-studio/emr-gold-layer.yml \
  --stack-name dl-gold-sfn-dev \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
      EmrApplicationId=<appId_de_EMR_Serverless> \
      EmrRuntimeRoleArn=arn:aws:iam::<acct>:role/dl-tfm-dgv-mbdde-dev-emr-serverless-role \
      JobsBucket=dl-tfm-dgv-mbdde-dev-emr-bucket \
      JobsKey=mbdde/.../gold-layer.py \
      WarehouseUri=s3://dl-tfm-dgv-mbdde-dev-bucket/gold \
      GoldDbName=tfm_dgv_mbdde_emt_gold \
      ScheduleCron='cron(0 10 * * ? *)'


## 5) Ingesta EMT (batch a Silver)

### 5.1) Flujo

1.    Scripts emt/ llaman a las APIs EMT y generan Excel:
    •    emt/arrivals/arrivals-bus.py
    •    emt/lines/lines-emt.py, stops-in-lines-emt.py, stops-emt.py
2.    Subes los Excel a s3://dl-.../raw/....
3.    S3 event → to-bronze-lambda mueve a bronze/.
4.    Otro S3 event en bronze/... invoca execute-emr-notebook-lambda (una por dominio).
5.    La Lambda llama EMR Serverless con emt-arrivals.py o emt-lines.py (Spark):
    •    Usa spark-excel vía --jars (bundle en S3).
    •    Escribe Iceberg Silver en Glue:
    •    tfm_dgv_mbdde_emt_silver.arrivals_predictions, stops
    •    ...emt_silver.lines, lines_stops, lines_timetable

### 5.2) Ejecución Spark (parametros)

emt-arrivasl.py
--input s3://dl-.../bronze/emt/arrivals/arrivals-YYYY-MM-DD.xlsx
--warehouse s3://dl-.../silver
--db tfm_dgv_mbdde_emt_silver

emt-lines.py
--input s3://dl-.../bronze/emt/lines/emt_lines.xlsx
--warehouse s3://dl-.../silver
--db tfm_dgv_mbdde_emt_silver

Importante: Los jobs se lanzan desde Lambda con --jars s3://config-.../spark-excel-bundle-3.5.1_0.20.4-all.jar

## 6) Ingesta Meteorology (near-real-time a Silver)

### 6.1) Flujo

1.    ECS Fargate ejecuta meteorology.py (Docker), lee WeatherAPI current cada 30 min.
2.    Envío a Kinesis Data Stream.
3.    Firehose (destino Iceberg) invoca transformation-lambda:
    •    Renombra/filtra clave → invoca to-iceberg-lambda (asegura tabla) → devuelve record transformado a Firehose.
4.    Firehose escribe en S3 Silver warehouse y actualiza Glue/Iceberg:
tfm_dgv_mbdde_meteorology_silver.meteorology_current.

### 6.2) Variables y secretos

•    En ECS:
    •    SECRET_NAME=dl-tfm-dgv-mbdde-<env>-meteorology-secret (CFN lo crea y rellena con tu api_key)
    •    KINESIS_STREAM_NAME (export de real-time.yml)

## 7) Capa GOLD (enriquecimientos y agregados)

### 7.1) Script

emr-studio/notebooks-jobs/gold-layer.py (aka emt_build_gold.py) realiza:
    •    Joins Silver arrivals con lines_timetable para derivar headway esperado por franja y tipo de día.
    •    Join temporal con meteorology_current (nearest dentro de ±--wx_recent_minutes).
    •    Calcula delay_ratio, delayed, severidad meteorológica y atribución delay_weather_attributed.
    •    Escribe/merge en Gold:
    •    ...emt_gold.arrivals_weather (partición date(event_ts))
    •    ...emt_gold.delay_weather_hourly (agregados por hora x línea)

### 7.2) Ejecución (Step Funtions + EMR Serverless)

Plantilla emr-gold-layer.yml crea:
  •    IAM para Step Functions.
  •    State Machine que hace:
    1.    emrserverless:startJobRun con:
      •    SparkSubmit.EntryPoint = s3://<JobsBucket>/<JobsKey> (gold-layer.py)
      •    EntryPointArguments = --warehouse ... --gold_db_emt ... --silver_db_emt ... --silver_db_met ...
      •    SparkSubmitParameters con config Iceberg.
    2.    Polling getJobRun → SUCCESS/FAILED.
      •    EventBridge rule programable vía ScheduleCron (UTC) para lanzar la state machine.

Nota importante sobre argumentos
El script no acepta --db; acepta --gold_db_emt, --silver_db_emt, --silver_db_met, --warehouse.
El template ya pasa los correctos (corrige el error típico “unrecognized arguments: --db ...”).


## 8) Modelo de datos (tablas principales)

### 8.1) Silver - EMT

•    tfm_dgv_mbdde_emt_silver.arrivals_predictions
    •    id (sha256), event_ts, line, stop_id, destination, is_head, deviation, bus, estimate_arrive_sec, distance_bus_m, position_type_bus, lon, lat, ingest_ts
    •    PARTITION: days(event_ts), truncate(line,2)
•    tfm_dgv_mbdde_emt_silver.stops
    •    stop_id, stop_name, lon, lat, direction, lines[], updated_ts
•    tfm_dgv_mbdde_emt_silver.lines
    •    line, label, name_a, name_b, ingest_ts
•    tfm_dgv_mbdde_emt_silver.lines_stops
    •    line, label, stop_id, stop_name, postal_address, lon, lat, pmv, data_lines[], ingest_ts
•    tfm_dgv_mbdde_emt_silver.lines_timetable
    •    line, label, name_a, name_b, day_type, d1_* / d2_* (Start/Stop/Min/Max/Freq), ingest_ts

### 8.2) Silver - Meteorology

•    tfm_dgv_mbdde_meteorology_silver.meteorology_current
    •    obs_ts, temp_c, wind_kph, precip_mm, …, air_pm10, air_pm2_5, …, ingest_ts
    •    PARTITION: hours(obs_ts)

### 8.3) GOLD - EMT

•    tfm_dgv_mbdde_emt_gold.arrivals_weather
    •    (campos de arrivals) + expected_headway_min, delay_ratio, delayed
    •    (meteorología cercana) obs_ts, temp_c, wind_kph, precip_mm, …
    •    derivados: weather_severity (‘good|moderate|bad|severe’), weather_adverse, delay_weather_attributed
    •    PARTITION: date(event_ts)
•    tfm_dgv_mbdde_emt_gold.delay_weather_hourly
    •    event_hour, line, total_preds, delayed_preds, delay_rate, …, weather_attrib_rate, avg_temp_c, …
    •    PARTITION: date(event_hour)

## 9) Opercion y scheduling

•    Carga EMT: sube Excel a raw/emt/... → se encadenan Lambdas y job Spark a Silver.
•    Gold diario: EventBridge (cron UTC) inicia la State Machine que lanza gold-layer.py.
•    Meteorología: ECS ejecuta cada 30 min (ciclo interno del contenedor) → Kinesis → Firehose.

## 10) Consultas con Athena

1.    Selecciona el Workgroup creado por datalake-resources.yml.
2.    En Database, verás:
    •    tfm_dgv_mbdde_emt_silver, tfm_dgv_mbdde_meteorology_silver, tfm_dgv_mbdde_emt_gold
3. Ejemplos

-- Últimas llegadas con clima
SELECT *
FROM tfm_dgv_mbdde_emt_gold.arrivals_weather
WHERE date(event_ts) = current_date;

-- Tasa de retraso por línea y hora
SELECT event_hour, line, delay_rate, weather_attrib_rate
FROM tfm_dgv_mbdde_emt_gold.delay_weather_hourly
WHERE event_hour >= date_trunc('day', current_timestamp - interval '1' day)
ORDER BY event_hour, line;

## 11) Parametrizacion relevante

emr-gold-layer.yml
    •    EmrApplicationId, EmrRuntimeRoleArn
    •    JobsBucket + JobsKey → ubicación de gold-layer.py
    •    WarehouseUri (p.ej. s3://dl-.../gold)
    •    GoldDbName (por defecto tfm_dgv_mbdde_emt_gold)
    •    DelayRatioThresh, WxRecentMinutes, ScheduleCron

execute-emr-notebook-lambda-resources.yml
    •    Domain: arrivals | lines | meteorology
    •    JobScriptS3: ruta al script Spark (Excel → Silver)
    •    DatabaseName: DB Silver destino (emt o meteorology)
    •    Variables de entorno: WAREHOUSE_S3 (por defecto s3://.../silver), S3_LOGS

real-time.yml
    •    kfhBufferMaxIntervalInSeconds, kfhBufferMaxSizeInMBs
    •    Glue Catalog como destino Iceberg

meteorology-resources.yml
    •    ApyKeyMeterology (Secrets Manager)
    •    Fargate Task/Service + SG + permisos Kinesis/S3/KMS


## 12) Troubleshooting (errores comunes)

•    Step Functions — ClientToken requerido
Usa el SDK integration con ClientToken.$: "States.UUID()" (ya incluido en emr-gold-layer.yml). Si ves SCHEMA_VALIDATION_FAILED: The field 'ClientToken' is required, revisa que el estado StartJobRun mantenga esa línea y que el Resource sea arn:aws:states:::aws-sdk:emrserverless:startJobRun.
    •    EMR job falla con unrecognized arguments: --db ...
gold-layer.py no acepta --db. Pasa --gold_db_emt, --silver_db_emt, --silver_db_met, --warehouse (el template ya lo hace). Para jobs Silver, sí se usa --db.
    •    Lectura Excel en Spark
Asegura el bundle spark-excel en S3 y añade --jars s3://config-.../spark-excel-bundle-3.5.1_0.20.4-all.jar. Evita --packages en EMR Serverless.
    •    Permisos S3/KMS/Glue
Si el job no puede leer/escribir, revisa:
    •    Rol de EMR Serverless (emr-resources.yml): S3 (ProjectBucket y DataLake), KMS (ProjectKmsKey + DL KMS), Glue catálogo/tablas.
    •    Rol de Step Functions: iam:PassRole al runtime role EMR y acceso S3 a JobsBucket/WarehouseBucket.
    •    Catálogo/warehouse inconsistentes
spark.sql.catalog.glue_catalog.warehouse debe apuntar al warehouse de la capa correspondiente (Silver para ingesta, Gold para enriquecido).
    •    ECS sin conectividad
Revisa subnets privadas, SG egress a 443 y que el secreto exista.


## 13) Desarrollo local (scripts EMT)

Los scripts en emt/ consumen las APIs EMT y generan Excel. Requieren variables de entorno:

export EMT_EMAIL="xxx@yyy.com"
export EMT_PASSWORD="*******"

# ARRIVALS (requiere Excel con la columna 'stops' como lista de dicts)
python emt/arrivals/arrivals-bus.py

# LÍNEAS
python emt/lines/lines-emt.py
# PARADAS (todas)
python emt/stops/stops-emt.py
# PARADAS por LÍNEA (entrada: bus_lines.xlsx)
python emt/lines/stops-in-lines-emt.py

Sube los Excel generados a s3://dl-.../raw/emt/... para activar la ingesta.

## 14) Extender el sistema

    •    Nueva fuente batch: publica Excel/CSV en raw/<dominio>/... → añade target S3 event → Lambda execute EMR → crea un job Spark <dominio>.py que escriba a silver.
    •    Nuevas métricas GOLD: modifica gold-layer.py para añadir features/joins y otro MERGE a nuevas tablas en gold.
    •    Nuevos agregados: crea vistas/materializaciones adicionales y planifícalas con otra State Machine/EventBridge.

## 15) Seguridad y costes

    •    KMS en todos los buckets.
    •    Versionado en S3 del data lake.
    •    AutoStart/AutoStop en EMR Serverless.
    •    ON_DEMAND en Kinesis (sin shards fijos).
    •    Límites de Firehose buffers (IntervalInSeconds, SizeInMBs) ajustados a tu flujo.

## 16) Referencias rapidas (snippets)

Spark Iceberg conf (ya en plantillas):
--conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog
--conf spark.sql.catalog.glue_catalog.warehouse=s3://.../<silver|gold>
--conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
--conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions

Estado Step Functions (StartJobRun) clave:
"Resource": "arn:aws:states:::aws-sdk:emrserverless:startJobRun",
"Parameters": {
  "ApplicationId": "<APP_ID>",
  "ExecutionRoleArn": "<RUNTIME_ROLE>",
  "ClientToken.$": "States.UUID()",
  "JobDriver": { "SparkSubmit": { ... } }
}
