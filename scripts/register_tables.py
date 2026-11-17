import subprocess
from pipeline.config import Config
from pipeline.spark_session import build_spark, ensure_namespace
cfg = Config()
spark = build_spark("register_tables", cfg)
ensure_namespace(spark, cfg)
for t in ["trips_clean", "trips_rejected", "daily_zone_metrics"]:
    prefix = f"s3://{cfg.bucket}/warehouse/{cfg.namespace}/{t}/metadata/"
    out = subprocess.run(["aws", "s3", "ls", prefix], capture_output=True, text=True).stdout
    latest = sorted(l.split()[-1] for l in out.splitlines() if l.endswith(".metadata.json"))[-1]
    spark.sql(f"CALL lake.system.register_table(table => '{cfg.namespace}.{t}', metadata_file => '{prefix}{latest}')")
    print(t, "->", latest, spark.table(cfg.table(t)).count(), "rows")
spark.stop()
