"""
# 02 — Silver Layer: Clean, Deduplicate, SCD Type 2

Reads Bronze Delta tables → writes:
- `silver_{resource}` — current cleaned rows
- `silver_{resource}_scd2` — full historical versions
"""

# CELL: code
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

RESOURCES = ["patient", "encounter", "observation", "condition"]
HASH_EXCLUDE = {
    "extraction_timestamp",
    "api_url_or_params",
    "raw_json",
    "effective_from",
    "effective_to",
    "is_current",
    "version_hash",
    "load_timestamp",
    "api_call_timestamp",
    "saved_timestamp",
}

# CELL: code
def clean_bronze(df):
    df = df.filter(F.col("resource_id").isNotNull() & (F.col("resource_id") != ""))
    w = Window.partitionBy("resource_id").orderBy(
        F.col("fhir_last_updated").desc_nulls_last(),
        F.col("extraction_timestamp").desc_nulls_last(),
    )
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def with_version_hash(df):
    # Hash business columns only (exclude metadata / SCD2 cols)
    cols = [c for c in df.columns if c not in HASH_EXCLUDE]
    return df.withColumn(
        "version_hash",
        F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]), 256),
    )


def apply_scd2_delta(resource: str, incoming):
    scd2_table = f"silver_{resource}_scd2"
    current_table = f"silver_{resource}"
    now = F.current_timestamp()

    incoming = (
        with_version_hash(incoming)
        .withColumn("effective_from", now)
        .withColumn("effective_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
    )

    if not spark.catalog.tableExists(scd2_table):
        (
            incoming.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(scd2_table)
        )
        incoming.write.format("delta").mode("overwrite").saveAsTable(current_table)
        print(f"Created {scd2_table} and {current_table}")
        return

    target = DeltaTable.forName(spark, scd2_table)

    # Expire changed current versions
    (
        target.alias("t")
        .merge(incoming.alias("s"), "t.resource_id = s.resource_id AND t.is_current = true")
        .whenMatchedUpdate(
            condition="t.version_hash <> s.version_hash",
            set={"is_current": "false", "effective_to": "s.effective_from"},
        )
        .execute()
    )

    # Insert brand-new keys OR new versions after change
    existing_current = spark.table(scd2_table).filter("is_current = true").select(
        "resource_id", F.col("version_hash").alias("cur_hash")
    )
    to_insert = (
        incoming.alias("s")
        .join(existing_current.alias("c"), "resource_id", "left")
        .filter(F.col("c.resource_id").isNull() | (F.col("s.version_hash") != F.col("c.cur_hash")))
        .select("s.*")
    )
    to_insert.write.format("delta").mode("append").saveAsTable(scd2_table)

    # Refresh current snapshot
    (
        spark.table(scd2_table)
        .filter("is_current = true")
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(current_table)
    )
    print(f"SCD2 upsert complete → {scd2_table} / {current_table}")

# CELL: code
for resource in RESOURCES:
    bronze = f"bronze_{resource}"
    if not spark.catalog.tableExists(bronze):
        print(f"Skip {resource}: {bronze} missing")
        continue
    cleaned = clean_bronze(spark.table(bronze))
    apply_scd2_delta(resource, cleaned)
    display(spark.sql(f"SELECT COUNT(*) AS current_rows FROM silver_{resource}"))
    display(
        spark.sql(
            f"SELECT is_current, COUNT(*) AS rows FROM silver_{resource}_scd2 GROUP BY is_current"
        )
    )
