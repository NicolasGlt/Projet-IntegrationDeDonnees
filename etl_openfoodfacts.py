#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETL OpenFoodFacts → Spark (Silver clean) → Parquet (+ sample CSV + metrics)
Usage (depuis WSL de préférence) :
  python etl_openfoodfacts.py --input /chemin/vers/en.openfoodfacts.org.products.csv --outdir ./out [--inferSchema]

Notes :
- Conserve explicitement la colonne `brands` (pour la dim_brand en GOLD).
- Évite le type VOID sur `product_name_pref` (cast en string).
- Harmonise sodium/sel, borne quelques nutriments, calcule la complétude.
- Écrit un Parquet complet + un CSV d’échantillon.
"""

import argparse
import json
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, lower, regexp_replace, when, lit, length, to_timestamp,
    avg as spark_avg
)
from pyspark.sql.types import DoubleType, StringType

# Colonnes attendues (texte et numériques)
EXPECTED_NUMERIC = [
    "energy-kcal_100g", "energy_100g", "fat_100g", "saturated-fat_100g",
    "sugars_100g", "salt_100g", "proteins_100g", "fiber_100g", "sodium_100g"
]
EXPECTED_TEXT = [
    # identifiants / noms
    "code", "product_name", "product_name_fr", "product_name_en",
    # marque & catég./pays (les tags servent pour GOLD)
    "brands", "categories", "categories_tags", "countries", "countries_tags",
    # scores
    "nutriscore_grade", "nova_group", "ecoscore_grade",
    # temps & langue
    "last_modified_t", "lang"
]

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="Chemin du CSV OFF (fr/en…)")
    p.add_argument("--outdir", required=True, help="Dossier de sortie")
    p.add_argument("--inferSchema", action="store_true", help="Laisser Spark inférer le schéma")
    return p.parse_args()

def start_spark(app="OFF_ETL_SILVER"):
    # En local/WSL, pas besoin de winutils. On peut réduire les partitions pour aller plus vite.
    spark = (
        SparkSession.builder
        .appName(app)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )
    # Baisse le niveau de logs si besoin
    spark.sparkContext.setLogLevel("WARN")
    return spark

def main():
    args = parse_args()
    os.makedirs(args.outdir, exist_ok=True)
    spark = start_spark()

    # --- Lecture CSV/TSV ---
    read_opts = dict(header=True, multiLine=False, escape='"', sep="\t")  # <-- IMPORTANT: TSV OFF
    if args.inferSchema:
        read_opts["inferSchema"] = True
    df = spark.read.csv(args.input, **read_opts)

    # --- Sélection de colonnes utiles : on conserve BRANDS et les TAGS ---
    keep_cols = [c for c in (EXPECTED_TEXT + EXPECTED_NUMERIC) if c in df.columns]
    df = df.select(*[col(c) for c in keep_cols])

    # --- Normalisation texte (minuscule, trim, espaces multiples → 1 espace) ---
    def norm_text(c):
        return trim(regexp_replace(c, r"\s+", " "))
    for c in [x for x in ["brands", "categories", "countries", "nutriscore_grade", "ecoscore_grade", "lang"] if x in df.columns]:
        df = df.withColumn(c, lower(norm_text(col(c))))

    # --- Nom produit préféré : fr > générique > en ; cast explicite en string pour éviter VOID ---
    name_fr = col("product_name_fr") if "product_name_fr" in df.columns else lit(None).cast(StringType())
    name = col("product_name") if "product_name" in df.columns else lit(None).cast(StringType())
    name_en = col("product_name_en") if "product_name_en" in df.columns else lit(None).cast(StringType())

    df = df.withColumn(
        "product_name_pref",
        when((length(name_fr) > 0), name_fr)
        .when((length(name) > 0), name)
        .otherwise(name_en)
        .cast(StringType())  # ← IMPORTANT : évite le type VOID lors d'une écriture CSV
    )

    # --- Nettoyage numérique (virgule → point + cast en float) ---
    for n in [x for x in EXPECTED_NUMERIC if x in df.columns]:
        df = df.withColumn(n, regexp_replace(col(n), ",", "."))
        df = df.withColumn(n, col(n).cast(DoubleType()))

    # --- Harmonisation sodium/sel ---
    has_salt = "salt_100g" in df.columns
    has_sodium = "sodium_100g" in df.columns
    if has_salt and not has_sodium:
        df = df.withColumn("sodium_100g", col("salt_100g") / lit(2.5))
    if has_sodium and not has_salt:
        df = df.withColumn("salt_100g", col("sodium_100g") * lit(2.5))

    # --- Timestamps / dédoublonnage par code le plus récent ---
    if "last_modified_t" in df.columns:
        df = df.withColumn("last_modified_ts", to_timestamp(col("last_modified_t").cast("long")))
    if "code" in df.columns:
        if "last_modified_ts" in df.columns:
            df = df.orderBy(col("last_modified_ts").desc_nulls_last()).dropDuplicates(["code"])
        else:
            df = df.dropDuplicates(["code"])

    # --- Indicateurs complétude (présence nom, marque, nutriments clés) ---
    df = df.withColumn("has_name", when(length(col("product_name_pref")) > 0, lit(1)).otherwise(lit(0)))
    df = df.withColumn(
        "has_brands",
        when(length(col("brands")) > 0, lit(1)).otherwise(lit(0)) if "brands" in df.columns else lit(0)
    )
    core_presence = []
    for c in ["energy-kcal_100g", "sugars_100g", "salt_100g", "proteins_100g", "fat_100g"]:
        if c in df.columns:
            core_presence.append(when(col(c).isNotNull(), lit(1)).otherwise(lit(0)))
    core_sum = None
    for expr in core_presence:
        core_sum = expr if core_sum is None else (core_sum + expr)
    df = df.withColumn("core_nutrients_count", core_sum if core_presence else lit(0))
    df = df.withColumn(
        "completeness_score",
        (col("has_name") + col("has_brands") + col("core_nutrients_count")) / (lit(2 + len(core_presence)))
    )

    # --- Bornes simples sur quelques nutriments (valeurs plausibles) ---
    def within(cname, low, high):
        c = col(cname)
        return when((c >= lit(low)) & (c <= lit(high)), c)
    for cname, low, high in [
        ("sugars_100g", 0.0, 100.0),
        ("salt_100g", 0.0, 25.0),
        ("fat_100g", 0.0, 100.0),
        ("proteins_100g", 0.0, 100.0),
    ]:
        if cname in df.columns:
            df = df.withColumn(cname, within(cname, low, high))

    # --- Métriques de run ---
    total = df.count()
    nonnull_code = df.filter(col("code").isNotNull()).count() if "code" in df.columns else 0
    valid_sugars = df.filter(col("sugars_100g").isNotNull()).count() if "sugars_100g" in df.columns else 0
    avg_compl = df.select(spark_avg("completeness_score")).first()[0] if "completeness_score" in df.columns else None

    metrics = {
        "run_ts": datetime.now(timezone.utc).isoformat(),  # timezone-aware
        "rows_total": total,
        "rows_with_code": nonnull_code,
        "rows_with_sugars_100g": valid_sugars,
        "avg_completeness_score": float(avg_compl) if avg_compl is not None else None,
        "columns": df.columns
    }

    # --- Écriture sorties ---
    outdir = args.outdir
    out_parquet = os.path.join(outdir, "off_clean.parquet")
    out_csv_sample = os.path.join(outdir, "off_clean_sample.csv")
    out_metrics = os.path.join(outdir, "metrics.json")

    # 1) Parquet complet (plusieurs part-* pour aller vite ; remets coalesce(1) si tu veux un seul fichier)
    (df.write.mode("overwrite").parquet(out_parquet))

    # 2) Petit CSV d’échantillon — cast explicite des string pour éviter le type VOID
    sample_cols = [c for c in df.columns]
    df_sample = df.select(*[
        col(c).cast(StringType()) if c == "product_name_pref" else col(c)
        for c in sample_cols
    ])
    (df_sample.limit(200).coalesce(1)
        .write.mode("overwrite").option("header", True).csv(out_csv_sample))

    # 3) Métriques JSON
    with open(out_metrics, "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)

    print(f"Wrote: {out_parquet}\nWrote: {out_csv_sample}\nWrote: {out_metrics}")

if __name__ == "__main__":
    main()