#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETL v2.1 OpenFoodFacts → Spark (Silver clean++) → Parquet (+ sample CSV + metrics)

- Compatible versions Spark/PySpark où array_filter/transform ne sont pas exposés dans pyspark.sql.functions
- Aucun UDF Python, uniquement des fonctions SQL via expr()
- Nettoyage marques / catégories / pays (suppression bruit, extraction codes après ":")
- Déduplication par code (version la plus récente), harmonisation sodium/sel, clamp numériques
- quality_issues_json
- Parquet + metrics.json + CSV d’échantillon (types complexes sérialisés en JSON)

Usage :
  python etl_openfoodfacts_v2.py --input /path/to/en.openfoodfacts.org.products.csv \
                                 --outdir ./data_clean --inferSchema --sample_rows 200
"""
import argparse
import json
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    array, array_remove, col, trim, lower, regexp_replace, when, lit, length, to_timestamp, to_date,
    avg as spark_avg, split, element_at, to_json, expr
)
from pyspark.sql.types import DoubleType, StringType, ArrayType, MapType, StructType

# --- Constants ---
NOISE_REGEX = r"^(0|none|unknown|sans marque|sans_marque|n/a|na|-|_|\\?)$"

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="Chemin du TSV OFF (en.openfoodfacts.org.products.csv)")
    p.add_argument("--outdir", required=True, help="Dossier de sortie")
    p.add_argument("--inferSchema", action="store_true", help="Laisser Spark inférer le schéma")
    p.add_argument("--sample_rows", type=int, default=200, help="Nombre de lignes dans le CSV d'échantillon")
    return p.parse_args()

def start_spark(app="OFF_ETL_V2_1_SILVER"):
    spark = (
        SparkSession.builder
        .appName(app)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

def clamp_numeric(df, cname, low, high):
    if cname in df.columns:
        df = df.withColumn(cname, regexp_replace(col(cname), ",", ".").cast(DoubleType()))
        df = df.withColumn(cname, when(col(cname) < 0, None).otherwise(col(cname)))
        df = df.withColumn(
            cname,
            when(col(cname).isNull(), None)
            .when(col(cname) < lit(low), None)
            .when(col(cname) > lit(high), None)
            .otherwise(col(cname))
        )
    return df

def main():
    args = parse_args()
    os.makedirs(args.outdir, exist_ok=True)
    spark = start_spark()

    # --- Lecture OFF TSV ---
    read_opts = dict(header=True, multiLine=False, escape='"', sep="\t")
    if args.inferSchema:
        read_opts["inferSchema"] = True
    df = spark.read.csv(args.input, **read_opts)

    # --- Colonnes utiles ---
    EXPECTED_NUMERIC = [
        "energy-kcal_100g", "energy_100g", "fat_100g", "saturated-fat_100g",
        "sugars_100g", "salt_100g", "proteins_100g", "fiber_100g", "sodium_100g"
    ]
    EXPECTED_TEXT = [
        "code", "product_name", "product_name_fr", "product_name_en",
        "brands", "categories", "categories_tags", "countries", "countries_tags",
        "nutriscore_grade", "nova_group", "ecoscore_grade",
        "last_modified_t", "lang"
    ]
    keep_cols = [c for c in (EXPECTED_TEXT + EXPECTED_NUMERIC) if c in df.columns]
    df = df.select(*[col(c) for c in keep_cols])

    # --- Normalisation texte générique ---
    def norm_text(c):
        return trim(regexp_replace(c, r"\s+", " "))
    for c in [x for x in ["brands","categories","countries","nutriscore_grade","ecoscore_grade","lang"] if x in df.columns]:
        df = df.withColumn(c, lower(norm_text(col(c))))

    # --- Nom préféré ---
    name_fr = col("product_name_fr") if "product_name_fr" in df.columns else lit(None).cast(StringType())
    name    = col("product_name")    if "product_name"    in df.columns else lit(None).cast(StringType())
    name_en = col("product_name_en") if "product_name_en" in df.columns else lit(None).cast(StringType())
    df = df.withColumn(
        "product_name_pref",
        when(length(name_fr) > 0, name_fr).when(length(name) > 0, name).otherwise(name_en).cast(StringType())
    )

    # --- Harmonisation sodium/sel ---
    if "salt_100g" in df.columns and "sodium_100g" not in df.columns:
        df = df.withColumn("sodium_100g", col("salt_100g") / lit(2.5))
    if "sodium_100g" in df.columns and "salt_100g" not in df.columns:
        df = df.withColumn("salt_100g", col("sodium_100g") * lit(2.5))

    # --- Timestamps & déduplication ---
    if "last_modified_t" in df.columns:
        df = df.withColumn("last_modified_ts", to_timestamp(col("last_modified_t").cast("long")))
    if "code" in df.columns:
        if "last_modified_ts" in df.columns:
            df = df.orderBy(col("last_modified_ts").desc_nulls_last()).dropDuplicates(["code"])
        else:
            df = df.dropDuplicates(["code"])

    # =========================
    #   BRANDS (via expr())
    # =========================
    if "brands" in df.columns:
        # Remplacement ; -> , puis split
        df = df.withColumn("brands_src", regexp_replace(col("brands"), ";", ","))
        df = df.withColumn("brands_arr", split(col("brands_src"), ","))  # array<string>
        # trim/lower/condense spaces
        df = df.withColumn(
            "brands_arr",
            expr("transform(brands_arr, t -> lower(trim(regexp_replace(t, '\\\\s+', ' '))))")
        )
        # filter: longueur>1, pas numérique pur, pas dans NOISE_REGEX
        df = df.withColumn(
            "brands_clean_arr",
            expr(f"filter(brands_arr, t -> length(t) > 1 AND NOT t RLIKE '^[0-9]+$' AND NOT t RLIKE '{NOISE_REGEX}')")
        )
        df = df.withColumn(
            "brand_primary",
            when(expr("size(brands_clean_arr) > 0"), element_at(col("brands_clean_arr"), 1)).otherwise(lit(None).cast(StringType()))
        ).drop("brands_src")
    else:
        df = df.withColumn("brand_primary", lit(None).cast(StringType()))

    # =========================
    #   CATEGORIES (via expr())
    # =========================
    cat_src = when(col("categories_tags").isNotNull(), col("categories_tags")).otherwise(col("categories"))
    cat_src = regexp_replace(cat_src, ";", ",")
    df = df.withColumn("categories_arr", split(cat_src, ","))  # array<string>
    df = df.withColumn("categories_arr", expr("transform(categories_arr, t -> lower(trim(regexp_replace(t, '\\\\s+', ' '))))"))
    df = df.withColumn(
        "categories_arr",
        expr(f"filter(categories_arr, t -> length(t) > 1 AND NOT t RLIKE '^[0-9]+$' AND NOT t RLIKE '{NOISE_REGEX}')")
    )
    # code après ':'
    df = df.withColumn(
        "categories_codes",
        expr("transform(categories_arr, t -> CASE WHEN instr(t, ':') > 0 THEN split(t, ':')[1] ELSE t END)")
    )
    df = df.withColumn(
        "categories_codes",
        expr(f"filter(categories_codes, t -> t IS NOT NULL AND length(t) > 1 AND NOT t RLIKE '^[0-9]+$' AND NOT t RLIKE '{NOISE_REGEX}')")
    )
    df = df.withColumn(
        "primary_category_code",
        when(expr("size(categories_codes) > 0"), element_at(col("categories_codes"), 1)).otherwise(lit(None).cast(StringType()))
    )

    # =========================
    #   COUNTRIES (via expr())
    # =========================
    ctry_src = when(col("countries_tags").isNotNull(), col("countries_tags")).otherwise(col("countries"))
    ctry_src = regexp_replace(ctry_src, ";", ",")
    df = df.withColumn("countries_arr", split(ctry_src, ","))  # array<string>
    df = df.withColumn("countries_arr", expr("transform(countries_arr, t -> lower(trim(regexp_replace(t, '\\\\s+', ' '))))"))
    df = df.withColumn(
        "countries_arr",
        expr(f"filter(countries_arr, t -> length(t) > 1 AND NOT t RLIKE '^[0-9]+$' AND NOT t RLIKE '{NOISE_REGEX}')")
    )
    df = df.withColumn(
        "countries_codes",
        expr("transform(countries_arr, t -> CASE WHEN instr(t, ':') > 0 THEN split(t, ':')[1] ELSE t END)")
    )
    df = df.withColumn(
        "countries_codes",
        expr(f"filter(countries_codes, t -> t IS NOT NULL AND length(t) > 1 AND NOT t RLIKE '^[0-9]+$' AND NOT t RLIKE '{NOISE_REGEX}')")
    )
    df = df.withColumn(
        "country_primary_code",
        when(expr("size(countries_codes) > 0"), element_at(col("countries_codes"), 1)).otherwise(lit(None).cast(StringType()))
    )

    # --- Clamp numériques ---
    for cname, low, high in [
        ("sugars_100g", 0.0, 100.0),
        ("salt_100g",   0.0, 25.0),
        ("fat_100g",    0.0, 100.0),
        ("proteins_100g", 0.0, 100.0),
        ("energy_100g", 0.0, 1300.0),
        ("energy-kcal_100g", 0.0, 800.0),
    ]:
        df = clamp_numeric(df, cname, low, high)

    # --- event_date ---
    df = df.withColumn(
        "event_date",
        when(col("last_modified_ts").isNotNull(), to_date(col("last_modified_ts"))).otherwise(to_date(lit("2000-01-01")))
    )

    # --- Complétude ---
    df = df.withColumn("has_name", when(length(col("product_name_pref")) > 0, lit(1)).otherwise(lit(0)))
    df = df.withColumn("has_brands", when(length(col("brand_primary")) > 0, lit(1)).otherwise(lit(0)))

    core_presence_cols = [c for c in ["energy-kcal_100g","sugars_100g","salt_100g","proteins_100g","fat_100g"] if c in df.columns]
    core_sum = None
    for c in core_presence_cols:
        add = when(col(c).isNotNull(), lit(1)).otherwise(lit(0))
        core_sum = add if core_sum is None else (core_sum + add)
    df = df.withColumn("core_nutrients_count", core_sum if core_presence_cols else lit(0))
    df = df.withColumn(
        "completeness_score",
        (col("has_name") + col("has_brands") + col("core_nutrients_count")) / (lit(2 + len(core_presence_cols)))
    )

    # --- quality_issues_json ---
    issue_salt   = when(col("salt_100g")   > 25, lit("salt_gt_25"))
    issue_sugars = when(col("sugars_100g") > 80, lit("sugars_gt_80"))
    issue_brand_missing = when((col("brand_primary").isNull()) | (length(col("brand_primary")) == 0), lit("brand_missing"))
    issue_cat_missing   = when((col("primary_category_code").isNull()) | (length(col("primary_category_code")) == 0), lit("cat_missing"))
    df = df.withColumn("quality_issues_json", to_json(array_remove(array(issue_salt, issue_sugars, issue_brand_missing, issue_cat_missing), lit(None))))

    # --- Métriques (avant CSV) ---
    total = df.count()
    nonnull_code = df.filter(col("code").isNotNull()).count() if "code" in df.columns else 0
    valid_sugars = df.filter(col("sugars_100g").isNotNull()).count() if "sugars_100g" in df.columns else 0
    avg_compl = df.select(spark_avg("completeness_score")).first()[0] if "completeness_score" in df.columns else None
    metrics = {
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "rows_total": total,
        "rows_with_code": nonnull_code,
        "rows_with_sugars_100g": valid_sugars,
        "avg_completeness_score": float(avg_compl) if avg_compl is not None else None,
        "columns": df.columns,
    }

    # --- Écritures ---
    out_parquet   = os.path.join(args.outdir, "off_clean_v2.parquet")
    out_csv_smpl  = os.path.join(args.outdir, "off_clean_v2_sample.csv")
    out_metrics   = os.path.join(args.outdir, "metrics_v2.json")

    # 1) Parquet
    df.write.mode("overwrite").parquet(out_parquet)

    # 2) Metrics
    with open(out_metrics, "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)

    # 3) CSV sample : sérialiser les types complexes en JSON
    def _col_for_csv(field):
        dt = field.dataType
        if isinstance(dt, (ArrayType, MapType, StructType)):
            return to_json(col(field.name)).alias(field.name)
        return col(field.name).cast(StringType()) if field.name == "product_name_pref" else col(field.name)

    df_sample = df.select(*[_col_for_csv(f) for f in df.schema.fields])
    (df_sample.limit(args.sample_rows)
        .coalesce(1)
        .write.mode("overwrite").option("header", True).csv(out_csv_smpl))

    print(f"Wrote: {out_parquet}\nWrote: {out_csv_smpl}\nWrote: {out_metrics}")

if __name__ == "__main__":
    main()