#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GOLD v2 OpenFoodFacts → Dimensions + Bridge N–N + Fact → MySQL
- Canonisation robuste des clés (unaccent + lower + espaces ASCII + condense) + filtre bruit
- Remplissage de la bridge N–N (toutes les catégories) à partir de `categories_codes`
- Append sans doublons via anti-join fiable, 1 seule partition JDBC pour éviter les deadlocks
Usage:
  python gold_openfoodfacts_v2.py --in_parquet ./out/off_clean_v2.parquet \
    --mysql_url jdbc:mysql://localhost:3306/off_dm --user root --password **** [--save_mode append]
"""
import argparse
import unicodedata
from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    expr, col, lit, lower, trim, split, element_at, when, size, to_date, to_json,
    xxhash64, date_format, weekofyear, udf, row_number, explode, abs as spark_abs
)
from pyspark.sql.types import StringType, ArrayType, DoubleType
from pyspark.sql.window import Window

# --- CLI ---
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--in_parquet", required=True)
    p.add_argument("--mysql_url", required=True)
    p.add_argument("--user", required=True)
    p.add_argument("--password", required=True)
    p.add_argument("--save_mode", default="append")
    return p.parse_args()

# --- Spark ---
def start_spark(app="OFF_GOLD_V2"):
    spark = (
        SparkSession.builder
            .appName(app)
            .config("spark.sql.shuffle.partitions", "16")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.jars.packages", "com.mysql:mysql-connector-j:8.4.0")
            .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

# --- Helpers ---
NOISE = {"0","none","unknown","sans marque","sans_marque","n/a","na","-","_","?"}

def has_col(df, name: str) -> bool:
    return name in df.columns

# Canonisation

def _canon(s: str):
    if s is None:
        return None
    s = s.strip().lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    buf = []
    for ch in s:
        if ch.isalnum():
            buf.append(ch)
        else:
            buf.append(" ")
    s = "".join(buf)
    s = " ".join(s.split())
    return s

udf_canon = udf(_canon, StringType())

# Filtre bruit

def drop_noise(s: str):
    if s is None:
        return None
    s = s.strip().lower()
    if s in NOISE or s.isdigit() or len(s) <= 1:
        return None
    return s

udf_drop_noise = udf(drop_noise, StringType())

# Ensure columns present

def ensure_cols(df, cols_types: dict):
    for cname, dtype in cols_types.items():
        if cname not in df.columns:
            df = df.withColumn(cname, lit(None).cast(dtype))
    return df

# JDBC helpers

def read_table_keys(spark, jdbc_opts: dict, table: str, key_cols: list):
    try:
        df = (
            spark.read.format("jdbc")
                .options(**jdbc_opts)
                .option("dbtable", table)
                .load()
                .select(*key_cols)
                .dropDuplicates()
        )
        return df
    except Exception as e:
        print(f"[WARN] Impossible de lire les clés existantes pour {table}: {e}")
        return None


def append_no_dupe(df_new, spark, jdbc_opts: dict, table: str, key_cols: list, column_order: list):
    existing = read_table_keys(spark, jdbc_opts, table, key_cols)
    if existing is None:
        to_insert = df_new
    else:
        cond_expr = reduce(lambda a, b: a & b, [col(f"n.{k}") == col(f"e.{k}") for k in key_cols])
        to_insert = df_new.alias("n").join(existing.alias("e"), cond_expr, "left_anti")
    to_insert = to_insert.dropDuplicates(key_cols)
    to_insert = to_insert.select(*[col(c) for c in column_order]).coalesce(1)
    (
        to_insert.write
            .format("jdbc")
            .options(**jdbc_opts)
            .option("dbtable", table)
            .option("numPartitions", "1")
            .mode("append")
            .save()
    )
    inserted = to_insert.count()
    print(f"→ {table}: +{inserted} lignes insérées (no-dup, single-partition).")

# --- MAIN ---

def main():
    args = parse_args()
    spark = start_spark()

    jdbc = {
        "url": args.mysql_url,
        "user": args.user,
        "password": args.password,
        "driver": "com.mysql.cj.jdbc.Driver",
        "rewriteBatchedStatements": "true",
        "batchsize": "500",
        "allowPublicKeyRetrieval": "true",
        "useSSL": "false",
        "numPartitions": "1",
        "autocommit": "true",
        "isolationLevel": "READ_COMMITTED",
    }

    # Lecture Silver v2
    df = spark.read.parquet(args.in_parquet)

    # Fallback colonnes
    df = ensure_cols(df, {
        "product_name_pref": StringType(),
        "brand_primary": StringType(),
        "categories_codes": ArrayType(StringType()),
        "primary_category_code": StringType(),
        "countries_codes": ArrayType(StringType()),
        "country_primary_code": StringType(),
        "last_modified_ts": StringType(),  # peut être timestamp
        "event_date": StringType(),        # sera cast en date
        "quality_issues_json": StringType(),
    })

    # event_date cast
    df = df.withColumn("event_date", to_date(col("event_date")))

    # BRAND canonique
    df = df.withColumn("brand_primary_clean", udf_drop_noise(col("brand_primary")))
    df = df.withColumn("brand_name", udf_canon(col("brand_primary_clean")))

    # CATEGORY canonique primaire
    df = df.withColumn("category_code", udf_canon(col("primary_category_code")))

    # COUNTRIES multi (JSON)
    df = df.withColumn("countries_multi_json", to_json(col("countries_codes")))

    # --- DIM TIME ---
    dim_time = (
        df.select("event_date").dropDuplicates()
          .withColumn("time_sk", date_format(col("event_date"), "yyyyMMdd").cast("int"))
          .withColumn("year", col("event_date").substr(1,4).cast("int"))
          .withColumn("month", col("event_date").substr(6,2).cast("int"))
          .withColumn("day", col("event_date").substr(9,2).cast("int"))
          .withColumn("week", weekofyear(col("event_date")).cast("int"))
          .withColumn("iso_week", weekofyear(col("event_date")).cast("int"))
          .withColumn("date", col("event_date"))
          .select("time_sk","date","year","month","day","week","iso_week")
    )

    # --- DIM BRAND ---
    w_brand = Window.partitionBy("brand_name").orderBy(lit(1))
    dim_brand = (
        df.select("brand_name")
          .where(col("brand_name").isNotNull() & (col("brand_name") != ""))
          .withColumn("rn", row_number().over(w_brand))
          .filter(col("rn") == 1).drop("rn")
          .withColumn("brand_sk", spark_abs(xxhash64(col("brand_name"))))
          .select("brand_sk","brand_name")
    )

    # --- DIM CATEGORY ---
    w_cat = Window.partitionBy("category_code").orderBy(lit(1))
    dim_category = (
        df.select("category_code")
          .where(col("category_code").isNotNull() & (col("category_code") != ""))
          .withColumn("rn", row_number().over(w_cat))
          .filter(col("rn") == 1).drop("rn")
          .withColumn("category_sk", spark_abs(xxhash64(col("category_code"))))
          .withColumn("category_name_fr", lit(None).cast(StringType()))
          .withColumn("level", lit(None).cast("int"))
          .withColumn("parent_category_sk", lit(None).cast("bigint"))
          .select("category_sk","category_code","category_name_fr","level","parent_category_sk")
    )

    # --- DIM COUNTRY ---
    # utiliser country_primary_code canonisé
    df = df.withColumn("country_code", udf_canon(col("country_primary_code")))
    w_ctry = Window.partitionBy("country_code").orderBy(lit(1))
    dim_country = (
        df.select("country_code")
          .where(col("country_code").isNotNull() & (col("country_code") != ""))
          .withColumn("rn", row_number().over(w_ctry))
          .filter(col("rn") == 1).drop("rn")
          .withColumn("country_sk", spark_abs(xxhash64(col("country_code"))))
          .withColumn("country_name_fr", lit(None).cast(StringType()))
          .select("country_sk","country_code","country_name_fr")
    )

    # --- DIM PRODUCT ---
    dim_product_src = (
        df.select(
            col("code").cast(StringType()).alias("code"),
            col("product_name_pref").cast(StringType()).alias("product_name"),
            col("brand_name").alias("brand_name"),
            col("category_code").alias("primary_category_code"),
            col("countries_multi_json").alias("countries_multi"),
            col("event_date")
        )
        .where(col("code").isNotNull() & (col("code") != ""))
    )

    dim_product = (
        dim_product_src.alias("s")
            .join(dim_brand.alias("b"), col("b.brand_name") == col("s.brand_name"), "left")
            .join(dim_category.alias("c"), col("c.category_code") == col("s.primary_category_code"), "left")
            .withColumn("product_sk", spark_abs(xxhash64(col("s.code"))))
            .withColumn("effective_from", col("s.event_date").cast("timestamp"))
            .withColumn("effective_to", lit("9999-12-31").cast("timestamp"))
            .withColumn("is_current", lit(1))
            .select(
                col("product_sk"),
                col("s.code").alias("code"),
                col("s.product_name").alias("product_name"),
                col("b.brand_sk").alias("brand_sk"),
                col("c.category_sk").alias("primary_category_sk"),
                col("s.countries_multi").alias("countries_multi"),
                col("effective_from"),
                col("effective_to"),
                col("is_current")
            )
    )

    # --- BRIDGE N–N product × categories ---
    # Exploser toutes les catégories, mapper sur dim_category, puis sur product_sk
    cats_exploded = (
        df.select("code","categories_codes")
          .withColumn("cat_code", explode(col("categories_codes")))
          .where(col("cat_code").isNotNull() & (col("cat_code") != ""))
          .withColumn("cat_code_norm", udf_canon(col("cat_code")))
          .dropDuplicates(["code","cat_code_norm"])
    )

    bridge_src = (
        cats_exploded.alias("x")
            .join(dim_category.select("category_sk","category_code").alias("d"), col("x.cat_code_norm") == col("d.category_code"), "left")
            .join(dim_product.select("product_sk","code").alias("p"), col("p.code") == col("x.code"), "left")
            .select(col("p.product_sk").alias("product_sk"), col("d.category_sk").alias("category_sk"))
            .where(col("product_sk").isNotNull() & col("category_sk").isNotNull())
            .dropDuplicates(["product_sk","category_sk"])
    )

    # --- FACT ---
    required_fact_cols = {
        "energy-kcal_100g": DoubleType(),
        "energy_100g": DoubleType(),
        "fat_100g": DoubleType(),
        "saturated-fat_100g": DoubleType(),
        "sugars_100g": DoubleType(),
        "salt_100g": DoubleType(),
        "proteins_100g": DoubleType(),
        "fiber_100g": DoubleType(),
        "sodium_100g": DoubleType(),
        "nutriscore_grade": StringType(),
        "nova_group": StringType(),
        "ecoscore_grade": StringType(),
        "completeness_score": DoubleType(),
        "quality_issues_json": StringType(),
    }
    df = ensure_cols(df, required_fact_cols)

    src = df.alias("src")
    prod = dim_product.select("product_sk","code").alias("p")
    t = dim_time.select("time_sk", col("date").alias("date")).alias("t")

    fact = (
        
        src.join(prod, col("src.code") == col("p.code"), "left")
            .join(t,   col("src.event_date") == col("t.date"), "left")
            .select(
                col("p.product_sk").alias("product_sk"),
                col("t.time_sk").alias("time_sk"),

                # 🔧 Colonnes avec tiret : utiliser expr() et backticks
                expr("`src`.`energy-kcal_100g`").alias("energy_kcal_100g"),
                col("src.energy_100g").alias("energy_100g"),
                col("src.fat_100g").alias("fat_100g"),
                expr("`src`.`saturated-fat_100g`").alias("saturated_fat_100g"),

                col("src.sugars_100g").alias("sugars_100g"),
                col("src.salt_100g").alias("salt_100g"),
                col("src.proteins_100g").alias("proteins_100g"),
                col("src.fiber_100g").alias("fiber_100g"),
                col("src.sodium_100g").alias("sodium_100g"),
                col("src.nutriscore_grade").alias("nutriscore_grade"),
                col("src.nova_group").alias("nova_group"),
                col("src.ecoscore_grade").alias("ecoscore_grade"),
                col("src.completeness_score").alias("completeness_score"),
                col("src.quality_issues_json").alias("quality_issues_json"),
            )
    )

    # --- Writes ---
    append_no_dupe(dim_time, spark, jdbc, "dim_time",
                   ["time_sk"],
                   ["time_sk","date","year","month","day","week","iso_week"])

    append_no_dupe(dim_brand, spark, jdbc, "dim_brand",
                   ["brand_name"],
                   ["brand_sk","brand_name"])

    append_no_dupe(dim_category, spark, jdbc, "dim_category",
                   ["category_code"],
                   ["category_sk","category_code","category_name_fr","level","parent_category_sk"])

    append_no_dupe(dim_country, spark, jdbc, "dim_country",
                   ["country_code"],
                   ["country_sk","country_code","country_name_fr"])

    append_no_dupe(dim_product, spark, jdbc, "dim_product",
                   ["code"],
                   ["product_sk","code","product_name","brand_sk","primary_category_sk","countries_multi","effective_from","effective_to","is_current"])

    append_no_dupe(bridge_src, spark, jdbc, "bridge_product_category",
                   ["product_sk","category_sk"],
                   ["product_sk","category_sk"])

    append_no_dupe(fact, spark, jdbc, "fact_nutrition_snapshot",
                   ["product_sk","time_sk"],
                   ["product_sk","time_sk","energy_kcal_100g","energy_100g","fat_100g","saturated_fat_100g","sugars_100g","salt_100g","proteins_100g","fiber_100g","sodium_100g","nutriscore_grade","nova_group","ecoscore_grade","completeness_score","quality_issues_json"])

    print("✔ GOLD v2 finished – Append sans doublons (single-partition) + bridge N–N remplie.")

if __name__ == "__main__":
    main()
