#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GOLD OpenFoodFacts → Dimensions + Fact → MySQL
- Canonisation robuste des clés (unaccent + lower + espaces ASCII + condense)
- Dédoublonnage garanti (row_number) + anti-join fiable sur clé logique
- Écritures JDBC anti-deadlock (1 partition/connexion, autocommit, batchsize)
"""

import argparse
import unicodedata
from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, lower, trim, split, element_at, when, size, to_date, to_json,
    xxhash64, date_format, weekofyear, udf, row_number
)
from pyspark.sql.types import StringType, ArrayType, DoubleType
from pyspark.sql.functions import abs as spark_abs
from pyspark.sql.window import Window


# ---------------------------- CLI ----------------------------
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--in_parquet", required=True)
    p.add_argument("--mysql_url", required=True)
    p.add_argument("--user", required=True)
    p.add_argument("--password", required=True)
    p.add_argument("--save_mode", default="append")  # on force append no-dup côté code
    return p.parse_args()


# ---------------------------- Spark ----------------------------
def start_spark(app="OFF_GOLD"):
    spark = (
        SparkSession.builder
        .appName(app)
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.session.timeZone", "UTC")
        # Driver MySQL auto (1er run → internet requis)
        .config("spark.jars.packages", "com.mysql:mysql-connector-j:8.4.0")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------- Helpers ----------------------------
def has_col(df, name: str) -> bool:
    return name in df.columns

def _canon(s: str):
    """
    Canonise une chaîne pour clé logique:
    - lower/strip
    - unaccent (NFD → retirer diacritiques)
    - transforme toute ponctuation/séparateur en ' ' (ASCII)
    - condense espaces successifs
    """
    if s is None:
        return None
    # lower + trim
    s = s.strip().lower()
    # unaccent
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    # remplace tout ce qui n'est pas alphanumérique par un espace ASCII
    buf = []
    for ch in s:
        if ch.isalnum():
            buf.append(ch)
        else:
            buf.append(" ")
    s = "".join(buf)
    # condense espaces
    s = " ".join(s.split())
    return s

udf_canon = udf(_canon, StringType())

def first_token(c):
    # "nestle,pepsi" -> "nestle"
    return lower(trim(element_at(split(c, ","), 1)))

def normalize_tags(c):
    return split(c, ",")

def extract_code_after_colon(c):
    return when(c.contains(":"), element_at(split(c, ":"), 2)).otherwise(c)

def ensure_cols(df, cols_types: dict):
    for cname, dtype in cols_types.items():
        if cname not in df.columns:
            df = df.withColumn(cname, lit(None).cast(dtype))
    return df

def read_table_keys(spark, jdbc_opts: dict, table: str, key_cols: list):
    """Lit la table MySQL et retourne uniquement les clés (dédupliquées)."""
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
    """
    Append uniquement des lignes absentes (clé logique key_cols).
    - Anti-join FIABLE: une seule expression 'cond_expr' (pas une liste).
    - 1 seule partition (connexion JDBC) → anti-deadlock.
    - Dédoublonnage final par key_cols.
    """
    existing = read_table_keys(spark, jdbc_opts, table, key_cols)
    if existing is None:
        to_insert = df_new
    else:
        cond_expr = reduce(lambda a, b: a & b, [col(f"n.{k}") == col(f"e.{k}") for k in key_cols])
        to_insert = df_new.alias("n").join(existing.alias("e"), cond_expr, "left_anti")

    # filet de sécurité: dédoublonne au cas où
    to_insert = to_insert.dropDuplicates(key_cols)

    # 1 seule partition = 1 seule connexion JDBC → pas de deadlock
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


# ---------------------------- MAIN ----------------------------
def main():
    args = parse_args()
    spark = start_spark()

    # JDBC (anti-deadlock & perfs correctes)
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

    # -------- Lecture Silver --------
    df = spark.read.parquet(args.in_parquet)

    # Fallback si colonnes manquantes
    if not has_col(df, "product_name_pref"):
        df = df.withColumn("product_name_pref", lit(None).cast(StringType()))
    if not has_col(df, "brands"):
        df = df.withColumn("brands", lit(None).cast(StringType()))
    if not has_col(df, "categories_tags") and not has_col(df, "categories"):
        df = df.withColumn("categories", lit(None).cast(StringType()))
    if not has_col(df, "countries_tags") and not has_col(df, "countries"):
        df = df.withColumn("countries", lit(None).cast(StringType()))

    # -------- BRAND (canonique) --------
    if has_col(df, "brands"):
        df = df.withColumn(
            "brand_primary",
            when(col("brands").isNotNull(), first_token(col("brands"))).otherwise(lit(None).cast(StringType()))
        )
    else:
        df = df.withColumn("brand_primary", lit(None).cast(StringType()))
    df = df.withColumn("brand_name", udf_canon(col("brand_primary")))

    # -------- CATEGORIES --------
    if has_col(df, "categories_tags"):
        df = df.withColumn("categories_arr", normalize_tags(col("categories_tags")))
    elif has_col(df, "categories"):
        df = df.withColumn("categories_arr", normalize_tags(col("categories")))
    else:
        df = df.withColumn("categories_arr", lit([]).cast(ArrayType(StringType())))
    df = df.withColumn(
        "primary_category_tag",
        when(size(col("categories_arr")) > 0, element_at(col("categories_arr"), 1)).otherwise(lit(None).cast(StringType()))
    )
    df = df.withColumn("primary_category_code_raw", extract_code_after_colon(col("primary_category_tag")))
    df = df.withColumn("category_code", udf_canon(col("primary_category_code_raw")))

    # -------- COUNTRIES --------
    if has_col(df, "countries_tags"):
        df = df.withColumn("countries_arr", split(col("countries_tags"), ","))
    elif has_col(df, "countries"):
        df = df.withColumn("countries_arr", split(col("countries"), ","))
    else:
        df = df.withColumn("countries_arr", lit([]).cast(ArrayType(StringType())))
    df = df.withColumn(
        "country_raw",
        when(size(col("countries_arr")) > 0, element_at(col("countries_arr"), 1)).otherwise(lit(None).cast(StringType()))
    )
    df = df.withColumn(
        "country_code_raw",
        when(col("country_raw").contains(":"), element_at(split(col("country_raw"), ":"), 2)).otherwise(col("country_raw"))
    )
    df = df.withColumn("country_code", udf_canon(col("country_code_raw")))
    df = df.withColumn("countries_multi_json", to_json(col("countries_arr")))

    # -------- TIME DIM --------
    if has_col(df, "last_modified_ts"):
        df = df.withColumn("event_date", to_date(col("last_modified_ts")))
    else:
        df = df.withColumn("event_date", to_date(lit("2000-01-01")))

    dim_time = (
        df.select("event_date").dropDuplicates()
        .withColumn("time_sk", date_format(col("event_date"), "yyyyMMdd").cast("int"))
        .withColumn("year", col("event_date").substr(1, 4).cast("int"))
        .withColumn("month", col("event_date").substr(6, 2).cast("int"))
        .withColumn("day", col("event_date").substr(9, 2).cast("int"))
        .withColumn("week", weekofyear(col("event_date")).cast("int"))
        .withColumn("iso_week", weekofyear(col("event_date")).cast("int"))
        .withColumn("date", col("event_date"))
        .select("time_sk", "date", "year", "month", "day", "week", "iso_week")
    )

    # -------- DIM BRAND (dédoublonnage window) --------
    w_brand = Window.partitionBy("brand_name").orderBy(lit(1))
    dim_brand = (
        df.select("brand_name")
          .where(col("brand_name").isNotNull() & (col("brand_name") != ""))
          .withColumn("rn", row_number().over(w_brand))
          .filter(col("rn") == 1).drop("rn")
          .withColumn("brand_sk", spark_abs(xxhash64(col("brand_name"))))
          .select("brand_sk", "brand_name")
    )

    # -------- DIM CATEGORY (dédoublonnage window) --------
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
          .select("category_sk", "category_code", "category_name_fr", "level", "parent_category_sk")
    )

    # -------- DIM COUNTRY (dédoublonnage window) --------
    w_ctry = Window.partitionBy("country_code").orderBy(lit(1))
    dim_country = (
        df.select("country_code")
          .where(col("country_code").isNotNull() & (col("country_code") != ""))
          .withColumn("rn", row_number().over(w_ctry))
          .filter(col("rn") == 1).drop("rn")
          .withColumn("country_sk", spark_abs(xxhash64(col("country_code"))))
          .withColumn("country_name_fr", lit(None).cast(StringType()))
          .select("country_sk", "country_code", "country_name_fr")
    )

    # -------- DIM PRODUCT --------
    dim_product_src = (
        df.select(
            col("code").cast(StringType()).alias("code"),
            col("product_name_pref").cast(StringType()).alias("product_name"),
            col("brand_name").alias("brand_name"),                # canonique
            col("category_code").alias("primary_category_code"),  # canonique
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

    # -------- FACT --------
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
    }
    df = ensure_cols(df, required_fact_cols)

    src = df.alias("src")
    prod = dim_product.select("product_sk", "code").alias("p")
    t = dim_time.select("time_sk", col("date").alias("date")).alias("t")

    fact = (
        src.join(prod, col("src.code") == col("p.code"), "left")
           .join(t, col("src.event_date") == col("t.date"), "left")
           .select(
                col("p.product_sk").alias("product_sk"),
                col("t.time_sk").alias("time_sk"),
                col("src.`energy-kcal_100g`").alias("energy_kcal_100g"),
                col("src.energy_100g").alias("energy_100g"),
                col("src.fat_100g").alias("fat_100g"),
                col("src.`saturated-fat_100g`").alias("saturated_fat_100g"),
                col("src.sugars_100g").alias("sugars_100g"),
                col("src.salt_100g").alias("salt_100g"),
                col("src.proteins_100g").alias("proteins_100g"),
                col("src.fiber_100g").alias("fiber_100g"),
                col("src.sodium_100g").alias("sodium_100g"),
                col("src.nutriscore_grade").alias("nutriscore_grade"),
                col("src.nova_group").alias("nova_group"),
                col("src.ecoscore_grade").alias("ecoscore_grade"),
                col("src.completeness_score").alias("completeness_score"),
                lit(None).cast(StringType()).alias("quality_issues_json"),
           )
    )

    # -------- Écritures JDBC : append, no-dup, 1 partition --------
    append_no_dupe(dim_time,      spark, jdbc, "dim_time",
                   ["time_sk"],
                   ["time_sk", "date", "year", "month", "day", "week", "iso_week"])

    # clés logiques: brand_name / category_code / country_code / code / (product_sk,time_sk)
    append_no_dupe(dim_brand,     spark, jdbc, "dim_brand",
                   ["brand_name"],
                   ["brand_sk", "brand_name"])

    append_no_dupe(dim_category,  spark, jdbc, "dim_category",
                   ["category_code"],
                   ["category_sk", "category_code", "category_name_fr", "level", "parent_category_sk"])

    append_no_dupe(dim_country,   spark, jdbc, "dim_country",
                   ["country_code"],
                   ["country_sk", "country_code", "country_name_fr"])

    append_no_dupe(dim_product,   spark, jdbc, "dim_product",
                   ["code"],
                   ["product_sk", "code", "product_name", "brand_sk",
                    "primary_category_sk", "countries_multi",
                    "effective_from", "effective_to", "is_current"])

    append_no_dupe(fact,          spark, jdbc, "fact_nutrition_snapshot",
                   ["product_sk", "time_sk"],
                   ["product_sk", "time_sk", "energy_kcal_100g", "energy_100g",
                    "fat_100g", "saturated_fat_100g", "sugars_100g", "salt_100g",
                    "proteins_100g", "fiber_100g", "sodium_100g",
                    "nutriscore_grade", "nova_group", "ecoscore_grade",
                    "completeness_score", "quality_issues_json"])

    print("✔ GOLD finished – Append sans doublons (single-partition).")


if __name__ == "__main__":
    main()