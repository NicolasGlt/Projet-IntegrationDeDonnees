-- --------------- SCHEMA & SETTINGS ---------------
CREATE DATABASE IF NOT EXISTS off_dm
  CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
USE off_dm;

-- --------------- DIMENSIONS ---------------

-- Temps (clé AAAAMMJJ)
CREATE TABLE IF NOT EXISTS dim_time (
  time_sk           INT PRIMARY KEY,      -- ex: 20260127
  `date`            DATE NOT NULL,
  `year`            SMALLINT NOT NULL,
  `month`           TINYINT  NOT NULL,    -- 1..12
  `day`             TINYINT  NOT NULL,    -- 1..31
  `week`            TINYINT  NOT NULL,    -- 0..53 (MySQL WEEK)
  `iso_week`        TINYINT  NOT NULL
) ENGINE=InnoDB;

-- Marque
CREATE TABLE IF NOT EXISTS dim_brand (
  brand_sk          BIGINT PRIMARY KEY,
  brand_name        VARCHAR(500) NOT NULL
) ENGINE=InnoDB;

-- Catégorie (référentiel minimal ; pour un vrai naming FR, charger les taxonomies OFF)
CREATE TABLE IF NOT EXISTS dim_category (
  category_sk       BIGINT PRIMARY KEY,
  category_code     VARCHAR(500) NOT NULL,     -- ex: beverages, biscuits
  category_name_fr  VARCHAR(500) NULL,
  `level`           TINYINT NULL,
  parent_category_sk BIGINT NULL,
  -- UNIQUE KEY uk_category_code (category_code),
  CONSTRAINT fk_category_parent
    FOREIGN KEY (parent_category_sk) REFERENCES dim_category(category_sk)
      ON UPDATE RESTRICT ON DELETE SET NULL
) ENGINE=InnoDB;

-- Pays
CREATE TABLE IF NOT EXISTS dim_country (
  country_sk        BIGINT PRIMARY KEY,
  country_code      VARCHAR(255) NOT NULL,     -- ex: france, spain, united-kingdom
  country_name_fr   VARCHAR(255) NULL,
  UNIQUE KEY uk_country_code (country_code)
) ENGINE=InnoDB;

-- Produit (SCD2-ready)
CREATE TABLE IF NOT EXISTS dim_product (
  product_sk           BIGINT PRIMARY KEY,
  `code`               VARCHAR(255) NOT NULL,   -- code-barres (EAN/GTIN)
  product_name         VARCHAR(2048) NULL,
  brand_sk             BIGINT NULL,
  primary_category_sk  BIGINT NULL,
  countries_multi      JSON NULL,              -- liste pays (JSON array)
  effective_from       DATETIME NOT NULL,
  effective_to         DATETIME NOT NULL,
  is_current           TINYINT(1) NOT NULL DEFAULT 1,
  UNIQUE KEY uk_product_code (code),
  KEY idx_product_brand (brand_sk),
  KEY idx_product_cat   (primary_category_sk),
  CONSTRAINT fk_product_brand
    FOREIGN KEY (brand_sk) REFERENCES dim_brand(brand_sk)
      ON UPDATE RESTRICT ON DELETE SET NULL,
  CONSTRAINT fk_product_category
    FOREIGN KEY (primary_category_sk) REFERENCES dim_category(category_sk)
      ON UPDATE RESTRICT ON DELETE SET NULL,
  CHECK (effective_to > effective_from)
) ENGINE=InnoDB;

-- (Optionnel) N-N si tu veux conserver toutes les catégories OFF
CREATE TABLE IF NOT EXISTS bridge_product_category (
  product_sk   BIGINT NOT NULL,
  category_sk  BIGINT NOT NULL,
  PRIMARY KEY (product_sk, category_sk),
  CONSTRAINT fk_bpc_prod FOREIGN KEY (product_sk) REFERENCES dim_product(product_sk)
    ON UPDATE RESTRICT ON DELETE CASCADE,
  CONSTRAINT fk_bpc_cat  FOREIGN KEY (category_sk)  REFERENCES dim_category(category_sk)
    ON UPDATE RESTRICT ON DELETE CASCADE
) ENGINE=InnoDB;

-- --------------- FACT ---------------

-- Snapshot nutritionnel par produit/date (PK composit pour éviter doublons)
CREATE TABLE IF NOT EXISTS fact_nutrition_snapshot (
  product_sk            BIGINT NOT NULL,
  time_sk               INT    NOT NULL,
  energy_kcal_100g      DOUBLE NULL,
  energy_100g           DOUBLE NULL,
  fat_100g              DOUBLE NULL,
  saturated_fat_100g    DOUBLE NULL,
  sugars_100g           DOUBLE NULL,
  salt_100g             DOUBLE NULL,
  proteins_100g         DOUBLE NULL,
  fiber_100g            DOUBLE NULL,
  sodium_100g           DOUBLE NULL,
  nutriscore_grade      VARCHAR(32) NULL,   -- a,b,c,d,e
  nova_group            VARCHAR(32) NULL,   -- 1..4
  ecoscore_grade        VARCHAR(32) NULL,   -- a..e
  completeness_score    DOUBLE NULL,       -- 0..1
  quality_issues_json   JSON   NULL,
  PRIMARY KEY (product_sk, time_sk),
  KEY idx_grade (nutriscore_grade),
  CONSTRAINT fk_fact_product FOREIGN KEY (product_sk) REFERENCES dim_product(product_sk),
  CONSTRAINT fk_fact_time    FOREIGN KEY (time_sk)    REFERENCES dim_time(time_sk)
) ENGINE=InnoDB;