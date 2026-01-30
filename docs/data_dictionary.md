# Data Dictionary – DataMart OpenFoodFacts (Gold)


## dim_product
**Rôle :** dimension produit (clé métier = `code`).

- `product_sk` (PK, BIGINT) : clé surrogate produit (hash).
- `code` (VARCHAR) : code-barres (clé naturelle).
- `product_name` (VARCHAR) : nom préféré du produit (priorité FR > fallback). :contentReference[oaicite:3]{index=3}
- `brand_sk` (FK -> dim_brand.brand_sk) : marque principale.
- `primary_category_sk` (FK -> dim_category.category_sk) : catégorie primaire (normalisée).
- `countries_multi` (JSON) : liste JSON de pays (ex: `["france","spain"]`).
- `effective_from` (DATETIME) : début de validité (SCD/traçabilité).
- `effective_to` (DATETIME) : fin de validité.
- `is_current` (TINYINT) : 1 = ligne courante.

---

## dim_brand
**Rôle :** dimension marque.

- `brand_sk` (PK, BIGINT)
- `brand_name` (VARCHAR) : nom normalisé (lower/trim).

---

## dim_category
**Rôle :** dimension catégorie.

- `category_sk` (PK, BIGINT)
- `category_code` (VARCHAR) : code catégorie (normalisé, ex: `beverages`).
- `category_name_fr` (VARCHAR, NULL possible) : libellé FR si disponible (sinon NULL).
- `level` (TINYINT/INT, NULL possible) : niveau hiérarchique si enrichi (sinon NULL).
- `parent_category_sk` (BIGINT, NULL possible) : parent si hiérarchie enrichie (sinon NULL).

---

## dim_country
**Rôle :** dimension pays.

- `country_sk` (PK, BIGINT)
- `country_code` (VARCHAR) : code pays normalisé (ex: `france`).
- `country_name_fr` (VARCHAR, NULL possible) : libellé FR si enrichi (sinon NULL).

---

## dim_time
**Rôle :** dimension temps (basée sur `last_modified_t` transformé en date).

- `time_sk` (PK, INT) : clé YYYYMMDD.
- `date` (DATE)
- `year` (SMALLINT)
- `month` (TINYINT)
- `day` (TINYINT)
- `week` (TINYINT) : semaine de l’année.
- `iso_week` (TINYINT) : semaine ISO (dans notre implémentation, identique à `week`).

---

## bridge_product_category
**Rôle :** table de pont N–N entre produits et catégories (toutes catégories, pas seulement la primaire).

- `product_sk` (FK -> dim_product.product_sk)
- `category_sk` (FK -> dim_category.category_sk)

Clé (logique) : (`product_sk`, `category_sk`).

---

## fact_nutrition_snapshot
**Rôle :** table de faits “nutrition/qualité” à une date donnée.

Clés :
- `product_sk` (FK)
- `time_sk` (FK)

Mesures (DOUBLE) :
- `energy_kcal_100g`
- `energy_100g`
- `fat_100g`
- `saturated_fat_100g`
- `sugars_100g`
- `salt_100g`
- `proteins_100g`
- `fiber_100g`
- `sodium_100g`

Attributs (VARCHAR) :
- `nutriscore_grade` (a–e)
- `nova_group`
- `ecoscore_grade`

Qualité :
- `completeness_score` (DOUBLE) : score [0..1] de complétude calculé côté ETL (présence nom, marque, nutriments clés). :contentReference[oaicite:4]{index=4}
- `quality_issues_json` (JSON) : liste JSON d’anomalies détectées.

 **Limite connue (projet groupe)** : dans notre chargement MySQL actuel, `quality_issues_json` se retrouve **NULL** en base (bug identifié, non bloquant).  
Les requêtes “anomalies” sont donc calculées directement via seuils (ex: sel > 25, sucres > 80) plutôt que via ce champ. :contentReference[oaicite:5]{index=5}
