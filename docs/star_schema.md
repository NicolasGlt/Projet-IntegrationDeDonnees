# Schéma en étoile – DataMart OpenFoodFacts (off_dm)

Ce DataMart est construit autour d’une table de faits nutritionnelle et de dimensions.
La relation produit–catégorie étant potentiellement N–N, elle est modélisée via une table de pont (bridge).

> Une illustration de la structure est disponible dans `docs/structure_bdd.png`.

---

## 1. Table de faits

### fact_nutrition_snapshot
**Grain (niveau de détail)** : 1 ligne = **1 produit** à **une date (time_sk)**, avec ses valeurs nutritionnelles et scores.

**Clés**
- `product_sk` (FK → dim_product)
- `time_sk` (FK → dim_time)

**Mesures / attributs**
- nutrition : `energy_kcal_100g`, `energy_100g`, `fat_100g`, `saturated_fat_100g`, `sugars_100g`,
  `salt_100g`, `proteins_100g`, `fiber_100g`, `sodium_100g`
- scores : `nutriscore_grade`, `nova_group`, `ecoscore_grade`
- qualité : `completeness_score`
- `quality_issues_json` (présent dans le schéma, mais peut être NULL selon le chargement)

---

## 2. Dimensions

### dim_product
Décrit le produit (version courante).
- `product_sk` (PK)
- `code`, `product_name`
- `brand_sk` (FK → dim_brand)
- `primary_category_sk` (FK → dim_category)
- `countries_multi` (JSON)
- `effective_from`, `effective_to`, `is_current`

### dim_brand
- `brand_sk` (PK)
- `brand_name`

### dim_category
Hiérarchie de catégories.
- `category_sk` (PK)
- `category_code`, `category_name_fr`
- `level`
- `parent_category_sk` (auto-FK vers dim_category)

### dim_time
Dimension temporelle.
- `time_sk` (PK)
- `date`, `year`, `month`, `day`, `week`, `iso_week`

### dim_country
Référentiel pays (présent dans la structure).
- `country_sk` (PK)
- `country_code`, `country_name_fr`

---

## 3. Table de pont (bridge)

### bridge_product_category
Modélise la relation **N–N** entre produits et catégories.
- `product_sk` (FK → dim_product)
- `category_sk` (FK → dim_category)

Remarque :
- `dim_product.primary_category_sk` conserve une **catégorie principale**,
- `bridge_product_category` permet d’associer le produit à plusieurs catégories si nécessaire.

---

## 4. Relations principales (résumé)
- `fact_nutrition_snapshot.product_sk` → `dim_product.product_sk`
- `fact_nutrition_snapshot.time_sk` → `dim_time.time_sk`
- `dim_product.brand_sk` → `dim_brand.brand_sk`
- `dim_product.primary_category_sk` → `dim_category.category_sk`
- `bridge_product_category.product_sk` → `dim_product.product_sk`
- `bridge_product_category.category_sk` → `dim_category.category_sk`
- `dim_category.parent_category_sk` → `dim_category.category_sk`
