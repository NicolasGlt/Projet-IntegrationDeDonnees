# Tests – Pipeline OpenFoodFacts (Bronze → Silver → Gold)

Objectif : vérifier que le pipeline est **reproductible**, que le **datamart** est chargé, et que les règles de qualité principales sont observables. :contentReference[oaicite:6]{index=6}

---

## 1) Tests Bronze (données brutes)
- Vérifier que le fichier source est présent :
  - `en.openfoodfacts.org.products.csv.gz`
- Vérifier la taille (ordre de grandeur : plusieurs Go).

---

## 2) Tests Silver (Parquet propre)
### 2.1 Exécution ETL Silver
- Lancer l’ETL Spark (WSL recommandé).
- Attendu : génération de
  - `data_clean/off_clean_v2.parquet`
  - `data_clean/metrics_v2.json`
  - `data_clean/off_clean_v2_sample.csv` (échantillon)

### 2.2 Contrôles Silver (qualité + cohérence)
Dans `metrics_v2.json`, vérifier :
- `rows_total` > 0
- `rows_with_code` proche de `rows_total` (unicité code après dédup).
- `avg_completeness_score` présent.

Contrôles Spark (si besoin) :
- Unicité : `code` dédoublonné (1 ligne par code conservant la plus récente). :contentReference[oaicite:7]{index=7}
- Bornes : nutriments dans des intervalles plausibles (ex: 0 ≤ sugars_100g ≤ 100). :contentReference[oaicite:8]{index=8}

---

## 3) Tests Gold (MySQL)
### 3.1 Vérifier conteneurs
- `docker compose up -d`
- MySQL accessible sur 3306 et phpMyAdmin sur 8080.

### 3.2 Vérifier tables attendues
Dans MySQL :
- `dim_time`, `dim_brand`, `dim_category`, `dim_country`, `dim_product`
- `bridge_product_category`
- `fact_nutrition_snapshot`

### 3.3 Tests SQL rapides (sanity checks)
Exécuter :

```sql
SELECT COUNT(*) FROM dim_product;
SELECT COUNT(*) FROM fact_nutrition_snapshot;
SELECT COUNT(*) FROM bridge_product_category;
