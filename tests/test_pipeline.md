# Tests de validation du pipeline OpenFoodFacts

## Test Silver (ETL PySpark)
- Exécuter le script `etl_openfoodfacts.py`
- Vérifier la génération du parquet Silver
- Vérifier la présence du fichier `metrics_v2.json`
- Vérifier que les métriques sont cohérentes (volumétrie, score moyen de complétude)

---

## Test Gold (Chargement DataMart MySQL)
- Exécuter le script `gold_openfoodfacts.py`
- Vérifier l’insertion des données dans :
  - `dim_product`, `dim_brand`, `dim_category`, `dim_time`, `dim_country`
  - `bridge_product_category`
  - `fact_nutrition_snapshot`
- Vérifier l’absence de doublons dans la table de faits (même couple `product_sk`, `time_sk`)
- Vérifier la cohérence des clés étrangères (jointures OK entre fact et dimensions)

---

## Test SQL (Analyses)
- Exécuter les requêtes analytiques dans `requete-analytique.sql`
- Vérifier que les agrégations retournent des résultats cohérents
- Exécuter la requête BONUS qualité basée sur `completeness_score` (top catégories les moins complètes)
