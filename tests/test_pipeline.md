# Tests de validation du pipeline OpenFoodFacts

## Test Silver (ETL PySpark)
- Exécuter le script `etl_openfoodfacts.py`
- Vérifier la génération du parquet Silver
- Vérifier la présence du fichier `metrics_v2.json`
- Vérifier la cohérence du score moyen de complétude

---

## Test Gold (Chargement DataMart MySQL)
- Exécuter le script `gold_openfoodfacts.py`
- Vérifier l’insertion des données dans les tables dimensionnelles
- Vérifier l’absence de doublons dans la table de faits
- Vérifier la cohérence des clés étrangères

---

## Test SQL (Analyses)
- Exécuter les requêtes analytiques standards (`requete-analytique.sql`)
- Vérifier la cohérence des résultats (volumétrie, agrégations)
- **Exécuter la requête BONUS qualité (catégories avec anomalies)** et vérifier
  l’identification des catégories présentant le plus de problèmes de qualité
