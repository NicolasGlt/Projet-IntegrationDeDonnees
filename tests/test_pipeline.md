# Tests de validation du pipeline

## Test Silver
- Exécuter etl_openfoodfacts.py
- Vérifier la génération du parquet Silver
- Vérifier le fichier metrics_v2.json

## Test Gold
- Exécuter gold_openfoodfacts.py
- Vérifier l’insertion des données dans MySQL
- Vérifier l’absence de doublons

## Test SQL
- Exécuter requete-analytique.sql
- Vérifier que les requêtes retournent des résultats cohérents
