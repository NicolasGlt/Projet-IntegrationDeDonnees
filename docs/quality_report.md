# Cahier de Qualité – OpenFoodFacts (TRDE703)

## 1. Objectif
Décrire les règles de qualité et les contrôles appliqués sur OpenFoodFacts afin de produire :
- une couche **Silver** propre et standardisée (Parquet),
- une couche **Gold** structurée (DataMart MySQL) exploitable analytiquement.

Le projet adopte une approche **non destructive** : neutraliser/qualifier les anomalies plutôt que supprimer massivement des lignes, afin de conserver un volume représentatif.

---

## 2. Source et pipeline
- **Source** : OpenFoodFacts (`en.openfoodfacts.org.products.csv.gz`)
- **Pipeline** : Bronze → Silver → Gold
- **Technos** : PySpark + Parquet (Silver), MySQL (Gold), Docker, WSL

---

## 3. Règles de qualité appliquées (couche Silver)

### 3.1 Unicité / doublons
- **Règle** : un code-barres (`code`) correspond à un produit courant.
- **Traitement** : déduplication par `code` en conservant la version la plus récente (`last_modified_t`).

### 3.2 Normalisation des champs textuels
- mise en minuscules, nettoyage des valeurs bruitées (`unknown`, `none`, `0`, etc.)
- normalisation ASCII
- sélection du nom produit prioritaire :
  1) `product_name_fr`, sinon 2) `product_name`, sinon 3) `product_name_en`.

### 3.3 Contrôles de bornes (valeurs nutritionnelles)
| Champ | Intervalle autorisé |
|------|---------------------|
| sugars_100g | 0 – 100 |
| salt_100g | 0 – 25 |
| fat_100g | 0 – 100 |
| proteins_100g | 0 – 100 |

**Traitement** : valeurs hors bornes → `NULL` (neutralisation).

### 3.4 Cohérence sel / sodium
- relation : `salt_100g ≈ sodium_100g × 2.5`
- recalcul si l’un des deux champs est manquant.

### 3.5 Mesure de complétude
- Calcul d’un `completeness_score` ∈ [0;1] à partir des champs essentiels (nom, marque, nutriments principaux).
- **Choix assumé** : pas de suppression agressive basée sur ce score, pour éviter de descendre sous un seuil de volumétrie exploitable.

### 3.6 Traçabilité des anomalies
- Un champ `quality_issues_json` existe dans le modèle Gold pour tracer des anomalies.
- **Remarque** : selon le chargement, ce champ peut être `NULL` dans la base MySQL.
- Les analyses qualité principales reposent donc sur :
  - les métriques globales `metrics_v2.json`
  - et le champ `completeness_score`.

---

## 4. Métriques Silver (metrics_v2.json)

D’après `metrics_v2.json` :

- **Lignes totales** : 4 296 093
- **Lignes avec code** : 4 296 093 (100%)
- **Lignes avec sugars_100g** : 2 930 506 (~68,2%)
- **Score moyen de complétude** : 0,7126 (≈ 0,713)

**Interprétation**
- Le code-barres est systématiquement présent, facilitant la déduplication.
- La complétude nutritionnelle est variable (attendue sur OpenFoodFacts), mais le score moyen (~0,71) indique une base globalement exploitable.
- Conserver les lignes partiellement renseignées garantit une meilleure représentativité pour l’analyse.

---

## 5. Contrôle qualité dans le Gold (DataMart MySQL)
Le DataMart permet d’exploiter la qualité via :
- `completeness_score` dans `fact_nutrition_snapshot`
- des analyses SQL dédiées (ex. catégories les moins complètes)

Une requête BONUS qualité est ajoutée dans `requete-analytique.sql` :
- **Top catégories les moins complètes** (moyenne de `completeness_score`), afin d’identifier des zones “à risque” en qualité de données.

---

## 6. Comparatif Bronze / Silver

| Indicateur | Bronze | Silver |
|-----------|--------|--------|
| Doublons | présents | déduplication par `code` |
| Valeurs aberrantes | présentes | neutralisées (`NULL`) |
| Standardisation texte | faible | améliorée |
| Volume | maximal | préservé (nettoyage non destructif) |

---

## 7. Conclusion
La stratégie de qualité est adaptée à une source collaborative et hétérogène :
- nettoyage ciblé (bornes, cohérences, standardisation),
- conservation du volume,
- mesure explicite de la qualité (`completeness_score` + métriques globales).

Le résultat est un DataMart exploitable et cohérent pour l’analyse nutritionnelle.
