# Cahier de Qualité – OpenFoodFacts (TRDE703)

## 1. Objectif
Décrire les règles de qualité et les contrôles appliqués sur OpenFoodFacts afin de produire :
- une couche **Silver** propre et standardisée (Parquet),
- une couche **Gold** structurée (DataMart MySQL) exploitable analytiquement.

Le choix principal du projet est une approche **non destructive** : neutraliser/qualifier les anomalies plutôt que supprimer massivement des lignes (afin de conserver la représentativité des données).

---

## 2. Périmètre et contexte
- **Source** : OpenFoodFacts (`en.openfoodfacts.org.products.csv.gz`)
- **Pipeline** : Bronze → Silver → Gold
- **Technos** : PySpark + Parquet (Silver), MySQL (Gold), Docker, WSL

---

## 3. Règles de qualité appliquées (couche Silver)

### 3.1 Unicité / doublons
**Règle :** un code-barres (`code`) correspond à un produit courant.  
**Problème :** OpenFoodFacts contient plusieurs versions d’un même produit.  
**Traitement :**
- déduplication par `code`,
- conservation de l’enregistrement le plus récent via `last_modified_t`.

### 3.2 Normalisation des champs textuels
**Traitements :**
- trim / normalisation espaces,
- mise en minuscules,
- suppression/normalisation des valeurs bruitées (ex. `unknown`, `none`, `0`, etc.),
- sélection d’un nom produit prioritaire :
  1) `product_name_fr`, sinon 2) `product_name`, sinon 3) `product_name_en`.

### 3.3 Contrôles de bornes (valeurs nutritionnelles)
Pour éviter des valeurs impossibles ou incohérentes, des bornes sont appliquées :

| Champ | Intervalle autorisé |
|------|---------------------|
| sugars_100g | 0 – 100 |
| salt_100g | 0 – 25 |
| fat_100g | 0 – 100 |
| proteins_100g | 0 – 100 |

**Traitement :** valeurs hors bornes → `NULL` (neutralisation).

### 3.4 Cohérence sel / sodium
Relation utilisée :
- `salt_100g ≈ sodium_100g × 2.5`

**Traitement :**
- si `salt_100g` existe et `sodium_100g` manque → calcul de `sodium_100g`,
- si `sodium_100g` existe et `salt_100g` manque → calcul de `salt_100g`.

### 3.5 Indicateur de complétude
Un **score de complétude** `completeness_score` (entre 0 et 1) est calculé pour évaluer
la présence des champs essentiels (nom, marque, nutriments principaux).

**Choix assumé :** pas de filtre éliminatoire strict sur ce score, afin d’éviter une perte excessive de volume.

### 3.6 Traçabilité des anomalies
Un champ `quality_issues_json` est construit pour conserver une trace des problèmes détectés
(ex. bornes, champs manquants, incohérences).  
Cela permet d’analyser la qualité **a posteriori** dans le DataMart Gold.

---

## 4. Métriques Silver (metrics_v2.json)

D’après `metrics_v2.json` :

- **Lignes totales** : 4 296 093
- **Lignes avec code** : 4 296 093 (100%)
- **Lignes avec sugars_100g** : 2 930 506 (~68,2%)
- **Score moyen de complétude** : 0,7126 (≈ 0,713)

**Interprétation :**
- Le code-barres est quasi systématiquement présent (excellent pour la déduplication).
- La complétude nutritionnelle est variable (normal pour OpenFoodFacts), mais le score moyen (~0,71)
indique une base globalement exploitable.
- Conserver ces lignes (plutôt que les supprimer) garantit des analyses plus représentatives.

---

## 5. Contrôle qualité dans le Gold (DataMart MySQL)

Dans la couche Gold, la qualité est exploitée via :
- `completeness_score`
- `quality_issues_json`

Les requêtes analytiques incluent une analyse dédiée, par exemple :
- **BONUS** : catégories présentant le plus d’anomalies (`quality_issues_json` non vide),
afin d’identifier des zones “à risque” en qualité de données.

---

## 6. Before / After (Bronze vs Silver)

| Indicateur | Bronze | Silver |
|-----------|--------|--------|
| Doublons | Présents | Déduplication par `code` |
| Valeurs aberrantes | Présentes | Neutralisées (`NULL`) |
| Traçabilité qualité | Faible | `quality_issues_json` + score |
| Volume | maximal | préservé (nettoyage non destructif) |

---

## 7. Conclusion
La stratégie de qualité choisie est adaptée à OpenFoodFacts :
- source collaborative, hétérogène, incomplète,
- objectif analytique nécessitant volume + représentativité.

Le pipeline privilégie :
- la standardisation,
- la traçabilité des anomalies,
- la conservation du volume,
plutôt qu’un filtrage trop agressif qui éliminerait une part importante des données.
