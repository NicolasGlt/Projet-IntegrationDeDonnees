# Cahier de Qualité – OpenFoodFacts

## 1. Objectif
Garantir un niveau de qualité suffisant des données OpenFoodFacts
pour permettre des analyses nutritionnelles fiables, tout en
préservant un volume de données exploitable.

---

## 2. Règles de qualité appliquées (couche Silver)

### 2.1 Unicité
- **Règle** : un code-barres (`code`) représente un produit courant.
- **Problème** : plusieurs versions d’un même produit existent.
- **Traitement** :
  - déduplication par `code`
  - conservation de la ligne la plus récente (`last_modified_t`).

---

### 2.2 Complétude
- Les champs suivants sont évalués :
  - nom du produit
  - marque
  - nutriments principaux (sucres, sel, lipides, protéines)
- Un **score de complétude** (`completeness_score ∈ [0,1]`) est calculé.
- Aucun seuil éliminatoire n’est appliqué afin d’éviter une perte
  excessive de données (>30%).

---

### 2.3 Valeurs aberrantes
Les bornes suivantes sont appliquées :

| Champ | Intervalle autorisé |
|------|---------------------|
| sugars_100g | 0 – 100 |
| salt_100g | 0 – 25 |
| fat_100g | 0 – 100 |
| proteins_100g | 0 – 100 |

- Valeurs hors bornes → `NULL`.

---

### 2.4 Cohérence sel / sodium
- Relation théorique : `salt ≈ sodium × 2.5`
- Si l’un des deux champs est manquant :
  - il est recalculé à partir de l’autre.

---

### 2.5 Normalisation des champs textuels
- Passage en minuscules
- Suppression des valeurs bruitées (`unknown`, `none`, etc.)
- Canonisation ASCII (suppression accents, caractères spéciaux)

---

## 3. Métriques de qualité
Les métriques sont générées automatiquement dans `metrics_v2.json` :
- nombre total de lignes
- nombre de produits uniques
- taux de complétude
- anomalies nutritionnelles détectées

---

## 4. Anomalies observées
- Valeurs nutritionnelles incohérentes
- Produits sans marque ou sans catégorie
- Codes-barres incomplets

Ces lignes sont conservées mais marquées via `quality_issues_json`.

---

## 5. Comparatif Bronze / Silver

| Critère | Bronze | Silver |
|-------|--------|--------|
| Doublons | Présents | Supprimés |
| Valeurs aberrantes | Présentes | Neutralisées |
| Cohérence | Faible | Améliorée |
| Volume | Maximal | Préservé |

---

## 6. Conclusion
Le nettoyage vise un compromis entre qualité et conservation
des données, cohérent avec la nature collaborative et hétérogène
d’OpenFoodFacts.
