# Cahier de Qualité – OpenFoodFacts

## 1. Objectif
Ce cahier de qualité décrit les règles de contrôle, de nettoyage et de validation
appliquées aux données OpenFoodFacts afin de garantir leur fiabilité avant
chargement dans le DataMart (couche Gold).

---

## 2. Règles de qualité appliquées

### 2.1 Unicité
- **Règle** : un code-barres (`code`) correspond à un seul produit courant.
- **Problème** : OpenFoodFacts contient plusieurs versions d’un même produit.
- **Traitement** : dédoublonnage par `code` en conservant la ligne la plus récente
  (`last_modified_t`).

---

### 2.2 Complétude
- **Champs essentiels** :
  - Nom du produit
  - Marque
  - Valeurs nutritionnelles principales
- **Score calculé** : `completeness_score ∈ [0;1]`
- **Objectif** : mesurer la qualité globale de chaque ligne sans supprimer trop
  de données (éviter une perte massive >30%).

---

### 2.3 Valeurs aberrantes (bornes)
| Champ | Borne autorisée |
|------|-----------------|
| sugars_100g | 0 à 100 |
| salt_100g | 0 à 25 |
| fat_100g | 0 à 100 |
| proteins_100g | 0 à 100 |

- **Traitement** : valeurs hors bornes mises à `NULL`.

---

### 2.4 Cohérence sel / sodium
- **Relation** : `salt_100g ≈ sodium_100g × 2.5`
- **Traitement** :
  - sodium manquant → calcul depuis sel
  - sel manquant → calcul depuis sodium

---

## 3. Coverage et métriques (Silver)

Les métriques sont générées automatiquement via le fichier `metrics_v2.json`.

### 3.1 Volumétrie
- Nombre total de lignes lues (Bronze) : voir `rows_total`
- Nombre de lignes après nettoyage (Silver)
- Nombre de doublons supprimés

### 3.2 Qualité globale
- % produits avec code valide
- % produits avec nutriments principaux
- Moyenne du `completeness_score`

---

## 4. Anomalies détectées
Exemples :
- Produits avec `sugars_100g > 100`
- Produits avec `salt_100g > 25`
- Produits sans code-barres

Ces lignes sont soit corrigées, soit conservées avec des valeurs nulles.

---

## 5. Comparatif Before / After

| Indicateur | Bronze | Silver |
|-----------|--------|--------|
| Nombre de lignes | Très élevé | Réduit |
| Doublons code | Présents | Supprimés |
| Valeurs aberrantes | Présentes | Corrigées |
| Cohérence nutritionnelle | Faible | Améliorée |

---

## 6. Conclusion
Le nettoyage vise un compromis entre qualité et volume de données,
afin de conserver une base exploitable pour l’analyse sans suppression excessive.
