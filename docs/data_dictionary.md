# Data Dictionary – DataMart OpenFoodFacts (off_dm)

## dim_product
- **product_sk (PK)** : clé technique du produit
- **code** : code-barres OpenFoodFacts
- **product_name** : nom du produit (priorité FR → générique → EN)
- **brand_sk (FK)** : référence vers la marque (`dim_brand`)
- **primary_category_sk (FK)** : catégorie principale (`dim_category`)
- **countries_multi (JSON)** : tableau JSON des pays associés au produit  
  *(exploité via `JSON_TABLE` dans certaines requêtes analytiques si MySQL 8+)*  
- **effective_from** : date de début de validité
- **effective_to** : date de fin de validité
- **is_current** : indicateur de version courante du produit

---

## dim_brand
- **brand_sk (PK)** : clé technique marque
- **brand_name** : nom normalisé de la marque

---

## dim_category
- **category_sk (PK)** : clé technique catégorie
- **category_code** : code catégorie normalisé
- **category_name_fr** : libellé (FR)
- **level** : niveau hiérarchique
- **parent_category_sk** : catégorie parente (hiérarchie)

---

## dim_country
- **country_sk (PK)** : clé technique pays
- **country_code** : code pays
- **country_name_fr** : nom du pays

---

## dim_time
- **time_sk (PK)** : clé technique temps
- **date** : date calendrier
- **year** : année
- **month** : mois
- **day** : jour
- **week** : semaine
- **iso_week** : semaine ISO

---

## bridge_product_category
- **product_sk (FK)** : référence produit (`dim_product`)
- **category_sk (FK)** : référence catégorie (`dim_category`)  
Table de pont pour modéliser la relation N–N produit–catégorie.

---

## fact_nutrition_snapshot
- **product_sk (FK)** : référence produit
- **time_sk (FK)** : référence temporelle
- **energy_kcal_100g** : énergie (kcal/100g)
- **energy_100g** : énergie (kJ/100g)
- **fat_100g** : lipides (g/100g)
- **saturated_fat_100g** : acides gras saturés (g/100g)
- **sugars_100g** : sucres (g/100g)
- **salt_100g** : sel (g/100g)
- **proteins_100g** : protéines (g/100g)
- **fiber_100g** : fibres (g/100g)
- **sodium_100g** : sodium (g/100g)
- **nutriscore_grade** : Nutri-Score (A → E)
- **nova_group** : groupe NOVA
- **ecoscore_grade** : Eco-Score
- **completeness_score** : score de complétude calculé en couche Silver
- **quality_issues_json** : champ JSON prévu pour tracer des anomalies ; selon le chargement il peut être NULL.  
  Les analyses qualité principales s’appuient sur `completeness_score` et `metrics_v2.json`.
- **metrics_json** : métriques techniques associées à la ligne (si présentes selon chargement)
