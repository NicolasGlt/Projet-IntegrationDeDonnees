# Data Dictionary – DataMart OpenFoodFacts

## dim_product
- **product_sk (PK)** : clé technique du produit
- **code** : code-barres OpenFoodFacts
- **product_name** : nom du produit (priorité FR → générique → EN)
- **brand_sk (FK)** : référence vers la marque
- **primary_category_sk (FK)** : catégorie principale du produit
- **countries_multi (JSON)** : tableau JSON des pays associés au produit  
  (exploité via `JSON_TABLE` dans les requêtes analytiques)
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
- **category_name_fr** : libellé de la catégorie
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
- **quality_issues_json** : champ JSON listant les anomalies détectées  
  *(exploité dans la requête BONUS d’analyse de la qualité)*
- **metrics_json** : métriques techniques associées à la ligne  
  (flags de contrôle, informations de transformation)

