# Schéma en étoile – DataMart OpenFoodFacts

Table de faits :
- fact_nutrition_snapshot
- grain : 1 produit, 1 date

Dimensions :
- dim_product
- dim_brand
- dim_category
- dim_country
- dim_time

Relation N–N :
- bridge_product_category

Ce modèle permet :
- analyses nutritionnelles par produit
- comparaisons par marque et catégorie
- analyses temporelles
