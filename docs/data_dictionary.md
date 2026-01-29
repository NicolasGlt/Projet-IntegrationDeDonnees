# Data Dictionary – DataMart OpenFoodFacts

## dim_product
- product_sk (PK)
- code
- product_name
- brand_sk
- primary_category_sk
- countries_multi (JSON)
- effective_from
- effective_to
- is_current

## dim_brand
- brand_sk (PK)
- brand_name

## dim_category
- category_sk (PK)
- category_code
- category_name_fr
- level
- parent_category_sk

## dim_country
- country_sk (PK)
- country_code
- country_name_fr

## dim_time
- time_sk (PK)
- date
- year
- month
- day
- week
- iso_week

## fact_nutrition_snapshot
- product_sk (FK)
- time_sk (FK)
- energy_kcal_100g
- energy_100g
- fat_100g
- saturated_fat_100g
- sugars_100g
- salt_100g
- proteins_100g
- fiber_100g
- sodium_100g
- nutriscore_grade
- nova_group
- ecoscore_grade
- completeness_score
- quality_issues_json
