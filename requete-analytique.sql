-- Top 10 marques par proportion de produits Nutri‑Score A/B
WITH brand_stats AS (
  SELECT
    b.brand_name,
    COUNT(*) AS n_total,
    SUM(CASE WHEN f.nutriscore_grade IN ('a','b') THEN 1 ELSE 0 END) AS n_ab
  FROM fact_nutrition_snapshot f
  JOIN dim_product p ON p.product_sk = f.product_sk
  JOIN dim_brand   b ON b.brand_sk   = p.brand_sk
  GROUP BY b.brand_name
)
SELECT brand_name,
       n_ab, n_total,
       ROUND(n_ab / NULLIF(n_total,0) * 100, 2) AS pct_ab
FROM brand_stats
WHERE n_total >= 20        -- filtre “volume” pour éviter le bruit
ORDER BY pct_ab DESC, n_total DESC
LIMIT 10;

-- Distribution Nutri‑Score par catégorie (primary_category)
SELECT
  c.category_code AS category,
  f.nutriscore_grade,
  COUNT(*) AS n
FROM fact_nutrition_snapshot f
JOIN dim_product   p ON p.product_sk = f.product_sk
LEFT JOIN dim_category c ON c.category_sk = p.primary_category_sk
GROUP BY c.category_code, f.nutriscore_grade
ORDER BY c.category_code, f.nutriscore_grade;

-- Heatmap pays × catégorie : moyenne sugars_100g
SELECT
  c.category_code AS category,
  jt.country_code,
  ROUND(AVG(f.sugars_100g), 2) AS avg_sugars_100g,
  COUNT(*) AS n
FROM fact_nutrition_snapshot f
JOIN dim_product p ON p.product_sk = f.product_sk
LEFT JOIN dim_category c ON c.category_sk = p.primary_category_sk
JOIN JSON_TABLE(
      p.countries_multi,
      "$[*]" COLUMNS (country_code VARCHAR(255) PATH "$")
     ) AS jt
GROUP BY c.category_code, jt.country_code
HAVING COUNT(*) >= 10
ORDER BY c.category_code, avg_sugars_100g DESC;


-- Taux de complétude des nutriments par marque
SELECT
  b.brand_name,
  ROUND(AVG(f.completeness_score), 3) AS avg_completeness,
  COUNT(*) AS n
FROM fact_nutrition_snapshot f
JOIN dim_product p ON p.product_sk = f.product_sk
JOIN dim_brand   b ON b.brand_sk   = p.brand_sk
GROUP BY b.brand_name
HAVING COUNT(*) >= 20
ORDER BY avg_completeness DESC, n DESC;

-- Liste des anomalies simples (ex. salt_100g > 25 ou sugars_100g > 80)
SELECT
  p.code,
  p.product_name,
  c.category_code,
  f.salt_100g,
  f.sugars_100g,
  f.quality_issues_json
FROM fact_nutrition_snapshot f
JOIN dim_product p ON p.product_sk = f.product_sk
LEFT JOIN dim_category c ON c.category_sk = p.primary_category_sk
WHERE (f.salt_100g   > 25)
   OR (f.sugars_100g > 80)
ORDER BY f.salt_100g DESC, f.sugars_100g DESC;

-- Évolution hebdo de la complétude (via dim_time)
SELECT
  t.year,
  t.iso_week,
  ROUND(AVG(f.completeness_score), 3) AS avg_completeness,
  COUNT(*) AS n
FROM fact_nutrition_snapshot f
JOIN dim_time t ON t.time_sk = f.time_sk
GROUP BY t.year, t.iso_week
ORDER BY t.year, t.iso_week;
