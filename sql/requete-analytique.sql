-- 1) Top 10 marques par proportion de produits Nutri‑Score A/B

WITH brand_base AS (
  SELECT
    b.brand_name,
    f.nutriscore_grade
  FROM fact_nutrition_snapshot f
  JOIN dim_product p ON p.product_sk = f.product_sk
  JOIN dim_brand   b ON b.brand_sk   = p.brand_sk
  WHERE b.brand_name IS NOT NULL
    AND b.brand_name NOT IN ('0','unknown','sans marque','n/a','-','_')
    AND f.nutriscore_grade IN ('a','b','c','d','e')  -- garde les valeurs connues
),
brand_stats AS (
  SELECT
    brand_name,
    COUNT(*) AS n_total,
    SUM(CASE WHEN nutriscore_grade IN ('a','b') THEN 1 ELSE 0 END) AS n_ab
  FROM brand_base
  GROUP BY brand_name
)
SELECT
  brand_name,
  n_ab,
  n_total,
  ROUND(n_ab / NULLIF(n_total,0) * 100, 2) AS pct_ab
FROM brand_stats
WHERE n_total >= 20
ORDER BY pct_ab DESC, n_total DESC
LIMIT 10;


-- 2) Distribution Nutri‑Score par catégorie primaire

SELECT
  COALESCE(c.category_code, 'inconnu') AS category,
  f.nutriscore_grade,
  COUNT(*) AS n
FROM fact_nutrition_snapshot f
JOIN dim_product   p ON p.product_sk = f.product_sk
LEFT JOIN dim_category c ON c.category_sk = p.primary_category_sk
WHERE f.nutriscore_grade IN ('a','b','c','d','e')
  AND (c.category_code IS NOT NULL AND c.category_code NOT IN ('0','unknown','n/a','-','_'))
GROUP BY COALESCE(c.category_code, 'inconnu'), f.nutriscore_grade
ORDER BY category, f.nutriscore_grade;


-- 3) Heatmap pays × catégorie : moyenne sugars_100g

SELECT
  COALESCE(c.category_code, 'inconnu') AS category,
  jt.country_code,
  ROUND(AVG(f.sugars_100g), 2) AS avg_sugars_100g,
  COUNT(*) AS n
FROM fact_nutrition_snapshot f
JOIN dim_product p ON p.product_sk = f.product_sk
LEFT JOIN dim_category c ON c.category_sk = p.primary_category_sk
JOIN JSON_TABLE(
      CASE WHEN JSON_VALID(p.countries_multi) THEN p.countries_multi ELSE '[]' END,
      "$[*]" COLUMNS (country_code VARCHAR(255) PATH "$")
     ) AS jt
WHERE f.sugars_100g IS NOT NULL
  AND jt.country_code IS NOT NULL AND jt.country_code <> ''
  AND COALESCE(c.category_code, '') NOT IN ('0','unknown','n/a','-','_')
GROUP BY COALESCE(c.category_code, 'inconnu'), jt.country_code
HAVING COUNT(*) >= 10
ORDER BY category, avg_sugars_100g DESC;


-- 4) Taux de complétude des nutriments par marque

SELECT
  b.brand_name,
  ROUND(AVG(f.completeness_score), 3) AS avg_completeness,
  COUNT(*) AS n
FROM fact_nutrition_snapshot f
JOIN dim_product p ON p.product_sk = f.product_sk
JOIN dim_brand   b ON b.brand_sk   = p.brand_sk
WHERE b.brand_name IS NOT NULL
  AND b.brand_name NOT IN ('0','unknown','sans marque','n/a','-','_')
  AND f.completeness_score IS NOT NULL
GROUP BY b.brand_name
HAVING COUNT(*) >= 20
ORDER BY avg_completeness DESC, n DESC;


-- 5) Liste des anomalies simples (sel > 25 ou sucre > 80)

SELECT
  p.code,
  COALESCE(p.product_name, 'inconnu') AS product_name,
  COALESCE(c.category_code, 'inconnu') AS category,
  f.salt_100g,
  f.sugars_100g
FROM fact_nutrition_snapshot f
JOIN dim_product p ON p.product_sk = f.product_sk
LEFT JOIN dim_category c ON c.category_sk = p.primary_category_sk
WHERE (f.salt_100g   > 25 OR f.sugars_100g > 80)
  AND p.code IS NOT NULL AND p.code <> ''
ORDER BY f.salt_100g DESC, f.sugars_100g DESC;


-- 6) Évolution hebdo de la complétude

SELECT
  t.year,
  t.iso_week,
  ROUND(AVG(f.completeness_score), 3) AS avg_completeness,
  COUNT(*) AS n
FROM fact_nutrition_snapshot f
JOIN dim_time t ON t.time_sk = f.time_sk
WHERE f.completeness_score IS NOT NULL
GROUP BY t.year, t.iso_week
HAVING COUNT(*) >= 20
ORDER BY t.year, t.iso_week;