# Résultats analytiques – DataMart OpenFoodFacts

Cette section présente l’analyse des données issues du DataMart Gold OpenFoodFacts, alimenté à partir du pipeline Bronze → Silver → Gold.  
Les requêtes SQL exécutées permettent d’évaluer à la fois la qualité nutritionnelle des produits, la complétude des données et leur exploitabilité analytique.

---

## 1. Top 10 marques par proportion de produits Nutri-Score A/B

Cette requête identifie les marques dont la proportion de produits ayant un Nutri-Score favorable (A ou B) est la plus élevée, en imposant un seuil minimal de représentativité.

Les résultats montrent que plusieurs marques atteignent une proportion de 100 % de produits A/B, mais avec un nombre de produits relativement limité. Cela correspond généralement à des marques positionnées sur des gammes spécifiques ou des segments orientés vers des produits perçus comme plus sains.

Cette analyse met en évidence l’importance de croiser la qualité nutritionnelle avec le volume afin d’éviter des interprétations biaisées basées uniquement sur les pourcentages.

---

## 2. Distribution du Nutri-Score par catégorie principale

L’analyse de la distribution des Nutri-Scores par catégorie montre une forte hétérogénéité selon les types de produits. Certaines catégories concentrent majoritairement des Nutri-Scores favorables (A et B), tandis que d’autres présentent une proportion plus élevée de scores défavorables (D et E).

On observe également la présence de catégories peu normalisées ou bruitées, ce qui reflète la nature hétérogène de la source OpenFoodFacts. Malgré cette limite, la requête met clairement en évidence la dépendance du Nutri-Score au type de produit.

---

## 3. Analyse croisée catégorie × pays – teneur moyenne en sucres

Cette requête croise les catégories de produits avec les pays de commercialisation afin d’analyser la teneur moyenne en sucres (`sugars_100g`).

Les résultats mettent en évidence des différences significatives entre pays pour une même catégorie, suggérant des variations de formulation liées aux habitudes alimentaires locales, aux réglementations ou aux stratégies industrielles.

Cette analyse illustre l’intérêt du modèle multidimensionnel du DataMart, combinant les dimensions produit, catégorie et pays.

---

## 4. Taux de complétude des données par marque

Cette requête mesure la complétude des données nutritionnelles par marque à l’aide du `completeness_score`.

Les résultats montrent que certaines marques présentent un score de complétude moyen très élevé, parfois égal à 1, indiquant des fiches produits systématiquement bien renseignées. À l’inverse, d’autres marques affichent une complétude plus faible, révélant une qualité de saisie plus hétérogène.

Cette analyse permet de relier directement la qualité des données à des acteurs économiques précis.

---

## 5. Détection d’anomalies nutritionnelles simples

Cette requête identifie les produits présentant des valeurs nutritionnelles extrêmes, notamment une teneur en sel supérieure à 25 g/100 g ou une teneur en sucres supérieure à 80 g/100 g.

Les produits détectés correspondent soit à des cas très spécifiques, soit à des valeurs potentiellement aberrantes issues de la saisie des données.  
Dans le cadre de ce projet, ces anomalies ne sont pas supprimées mais signalées, conformément à une approche non destructive de la qualité des données.

---

## 6. Évolution temporelle de la complétude des données

L’analyse de l’évolution hebdomadaire du score moyen de complétude montre que la qualité des données varie dans le temps. Certaines semaines présentent une complétude plus faible, souvent associée à un nombre réduit de produits modifiés, tandis que d’autres périodes affichent une qualité plus stable.

Cette analyse met en évidence l’intérêt de la dimension temporelle intégrée au DataMart.

---

## 7. Conclusion sur les analyses

Les résultats analytiques confirment la pertinence du pipeline Bronze → Silver → Gold mis en place.  
Le DataMart permet :

- des analyses nutritionnelles multi-dimensionnelles,
- l’évaluation explicite de la qualité des données,
- l’identification d’anomalies sans suppression excessive des données.

Malgré certaines limites liées à la nature collaborative de la source OpenFoodFacts (catégories bruitées, complétude variable), le modèle proposé fournit une base exploitable et cohérente pour l’analyse nutritionnelle à grande échelle.
