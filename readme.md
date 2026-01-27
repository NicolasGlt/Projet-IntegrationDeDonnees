<B>Pipeline d’Intégration de Données – OpenFoodFacts</B>

<B>Méthode :</B> Bronze → Silver → Gold

<B>Il couvre :</B>
- Téléchargement des données (Bronze)
- Nettoyage et transformation Silver via PySpark
- Mise en place MySQL (Docker)
- Chargement final dans la couche Gold

<br>
<B>1. Téléchargement des données (Bronze)</B>
Télécharger le fichier brut OpenFoodFacts : **en.openfoodfacts.org.products.csv**
Ce fichier constitue la couche Bronze : données brutes, non nettoyées, potentiellement incohérentes.

<br>
<B>2. Nettoyage & Préparation (Silver)</B>
La couche Silver consiste à :
- nettoyer les valeurs manquantes
- supprimer les lignes invalides
- formater dates/nombres/catégories
- sélectionner les colonnes pertinentes
- produire un fichier Parquet propre
Ce traitement utilise PySpark, dans un environnement Linux/WSL.

<br>
<B>2.1 — Pourquoi Spark sous Linux plutôt que Windows ?</B>

Nous utilisons Spark sous Linux/WSL car Spark est conçu pour fonctionner nativement sur Unix : 
L’installation est plus simple, plus stable et compatible à 100%, alors que sous Windows l’environnement Spark génère souvent des erreurs, des problèmes de chemins et des dépendances manquantes.

Conclusion :
Spark a été conçu pour Unix/Linux. WSL permet un environnement fiable, stable et compatible.

<br>
<B>2.2 — Lancer WSL</B>

Utilisation d'une distribution Ubuntu.

<br>
<B>2.3 — Installer Python + créer l’environnement virtuel</B>

sudo apt update
sudo apt install python3 python3-venv

python3 -m venv venv
source venv/bin/activate

<br>
<B>2.4 — Installer PySpark</B>

pip install pyspark

<br>
<B>2.5 — Installer Java (JDK pour Spark)</B>

sudo apt install default-jdk

<br>
<B>2.6 — Définir JAVA_HOME pour Spark</B>

export JAVA_HOME=/usr/lib/jvm/default-java

<br>
<B>2.7 — Lancer le script ETL Silver</B>

Depuis l’environnement virtuel (sur WSL):
python "/mnt/c/Users/ngrea/Documents/github/Projet Integration de donnees/etl_openfoodfacts.py" --input "/mnt/c/Users/ngrea/Documents/github/en.openfoodfacts.org.products.csv" --outdir "/mnt/c/Users/ngrea/Documents/github/Projet Integration de donnees/out"

Voir etl-openfoodfacts.py

<br>
<B>2.8 — Vérification du résultat</B>

Le dossier out/ doit contenir :

off_clean.parquet
_SUCCESS (optionnel, généré par Spark)

La couche Silver est terminée.

<br>
<B>3. Création de la base MySQL (Docker)</B>

Le DataMart Gold sera stocké dans une base MySQL accessible via Docker + phpMyAdmin.
Ce service démarre :
- MySQL (port 3306)
- phpMyAdmin (port 8080)

Voir docker-compose.yml

<br>
<B>3.1 — Importer la structure SQL</B>

Accéder à phpMyAdmin :
http://localhost:8080/

Importation du fichier SQL : bdd.sql

<br>
<B>4. Insertion des données dans MySQL (Gold)</B>

La couche Gold correspond aux données finales, prêtes pour l'analyse.

<br>
<B>4.1 — Lancer le script Gold</B>

Depuis l’environnement virtuel (sur WSL):
python "/mnt/c/Users/ngrea/Documents/github/Projet Integration de donnees/gold_openfoodfacts.py" --in_parquet "/mnt/c/Users/ngrea/Documents/github/Projet Integration de donnees/out/off_clean.parquet" --mysql_url "jdbc:mysql://127.0.0.1:3306/off_dm" --user projet --password projet --save_mode overwrite

Ce script :
- lit la couche Silver au format Parquet
- applique les transformations Gold
- insère les données dans MySQL

Voir fichier : gold_openfoodfacts.py

<br>
<B>5. Schéma visuel du pipeline</B>

 - 1 - CSV BRUT (BRONZE) PySpark
 - 2 - Nettoyage (SILVER)
 - 3 - Parquet Silver
 - 4 - Script Gold → Insertion MySQL (GOLD)
 - 5 - DataMart final

<br>
<B>Auteur</B>
Auteur : Nicolas GREAULT, Gabin EPANI, Louiza TABET
Projet : Intégration de données – OpenFoodFacts
Technos : PySpark • Python • MySQL • Docker • WSL
