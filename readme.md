**Pipeline d’Intégration de Données – OpenFoodFacts**

**Méthode :** Bronze → Silver → Gold

**Il couvre :**
- Téléchargement des données (Bronze)
- Nettoyage et transformation Silver via PySpark
- Mise en place MySQL (Docker)
- Chargement final dans la couche Gold


**1. Téléchargement des données (Bronze)**
Télécharger le fichier brut OpenFoodFacts : **en.openfoodfacts.org.products.csv**
Ce fichier constitue la couche Bronze : données brutes, non nettoyées, potentiellement incohérentes.


**2. Nettoyage & Préparation (Silver)**
La couche Silver consiste à :
✔ nettoyer les valeurs manquantes
✔ supprimer les lignes invalides
✔ formater dates/nombres/catégories
✔ sélectionner les colonnes pertinentes
✔ produire un fichier Parquet propre
Ce traitement utilise PySpark, dans un environnement Linux/WSL.


**2.1 — Pourquoi Spark sous Linux plutôt que Windows ?**

Nous utilisons Spark sous Linux/WSL car Spark est conçu pour fonctionner nativement sur Unix : 
L’installation est plus simple, plus stable et compatible à 100%, alors que sous Windows l’environnement Spark génère souvent des erreurs, des problèmes de chemins et des dépendances manquantes.

Conclusion :
Spark a été conçu pour Unix/Linux. WSL permet un environnement fiable, stable et compatible.


**2.2 — Lancer WSL**

Utilisation d'une distribution Ubuntu.


**2.3 — Installer Python + créer l’environnement virtuel**

sudo apt update
sudo apt install python3 python3-venv

python3 -m venv venv
source venv/bin/activate


**2.4 — Installer PySpark**

pip install pyspark


**2.5 — Installer Java (JDK pour Spark)**

sudo apt install default-jdk


**2.6 — Définir JAVA_HOME pour Spark**

export JAVA_HOME=/usr/lib/jvm/default-java


**2.7 — Lancer le script ETL Silver**

python "/mnt/c/Users/ngrea/Documents/github/Projet Integration de donnees/etl_openfoodfacts.py" --input "/mnt/c/Users/ngrea/Documents/github/en.openfoodfacts.org.products.csv" --outdir "/mnt/c/Users/ngrea/Documents/github/Projet Integration de donnees/out"

Voir etl-openfoodfacts.py


**2.8 — Vérification du résultat**

Le dossier out/ doit contenir :

off_clean.parquet
_SUCCESS (optionnel, généré par Spark)

La couche Silver est terminée.


**3. Création de la base MySQL (Docker)**

Le DataMart Gold sera stocké dans une base MySQL accessible via Docker + phpMyAdmin.
Ce service démarre :
- MySQL (port 3306)
- phpMyAdmin (port 8080)

Voir docker-compose.yml


**3.1 — Importer la structure SQL**

Accéder à phpMyAdmin :
http://localhost:8080/

Importation du fichier SQL : bdd.sql


**4. Insertion des données dans MySQL (Gold)**

La couche Gold correspond aux données finales, prêtes pour l'analyse.


**4.1 — Lancer le script Gold**

Depuis l’environnement virtuel (sur WSL):
python "/mnt/c/Users/ngrea/Documents/github/Projet Integration de donnees/gold_openfoodfacts.py" --in_parquet "/mnt/c/Users/ngrea/Documents/github/Projet Integration de donnees/out/off_clean.parquet" --mysql_url "jdbc:mysql://127.0.0.1:3306/off_dm" --user projet --password projet --save_mode overwrite

Ce script :
- lit la couche Silver au format Parquet
- applique les transformations Gold
- insère les données dans MySQL

Voir fichier : gold_openfoodfacts.py


**5. Schéma visuel du pipeline**
         +-----------------------+
         |  CSV BRUT (BRONZE)    |
         +-----------+-----------+
                     |
                     v
      +--------------+---------------+
      |  PySpark — Nettoyage (SILVER)|
      +--------------+---------------+
                     |
                     v
         +-----------+-----------+
         |     Parquet Silver    |
         +-----------+-----------+
                     |
                     v
  +------------------+------------------+
  | Script Gold → Insertion MySQL (GOLD)|
  +------------------+------------------+
                     |
                     v
         +-----------+-----------+
         |     DataMart final    |
         +-----------------------+



**Auteur**
Auteur : Nicolas GREAULT, Gabin EPANI, Louiza TABET
Projet : Intégration de données – OpenFoodFacts
Technos : PySpark • Python • MySQL • Docker • WSL