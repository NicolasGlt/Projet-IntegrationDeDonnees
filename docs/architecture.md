# 🏗️ Architecture & Implémentation du Pipeline d'Intégration

Ce document décrit l'architecture logicielle et la logique de réconciliation des données du projet. L'objectif est de transformer des sources disparates en un **Golden Record** (enregistrement de référence).

---

## 1. Schéma de l'Architecture Système

Nous utilisons une architecture **Mediator-Wrapper**. Ce motif permet d'isoler la complexité de chaque source (XML, CSV, JSON) derrière un contrat d'interface unique.

```mermaid
graph TD
    subgraph Sources
        S1[IMDb - XML]
        S2[TMDb - JSON]
        S3[Autre - CSV]
    end

    subgraph "Couche d'Abstraction (Wrappers)"
        W1[Wrapper IMDb]
        W2[Wrapper TMDb]
        W3[Wrapper CSV]
    end

    subgraph "Moteur de Médiation"
        M[Integration Engine]
        L[Linkage & Matching]
        U[Upsert Logic]
    end

    S1 --> W1
    S2 --> W2
    S3 --> W3
    W1 --> M
    W2 --> M
    W3 --> M
    M --> L
    L --> U
    U --> DB[(Database / Gold Standard)]```

## 2. Logique de Réconciliation (Record Linkage)

Pour lier deux films sans identifiant commun, nous appliquons une mesure de similarité textuelle sur le titre, pondérée par l'année de sortie.Métrique de SimilaritéNous utilisons la distance de Levenshtein pour gérer les fautes de frappe ou les variations de titres :$$dist(s_1, s_2) = \min(insertions, deletions, substitutions)$$Algorithme d'Upsert (Flow)Extrait de codeflowchart TD
    A[Nouvelle donnée entrante] --> B{Calcul du Hash ID}
    B --> C{ID existe déjà ?}
    C -- Oui --> D[Vérification de similarité]
    C -- Non --> E[INSERT : Créer nouvelle entrée]
    D -- Score > 85% --> F[UPDATE : Fusion des données]
    D -- Score < 85% --> G[LOG : Conflit potentiel]

## 3. Implémentation de Référence (Python)

Voici le squelette technique utilisé pour l'intégration. Il repose sur le principe de Data Class pour le schéma médiateur.A. Le Modèle de Données (models.py)Pythonfrom dataclasses import dataclass
from typing import Optional

@dataclass
class Movie:
    title: str
    year: int
    director: Optional[str] = None
    rating: Optional[float] = None
    source: str = ""

    def get_pivot_id(self):
        """Génère une clé de blocage pour accélérer le matching"""
        clean_title = "".join(filter(str.isalnum, self.title)).lower()
        return f"{clean_title}_{self.year}"
B. Le Moteur d'Intégration (engine.py)Pythonfrom rapidfuzz import fuzz

class IntegrationEngine:
    def __init__(self, threshold=85):
        self.registry = {} # Stockage du Golden Record
        self.threshold = threshold

    def upsert(self, movie: Movie):
        p_id = movie.get_pivot_id()
        
        if p_id in self.registry:
            # Logique d'Update (Enrichissement)
            existing = self.registry[p_id]
            if not existing.director: existing.director = movie.director
            existing.rating = max(existing.rating or 0, movie.rating or 0)
        else:
            # Logique d'Insertion
            self.registry[p_id] = movie

## 4. Choix Techniques Justifiés

ChoixJustificationPython / PandasFlexibilité totale pour le nettoyage de chaînes de caractères complexes.Fuzzy MatchingIndispensable pour réconcilier "The Matrix" et "Matrix, The".Blocking (Hash ID)Réduit la complexité algorithmique de $O(n^2)$ à $O(n)$ pour les gros volumes.JSON/CSV ExportFormat pivot universel pour la consommation par des outils tiers (BI, App Web).

