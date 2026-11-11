# Steam Reviews Big Data Pipeline

## Présentation

Pipeline de Data Engineering permettant le traitement et l'analyse de plus de **113 millions d'avis Steam**, en utilisant PySpark, Apache Airflow et Docker.

Le projet repose sur une architecture **Medallion (Bronze, Silver, Gold)** pour organiser l'ingestion, le nettoyage, la transformation et l'analyse des données.

## Architecture

```text
Steam Reviews Dataset
        |
        v
Bronze — Raw Parquet
        |
        v
Silver — Cleaning & Deduplication
        |
        v
Gold — Analytical Aggregations
        |
        v
Data Quality
        |
        v
CSV Export → Power BI (prévu)
```

Apache Airflow orchestre les différentes étapes du pipeline.

## Technologies

- Python
- Apache Spark / PySpark 4.2.0
- Apache Airflow 3.3.1
- Docker & Docker Compose
- Apache Parquet
- Git & GitHub

## Pipeline ETL

### Bronze — Ingestion

Téléchargement et stockage des fichiers Parquet du dataset Steam Reviews.

### Silver — Nettoyage

- Sélection des colonnes pertinentes
- Gestion des valeurs manquantes
- Suppression des doublons
- Validation de certaines données

**Volume traité : 113 530 413 avis.**

### Gold — Analytics

Création de trois tables analytiques :

- `games_stats` : nombre d'avis et taux d'évaluations positives par jeu.
- `language_stats` : répartition des avis par langue.
- `playtime_stats` : analyse des avis selon le temps de jeu.

### Data Quality

Contrôles sur les colonnes obligatoires et validation des volumes d'avis dans les tables Gold.

## Orchestration Airflow

DAG : `steam_bigdata_pipeline`

Le pipeline est organisé en quatre tâches :

```text
bronze >> silver >> gold >> data_quality
```

Le DAG possède un mode de vérification des résultats existants et un mode de reconstruction des données.

## Résultats

**Top jeux par nombre d'avis :**

| Jeu | Nombre d'avis | Avis positifs |
|---|---:|---:|
| Counter-Strike 2 | 7 704 653 | 87,89 % |
| PUBG | 2 235 431 | 57,48 % |
| Grand Theft Auto V | 1 659 263 | 86,18 % |
| Terraria | 1 205 564 | 97,68 % |

**Langues les plus représentées :**

| Langue | Nombre d'avis |
|---|---:|
| Anglais | 51 352 401 |
| Chinois simplifié | 17 571 098 |
| Russe | 12 860 588 |

## Exécution

Installer les dépendances Python :

```bash
pip install -r requirements.txt
```

Lancer Airflow avec Docker :

```bash
docker compose up -d --build
```

Interface Airflow :

`http://localhost:8081`

Une fois les fichiers de données disponibles, lancer le DAG avec `rebuild=false` pour vérifier les sorties existantes.

Pour une reconstruction complète, adapter les ressources Docker et Spark avant de déclencher `rebuild=true`.

## Dataset

Source : [Steam Reviews — Hugging Face](https://huggingface.co/datasets/adde023/steam-reviews)

Les fichiers de données ne sont pas inclus dans ce dépôt en raison de leur volume.

## Évolutions prévues

- Dashboard interactif Power BI
- Optimisation des performances Spark
- Contrôles qualité supplémentaires
