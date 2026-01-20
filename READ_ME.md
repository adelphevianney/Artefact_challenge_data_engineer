
---
# Artefact - Pipeline d'ingestion et de modélisation des ventes

## Description

```markdown

Ce projet vise à ingérer quotidiennement les fichiers de ventes d'une boutique de mode depuis un stockage objet (MinIO) vers une base de données PostgreSQL, en respectant un modèle en étoile simple.  
Il est dockerisé et prépare la transition vers une orchestration complète avec Airflow.

Objectifs principaux :
- Automatiser l’ingestion des fichiers de ventes.
- Stocker les données dans PostgreSQL selon un schéma en étoile.
- Fournir un pipeline modulaire et extensible.
- Préparer l’orchestration avec Airflow et la gestion des alertes.

```
---


## Structure du projet

``` markdown
        Artefact_project/
        ├── data/
        │   └── fashion_store_sales.csv        # fichier source 
        ├── data_analysis/
        │   ├── analyse_exploratoire.ipynb
        │   ├── analyse_exploratoire.html
        │   └── Classe UML.png                 # schéma physique
        ├── ingestion/
        │   ├── main.py                        # point d'entrée du script
        │   ├── config.py
        │   ├── logger.py
        │   ├── minio_client.py
        │   ├── postgres_client.py
        │   └── utils.py
        ├── tests/
        │   ├── test_config.py
        │   ├── test_ingestion.py
        │   └── test_schema.py
        ├── docker/
        │   └── docker-compose.yml
        ├── docker-compose-airflow.yml         # stack airflow
        ├── data_modeling.md
        ├── README.md
        └── .gitignore

```

---

## Prérequis

- Docker 20+ et docker-compose 2+
- Python 3.9+ (recommandé 3.10/3.11)
- git

---

## Installation et lancement rapide

### 1. Lancer PostgreSQL + MinIO

```bash
cd docker
docker-compose up -d
````

Vérifier que les services fonctionnent :

```bash
docker-compose ps
docker-compose logs -f postgres
docker-compose logs -f minio
```

> MinIO doit être accessible depuis Airflow via `http://localhost:9001` si exposé sur l’hôte.

---

### 2. Exécuter le script d’ingestion

```bash
# Depuis la racine du projet
python ingestion/main.py 20260116
```

* Le paramètre est la date à ingérer au format `AAAAMMJJ`.
* Les logs détaillés se trouvent dans `ingestion/logs/`.

Vérification dans PostgreSQL :

```bash
docker exec -it artefact-postgres psql -U airflow -d airflow
\dn      # voir les schémas
\dt      # voir les tables
SELECT * FROM sales.order LIMIT 10;
```

---

### 3. Lancer Airflow (Work in progress)

```bash
docker-compose -f docker-compose-airflow.yml up -d
```

* UI Airflow : [http://localhost:8081](http://localhost:8081)
* login : `airflow`
* password : `airflow`

Initialisation de la base et création d’utilisateur :

```bash
docker exec -it docker-airflow-webserver-1 airflow db init

docker exec -it docker-airflow-webserver-1 airflow users create \
    --username airflow \
    --password airflow \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```

---

### 4. Configurer les connexions Airflow

```bash
# Connexion MinIO
docker exec -it docker-airflow-webserver-1 airflow connections add minio_default `
    --conn-type aws `
    --conn-login minioadmin `
    --conn-password minioadminsecret `
    --conn-extra '{"endpoint_url": "http://minio:9000"}'

# Connexion Postgres
docker exec -it docker-airflow-webserver-1 airflow connections add postgres_ecommerce `
    --conn-type postgres `
    --conn-login airflow `
    --conn-password airflow `
    --conn-host postgres-airflow `
    --conn-schema airflow `
    --conn-port 5432
```

---

## Commandes utiles

```bash
# Arrêt propre
docker-compose down

# Reset complet (supprime les volumes)
docker-compose down -v

# Vérifier les logs d’un conteneur
docker-compose logs -f airflow-webserver

# Tester une requête SQL
docker-compose exec postgres psql -U airflow -d airflow -c "SELECT count(*) FROM sales.order_item;"
```

---

## Tests

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
pytest --cov=ingestion tests/ --cov-report=html
```

---

## Modèle conceptuel

```
CUSTOMER ───(1,N)─── ORDER ───(1,N)─── ORDER_ITEM ───(N,1)─── PRODUCT
                    │
                 DATE (dimension)
```

* Le modèle physique détaillé est disponible dans `data_analysis/Classe UML.png`.
* Le schéma en étoile permet des requêtes analytiques simples (CA par date, par produit, etc.)

---

## Prochaines étapes

* Finaliser le DAG Airflow complet
* Gestion des retries, alertes et monitoring
* Contrôles de qualité des données (doublons, anomalies)
* Documentation technique & guide pour dev/QA
* Possibilité d’ajouter dbt pour transformations futures

---

## Conventions Git

```bash
git config --global core.autocrlf input  # fortement recommandé sous Windows

git add .
git commit -m "feat: message clair"
git push origin main
```

---

**Dernière mise à jour : janvier 2026**

```
