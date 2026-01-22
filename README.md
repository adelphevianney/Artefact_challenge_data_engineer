---

## Artefact Project

**Pipeline d’ingestion et d’orchestration des ventes**

---

## 1. Présentation

Ce projet met en place un **pipeline de données industrialisé** permettant l’ingestion quotidienne de fichiers de ventes depuis un stockage objet **MinIO** vers une base de données **PostgreSQL**, selon un **modèle en étoile** orienté analytique.

L’ensemble est **conteneurisé avec Docker** et orchestré via **Apache Airflow**, dans une logique proche d’un environnement professionnel (séparation des responsabilités, testabilité, extensibilité).

---

## 2. Objectifs du projet

* Automatiser l’ingestion quotidienne des ventes
* Centraliser les données dans un schéma analytique (PostgreSQL)
* Mettre en place une orchestration robuste avec Airflow
* Préparer l’évolution vers des contrôles qualité et du monitoring
* Appliquer les bonnes pratiques Data Engineering

---

## 3. Architecture globale

```text
MinIO (Object Storage)
        │
        ▼
Pipeline d’ingestion Python
        │
        ▼
PostgreSQL (Data Warehouse – schéma en étoile)
        │
        ▼
Apache Airflow (orchestration & supervision)
```

---

## 4. Structure du projet

```text
Artefact_project/
├── data/
│   └── fashion_store_sales.csv
│
├── data_analysis/
│   ├── analyse_exploratoire.ipynb
│   ├── analyse_exploratoire.html
│   ├── Classe_UML.png
│   └── data_modeling.md
│
├── docker/
│   ├── airflow/
│   │   ├── config/
│   │   ├── dags/
│   │   ├── logs/
│   │   └── plugins/
│   │       └── __init__.py
│   │
│   ├── postgres/
│   │   └── init/
│   │       ├── 01_schema.sql
│   │       └── 02_tables.sql
│   │
│   ├── .env
│   ├── docker-compose.yml
│   └── docker-compose-airflow.yml
│
├── ingestion/
│   ├── config.py
│   ├── logger.py
│   ├── main.py
│   ├── minio_client.py
│   ├── postgres_client.py
│   └── utils.py
│
├── test/
│   ├── data/
│   │   └── mock_sales_20250616.csv
│   ├── requirements_test
│   ├── test_config.py
│   ├── test_ingestion.py
│   └── test_schema.py
│
├── .gitattributes
├── .gitignore
└── READ_ME.md
```

---

# PARTIE 1 – Infrastructure Docker (MinIO & PostgreSQL)

Cette partie décrit la **mise en place de l’infrastructure de stockage et de persistance**, indépendante de l’orchestration.

---

## 5. Prérequis techniques

* Docker ≥ 20
* Docker Compose ≥ 2
* Python ≥ 3.9
* Git

---

## 6. Déploiement de l’infrastructure

Les services **MinIO** et **PostgreSQL** sont définis dans :

```text
docker/docker-compose.yml
```

### Démarrage

```bash
cd docker
docker-compose up -d
```

---

## 7. Vérifications opérationnelles

### État des conteneurs

```bash
docker-compose ps
```

### Logs PostgreSQL

```bash
docker-compose logs -f postgres
```

### Logs MinIO

```bash
docker-compose logs -f minio
```

---

## 8. Accès aux services

### MinIO

* Interface Web : [http://localhost:9001](http://localhost:9001)
* Identifiants par défaut :

  * utilisateur : `minioadmin`
  * mot de passe : `secretminio`

### PostgreSQL

```bash
docker exec -it artefact-postgres psql -U postgres -d ecommerce
```

Commandes utiles :

```sql
\dn
\dt
SELECT * FROM sales.orders LIMIT 10;
```

---

## 9. Exécution manuelle du pipeline (hors Airflow)

Cette étape permet de tester le pipeline **sans orchestration**.

```bash
python ingestion/main.py 20250616
```

* Le paramètre correspond à la date d’ingestion (`AAAAMMJJ`)
* Les données sont chargées dans le schéma `sales`

---

## 10. Arrêt et nettoyage

```bash
docker-compose down
docker-compose down -v   # suppression des volumes
```

---

# PARTIE 2 – Orchestration avec Apache Airflow

Cette partie décrit la **mise en place et la configuration d’Airflow**.

---

## 11. Sécurité et variables d’environnement

une clé frenet doit être générée et ajoutée dans `docker/.env`.

### Fernet Key

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

---

## 12. Démarrage d’Airflow

```bash
docker-compose -f docker/docker-compose-airflow.yml up -d
```

### Accès à l’UI

* URL : [http://localhost:8080](http://localhost:8080)

---

## 13. Création de l’utilisateur administrateur(si besoin)

sur windows remplacez \ par `

```bash
docker compose -f docker/docker-compose-airflow.yml exec airflow-apiserver \
  airflow users create \
    --username airflow \
    --password airflow \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```

---

## 14. Configuration des connexions (UI Airflow)

👉 **Toutes les connexions sont créées depuis l’interface Airflow**, conformément aux bonnes pratiques professionnelles.

### Accès

**Admin → Connections**

---

### Connexion MinIO

* Conn Id : `minio_default`
* Conn Type : `Amazon Web Services`
* Login : `minioadmin`
* Password : `secretminio`

**Extra :**

```json
{
  "endpoint_url": "http://minio:9000",
  "aws_access_key_id": "minioadmin",
  "aws_secret_access_key": "secretminio",
  "region_name": "us-east-1"
}
```

---

### Connexion PostgreSQL

* Conn Id : `postgres_ecommerce`
* Conn Type : `Postgres`
* Host : `artefact-postgres`
* Schema : `ecommerce`
* Login : `postgres`
* Password : `secretpostgres`
* Port : `5432`

---

---

## Exécution des DAGs Airflow

Ce projet contient deux DAGs principaux et un DAG test :

1. **sales_ingestion_semi-modulaire** : une version intermédiaire où certaines tâches sont réutilisables mais pas entièrement découplées.
2. **sales_ingestion_modulaire** : chaque tâche est complètement indépendante et réutilisable, ce qui facilite la maintenance et l’évolution du workflow.
3. **test_minimal_airflow3** : le DAG de test 

### 1. Prérequis

Avant de lancer les DAGs, assurez-vous que :

* toutes les étapes précédentes sont déjà effectuées et valides.

---

### 2. Lancer le DAG semi-modulaire

1. Accédez à l’interface web d’Airflow : [http://localhost:8080](http://localhost:8080)
2. Recherchez le DAG nommé : `sales_ingestion_semi-modulaire`.
3. Activez-le en cliquant sur le bouton **Off → On**.
4. Cliquez sur **Trigger DAG** pour lancer l’exécution immédiate.
5. Surveillez les logs et l’état des tâches dans la vue **Graph** ou **Tree** pour suivre l’avancement.

> 🔹 Le DAG semi-modulaire conserve certaines tâches dépendantes directement entre elles, donc les modifications de structure peuvent nécessiter un ajustement manuel.

---

### 3. Lancer le DAG modulaires

1. Dans la même interface Airflow, repérez le DAG nommé : `sales_ingestion_modulaire`.
2. Activez-le (**Off → On**).
3. Cliquez sur **Trigger DAG** pour démarrer l’exécution.
4. Utilisez la vue **Graph** pour observer la structure complètement modulaire et la réutilisation des tâches.

> 🔹 Grâce à sa modularité, chaque tâche peut être réutilisée dans d’autres DAGs ou workflows sans toucher au reste du pipeline.

---

### 4. Conseils de suivi et debug

* **Logs** : toutes les sorties de tâches sont disponibles dans la vue **Logs** pour chaque tâche.
* **Re-lancement d’une tâche** : si une tâche échoue, vous pouvez la **ré-exécuter individuellement** sans relancer le DAG complet.
* **Version des DAGs** : assurez-vous que votre dépôt est à jour avant de lancer, pour éviter des conflits de dépendances ou de chemins de fichiers.

---

## 15. Modélisation des données

```
CUSTOMER ───(1,N)─── ORDER ───(1,N)─── ORDER_ITEM ───(N,1)─── PRODUCT
                    │
                 DATE
```

* Modèle en étoile
* Optimisé pour l’analyse
* Schéma UML : `data_analysis/Classe_UML.png`

---

## 16. Tests et qualité

```bash
pip install -r test/requirements_test
pytest test/ -v
```

---

## 17. Perspectives d’évolution

* DAG Airflow complet avec scheduling quotidien
* Retries, alertes et SLA
* Contrôles qualité des données
* Monitoring et observabilité
* Intégration dbt
* Séparation dev / staging / prod

---

**Dernière mise à jour : janvier 2026**

---
