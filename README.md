# Démarrer les services
docker compose up -d

# Voir les logs (utile pour debugger)
docker compose logs -f postgres
docker compose logs -f minio

# Arrêter tout
docker compose down

# Arrêter + supprimer les volumes (remise à zéro complète)
docker compose down -v

# avoir accès à l'outil de base de donnée
docker exec -it artefact-postgres psql -U postgres -d ecommerce
# voir les schémas
\dn 
# voir les tables
\dt

# syntaxe pour faire une requete directement dans l'invite de commande
docker compose exec postgres psql -U postgres -d ecommerce -c " ici tu mets ta requete SQL" 

# insertion direct depuis le script python
python ../ingestion/main.py 20251006

````commandline
ingestion/
├── main.py               # point d’entrée (lancement avec argument date)
├── config.py             # constantes, paramètres de connexion
├── logger.py             # configuration du logging
├── minio_client.py       # gestion de la connexion et lecture Minio
├── postgres_client.py    # gestion de la connexion et insertions PostgreSQL
└── utils.py              # fonctions utilitaires (parsing date, transformations...)

````
# modèle EA

```commandline
CUSTOMER
   │1
   │
   │N
 ORDER ─────────── DATE
   │1
   │
   │N
ORDER_ITEM
   │N
   │
   │1
 PRODUCT

```

## Tests

Un dossier `tests/` contient des tests unitaires et d'intégration :

```bash
# Installer les dépendances de test
pip install -r requirements-dev.txt

# Lancer tous les tests
pytest tests/ -v

# Lancer avec couverture de code
pytest --cov=ingestion tests/ --cov-report=html
# Ouvrir htmlcov/index.html
```
---
# lancer airflow
docker compose -f docker-compose-airflow.yml down
docker compose -f docker-compose-airflow.yml up -d

Attends 1–2 minutes, puis ouvre ton navigateur :
http://localhost:8081Identifiants par défaut :
Login : airflow
Password : airflow

---
git config --global core.autocrlf input
 
git add .
git commit -m "..."
git push origin main
---

---
# initialiser la base airflow
docker exec -it docker-airflow-webserver-1 airflow db init

# créer user admin 
docker exec -it docker-airflow-webserver-1 airflow users create `
  --username airflow `
  --password airflow `
  --firstname Admin `
  --lastname User `
  --role Admin `
  --email admin@example.com

# connexion minio

docker exec -it docker-airflow-webserver-1 airflow connections add minio_default `
    --conn-type aws `
    --conn-login minioadmin `
    --conn-password minioadminsecret `
    --conn-extra {"endpoint_url":"http://minio:9001"}

# connexion postgres

docker exec -it docker-airflow-webserver-1 airflow connections add postgres_ecommerce `
    --conn-type postgres `
    --conn-host postgres-airflow `
    --conn-schema airflow `
    --conn-login airflow `
    --conn-password airflow `
    --conn-port 5432

# Lister les DAG runs
docker exec -it docker-airflow-webserver-1 airflow dags list-runs -d sale_ingestion_dag

# Stopper une run spécifique en la marquant comme failed
docker exec -it docker-airflow-webserver-1 airflow tasks clear sale_ingestion_dag --run-id run_id --all-tasks

