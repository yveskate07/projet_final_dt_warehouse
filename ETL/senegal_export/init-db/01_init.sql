-- Bases existantes
CREATE DATABASE data_warehouse;
CREATE DATABASE metabase_app;
CREATE DATABASE metabase;

-- AJOUTER CETTE LIGNE : Indispensable pour l'orchestrateur Airflow
CREATE DATABASE airflow_db;

-- On donne les droits à l'étudiant sur TOUTES les bases
GRANT ALL PRIVILEGES ON DATABASE data_warehouse TO etudiant;
GRANT ALL PRIVILEGES ON DATABASE metabase_app TO etudiant;
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO etudiant;

