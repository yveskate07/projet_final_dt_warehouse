# airflow/dags/senegal_export_pipeline.py
"""
DAG Airflow : senegal_export_pipeline
Orchestre le pipeline ETL complet du projet SENEGAL EXPORT.

Flux :
  check_postgres_health
    → load_csv_seeds          (dbt seed)
    → dbt_run_staging         (dbt run staging.*)
    → dbt_test_staging        (dbt test staging.*)
    → dbt_run_marts           (dbt run marts.*)
    → dbt_test_marts          (dbt test marts.*)
    → dbt_generate_docs       (dbt docs generate)
    → refresh_metabase_cache  (API Metabase)
    → notify_success          (email)

Schedule : chaque lundi à 06h00 UTC (08h00 Dakar WAT)
"""

from __future__ import annotations

from datetime import datetime, timedelta

import psycopg2
import requests

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator

# ── Constantes ────────────────────────────────────────────────────────────────
DBT_DIR      = "/opt/dbt"
DBT_PROFILES = "/opt/dbt/profiles"
DBT_TARGET   = "prod"

METABASE_URL  = "http://metabase:3000"
METABASE_USER = "admin@senagri.sn"
METABASE_PASS = "admin2026"

PG_CONN = dict(
    host="postgres",
    port=5432,
    dbname="senagri_dw",
    user="senagri",
    password="senagri2026",
)

# ── Default args ───────────────────────────────────────────────────────────────
default_args = {
    "owner": "data_team",
    "depends_on_past": False,
    "email": ["dg@senagri.sn"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=60),
}

# ── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="senegal_export_pipeline",
    default_args=default_args,
    description="Pipeline ETL SenAgri : dbt seeds → staging → marts → Metabase",
    # schedule_interval="0 6 * * 1", # Lundi 06:00 UTC
    schedule_interval="@hourly",  
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["senagri", "dbt", "dw", "agro"],
    doc_md="""
## Pipeline SENEGAL EXPORT
Orchestre le pipeline ETL hebdomadaire du Data Warehouse SenAgri.
- **Sources** : 6 fichiers CSV (parcelles, produits, campagnes, météo, dates, fournisseurs)
- **Transformations** : dbt Core (staging + marts)
- **Destination** : PostgreSQL schéma `dwh`
- **Dashboard** : Metabase
    """,
) as dag:

    # ── 1. Vérification PostgreSQL ────────────────────────────────────────────
    def _check_postgres(**kwargs):
        """Vérifie que PostgreSQL est accessible avant de lancer le pipeline."""
        try:
            conn = psycopg2.connect(**PG_CONN, connect_timeout=10)
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name IN ('raw','staging','dwh')")
            count = cur.fetchone()[0]
            conn.close()
            if count < 3:
                raise RuntimeError(f"Schémas manquants : seulement {count}/3 trouvés")
            print(f"✅ PostgreSQL OK — 3 schémas présents")
        except psycopg2.OperationalError as e:
            raise RuntimeError(f"PostgreSQL inaccessible : {e}")

    check_postgres = PythonOperator(
        task_id="check_postgres_health",
        python_callable=_check_postgres,
    )

    # ── 2. Chargement des seeds CSV → schéma raw ──────────────────────────────
    load_seeds = BashOperator(
        task_id="load_csv_seeds",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt deps --profiles-dir {DBT_PROFILES} && "
            f"dbt seed --profiles-dir {DBT_PROFILES} --target {DBT_TARGET} --full-refresh"
        ),
        execution_timeout=timedelta(minutes=10),
    )

    # ── 3. dbt run — couche staging ───────────────────────────────────────────
    run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES} --target {DBT_TARGET} "
            f"--models staging.* "
            f"--vars '{{\"run_date\": \"{{{{ ds }}}}\"}}'"
        ),
        execution_timeout=timedelta(minutes=15),
    )

    # ── 4. dbt test — staging ─────────────────────────────────────────────────
    test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES} --target {DBT_TARGET} "
            f"--models staging.*"
        ),
        execution_timeout=timedelta(minutes=10),
    )

    # ── 5. dbt run — couche marts (dimensions + fait) ─────────────────────────
    run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES} --target {DBT_TARGET} "
            f"--models marts.*"
        ),
        execution_timeout=timedelta(minutes=20),
    )

    # ── 6. dbt test — marts ───────────────────────────────────────────────────
    test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES} --target {DBT_TARGET} "
            f"--models marts.*"
        ),
        execution_timeout=timedelta(minutes=10),
    )

    # ── 7. Génération doc dbt ────────────────────────────────────────────────
    generate_docs = BashOperator(
        task_id="dbt_generate_docs",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt docs generate --profiles-dir {DBT_PROFILES} --target {DBT_TARGET}"
        ),
        execution_timeout=timedelta(minutes=5),
    )

    # ── 8. Rafraîchissement du cache Metabase ─────────────────────────────────
    def _refresh_metabase(**kwargs):
        """Ouvre une session Metabase et rafraîchit tous les dashboards SENAGRI."""
        # Authentification
        resp = requests.post(
            f"{METABASE_URL}/api/session",
            json={"username": METABASE_USER, "password": METABASE_PASS},
            timeout=30,
        )
        resp.raise_for_status()
        token = resp.json()["id"]
        headers = {"X-Metabase-Session": token}

        # Récupération des dashboards
        dbs_resp = requests.get(f"{METABASE_URL}/api/dashboard", headers=headers, timeout=30)
        dbs_resp.raise_for_status()
        dashboards = dbs_resp.json()

        refreshed = 0
        for db in dashboards:
            name = db.get("name", "")
            if "SENAGRI" in name.upper() or "SENEGAL" in name.upper():
                # Rafraîchissement des cards du dashboard
                detail = requests.get(f"{METABASE_URL}/api/dashboard/{db['id']}", headers=headers).json()
                for card in detail.get("ordered_cards", []):
                    card_id = card.get("card_id")
                    if card_id:
                        requests.post(
                            f"{METABASE_URL}/api/card/{card_id}/query",
                            headers=headers,
                            timeout=60,
                        )
                refreshed += 1
                print(f"✅ Dashboard '{name}' rafraîchi")

        if refreshed == 0:
            print("⚠️  Aucun dashboard SENAGRI trouvé — vérifier le nom dans Metabase")
        else:
            print(f"🎉 {refreshed} dashboard(s) rafraîchi(s)")

        # Déconnexion
        requests.delete(f"{METABASE_URL}/api/session", headers=headers, timeout=10)

    refresh_metabase = PythonOperator(
        task_id="refresh_metabase_cache",
        python_callable=_refresh_metabase,
        execution_timeout=timedelta(minutes=10),
    )

    # ── 9. Notification email succès ──────────────────────────────────────────
    notify_success = EmailOperator(
        task_id="notify_success",
        to=["dg@senagri.sn", "data_team@senagri.sn"],
        subject="[SENAGRI] ✅ Pipeline ETL SUCCÈS — {{ ds }}",
        html_content="""
        <h2 style="color:#1A5276;">Pipeline SENEGAL EXPORT — Exécution réussie ✅</h2>
        <table border="1" cellpadding="6" style="border-collapse:collapse;">
            <tr><td><b>Date d'exécution</b></td><td>{{ ds }}</td></tr>
            <tr><td><b>DAG</b></td><td>senegal_export_pipeline</td></tr>
            <tr><td><b>Prochaine exécution</b></td><td>{{ next_ds }}</td></tr>
        </table>
        <p>
            📊 <a href="http://metabase:3000">Ouvrir le Dashboard Metabase</a><br>
            📖 <a href="http://localhost:8082">Documentation dbt</a>
        </p>
        """,
    )

    # ── Dépendances ───────────────────────────────────────────────────────────
    (
        check_postgres
        >> load_seeds
        >> run_staging
        >> test_staging
        >> run_marts
        >> test_marts
        >> generate_docs
        >> refresh_metabase
        >> notify_success
    )
