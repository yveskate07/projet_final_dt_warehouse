import subprocess
import psycopg2 # type: ignore
import time

def run_dbt():
    print("🚀 Démarrage des transformations dbt...")
    start_time = time.time()
    
    # Exécution de dbt (nettoyage + transformation + chargement)
    result = subprocess.run(["dbt", "run"], capture_output=True, text=True)
    
    end_time = time.time()
    execution_time = round(end_time - start_time, 2)
    
    if result.returncode == 0:
        print(f"✅ dbt run terminé avec succès en {execution_time} secondes.")
        check_results(execution_time)
    else:
        print("❌ Erreur dbt :", result.stderr)

def check_results(exec_time):
    # Connexion à ta DB Postgres Docker
    conn = psycopg2.connect(
        dbname="votre_db", user="user", password="password", host="localhost", port="5432"
    )
    cur = conn.cursor()

    # Compte des lignes dans la table de faits
    cur.execute("SELECT COUNT(*) FROM fct_campagne_performances")
    rows_fact = cur.fetchone()[0]

    # Somme des lignes dans les dimensions principales
    cur.execute("SELECT (SELECT COUNT(*) FROM dim_produits) + (SELECT COUNT(*) FROM dim_parcelles)")
    rows_dims = cur.fetchone()[0]

    print("-" * 30)
    print(f"📊 RÉSULTAT DU CHARGEMENT :")
    print(f"• {rows_fact} lignes chargées dans Fact_Campagne")
    print(f"• {rows_dims} lignes au total dans les dimensions")
    print(f"• Temps d’exécution : {exec_time} secondes")
    print("-" * 30)

    cur.close()
    conn.close()

if __name__ == "__main__":
    run_dbt()