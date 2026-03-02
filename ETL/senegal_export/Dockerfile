# On part de l'image officielle Airflow (légère)
FROM apache/airflow:2.7.1-python3.10

# On passe en root pour installer les dépendances système si besoin (git est requis pour dbt)
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        git \
        libpq-dev \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# On repasse en utilisateur airflow pour installer les paquets python
USER airflow

# Installation de dbt et des connecteurs
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt





# dockerfile optimise
FROM apache/airflow:2.7.1-python3.10

USER root
# Installation des dépendances système nécessaires pour dbt et postgres
RUN apt-get update && apt-get install -y --no-install-recommends \
    git libpq-dev gcc python3-dev && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
ENV PATH="${PATH}:/home/airflow/.local/bin"

# CORRECTIF CRUCIAL : Force une version de protobuf compatible
# Cela évite le crash du serveur web que nous avons vu dans les logs
RUN pip install --no-cache-dir --user "protobuf<5.0.0" dbt-postgres

# Installation des dépendances du cours
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --user -r /requirements.txt
