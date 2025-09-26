FROM apache/airflow:3.0.6

# Copier le fichier requirements.txt
COPY requirements.txt /opt/airflow/requirements.txt

# Installer les dépendances supplémentaires
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt


