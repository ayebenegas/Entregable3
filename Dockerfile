# Usa la imagen base de Apache Airflow
FROM apache/airflow:2.3.3

# Copia el requirements.txt y luego instala las dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
