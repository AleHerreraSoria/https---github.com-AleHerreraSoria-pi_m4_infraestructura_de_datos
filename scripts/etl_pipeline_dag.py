from airflow.decorators import dag
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime

# Definimos el comando de spark-submit que queremos ejecutar en la otra máquina
SPARK_COMMAND = """
cd ~/spark_environment && \
sudo docker-compose exec spark /opt/spark/bin/spark-submit \
    --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    /app/process_weather_data.py
"""

@dag(
    dag_id="etl_weather_pipeline",
    description="Pipeline ETL principal que procesa datos de Bronze a Silver.",
    start_date=datetime(2025, 10, 19), # Usa una fecha en el pasado
    schedule="@daily",        # Lo programamos para que corra una vez al día
    catchup=False,
    tags=["etl", "spark", "pi_m4"],
)
def etl_weather_pipeline():
    """
    Este DAG ejecuta el script de PySpark en la instancia de Spark
    para transformar los datos de Bronze a Silver.
    """

    run_spark_job = SSHOperator(
        task_id="run_spark_transformation",
        ssh_conn_id="ssh_spark_worker", # ¡El ID de la conexión SSH que acabas de crear!
        command=SPARK_COMMAND,
        cmd_timeout=600, # Timeout de 10 minutos
        get_pty=True
    )

# Llamamos a la función para instanciar el DAG
etl_weather_pipeline()