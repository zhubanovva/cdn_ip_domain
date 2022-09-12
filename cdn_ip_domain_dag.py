from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils import timezone
from datetime import timedelta, datetime
import pendulum
import logging

hadoop_conf_dir = Variable.get("HADOOP_CONF_DIR", "/etc/hadoop/conf/")
spark_home = Variable.get("SPARK_HOME", "/usr/local/spark")

env = {
    "HADOOP_CONF_DIR": f"{hadoop_conf_dir}",
    "SPARK_HOME": f"{spark_home}"
}
# DAG for airflow task

with DAG(
    dag_id='cdn_ip_domain_dag',
    schedule_interval = '0 7,19 * * *',
    start_date=pendulum.datetime(2022, 8, 10, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=600),
    
) as dag:
    get_ips = BashOperator(
        task_id='get_ips',
        bash_command=(
            "/opt/miniconda3/envs/airflow_env/bin/python3 "
            "/opt/airflow/dags/diana/get_ips_ipv6_distinct.py"
        ),
        email_on_failure = True,
        email = "diana.zhubanova@example.kz",
        env=env
    )
    
    get_ips

if __name__ == "__main__":
    try:
        dag.cli()
    except Exception as e:
        print(str(e))