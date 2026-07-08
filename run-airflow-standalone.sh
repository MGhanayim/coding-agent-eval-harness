set -euo pipefail

export AIRFLOW_HOME=~/airflow
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=false

mkdir -p $AIRFLOW_HOME

echo '{"admin": "admin"}' > $AIRFLOW_HOME/simple_auth_manager_passwords.json.generated

# --with adds the Docker provider so EXECUTION_MODE=docker (Block H) parses;
# harmless in the default subprocess mode.
uv tool run --with apache-airflow-providers-docker apache-airflow standalone
