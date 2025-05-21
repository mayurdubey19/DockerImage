#!/bin/bash
export AIRFLOW_HOME=/opt/airflow
export PGPASSWORD=airflow

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL..."
until pg_isready -h postgres -p 5432 -U airflow; do
  echo "PostgreSQL is not ready yet. Waiting..."
  sleep 1
done

# Check if Airflow database is already initialized
echo "Checking if Airflow database is initialized..."
DB_CHECK=$(psql -h postgres -U airflow -d airflow -tAc "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'dag');")

if [ "$DB_CHECK" == "t" ]; then
    echo "Airflow database already initialized. Skipping db init."
else
    echo "Airflow database not initialized. Running airflow db init..."
    airflow db init
fi

# Check if admin user already exists
if ! airflow users list | grep -q "admin@example.com"; then
  echo "Creating admin user..."
  airflow users create \
    --username test_admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
else
  echo "Admin user already exists. Skipping creation."
fi

# Start Airflow services
airflow scheduler &   # background
exec airflow webserver -p 8080 # foreground