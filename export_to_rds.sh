#!/bin/bash

source ./db_credentials.sh

HIVE_DB="flights_db"
HIVE_WAREHOUSE_PATH="/user/hive/warehouse/${HIVE_DB}.db"

TABLES=(
  "busiest_airports"
  "busiest_airlines"
  "busiest_routes"
  "airline_delay_stats"
  "airline_delay_percentages"
  "airport_taxi_times"
  "elapsed_time_comparison"
  "airline_cancellation_rate"
  "airline_diversion_rate"
  "top_divert_airports"
)

for TABLE in "${TABLES[@]}"; do
  echo "Exporting $TABLE to MySQL..."

  sqoop export \
    --connect "jdbc:mysql://${RDS_HOST}:3306/${DB_NAME}" \
    --username "${DB_USER}" \
    --password "${DB_PASS}" \
    --table "${TABLE}" \
    --export-dir "${HIVE_WAREHOUSE_PATH}/${TABLE}" \
    --input-fields-terminated-by '\001' \
    --num-mappers 1

  if [ $? -eq 0 ]; then
    echo "Exported $TABLE successfully ✅"
  else
    echo "Failed to export $TABLE ❌"
  fi
done

echo "All exports complete."
