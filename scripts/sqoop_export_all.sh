#!/bin/bash

MYSQL_HOST="rds-flight-db.c9e2446ms42q.ap.south-1.rds.amazonaws.com"
MYSQL_PORT=3306
MYSQL_DB="flights_db"
MYSQL_USER="shabaz"
MYSQL_PASSWORD="**********"

HDFS_BASE_PATH="/user/shabaz/flight_outputs/"
SQOOP_JDBC_URL="jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DB}?useSSL=false"

export_table() {
    TABLE_NAME=$1
    sqoop export \
        --connect "$SQOOP_JDBC_URL" \
        --username "$MYSQL_USER" \
        --password "$MYSQL_PASSWORD" \
        --table "$TABLE_NAME" \
        --export-dir "${HDFS_BASE_PATH}/${TABLE_NAME}" \
        --input-fields-terminated-by ',' \
        --input-lines-terminated-by '\n' \
        --null-string '\\N' \
        --null-non-string '\\N' \
        --num-mappers 1
}

TABLES=(
    "top10_departure_delay_airports"
    "top10_arrival_delay_airlines"
    "delay_count_by_airline"
    "national_avg_delay"
    "cancellation_rate_by_airline"
    "cancellation_rate_by_airport"
    "cancellation_rate_by_month"
    "cancellation_reasons"
    "common_diversion_airports"
    "diversion_rate_by_airline"
    "diversion_impact"
)

for TABLE in "${TABLES[@]}"
do
    echo "Exporting table: $TABLE"
    export_table "$TABLE"
    echo "---------------------------------------"
done
