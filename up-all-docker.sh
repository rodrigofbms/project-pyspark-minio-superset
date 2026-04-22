#!/bin/bash

# List of services directories
SERVICES=(
    "/home/rodri/docker/project-pyspark-minio-superset/apps/nginx_proxy_manager"
    "/home/rodri/docker/project-pyspark-minio-superset/apps/portainer"
    "/home/rodri/docker/project-pyspark-minio-superset/documentation"
    "/home/rodri/docker/project-pyspark-minio-superset/apps/postgres_adventureworks"
    "/home/rodri/docker/project-pyspark-minio-superset/apps/airflow"
    "/home/rodri/docker/project-pyspark-minio-superset/apps/minio"
    "/home/rodri/docker/project-pyspark-minio-superset/apps/open_metadata"
    "/home/rodri/docker/project-pyspark-minio-superset/apps/spark"
    "/home/rodri/docker/project-pyspark-minio-superset/apps/superset"
    "/home/rodri/docker/project-pyspark-minio-superset/apps/trino"
)

for SERVICE in "${SERVICES[@]}"; do
    echo "UP SERVICE IN: $SERVICE"
    
    if [ -d "$SERVICE" ]; then
        cd "$SERVICE" || continue

        if [ -f "docker-compose.yml" ] || [ -f "compose.yml" ]; then
            sudo docker compose up -d
        else
            echo "Did not find docker-compose in $SERVICE"
        fi
    else
        echo "Directory does not exist: $SERVICE"
    fi

    echo "----------------------------------"
done

echo "ALL SERVICES WERE UP!"