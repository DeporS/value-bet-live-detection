#!/bin/bash
airflow db migrate
airflow users create \
  --username $AIRFLOW_ADMIN_USERNAME \
  --firstname Admin \
  --lastname Admin \
  --role Admin \
  --email $AIRFLOW_ADMIN_EMAIL \
  --password $AIRFLOW_ADMIN_PASSWORD