#!/bin/bash
if [ -n "$MODEL_TYPE" ]; then
  if [ $MODEL_TYPE == "revenue_spark" ]; then
    exec gunicorn -b :$SERVICE_PORT --workers $SERVICE_WORKERS --threads $SERVICE_THREADS --timeout $GUNICORN_TIMEOUT --access-logfile - --error-logfile - --log-level=debug models.revenue_spark.app.server:app
    exit
  elif [ $MODEL_TYPE == "similar_companies_master" ]; then
    exec gunicorn -b :$SERVICE_PORT --workers $SERVICE_WORKERS --threads $SERVICE_THREADS --timeout $GUNICORN_TIMEOUT --access-logfile - --error-logfile - --log-level=debug models.similar_companies.app.master:app
    exit
  elif [ $MODEL_TYPE == "similar_companies_server" ]; then
    exec gunicorn -b :$SERVICE_PORT --workers $SERVICE_WORKERS --threads $SERVICE_THREADS --timeout $GUNICORN_TIMEOUT --access-logfile - --error-logfile - --log-level=debug models.similar_companies.app.server:app
    exit
  fi
else
  echo -e "MODEL_TYP not set\n"
fi