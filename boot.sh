#!/bin/bash
if [ -n "$MODEL_TYPE" ]; then
  if [ $MODEL_TYPE == "revenue_spark" ]; then
    exec gunicorn -b :$SERVICE_PORT --workers $SERVICE_WORKERS --threads $SERVICE_THREADS --timeout $GUNICORN_TIMEOUT --access-logfile - --log-level=debug models.revenue_spark.app.server:app
    exit
  elif [ $MODEL_TYPE == "entry_compliance" ]; then
    exec gunicorn -b :$SERVICE_PORT --workers $SERVICE_WORKERS --threads $SERVICE_THREADS --timeout $GUNICORN_TIMEOUT --access-logfile - --log-level=debug models.entry_compliance.app.server:app
    exit
  elif [ $MODEL_TYPE == "similar_clients" ]; then
    exec gunicorn -b :$SERVICE_PORT --workers $SERVICE_WORKERS --threads $SERVICE_THREADS --timeout $GUNICORN_TIMEOUT --access-logfile - --log-level=debug models.similar_clients.app.server:app
    exit
  elif [ $MODEL_TYPE == "similar_clients_stats" ]; then
    exec gunicorn -b :$SERVICE_PORT --workers $SERVICE_WORKERS --threads $SERVICE_THREADS --timeout $GUNICORN_TIMEOUT --access-logfile - --log-level=debug models.similar_clients_stats.app.server:app
    exit
  elif [ $MODEL_TYPE == "ml_worker" ]; then
    exec rq worker --url redis://redis:6379 --with-scheduler
    exit
  fi
else
  echo -e "MODEL_TYPE not set\n"
fi