#!/bin/sh


i=0
while [ $i -lt 20 ]; do

  # check if there is a password defined for elastic
  if [[ -z "${ES_PASSWORD}" ]]; then
  curl -s "http://${ES_USERNAME}@${ES_HOST}:${ES_PORT}/_cluster/health?wait_for_status=yellow&timeout=10s" > /tmp/output
  else
  curl -s "http://${ES_USERNAME}:${ES_PASSWORD}@${ES_HOST}:${ES_PORT}/_cluster/health?wait_for_status=yellow&timeout=10s" > /tmp/output
  fi

  if [ $? -eq 0 ] && cat /tmp/output | jq -e "select(.timed_out == false)"; then
  docker exec mongo mongo --username=${MONGO_USERNAME} --password=${MONGO_PASSWORD} --eval "rs.initiate()"
  break;
  fi
    echo "ElasticSearch is unavailable - sleeping" >&2
    sleep 2
    i=$((i+1))
done
if [ $i -eq 20 ]; then exit 1; fi


echo "Replica Set is initiated - resuming execution"