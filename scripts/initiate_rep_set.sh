#!/bin/sh


i=0
while [ $i -lt 20 ]; do
  curl -s -u elastic:SuperSecurePassword2019 'localhost:9200/_cluster/health?wait_for_status=yellow&timeout=10s' > /tmp/output
  if [ $? -eq 0 ] && cat /tmp/output | jq -e "select(.timed_out == false)"; then
  docker exec pyfhirstore_mongo_1 mongo --username="arkhn" --password="SuperSecurePassword2019" --eval "rs.initiate()"
  break;
  fi
    echo "ElasticSearch is unavailable - sleeping" >&2
    sleep 2
    i=$((i+1))
done
if [ $i -eq 20 ]; then exit 1; fi


echo "Replica Set is initiated - resuming execution"