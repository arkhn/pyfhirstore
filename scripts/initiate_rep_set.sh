#!/bin/sh


i=0
while [ $i -lt 10 ]; do
  curl -s -u elastic:SuperSecurePassword2019 'localhost:9200/_cluster/health?wait_for_status=yellow&timeout=10s' > /tmp/output
  if [ $? -eq 0 ] && cat /tmp/output | jq -e "select(.timed_out == false)"; then
  docker exec pyfhirstore_mongo_1 mongo --username="arkhn" --password="SuperSecurePassword2019" --eval "rs.initiate()"
  break;
  fi
    sleep 1
    i=$((i+1))
done
if [ $i -eq 10 ]; then exit 1; fi


echo "Replica Set is initiated - resuming execution"