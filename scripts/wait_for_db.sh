#!/bin/sh


i=0
while [ $i -lt 15 ]; do
  if mongo --eval "quit()" > /dev/null; then break; fi
  echo "Mongo is unavailable - sleeping" >&2
  sleep 1
  i=$((i+1))
done
if [ $i -eq 15 ]; then exit 1; fi

echo "Mongo is up - resuming execution"
