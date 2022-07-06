#!/bin/bash

gradle shadowJar

echo "lng.csv"
java -jar -Xmx1G build/libs/BaltInfoCom.jar -i lng.csv -o groups.csv

echo "lng.txt"
java -jar -Xmx1G build/libs/BaltInfoCom.jar -i lng.txt -o groups.csv
