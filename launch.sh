#!/bin/bash

gradle shadowJar

echo "lng.csv"
java -jar -Xmx1G build/libs/BaltInfoCom.jar -i lng.csv -o groups1.csv

echo "lng.txt"
java -jar -Xmx1G build/libs/BaltInfoCom.jar -i lng.txt -o groups2.csv
