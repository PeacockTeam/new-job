#!/bin/bash

gradle shadowJar
java -jar -Xmx1G build/libs/BaltInfoCom.jar -i lng.csv -o groups.csv
