#!/bin/sh

java -Xmx8g -cp ie-validation-tools.jar:./lib/* ietools.CreateDBTables $1 $2 $3 $4 $5 $6