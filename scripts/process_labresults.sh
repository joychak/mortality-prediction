#!/bin/bash

sed 's/\"\"/ /g' $1 | awk -F'"' -v OFS='"' '{ for (i=2; i<=NF; i+=2) gsub(",", ";", $i) } 1' > $2