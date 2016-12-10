#!/bin/bash

sed 's/\"\"/ /g' $1 | sed 's/  / /g' | sed '/^\s*$/d' | awk -F\" '(NF%2==0) {if (append) {append=0; print buf sep $0; next} else {append=1; buf=sep=""}} (append) {sub("\r$",""); buf=buf sep $0; sep=OFS; next} {print}' | awk -F'"' -v OFS='"' '{ for (i=2; i<=NF; i+=2) gsub(",", ";", $i) } 1' > $2

