#!/usr/bin/bash

pwd

which sex

echo "====================== $1"


# /usr/local/bin/sex -c /usr/local/share/sextractor/default.sex -PARAMETERS_NAME /lsst/data/CFHT/default.param -FILTER_NAME /usr/local/share/sextractor/default.conv -CATALOG_NAME test.cat $1

/usr/local/bin/sex -c /usr/local/share/sextractor/default.sex -PARAMETERS_NAME /lsst/data/CFHT/default.param -FILTER_NAME /usr/local/share/sextractor/default.conv -CATALOG_NAME STDOUT -CATALOG_TYPE ASCII_SKYCAT $1 | sed -re "s/^[ \t]+//g" | sed -re "s/[ \t]+/;/g"


###egrep '^;[0-9]*;[0-9]+;' 



##cat test.cat
