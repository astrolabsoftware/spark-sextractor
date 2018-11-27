#!/usr/bin/bash

# Copyright 2018 Christian Arnault
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#================================================
#
# This script runs one instance of the sextractor application over one image file
# We need several configuration files and some parameters :
#
#  - the default configuration file :    -c <conf_file>
#  - the default parameter file :        -PARAMETERS_NAME <param_file>
#  - the default filter file :           -FILTER_NAME <filter_file>
#  - output destination :                -CATALOG_NAME STDOUT
#
#================================================

# we use the default provided from the sextractor package
conf = "-c /usr/local/share/sextractor/default.sex"
filter = "-FILTER_NAME /usr/local/share/sextractor/default.conv"

# Here we select the fields that will be used to fill in the output catalog
params = "-PARAMETERS_NAME /lsst/data/CFHT/default.param"

# send output to stdout
output = "-CATALOG_NAME STDOUT -CATALOG_TYPE ASCII_SKYCAT"

input = "$1"

/usr/local/bin/sex ${conf} ${params} ${filter} ${output} ${input} | sed -re "s/^[ \t]+//g" | sed -re "s/[ \t]+/;/g"

