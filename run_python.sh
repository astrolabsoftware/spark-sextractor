#!/bin/bash
# Copyright 2018 Julien Peloton
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


master="--master spark://134.158.75.222:7077"
memory="--driver-memory 4g --executor-memory 18g"
conf="--conf spark.kryoserializer.buffer.max=1g"
package="--packages com.github.astrolabsoftware:spark-fits_2.11:0.7.1"



# Run it!
time spark-submit ${master} ${memory} ${package} s.py

