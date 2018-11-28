#!/usr/bin/env python
# -*- coding: utf-8 -*-

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

"""

Installation of the sextractor object detection process into Apache Spark
The principle is the following:
- the sextractor utility is used unmodified to detect objects for a collection of astronomic images
- Each image is handled independently to others
- The default parameters are used
- Catalog variables are setup on the flight
- Catalogs are produced to stdout and injected into the Spark flow
- all catalog lines are added to the global Spark data flow
- Complete assembled catalog is then handled as a table that can therefore analyzed using Spark

"""

from pyspark.sql import SparkSession

import re
import random
import glob
import subprocess

from typing import List

def run_it(keys: List[str], image_file: str) -> List[str]:
  """
  Handle one single image using the Sextractor application

  :param keys: selected list of catalog variables
  :param image_file: One image file
  :return: a list of catalog lines (CSV format ; separated)

  The Catalog variable are specified here, and written in a text default.param file and passed tp sextractor.

  Default parameters are taken from the Sextractor package as they are.
  """

  # Construct the default.param file in the current worker directory from the specified keys
  with open("default.param", "w") as f:
    for k in keys:
      f.write(k + "\n")

  # Construct the command line for the Sextractor application
  conf = "-c /usr/local/share/sextractor/default.sex"
  filter = "-FILTER_NAME /usr/local/share/sextractor/default.conv"
  params = "-PARAMETERS_NAME default.param"
  sex = "/usr/local/bin/sex {} {} {} -CATALOG_NAME STDOUT".format(conf, params, filter)
  command = "{} {}".format(sex, image_file)

  # Starts the Sextractor application, catalog is produced on STDOUT and decoded and formatted as CVS lines
  rawout = subprocess.run(command.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout.decode('utf-8').split("\n")
  out = [re.sub("[ \t]+", ";", i) for i in rawout]

  return out


def tofloat(i):
    """
    This UDF transform catalog fields as floats
    :param i:
    :return:
    """

    try:
        return float(i)
    except:
        return i


if __name__ == "__main__":
  ## Initialise your SparkSession

  spark = SparkSession\
    .builder\
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  # define the possible datasets
  images_for_EFIGI_dataset = "/lsst/efigi-1.6/ima_g/PGC002331*_g.fits"
  images_for_CFHT_dataset = "/lsst/data/CFHT/rawDownload/*/*.fits"

  # Get the possible catalog variables fro Sextractor package (from /usr/local/share/sextractor/default.param)
  with open("/usr/local/share/sextractor/default.param", "r") as f:
      lines = f.readlines()

  all_keys = [i.split()[0][1:] for i in lines]

  # Consider one subset of the possible variable keys that will be broadcasted to all workers
  keys = ["NUMBER", "EXT_NUMBER", "FLUX_ISO", "MAG_ISO", "FLUX_ISOCOR", "MAG_ISOCOR", "XPEAK_WORLD", "YPEAK_WORLD",
          "ALPHA_SKY", "DELTA_SKY", "FLUXERR_ISO", "MAGERR_ISO"]

  # We select a sampled subset (of size=N) of the dataset
  N = 2
  import random
  files = random.sample(glob.glob(images_for_CFHT_dataset), N)

  # Run the Sextractor application onto all selected image files and produce the global catalog
  rdd0 = spark.sparkContext.parallelize(files, len(files)).flatMap(lambda x: run_it(keys, x)).cache()

  # Makes the assembled catalog as a table of floats
  #
  # - filters out the useless lines (without data)
  # - suppress the heading ";" character
  # - convert catalog values to float
  #
  rdd = rdd0.filter(lambda x: re.match('^[;]', x)).map(lambda x: x.split(';')[1:]).map(lambda x: [tofloat(i) for i in x])

  # display a sample of catalog lines
  for i in rdd.takeSample(False, 10): print(i)

  # Convert to dataframe
  df = rdd2.toDF(keys)

  df.show(10)

  # Construct some data samples for plots
  rawdata = df.select("FLUXERR_ISO", "MAGERR_ISO", "MAG_ISO")
  data = rawdata.toPandas().get_values().transpose()

  import matplotlib.pyplot as plt

  x = data[0].astype(float)
  y = data[1].astype(float)
  z = abs(data[2].astype(float))

  plt.scatter(x, y, c=z, marker='.')

  plt.show()

