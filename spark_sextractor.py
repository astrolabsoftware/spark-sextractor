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

def run_it(x: str) -> List[str]:
  """
  Handle one single image using the Sextractor application


  :param x: One image file
  :return: a list of catalog lines (CSV format ; separated)

  The Catalog variable are specified here, and written in a text default.param file and passed tp sextractor.

  Default parameters are taken from the Sextractor package as they are.
  """

  # Selecting the catalog variables (from /usr/local/share/sextractor/default.param)
  text = """
NUMBER                   Running object number
EXT_NUMBER               FITS extension number
FLUX_ISO                 Isophotal flux                                            [count]
MAG_ISO                  Isophotal magnitude                                       [mag]
FLUX_ISOCOR              Corrected isophotal flux                                  [count]
MAG_ISOCOR               Corrected isophotal magnitude                             [mag]
XPEAK_WORLD              World-x coordinate of the brightest pixel                 [deg]
YPEAK_WORLD              World-y coordinate of the brightest pixel                 [deg]
ALPHA_SKY                Right ascension of barycenter (native)                    [deg]
DELTA_SKY                Declination of barycenter (native)                        [deg]
FLUXERR_ISO              RMS error for isophotal flux                              [count]
MAGERR_ISO               RMS error for isophotal magnitude                         [mag]
"""

  # Construct the default.param file in the current worker directory
  with open("default.param", "w") as f:
    f.write(text)

  # Construct the command line for the Sextractor application
  conf = "-c /usr/local/share/sextractor/default.sex"
  filter = "-FILTER_NAME /usr/local/share/sextractor/default.conv"
  params = "-PARAMETERS_NAME default.param"
  sex = "/usr/local/bin/sex {} {} {} -CATALOG_NAME STDOUT".format(conf, params, filter)
  command = "{} {}".format(sex, x)

  # Starts the Sextractor application, catalog is produced on STDOUT and decoded and formatted as CVS lines
  rawout = subprocess.run(command.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout.decode('utf-8').split("\n")
  out = [re.sub("[ \t]+", ";", i) for i in rawout]

  return out

if __name__ == "__main__":
  ## Initialise your SparkSession

  spark = SparkSession\
    .builder\
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  # define the possible datasets
  images_for_EFIGI_dataset = "/lsst/efigi-1.6/ima_g/PGC002331*_g.fits"
  images_for_CFHT_dataset = "/lsst/data/CFHT/rawDownload/*/*.fits"

  # We select a sampled subset of the dataset
  N = 2
  import random
  files = random.sample(glob.glob(images_for_CFHT_dataset), N)

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

  """

# run sextractor
# flatten the array
# select only pure catalog lines
# format the catalog line as an array of words
# convert words into floats

  """

  # Run the Sextractor application onto all selected image files and produce the global catalog
  rdd0 = spark.sparkContext.parallelize(files, len(files)).flatMap(lambda x: run_it(x))

  # Makes the assembled catalog as a table of floats
  rdd2 = rdd0.map(lambda x: x.split(';'))
  rdd3 = rdd2.map(lambda x: [tofloat(i) for i in x]).cache().filter(lambda x: len(x) > 1)

  # display a sample of catalog lines
  for i in rdd3.takeSample(False, 10): print(i)

  # Construct some data samples for plots
  rdd4 = rdd3.map(lambda x: (x[7], x[8], abs(x[4])))
  rdd5 = rdd3.map(lambda x: (x[1], x[2], x[7], x[8], abs(x[4])))
  rdd6 = rdd3.map(lambda x: (x[11], x[12], abs(x[4])))

  import numpy as np

  raw_data = rdd6.collect()
  data = np.array(raw_data).transpose()

  import matplotlib.pyplot as plt

  x = data[0]
  y = data[1]
  z = data[2]

  plt.scatter(x, y, c=z, marker='.')

  plt.show()

