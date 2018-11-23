
## Import SparkSession
from pyspark.sql import SparkSession
import glob
import os


import subprocess
def bash(command):
   tcmd = "time {}".format(command)
   result = subprocess.run(command.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
   return result.stdout.decode('utf-8').split("\n")

def runit(spark, files, command):
  print("==================================================== [{}]".format(command))
  rdd = spark.sparkContext.parallelize(files, len(files)).map( lambda x : bash('{} {}'.format(command, x))  )
  return rdd

if __name__ == "__main__":
  ## Initialise your SparkSession
  spark = SparkSession\
    .builder\
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  ## Read as a DataFrame a HDU of a table fits.

  image = "/lsst/efigi-1.6/ima_g/PGC002331*_g.fits"
  image = "file:/lsst/data/CFHT/rawDownload/g/*.fits"

  N = 100

  import random
  files = random.sample(glob.glob("/lsst/data/CFHT/rawDownload/*/*.fits"), N)

  SEXDIR = "/usr/local/share/sextractor/"

  conf = SEXDIR + "default.sex"
  param = "/lsst/data/CFHT/default.param"
  filter = SEXDIR + "default.conv"

  sex = "sex -c {} -PARAMETERS_NAME {} -FILTER_NAME {} -CATALOG_NAME {} ".format(conf, param, filter, "test.cat")

  def f(x):
    import re
    return re.match("^[0-9]+[;][0-9]+[;]", x)

  def tofloat(i):
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
  rdd0 = runit(spark, files, "/lsst/data/CFHT/test.sh").flatMap(lambda x : [i for i in x] ).filter(lambda x : f(x)).map(lambda x : x.split(';')).map(lambda x : [tofloat(i) for i in x]).cache().sample(0, 0.01/float(N))

  rdd = rdd0.map(lambda x: (x[6], x[7], abs(x[3])))
  rdd1 = rdd0.map(lambda x: (x[0], x[1], x[6], x[7], abs(x[3])))

  import numpy as np

  raw_image = rdd1.collect()

  for row in raw_image:
    print(row)

  exit()

  raw_image = rdd.collect()

  image = np.array(raw_image).transpose()

  """
  for row in image:
    print(row)
  """

  import matplotlib.pyplot as plt

  x = image[0]
  y = image[1]
  z = image[2]

  plt.scatter(x, y, c=z, marker='.')

  plt.show()


