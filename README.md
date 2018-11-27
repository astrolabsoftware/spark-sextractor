# sextractor
Adaptation of SExtractor to Spark

# First install of the SExtractor tool
In a ubuntu-16-4 docker image

```
mkdir /home/sextractor
cd /home/sextractor
wget https://www.astromatic.net/download/sextractor/sextractor-2.19.5.tar.gz
tar xzvf sextractor-2.19.5.tar.gz
cd sextractor-2.19.5
apt-get install -y fftw3-dev libatlas-base-dev
update-alternatives --set liblapack.so /usr/lib/atlas-base/atlas/liblapack.so
./configure --with-atlas-incdir=/usr/include/atlas
make
make install
cd tests
make check-TESTS
```

# installing the Data set

see: https://www.astromatic.net/projects/efigi

This archive contains version 1.6.2 of the EFIGI reference dataset. The EFIGI
reference dataset contains SDSS images and visual morphology for 4458
galaxies from the RC3 catalogue. See Baillard et al. 2011 (A&A 532, A74) and
http://efigi.org for details.

The EFIGI dataset is distributed in 7 separate compressed archives (gzipped
tar format):
- efigi_tables-1.6.2.tgz: 6 ASCII tables, including morphological information
- efigi_ima_u-1.6.tgz: 4458 galaxy images in the SDSS u-band (FITS format)
- efigi_ima_g-1.6.tgz: 4458 galaxy images in the SDSS g-band (FITS format)
- efigi_ima_r-1.6.tgz: 4458 galaxy images in the SDSS r-band (FITS format)
- efigi_ima_i-1.6.tgz: 4458 galaxy images in the SDSS i-band (FITS format)
- efigi_ima_z-1.6.tgz: 4458 galaxy images in the SDSS z-band (FITS format)
- efigi_psf-1.6.tgz: 4458x5 PSF images.

Second dataset is from CCIN2P3 (CFHT) with filters for g/ i/ r/ u/ z/

```
/lsst/data/CFHT/rawDownload/*/*.fits:

du -h /lsst/data/CFHT/rawDownload/
366G    /lsst/data/CFHT/rawDownload/g
499G    /lsst/data/CFHT/rawDownload/i
441G    /lsst/data/CFHT/rawDownload/r
113G    /lsst/data/CFHT/rawDownload/u
460G    /lsst/data/CFHT/rawDownload/z
1.9T    /lsst/data/CFHT/rawDownload/
```


# Installing Sextractor in a Spark Pipeline

The principle is as follows:

- the sextractor utility is used unmodified to detect objects for a collection of astronomic images
- Each image is handled independently to others (one image per worker task)
- The default parameters are used (from the installed package)
- Catalog variables are setup on the flight
- Catalogs are produced to stdout and injected into the Spark flow
- all catalog lines are added (assembled) to the global Spark data flow
- Complete assembled catalog is then handled as a table that can therefore analyzed using Spark

