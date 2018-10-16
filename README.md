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

