#
# Copyright 2018 Christian Arnault
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

FROM ubuntu:16.04


# Ensure use of bash
SHELL ["/bin/bash","-c"]

ENV DEBIAN_FRONTEND noninteractive
ENV JAVA_HOME       /usr/lib/jvm/java-8-oracle

#-------- various settings related with LOCALE (ignored) 
# ENV LANG            en_US.UTF-8
# ENV LC_ALL          en_US.UTF-8
# && apt-get install -y --no-install-recommends locales \
# && locale-gen en_US.UTF-8 \
#---------------------------------------------

RUN echo "================ Basic installation ===============" \
&& apt-get update \
&& apt-get install -y apt-utils \
&& apt-get install -y vim \
&& apt-get install -y x11-apps \
&& apt-get install -y xemacs21

RUN echo "================ C++ installation ===============" \
&& apt-get update \
&& apt-get install -y build-essential

RUN echo "================ Apt-get installations ===============" \
&& apt-get update \
&& apt-get install -y wget


# RUN echo "================ Libraries ===============" \
# && apt install -y libfftw3-dev \
# && apt install -y liblapack-dev \
# && apt install -y libatlas-dev

RUN echo "================ SExtractor ===============" \
&& cd /home \
&& wget https://www.astromatic.net/download/sextractor/sextractor-2.19.5.tar.gz \
&& tar xzvf sextractor-2.19.5.tar.gz \
&& cd sextractor-2.19.5 \
&& apt-get install -y fftw3-dev libatlas-base-dev \
&& update-alternatives --set liblapack.so /usr/lib/atlas-base/atlas/liblapack.so \
&& ./configure --with-atlas-incdir=/usr/include/atlas \
&& make \
&& make install \
&& cd tests/ \
&& make check-TESTS

RUN echo "================ Prepare /work ==============="
RUN mkdir /work
WORKDIR /work


