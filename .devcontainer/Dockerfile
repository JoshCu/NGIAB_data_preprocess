FROM ubuntu:22.04 as base
RUN apt-get update
RUN apt-get install -y python3 python3-pip libgdal-dev gdal-bin git libgeos3.10.2 libgeos-dev libgeos++-dev
RUN pip3 install "pybind11[global]"
RUN git clone https://github.com/isciences/exactextract.git /exactextract
WORKDIR /exactextract
RUN pip3 install .
COPY ../requirements.txt /tmp/requirements.txt
RUN pip3 install -r /tmp/requirements.txt


