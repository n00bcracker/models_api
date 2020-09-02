FROM python:3.7-slim

WORKDIR /home/ml_model

RUN python -m venv venv
RUN . venv/bin/activate

RUN apt-get update
RUN apt-get install -y gcc && apt-get install -y g++

COPY oracle-client.deb oracle-client.deb
RUN apt-get install libaio1
RUN dpkg -i oracle-client.deb

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt; exit 0
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY common common
COPY utils utils
COPY models models
COPY config.py boot.sh ./
WORKDIR ./models/similar_clients
RUN python setup.py build_ext --inplace
RUN  rm -rf ./build ./*.c ./*.pyx
WORKDIR /home/ml_model

RUN chmod a+x boot.sh

ENTRYPOINT ["./boot.sh"]