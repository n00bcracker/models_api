FROM python:3.7-slim

WORKDIR /home/ml_model

COPY oracle-client.deb oracle-client.deb

RUN apt-get update && \
    apt-get install -y gcc g++ libaio1 && \
    dpkg -i oracle-client.deb && rm oracle-client.deb && \
    apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY requirements.txt requirements.txt

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY common common
COPY utils utils
COPY models models
COPY config.py boot.sh ./

WORKDIR ./models/similar_clients
RUN python setup.py build_ext --inplace && rm -rf ./build ./*.c ./*.pyx
WORKDIR /home/ml_model

RUN chmod a+x boot.sh

ENTRYPOINT ["./boot.sh"]