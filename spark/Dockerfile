FROM apache/spark:3.5.1-python3

USER root
WORKDIR /opt/spark
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY app /opt/spark/app
RUN useradd -u 1001 -m -d /home/spark sparkuser \
    && mkdir -p /home/spark/.ivy2 \
    && chown -R 1001:0 /home/spark /opt/spark/app

ENV HOME=/home/spark
USER 1001