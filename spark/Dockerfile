FROM apache/spark:3.5.1-python3

USER root
WORKDIR /opt/spark
COPY requirements.txt /opt/spark/requirements.txt
RUN pip install --no-cache-dir -r /opt/spark/requirements.txt
COPY app /opt/spark/app
USER 1001