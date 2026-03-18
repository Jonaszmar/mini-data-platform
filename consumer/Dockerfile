FROM python:3.11-slim

WORKDIR /consumer
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY consumer.py .

CMD ["python", "consumer.py"]