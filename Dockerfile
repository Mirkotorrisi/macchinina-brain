FROM python:3.11-slim

# Installiamo le dipendenze di sistema
RUN apt-get update && apt-get install -y \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Installiamo le librerie Python necessarie
RUN pip install --no-cache-dir \
    paho-mqtt \
    requests

# Copiamo il tuo script brain.py
COPY brain.py .

CMD ["python", "brain.py"]