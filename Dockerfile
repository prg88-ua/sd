FROM python:3.11-slim

# Mostrar logs inmediatamente y permitir importar wm_common desde cualquier modulo.
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app WM_STATE_DIR=/data

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5000 6000

CMD ["python", "WM_Central/WM_Central.py", "--socket-port", "6000", "--kafka-broker", "broker:29092"]
