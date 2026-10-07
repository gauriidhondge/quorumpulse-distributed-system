FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000 8001 8002 8003 8090 8095

CMD ["uvicorn", "request_service.main:app", "--host", "0.0.0.0", "--port", "8000"]
