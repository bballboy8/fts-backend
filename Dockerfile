FROM python:3.10-slim

WORKDIR /app

COPY . /app
COPY .env /app/.env

RUN pip install --no-cache-dir -r /app/requirements.txt

EXPOSE 8000

# Run uvicorn with live reload when the container launches
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
