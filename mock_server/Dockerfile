FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir fastapi==0.115.0 uvicorn[standard]==0.30.6

COPY mock_receiver.py .

EXPOSE 9000

CMD ["uvicorn", "mock_receiver:app", "--host", "0.0.0.0", "--port", "9000"]
