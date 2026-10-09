# Dockerfile cho FastAPI Model Serving & Quản trị Tồn kho (Tuần 7)
FROM python:3.12-slim

WORKDIR /app

# Cài đặt curl cho container healthcheck và libgomp cho LightGBM
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# MLflow 2.14 imports pkg_resources; setuptools 81 removed that module.
RUN pip install --no-cache-dir "setuptools<81"

# Copy mã nguồn serving theo cấu trúc package serving/app
COPY ./app ./serving/app
ENV PYTHONPATH="/app"

EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=5s --retries=5 \
    CMD curl -f http://localhost:8000/ready || exit 1

CMD ["uvicorn", "serving.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
