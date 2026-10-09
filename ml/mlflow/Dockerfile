# Dockerfile cho MLflow Tracking Server
# Kết nối PostgreSQL (Backend Store) và MinIO (Artifact Store)

FROM python:3.12-slim

WORKDIR /mlflow

# Cài đặt các thư viện cần thiết cho MLflow, PostgreSQL và MinIO S3
RUN pip install --no-cache-dir \
    mlflow==2.14.1 \
    psycopg2-binary==2.9.9 \
    boto3==1.34.131

# MLflow 2.14 imports pkg_resources; setuptools 81 removed that module.
# It is also incompatible with SQLAlchemy 2.1, so keep the tested 2.0.31 version.
RUN pip install --no-cache-dir "setuptools<81" "SQLAlchemy==2.0.31"

EXPOSE 5000

HEALTHCHECK --interval=10s --timeout=5s --retries=10 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:5000/health', timeout=3)" || exit 1

# Khởi chạy MLflow Server với backend PostgreSQL và S3 artifact store
CMD ["sh", "-c", "mlflow server \
    --backend-store-uri ${MLFLOW_BACKEND_STORE_URI:-postgresql://ecom:ecom_password@postgres:5432/ecom_warehouse} \
    --default-artifact-root ${MLFLOW_DEFAULT_ARTIFACT_ROOT:-s3://ecom-raw-lake/mlflow/} \
    --host 0.0.0.0 \
    --port 5000"]
