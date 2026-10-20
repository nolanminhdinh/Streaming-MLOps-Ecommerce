FROM python:3.12-slim
WORKDIR /workspace
COPY requirements-runtime.txt /tmp/requirements-runtime.txt
RUN pip install --no-cache-dir -r /tmp/requirements-runtime.txt
ENV PYTHONPATH=/workspace PYTHONUNBUFFERED=1
CMD ["python", "scripts/run_pipeline_worker.py"]
