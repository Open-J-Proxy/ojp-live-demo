FROM python:3.12-slim
RUN mkdir /work && chown 65534:65534 /work
WORKDIR /work
COPY load/runner.py /app/runner.py
USER 65534:65534
ENTRYPOINT ["python", "-u", "/app/runner.py"]
