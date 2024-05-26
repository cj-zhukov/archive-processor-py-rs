FROM ghcr.io/pyo3/maturin AS builder
WORKDIR /build
COPY . .
RUN maturin build --release -i python3.11

FROM python:3.11-slim as runtime
WORKDIR /app
COPY ./requirements.txt /app
ARG BIN="archive_processor_py_rs"
COPY --from=builder ./build/target/wheels/${BIN}-0.1.0-cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64.whl /app
RUN pip install --no-cache-dir --upgrade -r requirements.txt
COPY ./python /app
CMD ["python", "/app/main.py"]
