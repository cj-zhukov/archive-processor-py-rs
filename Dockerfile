FROM python:3.11-slim AS builder
ENV RUST_VERSION=1.81

RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    python3-dev \
    libssl-dev \
    libffi-dev \
    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain ${RUST_VERSION} \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV PATH="/root/.cargo/bin:${PATH}"
RUN cargo install --locked maturin \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . .
RUN maturin build --release

FROM python:3.11-slim AS runtime
WORKDIR /app
COPY --from=builder /build/target/wheels /app/wheels
COPY --from=builder /build/python /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt /app/wheels/*.whl
CMD ["python", "/app/main.py"]