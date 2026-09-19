FROM eclipse-temurin:11-jdk-jammy

# Install Python 3.10 and basic utilities
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        python3-dev \
        procps \
        curl \
        wget \
    && rm -rf /var/lib/apt/lists/*

# Make python/pip available as python/pip
RUN ln -s /usr/bin/python3 /usr/local/bin/python

ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}" 

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY docker/requirements.txt /app/requirements.txt

RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir -r requirements.txt

RUN mkdir -p \
    /app/data/raw \
    /app/data/bronze \
    /app/data/silver \
    /app/data/gold \
    /app/checkpoints

CMD ["tail", "-f", "/dev/null"]