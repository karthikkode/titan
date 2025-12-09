# Use Debian Bookworm (Stable) to ensure OpenJDK 17 availability
FROM python:3.9-slim-bookworm

# 1. Install Java (Required for PySpark) & Curl
# Fix for slim images: create man directory to prevent install errors
RUN mkdir -p /usr/share/man/man1 && \
    apt-get update && \
    apt-get install -y openjdk-17-jre-headless curl procps && \
    apt-get clean;

# 2. Set Environment Variables for Spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_VERSION=3.5.0

# 3. Work Directory
WORKDIR /app

# 4. Install Python Dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copy the Source Code
COPY src/ /app/src/

# 6. Default Command
CMD ["python", "src/main.py"]