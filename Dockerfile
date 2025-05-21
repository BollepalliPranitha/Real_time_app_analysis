FROM bitnami/spark:3.4.0

USER root

# Install dependencies
RUN apt-get update && apt-get install -y \
    curl \
    python3-pip \
    openjdk-11-jdk \
    supervisor \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Install Python packages
COPY requirements.txt /app/requirements.txt
RUN pip3 install --no-cache-dir -r /app/requirements.txt

# Download Kafka and Snowflake JARs
RUN mkdir -p /opt/bitnami/spark/jars && \
    curl -o /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.4.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.0/spark-sql-kafka-0-10_2.12-3.4.0.jar && \
    curl -o /opt/bitnami/spark/jars/kafka-clients-3.2.0.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.2.0/kafka-clients-3.2.0.jar && \
    curl -o /opt/bitnami/spark/jars/spark-snowflake_2.12-2.15.0-spark_3.4.jar \
    https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/2.15.0-spark_3.4/spark-snowflake_2.12-2.15.0-spark_3.4.jar && \
    curl -o /opt/bitnami/spark/jars/snowflake-jdbc-3.16.1.jar \
    https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.16.1/snowflake-jdbc-3.16.1.jar && \
    curl -o /opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.4.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.4.0/spark-token-provider-kafka-0-10_2.12-3.4.0.jar && \
    curl -o /opt/bitnami/spark/jars/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# Create directories
RUN mkdir -p /app/logs /app/checkpoints /app/config /app/scripts /var/run/supervisor /var/log/supervisor && \
    chmod -R 777 /var/run/supervisor /var/log/supervisor /app/logs /app/checkpoints

# Copy scripts and configurations
COPY scripts/ /app/scripts/
COPY config/ /app/config/
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Set working directory
WORKDIR /app

# Set environment variables for PySpark
ENV PYSPARK_PYTHON=python3
ENV SPARK_HOME=/opt/bitnami/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Run supervisord
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]