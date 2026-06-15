FROM apache/spark:3.5.1

USER root
# Install dependencies for Python and S3 access
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3-pip && \
    rm -rf /var/lib/apt/lists/* && \
    pip3 install --no-cache-dir boto3

# Revert to the default Spark non-root user (UID 185)
USER 185
COPY processor.py /opt/spark/work-dir/processor.py

ENTRYPOINT [ "/opt/spark/bin/spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4", "/opt/spark/work-dir/processor.py" ]
