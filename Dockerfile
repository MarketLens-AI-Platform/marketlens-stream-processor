FROM bitnami/spark:3.5

USER root
RUN pip install boto3

USER 1001
COPY processor.py /opt/bitnami/spark/processor.py

ENTRYPOINT [ "spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4", "/opt/bitnami/spark/processor.py" ]
