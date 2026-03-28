FROM apache/spark:3.5.1

USER root

RUN pip install \
    boto3==1.34.0 \
    requests==2.31.0 \
    pydantic==2.7.0

# Hadoop-AWS connector for S3 access
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/bitnami/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/bitnami/spark/jars/

USER 1001