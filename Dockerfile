FROM bitnami/spark:latest

USER root

# Install curl
RUN apt-get update && apt-get install -y curl iputils-ping && apt-get clean

# Install AWS Hadoop libraries
RUN curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    mv hadoop-aws-3.3.4.jar /opt/bitnami/spark/jars/

RUN curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    mv aws-java-sdk-bundle-1.12.262.jar /opt/bitnami/spark/jars/

USER 1001
